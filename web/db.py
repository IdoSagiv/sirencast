import os, sqlite3
from collector import config

def get_connection():
    path = os.path.join(config.DATA_DIR, 'sirencast.db')
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    return conn

def query_historical_counts(areas: list) -> dict:
    """
    Given a set of alert areas (from current cat=10 active warning),
    find all past incidents whose LATEST cat10_snapshot has EXACTLY
    this area set (sorted, exact match — no fuzzy).

    Returns:
    {
        'total_matching_incidents': int,
        'counts': [{'area': str, 'count': int, 'pct': float}, ...]
    }
    """
    if not areas:
        return {'total_matching_incidents': 0, 'counts': []}

    normalized = sorted(areas)
    n = len(normalized)
    conn = get_connection()

    placeholders = ','.join(['?' for _ in normalized])

    rows = conn.execute(f"""
        WITH latest_snapshots AS (
            SELECT incident_id, MAX(id) as snap_id
            FROM cat10_snapshots
            GROUP BY incident_id
        ),
        matching_snapshots AS (
            SELECT ls.incident_id, ls.snap_id
            FROM latest_snapshots ls
            WHERE (
                SELECT COUNT(*) FROM cat10_areas WHERE snapshot_id = ls.snap_id
            ) = ?
            AND (
                SELECT COUNT(*) FROM cat10_areas
                WHERE snapshot_id = ls.snap_id AND area IN ({placeholders})
            ) = ?
        )
        SELECT i.id as incident_id, i.had_siren
        FROM matching_snapshots ms
        JOIN incidents i ON i.id = ms.incident_id
    """, [n] + normalized + [n]).fetchall()

    total = len(rows)
    siren_incident_ids = [r['incident_id'] for r in rows if r['had_siren']]

    counts = []
    for area in normalized:
        if not siren_incident_ids:
            count = 0
        else:
            id_placeholders = ','.join(['?' for _ in siren_incident_ids])
            count = conn.execute(f"""
                SELECT COUNT(DISTINCT ca.alert_id)
                FROM cat1_areas ca
                JOIN cat1_alerts cal ON cal.id = ca.alert_id
                WHERE ca.area = ? AND cal.incident_id IN ({id_placeholders})
            """, [area] + siren_incident_ids).fetchone()[0]
        pct = round(count / total * 100, 1) if total > 0 else 0.0
        counts.append({'area': area, 'count': count, 'pct': pct})

    counts.sort(key=lambda x: x['count'], reverse=True)
    conn.close()
    return {'total_matching_incidents': total, 'counts': counts}

def _get_canonical_cat10_areas(conn, incident_id: int) -> list:
    """Return the area list from the last cat10 snapshot for an incident."""
    last_snap = conn.execute(
        'SELECT id FROM cat10_snapshots WHERE incident_id = ? ORDER BY id DESC LIMIT 1',
        [incident_id]
    ).fetchone()
    if not last_snap:
        return []
    rows = conn.execute(
        'SELECT area FROM cat10_areas WHERE snapshot_id = ? ORDER BY area',
        [last_snap['id']]
    ).fetchall()
    return [r['area'] for r in rows]


def get_incidents_for_area(area: str) -> list:
    """
    Return all incidents where the given area appeared in any cat10_snapshot,
    ordered by started_at DESC.
    Each entry includes:
      - id, started_at, had_siren
      - cat10_areas: areas from the last snapshot
      - cat1_areas: areas that got the siren (if had_siren)
      - prediction: {count, total} — among prior incidents with the same cat10
        set, how many resulted in a siren for this area
    """
    conn = get_connection()

    incident_rows = conn.execute("""
        SELECT DISTINCT s.incident_id
        FROM cat10_snapshots s
        JOIN cat10_areas a ON a.snapshot_id = s.id
        WHERE a.area = ?
    """, [area]).fetchall()

    incident_ids = [r['incident_id'] for r in incident_rows]

    if not incident_ids:
        conn.close()
        return []

    result = []
    for inc_id in incident_ids:
        inc = conn.execute('SELECT * FROM incidents WHERE id = ?', [inc_id]).fetchone()
        if not inc:
            continue

        cat10_areas = _get_canonical_cat10_areas(conn, inc_id)

        cat1_areas = []
        if inc['had_siren']:
            rows = conn.execute("""
                SELECT DISTINCT ca.area FROM cat1_areas ca
                JOIN cat1_alerts cal ON cal.id = ca.alert_id
                WHERE cal.incident_id = ?
                ORDER BY ca.area
            """, [inc_id]).fetchall()
            cat1_areas = [r['area'] for r in rows]

        result.append({
            'id': inc['id'],
            'started_at': inc['started_at'],
            'had_siren': bool(inc['had_siren']),
            'cat10_areas': cat10_areas,
            'cat1_areas': cat1_areas,
            'area_got_siren': None,  # filled in after area_siren_set is computed
        })

    # Sort ascending to compute rolling prediction
    result.sort(key=lambda x: x['started_at'])

    # For each incident: did the selected area specifically get a siren?
    siren_inc_ids = [r['id'] for r in result if r['had_siren']]
    area_siren_set = set()
    if siren_inc_ids:
        ph = ','.join(['?' for _ in siren_inc_ids])
        rows = conn.execute(f"""
            SELECT DISTINCT cal.incident_id
            FROM cat1_alerts cal
            JOIN cat1_areas ca ON ca.alert_id = cal.id
            WHERE ca.area = ? AND cal.incident_id IN ({ph})
        """, [area] + siren_inc_ids).fetchall()
        area_siren_set = {r['incident_id'] for r in rows}

    # Fill in area_got_siren now that we have area_siren_set
    for item in result:
        item['area_got_siren'] = item['id'] in area_siren_set

    # Rolling prediction: among prior incidents where this area was in cat10,
    # how many had a siren for this area specifically?
    running_total = 0
    running_siren = 0
    for item in result:
        item['prediction'] = {'count': running_siren, 'total': running_total}
        # Update counters for the NEXT incident
        running_total += 1
        if item['area_got_siren']:
            running_siren += 1

    result.sort(key=lambda x: x['started_at'], reverse=True)
    conn.close()
    return result


def get_area_stats(area: str) -> dict:
    """
    Aggregate stats for a single area:
    - total_incidents: how many incidents had this area in their cat=10 warning
    - had_siren_incidents: how many of those had any siren at all
    - area_siren_count: how many of those had a siren specifically for this area
    - area_siren_pct: area_siren_count / total_incidents * 100
    """
    conn = get_connection()

    # Incidents where this area appeared in any cat10_snapshot
    inc_rows = conn.execute("""
        SELECT DISTINCT i.id, i.had_siren
        FROM incidents i
        JOIN cat10_snapshots s ON s.incident_id = i.id
        JOIN cat10_areas a ON a.snapshot_id = s.id
        WHERE a.area = ?
    """, [area]).fetchall()

    total = len(inc_rows)
    if total == 0:
        conn.close()
        return {'total_incidents': 0, 'had_siren_incidents': 0, 'area_siren_count': 0, 'area_siren_pct': 0.0}

    had_siren = sum(1 for r in inc_rows if r['had_siren'])
    siren_inc_ids = [r['id'] for r in inc_rows if r['had_siren']]

    area_siren_count = 0
    if siren_inc_ids:
        ph = ','.join(['?' for _ in siren_inc_ids])
        area_siren_count = conn.execute(f"""
            SELECT COUNT(DISTINCT cal.incident_id)
            FROM cat1_alerts cal
            JOIN cat1_areas ca ON ca.alert_id = cal.id
            WHERE ca.area = ? AND cal.incident_id IN ({ph})
        """, [area] + siren_inc_ids).fetchone()[0]

    conn.close()
    return {
        'total_incidents': total,
        'had_siren_incidents': had_siren,
        'area_siren_count': area_siren_count,
        'area_siren_pct': round(area_siren_count / total * 100, 1) if total > 0 else 0.0,
    }


def get_all_known_areas() -> list:
    """Return sorted list of all distinct area strings seen in cat10_areas + cat1_areas."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT DISTINCT area FROM cat10_areas
        UNION
        SELECT DISTINCT area FROM cat1_areas
        ORDER BY area
    """).fetchall()
    conn.close()
    return [r['area'] for r in rows]
