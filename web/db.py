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

def get_incidents_for_area(area: str) -> list:
    """
    Return all incidents where the given area appeared in any cat10_snapshot,
    ordered by started_at DESC.
    Each entry includes:
      - id, started_at, had_siren
      - cat10_areas: areas from the last snapshot
      - cat1_areas: areas that got the siren (if had_siren)
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

        last_snap = conn.execute("""
            SELECT id FROM cat10_snapshots WHERE incident_id = ? ORDER BY id DESC LIMIT 1
        """, [inc_id]).fetchone()

        cat10_areas = []
        if last_snap:
            rows = conn.execute(
                'SELECT area FROM cat10_areas WHERE snapshot_id = ? ORDER BY area',
                [last_snap['id']]
            ).fetchall()
            cat10_areas = [r['area'] for r in rows]

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
        })

    result.sort(key=lambda x: x['started_at'], reverse=True)
    conn.close()
    return result


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
