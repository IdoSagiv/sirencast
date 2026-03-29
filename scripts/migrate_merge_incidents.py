#!/usr/bin/env python3
"""
Migration: merge sequential incidents within the 10-minute window into one,
then re-link orphan cat=1 alerts using closest-candidate rule.

Safe to re-run — creates a .backup before touching anything.
"""

import os
import shutil
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from collector.config import DATA_DIR, SIREN_LINKAGE_WINDOW_SECONDS

WINDOW = SIREN_LINKAGE_WINDOW_SECONDS


def merge_incidents(conn):
    # Only process fully closed incidents (skip any currently open one)
    rows = conn.execute("""
        SELECT id, started_at, cat10_ended, ended_at, had_siren
        FROM incidents
        WHERE ended_at IS NOT NULL
        ORDER BY started_at ASC
    """).fetchall()
    incidents = [dict(r) for r in rows]

    if not incidents:
        print("  No closed incidents found.")
        return 0

    # Build merge groups: each new group starts when the gap exceeds WINDOW
    groups = [[incidents[0]]]
    for curr in incidents[1:]:
        last = groups[-1][-1]
        gap = curr["started_at"] - (last["cat10_ended"] or last["ended_at"])
        if gap <= WINDOW:
            groups[-1].append(curr)
        else:
            groups.append([curr])

    merge_count = 0
    for group in groups:
        if len(group) == 1:
            continue

        primary_id = group[0]["id"]
        secondary_ids = [g["id"] for g in group[1:]]

        # Merged field values
        cat10_ends = [g["cat10_ended"] for g in group if g["cat10_ended"]]
        last_cat10_ended = max(cat10_ends) if cat10_ends else None
        last_ended_at = max(g["ended_at"] for g in group if g["ended_at"])
        had_siren = 1 if any(g["had_siren"] for g in group) else 0

        # Re-point all child records to primary
        for sec_id in secondary_ids:
            conn.execute(
                "UPDATE cat10_snapshots SET incident_id=? WHERE incident_id=?",
                (primary_id, sec_id),
            )
            conn.execute(
                "UPDATE cat1_alerts SET incident_id=? WHERE incident_id=?",
                (primary_id, sec_id),
            )
            conn.execute("DELETE FROM incidents WHERE id=?", (sec_id,))

        conn.execute(
            "UPDATE incidents SET cat10_ended=?, ended_at=?, had_siren=? WHERE id=?",
            (last_cat10_ended, last_ended_at, had_siren, primary_id),
        )

        ids = [g["id"] for g in group]
        print(f"  Merged incidents {ids} → #{primary_id}")
        merge_count += 1

    conn.commit()
    return merge_count


def relink_orphans(conn):
    orphans = conn.execute(
        "SELECT id, fired_at FROM cat1_alerts WHERE incident_id IS NULL"
    ).fetchall()

    linked = 0
    skipped = 0
    for orphan in orphans:
        # Find candidates: incidents whose cat10_ended is within WINDOW before the siren
        candidates = conn.execute(
            """
            SELECT id, cat10_ended FROM incidents
            WHERE cat10_ended IS NOT NULL
              AND cat10_ended <= :fired AND cat10_ended >= :fired - :window
            ORDER BY (:fired - cat10_ended) ASC
            """,
            {"fired": orphan["fired_at"], "window": WINDOW},
        ).fetchall()

        if not candidates:
            skipped += 1
            continue

        best_id = candidates[0]["id"]
        delta = orphan["fired_at"] - candidates[0]["cat10_ended"]
        conn.execute(
            "UPDATE cat1_alerts SET incident_id=? WHERE id=?",
            (best_id, orphan["id"]),
        )
        conn.execute("UPDATE incidents SET had_siren=1 WHERE id=?", (best_id,))
        print(f"  Orphan cat1 #{orphan['id']} → incident #{best_id} (delta {delta}s)")
        linked += 1

    conn.commit()
    print(f"  Linked {linked} orphans, {skipped} had no candidate (kept as orphans)")
    return linked


def print_summary(conn):
    total = conn.execute("SELECT COUNT(*) FROM incidents").fetchone()[0]
    with_siren = conn.execute(
        "SELECT COUNT(*) FROM incidents WHERE had_siren=1"
    ).fetchone()[0]
    orphans = conn.execute(
        "SELECT COUNT(*) FROM cat1_alerts WHERE incident_id IS NULL"
    ).fetchone()[0]
    print(f"\n  Incidents: {total} total, {with_siren} with siren")
    print(f"  Remaining orphan cat1 alerts: {orphans}")


def main():
    db_path = os.path.join(DATA_DIR, "sirencast.db")
    backup_path = db_path + ".pre_merge_backup"

    print(f"DB: {db_path}")
    shutil.copy2(db_path, backup_path)
    print(f"Backup: {backup_path}\n")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")

    print("=== Before ===")
    print_summary(conn)

    print("\n=== Merging incidents ===")
    n_merged = merge_incidents(conn)
    print(f"  {n_merged} groups merged")

    print("\n=== Re-linking orphan cat=1 alerts ===")
    relink_orphans(conn)

    print("\n=== After ===")
    print_summary(conn)

    conn.close()
    print("\nDone. Backup at:", backup_path)


if __name__ == "__main__":
    main()
