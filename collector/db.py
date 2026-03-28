import os
import sqlite3

from collector import config


def init():
    os.makedirs(config.DATA_DIR, exist_ok=True)
    path = os.path.join(config.DATA_DIR, "sirencast.db")
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS incidents (
            id INTEGER PRIMARY KEY,
            started_at INTEGER,
            cat10_ended INTEGER,
            ended_at INTEGER,
            had_siren INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS cat10_snapshots (
            id INTEGER PRIMARY KEY,
            incident_id INTEGER REFERENCES incidents(id),
            polled_at INTEGER,
            oref_id TEXT,
            snapshot_n INTEGER
        );

        CREATE TABLE IF NOT EXISTS cat10_areas (
            snapshot_id INTEGER REFERENCES cat10_snapshots(id),
            area TEXT
        );

        CREATE TABLE IF NOT EXISTS cat1_alerts (
            id INTEGER PRIMARY KEY,
            incident_id INTEGER REFERENCES incidents(id),
            fired_at INTEGER,
            oref_id TEXT
        );

        CREATE TABLE IF NOT EXISTS cat1_areas (
            alert_id INTEGER REFERENCES cat1_alerts(id),
            area TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_cat10_areas_snapshot ON cat10_areas(snapshot_id);
        CREATE INDEX IF NOT EXISTS idx_cat10_snapshots_incident ON cat10_snapshots(incident_id);
        CREATE INDEX IF NOT EXISTS idx_cat1_alerts_incident ON cat1_alerts(incident_id);
        CREATE INDEX IF NOT EXISTS idx_cat1_areas_alert ON cat1_areas(alert_id);
    """)
    conn.commit()
    return conn
