import time

from collector import config

IDLE = "IDLE"
CAT10_ACTIVE = "CAT10_ACTIVE"
COOLING = "COOLING"


class IncidentTracker:
    def __init__(self, db):
        self.db = db
        self.state = IDLE
        self.incident_id = None
        self.snapshot_n = 0
        self.last_oref_id = None
        self.last_areas = None
        self.cat10_ended = None

    def process(self, alert):
        cat = alert["cat"] if alert else None
        now = int(time.time())

        if self.state == IDLE:
            if cat == "10":
                self._open_incident(alert, now)
                self.state = CAT10_ACTIVE
            elif cat == "1":
                self._store_orphan_cat1(alert, now)

        elif self.state == CAT10_ACTIVE:
            if cat == "10":
                if alert["oref_id"] != self.last_oref_id or alert["areas"] != self.last_areas:
                    self._store_snapshot(alert, now)
            elif cat == "1":
                self._link_cat1(alert, now)
            elif cat is None:
                self.cat10_ended = now
                self.db.execute(
                    "UPDATE incidents SET cat10_ended=? WHERE id=?",
                    (now, self.incident_id),
                )
                self.db.commit()
                self.state = COOLING

        elif self.state == COOLING:
            if cat == "10":
                self._close_incident(now)
                self._open_incident(alert, now)
                self.state = CAT10_ACTIVE
            elif cat == "1":
                self._link_cat1(alert, now)
            elif cat is None:
                if now - self.cat10_ended > config.SIREN_LINKAGE_WINDOW_SECONDS:
                    self._close_incident(now)
                    self.state = IDLE

    def _open_incident(self, alert, now):
        cur = self.db.execute(
            "INSERT INTO incidents (started_at) VALUES (?)", (now,)
        )
        self.incident_id = cur.lastrowid
        self.snapshot_n = 0
        self.db.commit()
        self._store_snapshot(alert, now)

    def _store_snapshot(self, alert, now):
        self.snapshot_n += 1
        self.last_oref_id = alert["oref_id"]
        self.last_areas = list(alert["areas"])
        cur = self.db.execute(
            "INSERT INTO cat10_snapshots (incident_id, polled_at, oref_id, snapshot_n) VALUES (?,?,?,?)",
            (self.incident_id, now, alert["oref_id"], self.snapshot_n),
        )
        snap_id = cur.lastrowid
        for area in alert["areas"]:
            self.db.execute(
                "INSERT INTO cat10_areas (snapshot_id, area) VALUES (?,?)",
                (snap_id, area),
            )
        self.db.commit()

    def _store_orphan_cat1(self, alert, now):
        cur = self.db.execute(
            "INSERT INTO cat1_alerts (incident_id, fired_at, oref_id) VALUES (NULL,?,?)",
            (now, alert["oref_id"]),
        )
        alert_id = cur.lastrowid
        for area in alert["areas"]:
            self.db.execute(
                "INSERT INTO cat1_areas (alert_id, area) VALUES (?,?)",
                (alert_id, area),
            )
        self.db.commit()

    def _link_cat1(self, alert, now):
        self.db.execute(
            "UPDATE incidents SET had_siren=1 WHERE id=?", (self.incident_id,)
        )
        cur = self.db.execute(
            "INSERT INTO cat1_alerts (incident_id, fired_at, oref_id) VALUES (?,?,?)",
            (self.incident_id, now, alert["oref_id"]),
        )
        alert_id = cur.lastrowid
        for area in alert["areas"]:
            self.db.execute(
                "INSERT INTO cat1_areas (alert_id, area) VALUES (?,?)",
                (alert_id, area),
            )
        self.db.commit()

    def _close_incident(self, now):
        self.db.execute(
            "UPDATE incidents SET ended_at=? WHERE id=?", (now, self.incident_id)
        )
        self.db.commit()
        self.incident_id = None
        self.snapshot_n = 0
        self.last_oref_id = None
        self.last_areas = None
        self.cat10_ended = None
