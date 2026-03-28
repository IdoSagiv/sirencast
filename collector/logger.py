import json
import os
from datetime import datetime, timezone

from collector import config


class Logger:
    def __init__(self):
        self._last_oref_id = None
        self._last_areas = None

    def write(self, alert):
        oref_id = alert["oref_id"]
        areas = alert["areas"]

        if oref_id == self._last_oref_id and areas == self._last_areas:
            return

        self._last_oref_id = oref_id
        self._last_areas = list(areas)

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        raw_dir = os.path.join(config.DATA_DIR, "raw")
        os.makedirs(raw_dir, exist_ok=True)
        path = os.path.join(raw_dir, f"{today}.jsonl")

        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(alert, ensure_ascii=False) + "\n")
            f.flush()
