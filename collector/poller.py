import json
import logging
import time
import requests


URL = "https://www.oref.org.il/warningMessages/alert/alerts.json"
HEADERS = {
    "Referer": "https://www.oref.org.il/",
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}


def poll():
    try:
        resp = requests.get(URL, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        text = resp.content.decode("utf-8-sig").strip()
        if not text:
            return None
        data = json.loads(text)
        if isinstance(data, list) and len(data) > 0:
            data = data[0]
        if str(data.get("cat", "")) not in {"1", "10"}:
            logging.warning(f'[poller] unexpected cat value: {data.get("cat")}')
            return None
        return {
            "ts": int(time.time()),
            "cat": str(data.get("cat", "")),
            "oref_id": str(data.get("id", "")),
            "areas": list(data.get("data", [])),
        }
    except Exception as e:
        logging.error(f"[poller] error: {e}")
        return None
