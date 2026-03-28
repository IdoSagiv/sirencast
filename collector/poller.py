import json
import time
import requests


URL = "https://www.oref.org.il/warningMessages/alert/alerts.json"
HEADERS = {
    "Referer": "https://www.oref.org.il/",
    "X-Requested-With": "XMLHttpRequest",
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
        return {
            "ts": int(time.time()),
            "cat": str(data.get("cat", "")),
            "oref_id": str(data.get("id", "")),
            "areas": list(data.get("data", [])),
        }
    except Exception as e:
        print(f"[poller] error: {e}")
        return None
