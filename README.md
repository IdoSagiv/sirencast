# SirenCast

Collector that monitors Israeli Home Front Command alerts and builds a historical dataset.

### Quick start
```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python -m collector.main
```

### Config (.env)
| Var | Default | Description |
|-----|---------|-------------|
| DATA_DIR | ./data | Where to store DB + JSONL files |
| POLL_INTERVAL_SECONDS | 1.0 | How often to poll oref API |
| SIREN_LINKAGE_WINDOW_SECONDS | 120 | Seconds after cat=10 ends to still link cat=1 |

### Systemd (optional)
```bash
sudo cp sirencast-collector.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now sirencast-collector
```
Adjust WorkingDirectory in the service file to your install path.

## Web App

### Dev server
```bash
uvicorn web.main:app --reload --port 8000
```
Open http://localhost:8000 in your browser.

### Systemd (production)
```bash
sudo cp sirencast-web.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now sirencast-web
```
Adjust `WorkingDirectory` in the service file to match your install path.
