import os
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = os.getenv("DATA_DIR", "./data")
POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "1.0"))
SIREN_LINKAGE_WINDOW_SECONDS = int(os.getenv("SIREN_LINKAGE_WINDOW_SECONDS", "120"))
