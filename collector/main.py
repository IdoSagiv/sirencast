import time
import signal
import sys

from collector import config, poller, logger, db, incidents


def main():
    print("SirenCast collector starting...")
    database = db.init()
    log = logger.Logger()
    inc = incidents.IncidentTracker(database)

    def shutdown(sig, frame):
        print("Shutting down...")
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    print(f"Polling every {config.POLL_INTERVAL_SECONDS}s. Data dir: {config.DATA_DIR}")
    while True:
        alert = poller.poll()
        if alert:
            log.write(alert)
        inc.process(alert)
        time.sleep(config.POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
