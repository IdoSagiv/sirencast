import logging
import time
import signal

from collector import config, poller, logger, db, incidents


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S',
    )

    logging.info("SirenCast collector starting...")
    database = db.init()
    log = logger.Logger()
    inc = incidents.IncidentTracker(database)

    running = True

    def shutdown(sig, frame):
        nonlocal running
        logging.info('Shutdown signal received, exiting cleanly...')
        running = False

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    logging.info(f"Polling every {config.POLL_INTERVAL_SECONDS}s. Data dir: {config.DATA_DIR}")
    while running:
        alert = poller.poll()
        if alert:
            log.write(alert)
        inc.process(alert)
        time.sleep(config.POLL_INTERVAL_SECONDS)

    logging.info('Collector stopped.')


if __name__ == "__main__":
    main()
