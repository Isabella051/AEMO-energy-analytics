"""
Automated scheduler — runs the AEMO ingestion pipeline daily.
Uses APScheduler so this can run as a long-lived process
(e.g. on a VM, Docker container, or local machine).

Usage:
    python ingestion/scheduler.py

Environment variables:
    BRONZE_PATH   Local or mounted path for Bronze Parquet files
    LOOKBACK_DAYS How many days to pull each run (default: 2)
"""

import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from ingest import run

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

scheduler = BlockingScheduler(timezone="Australia/Sydney")


@scheduler.scheduled_job("cron", hour=6, minute=0)
def daily_ingest():
    """Pull yesterday + today's data every morning at 06:00 AEST."""
    log.info("Scheduler triggered — starting daily ingestion")
    run(lookback_days=2)
    log.info("Daily ingestion complete")


if __name__ == "__main__":
    log.info("Starting AEMO ingestion scheduler (daily @ 06:00 AEST)")
    scheduler.start()
