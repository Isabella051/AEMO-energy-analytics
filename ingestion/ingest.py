"""
AEMO NEM Energy Data Ingestion Pipeline
Pulls electricity price and demand data via nem-data,
lands raw Parquet files into the Bronze layer (local or ADLS).
"""

import os
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from nemdata import load

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

REGIONS = ["NSW1", "VIC1", "QLD1", "SA1", "TAS1"]
BRONZE_PATH = Path(os.getenv("BRONZE_PATH", "data/bronze"))

# Tables to pull — see nem-data docs for full list
TABLES = {
    "trading-price": "wholesale_price",   # 30-min settlement price per region
    "dispatch-price": "dispatch_price",   # 5-min dispatch price per region
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_date_range(lookback_days: int = 30):
    """Return (start, end) as YYYY-MM-DD strings."""
    end = datetime.utcnow().date()
    start = end - timedelta(days=lookback_days)
    return str(start), str(end)


def pull_table(table_name: str, start: str, end: str) -> pd.DataFrame:
    """Download a NEM table for the given date range."""
    log.info(f"Pulling {table_name}  {start} → {end}")
    df = load(table_name, start=start, end=end)
    log.info(f"  Rows fetched: {len(df):,}")
    return df


def add_metadata(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Stamp every row with ingestion metadata."""
    df = df.copy()
    df["_source_table"] = table_name
    df["_ingested_at"] = datetime.utcnow().isoformat()
    return df


def save_bronze(df: pd.DataFrame, layer_name: str, start: str, end: str):
    """Partition by year-month and write Parquet to Bronze layer."""
    out_dir = BRONZE_PATH / layer_name
    out_dir.mkdir(parents=True, exist_ok=True)

    # Partition by month for efficient downstream reads
    if "SETTLEMENTDATE" in df.columns:
        df["SETTLEMENTDATE"] = pd.to_datetime(df["SETTLEMENTDATE"])
        for ym, group in df.groupby(df["SETTLEMENTDATE"].dt.to_period("M")):
            path = out_dir / f"{ym}.parquet"
            group.to_parquet(path, index=False)
            log.info(f"  Saved {len(group):,} rows → {path}")
    else:
        path = out_dir / f"{start}_{end}.parquet"
        df.to_parquet(path, index=False)
        log.info(f"  Saved {len(df):,} rows → {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(lookback_days: int = 30):
    start, end = get_date_range(lookback_days)
    log.info(f"=== AEMO ingestion run: {start} → {end} ===")

    for table_name, layer_name in TABLES.items():
        try:
            df = pull_table(table_name, start, end)
            df = add_metadata(df, table_name)
            save_bronze(df, layer_name, start, end)
        except Exception as e:
            log.error(f"Failed to pull {table_name}: {e}")

    log.info("=== Ingestion complete ===")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--lookback-days", type=int, default=30,
        help="How many days of history to pull (default: 30)"
    )
    args = parser.parse_args()
    run(args.lookback_days)
