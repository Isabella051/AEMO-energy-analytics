"""
AEMO NEM Energy Data Ingestion Pipeline
Pulls electricity price & demand data directly from AEMO official CSV URLs,
lands raw Parquet files into the Bronze layer.

Data source: AEMO Price and Demand CSV files
URL pattern: https://aemo.com.au/aemo/data/nem/priceanddemand/PRICE_AND_DEMAND_{YYYYMM}_{REGION}.csv
"""

import logging
import requests
from datetime import datetime, timedelta
from pathlib import Path
import os

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

BRONZE_PATH = Path(os.getenv("BRONZE_PATH", "data/bronze"))
REGIONS = ["NSW1", "VIC1", "QLD1", "SA1", "TAS1"]
BASE_URL = "https://aemo.com.au/aemo/data/nem/priceanddemand"


def get_months(lookback_days: int = 60):
    """Return list of YYYYMM strings covering the lookback period."""
    end = datetime.utcnow().date()
    start = end - timedelta(days=lookback_days)
    months = set()
    current = start.replace(day=1)
    while current <= end:
        months.add(current.strftime("%Y%m"))
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    return sorted(months)


def fetch_region_month(region: str, yyyymm: str):
    """Fetch one region/month CSV from AEMO and return as DataFrame."""
    url = f"{BASE_URL}/PRICE_AND_DEMAND_{yyyymm}_{region}.csv"
    try:
        r = requests.get(url, timeout=30)
        if r.status_code != 200:
            log.warning(f"  Not available: {region} {yyyymm} (HTTP {r.status_code})")
            return None
        from io import StringIO
        df = pd.read_csv(StringIO(r.text))
        df["region_id"] = region
        df["_source_url"] = url
        df["_ingested_at"] = datetime.utcnow().isoformat()
        log.info(f"  Fetched {len(df):,} rows — {region} {yyyymm}")
        return df
    except Exception as e:
        log.error(f"  Error fetching {region} {yyyymm}: {e}")
        return None


def save_bronze(df: pd.DataFrame, yyyymm: str):
    """Save a month's data to Bronze layer as Parquet."""
    out_dir = BRONZE_PATH / "price_and_demand"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{yyyymm}.parquet"
    df.to_parquet(path, index=False)
    log.info(f"Saved {len(df):,} rows → {path}")


def run(lookback_days: int = 60):
    months = get_months(lookback_days)
    log.info(f"=== AEMO ingestion: {months[0]} → {months[-1]} ({len(months)} months) ===")

    for yyyymm in months:
        frames = []
        for region in REGIONS:
            df = fetch_region_month(region, yyyymm)
            if df is not None:
                frames.append(df)

        if frames:
            combined = pd.concat(frames, ignore_index=True)
            save_bronze(combined, yyyymm)
        else:
            log.warning(f"No data fetched for {yyyymm}")

    log.info("=== Ingestion complete ===")
    log.info(f"Bronze files saved to: {BRONZE_PATH / 'price_and_demand'}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--lookback-days", type=int, default=60)
    args = parser.parse_args()
    run(args.lookback_days)
