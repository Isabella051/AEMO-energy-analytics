"""
Load ADLS Bronze Parquet files into Azure SQL Database.
"""

import os
import logging
from io import BytesIO

import pandas as pd
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

ADLS_CONNECTION_STRING = os.getenv(
    "ADLS_CONNECTION_STRING",
    "DefaultEndpointsProtocol=https;AccountName=aemobronze;AccountKey=wYtItEuNUXOE+K8jf14McFdLeF588tTfm4fp007Jo//ZhGALH7fndKz4HWp/8MLGSB7DofK37NLb+AStw8ky0A==;EndpointSuffix=core.windows.net"
)

CONTAINER_NAME = "nemdata"
BRONZE_FOLDER  = "bronze"

SQL_SERVER   = "aemo-analytics-server2025.database.windows.net"
SQL_DATABASE = "aemo-analytics-db"
SQL_USER     = "aemoadmin"
SQL_PASSWORD = os.getenv("SQL_PASSWORD", "pwd12345.")

CONNECTION_URL = (
    f"mssql+pyodbc://{SQL_USER}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DATABASE}"
    f"?driver=ODBC+Driver+18+for+SQL+Server"
    f"&Encrypt=yes&TrustServerCertificate=no"
)


def read_bronze_from_adls() -> pd.DataFrame:
    log.info("Connecting to ADLS...")
    client = BlobServiceClient.from_connection_string(ADLS_CONNECTION_STRING)
    container = client.get_container_client(CONTAINER_NAME)

    frames = []
    blobs = list(container.list_blobs(name_starts_with=f"{BRONZE_FOLDER}/"))
    log.info(f"Found {len(blobs)} blobs in bronze/")

    for blob in blobs:
        if not blob.name.endswith(".parquet"):
            continue
        log.info(f"  Reading {blob.name}...")
        data = container.get_blob_client(blob.name).download_blob().readall()
        df = pd.read_parquet(BytesIO(data))
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)

    # Drop duplicate region column — keep only REGION
    if "region_id" in combined.columns:
        combined = combined.drop(columns=["region_id"])

    log.info(f"Total rows from Bronze: {len(combined):,}")
    log.info(f"Columns: {list(combined.columns)}")
    return combined


def transform_to_silver(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Transforming to Silver layer...")
    df = df.copy()

    df["SETTLEMENTDATE"] = pd.to_datetime(df["SETTLEMENTDATE"], format="%Y/%m/%d %H:%M:%S")
    df["settlement_ts"]    = df["SETTLEMENTDATE"]
    df["settlement_date"]  = df["SETTLEMENTDATE"].dt.date
    df["settlement_month"] = df["SETTLEMENTDATE"].dt.to_period("M").dt.to_timestamp()
    df["hour_of_day"]      = df["SETTLEMENTDATE"].dt.hour
    df["time_of_use_band"] = df["hour_of_day"].apply(
        lambda h: "peak" if 7 <= h <= 21 else "off_peak"
    )
    df["is_price_spike"] = df["RRP"] > 300

    silver = df.rename(columns={
        "REGION":       "region_id",
        "RRP":          "rrp_aud_mwh",
        "TOTALDEMAND":  "total_demand_mw",
        "PERIODTYPE":   "period_type",
        "_ingested_at": "ingested_at",
    })[[
        "region_id", "settlement_ts", "settlement_date", "settlement_month",
        "hour_of_day", "time_of_use_band", "rrp_aud_mwh", "total_demand_mw",
        "period_type", "is_price_spike", "ingested_at"
    ]].dropna(subset=["rrp_aud_mwh", "total_demand_mw"])

    log.info(f"Silver rows: {len(silver):,}")
    return silver


def transform_to_gold(silver: pd.DataFrame):
    log.info("Aggregating to Gold layer...")

    # Ensure region_id is a simple string series
    silver = silver.copy()
    silver["region_id"] = silver["region_id"].astype(str)
    silver["settlement_date"] = pd.to_datetime(silver["settlement_date"])
    silver["settlement_month"] = pd.to_datetime(silver["settlement_month"])

    fct = (
        silver.groupby(
            ["region_id", "settlement_date", "settlement_month", "time_of_use_band"],
            as_index=False
        )
        .agg(
            avg_price_aud_mwh    = ("rrp_aud_mwh", "mean"),
            min_price_aud_mwh    = ("rrp_aud_mwh", "min"),
            max_price_aud_mwh    = ("rrp_aud_mwh", "max"),
            median_price_aud_mwh = ("rrp_aud_mwh", "median"),
            avg_demand_mw        = ("total_demand_mw", "mean"),
            peak_demand_mw       = ("total_demand_mw", "max"),
            spike_intervals      = ("is_price_spike", "sum"),
            total_intervals      = ("is_price_spike", "count"),
        )
    )

    fct["est_market_cost_aud"] = (
        fct["avg_price_aud_mwh"] * fct["avg_demand_mw"] * 5 / 60
    ).round(2)

    fct["spike_pct"] = (
        fct["spike_intervals"] / fct["total_intervals"].replace(0, float("nan")) * 100
    ).round(2)

    fct["price_change_dod"] = (
        fct.sort_values("settlement_date")
        .groupby(["region_id", "time_of_use_band"])["avg_price_aud_mwh"]
        .diff()
        .round(2)
    )

    fct = fct.round(2)
    log.info(f"Gold fct_energy_cost rows: {len(fct):,}")

    dim_region = pd.DataFrame({
        "region_id":          ["NSW1",            "VIC1",      "QLD1",        "SA1",              "TAS1"],
        "region_name":        ["New South Wales",  "Victoria",  "Queensland",  "South Australia",  "Tasmania"],
        "interconnect_zone":  ["East Coast"] * 5,
        "market":             ["NEM"] * 5,
    })

    return fct, dim_region


def load_to_sql(silver: pd.DataFrame, fct: pd.DataFrame, dim: pd.DataFrame):
    log.info("Connecting to Azure SQL...")
    engine = create_engine(CONNECTION_URL)

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE staging.stg_price_and_demand"))
        conn.execute(text("TRUNCATE TABLE marts.fct_energy_cost"))
        conn.execute(text("DELETE FROM marts.dim_region"))

    log.info("Loading staging.stg_price_and_demand...")
    silver.to_sql("stg_price_and_demand", engine, schema="staging",
                  if_exists="append", index=False, chunksize=1000)
    log.info(f"  Loaded {len(silver):,} rows")

    log.info("Loading marts.fct_energy_cost...")
    fct.to_sql("fct_energy_cost", engine, schema="marts",
               if_exists="append", index=False, chunksize=1000)
    log.info(f"  Loaded {len(fct):,} rows")

    log.info("Loading marts.dim_region...")
    dim.to_sql("dim_region", engine, schema="marts",
               if_exists="append", index=False, chunksize=100)
    log.info(f"  Loaded {len(dim):,} rows")

    log.info("=== Load complete ===")


if __name__ == "__main__":
    raw      = read_bronze_from_adls()
    silver   = transform_to_silver(raw)
    fct, dim = transform_to_gold(silver)
    load_to_sql(silver, fct, dim)
