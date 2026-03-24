# AEMO Energy Analytics Platform

An end-to-end analytics pipeline ingesting Australian electricity market data from AEMO's National Electricity Market (NEM), transforming it through a Medallion lakehouse architecture, and delivering a Power BI dashboard for energy cost analysis and procurement decision support.

---

## Architecture

```
AEMO NEM API
    │
    ▼
Python ingestion (nem-data)          ← automated daily via APScheduler
    │
    ▼
Bronze layer — raw Parquet           ← partitioned by month, append-only
    │
    ▼
dbt Silver — stg_dispatch_price      ← cleaned, typed, standardised
             stg_trading_price
    │
    ▼
dbt Gold   — fct_energy_cost         ← daily cost facts, spike detection
             dim_region              ← NEM region reference
    │
    ▼
Power BI Dashboard                   ← auto-refreshed via REST API
    │
GitHub Actions CI/CD                 ← dbt test on every PR, deploy on merge
```

---

## Business questions answered

| Dashboard page | Question |
|---|---|
| Price overview | How does wholesale electricity price vary across NSW, VIC, QLD, SA, TAS? |
| Peak vs off-peak | What is the peak/off-peak price spread — and how has it changed? |
| Price spike risk | Which regions experience the most price spikes (>$300/MWh)? |
| Cost forecasting | What would energy procurement cost at current market prices? |

---

## Tech stack

| Layer | Tool |
|---|---|
| Data ingestion | Python, `nem-data`, `APScheduler` |
| Bronze storage | Parquet files (local / ADLS) |
| Transformation | dbt Core + DuckDB |
| Data quality | dbt schema tests (`not_null`, `accepted_values`, `unique`) |
| CI/CD | GitHub Actions |
| Visualisation | Power BI (auto-refresh via REST API) |

---

## Quick start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Pull last 30 days of AEMO data into Bronze
python ingestion/ingest.py --lookback-days 30

# 3. Run dbt transformations
cd dbt_project
dbt build

# 4. (Optional) Start daily scheduler
python ingestion/scheduler.py
```

---

## dbt model lineage

```
bronze.dispatch_price   bronze.wholesale_price
        │                       │
        ▼                       ▼
stg_dispatch_price      stg_trading_price
                                │
                                ▼
                        fct_energy_cost ◄── dim_region
```

---

## CI/CD pipeline

Every pull request to `main` triggers:
1. Data pull (last 7 days)
2. `dbt build` — compiles all models and runs all schema tests
3. On merge to `main`: `dbt run` on production + Power BI refresh via REST API

Secrets required in GitHub repository settings:
- `POWERBI_TENANT_ID`
- `POWERBI_CLIENT_ID`
- `POWERBI_CLIENT_SECRET`
- `POWERBI_DATASET_ID`

---

## Power BI setup

1. Open `powerbi/AEMO_Energy_Dashboard.pbix`
2. Update the data source path to your local Bronze folder or ADLS endpoint
3. Publish to Power BI Service
4. Configure scheduled refresh (or use the GitHub Actions REST API trigger)

---

## Data source

All data sourced from the **Australian Energy Market Operator (AEMO)** NEM public data portal. Data is publicly available under AEMO's terms of use.

- Dispatch price: 5-minute settlement prices per NEM region
- Trading price: 30-minute wholesale prices per NEM region
- Regions covered: NSW1, VIC1, QLD1, SA1, TAS1
