"""
Trigger a Power BI dataset refresh via the REST API.
Called by GitHub Actions after dbt run completes.

Requires environment variables:
    POWERBI_TENANT_ID
    POWERBI_CLIENT_ID
    POWERBI_CLIENT_SECRET
    POWERBI_DATASET_ID
"""

import os
import sys
import logging
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def get_access_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    resp = requests.post(url, data={
        "grant_type":    "client_credentials",
        "client_id":     client_id,
        "client_secret": client_secret,
        "scope":         "https://analysis.windows.net/powerbi/api/.default",
    })
    resp.raise_for_status()
    return resp.json()["access_token"]


def trigger_refresh(dataset_id: str, token: str):
    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/refreshes"
    resp = requests.post(url, headers={"Authorization": f"Bearer {token}"})

    if resp.status_code == 202:
        log.info(f"Power BI refresh triggered successfully for dataset {dataset_id}")
    else:
        log.error(f"Refresh failed: {resp.status_code} — {resp.text}")
        sys.exit(1)


if __name__ == "__main__":
    tenant_id     = os.environ["POWERBI_TENANT_ID"]
    client_id     = os.environ["POWERBI_CLIENT_ID"]
    client_secret = os.environ["POWERBI_CLIENT_SECRET"]
    dataset_id    = os.environ["POWERBI_DATASET_ID"]

    log.info("Authenticating with Azure AD...")
    token = get_access_token(tenant_id, client_id, client_secret)

    log.info("Triggering Power BI dataset refresh...")
    trigger_refresh(dataset_id, token)
