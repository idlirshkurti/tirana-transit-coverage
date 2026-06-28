"""Open Data Albania ingestion assets — businesses and education datasets."""

import io
import json
from pathlib import Path

import pandas as pd
import requests
from dagster import AssetExecutionContext, asset

from tirana_pipeline.resources import MotherDuckResource

RAW_DIR = Path("/data/raw/opendata")

BUSINESS_BY_REGION_URL = (
    "https://opendata.gov.al/files/Dataset/service/"
    "a9b8b467-a32d-4001-8077-c70e29819cd2/"
    "a9b8b467-a32d-4001-8077-c70e29819cd2_csv.csv"
)
BUSINESS_BY_LEGAL_FORM_URL = (
    "https://opendata.gov.al/files/Dataset/service/"
    "96bac749-2fe2-45c9-aca0-8251127d2f0f/"
    "96bac749-2fe2-45c9-aca0-8251127d2f0f_csv.csv"
)


def _fetch_csv(url: str, name: str, context: AssetExecutionContext) -> pd.DataFrame:
    """Helper: download a CSV with caching to /data/raw/opendata."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    cached = RAW_DIR / f"{name}.csv"

    context.log.info(f"Fetching {name} from {url}")
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        cached.write_bytes(resp.content)
        df = pd.read_csv(io.BytesIO(resp.content))
        context.log.info(f"Downloaded {name}: {len(df)} rows")
    except Exception as exc:
        if cached.exists():
            context.log.warning(f"Download failed ({exc}), loading cached version")
            df = pd.read_csv(cached)
        else:
            raise

    return df


@asset(group_name="ingestion", description="Download businesses-by-region CSV from opendata.gov.al")
def businesses_by_region(context: AssetExecutionContext) -> pd.DataFrame:
    """
    QKB dataset: number of registered businesses per Albanian region (2026).
    Columns expected: region, count (exact column names may vary — inspect on first run).
    """
    df = _fetch_csv(BUSINESS_BY_REGION_URL, "businesses_by_region", context)
    context.log.info(f"Business-by-region columns: {list(df.columns)}")
    return df


@asset(
    group_name="ingestion",
    description="Download businesses-by-legal-form CSV from opendata.gov.al",
)
def businesses_by_legal_form(context: AssetExecutionContext) -> pd.DataFrame:
    """QKB dataset: number of registered businesses per legal form (2026)."""
    df = _fetch_csv(BUSINESS_BY_LEGAL_FORM_URL, "businesses_by_legal_form", context)
    context.log.info(f"Business-by-legal-form columns: {list(df.columns)}")
    return df


@asset(
    group_name="storage",
    description="Store businesses-by-region in MotherDuck for spatial join in Phase 2",
)
def businesses_to_db(
    context: AssetExecutionContext,
    businesses_by_region: pd.DataFrame,
    db: MotherDuckResource,
) -> None:
    """
    Persist the region-level business counts to a staging table in MotherDuck.
    Phase 2 will spatially disaggregate these onto neighbourhood polygons.
    """
    df = businesses_by_region.copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    rows = [
        (str(row.get("region", str(idx))), json.dumps(row.to_dict()))
        for idx, row in df.iterrows()
    ]

    with db.get_connection() as conn:
        conn.execute("DELETE FROM staging_businesses_by_region;")
        conn.executemany(
            "INSERT INTO staging_businesses_by_region (region, data) VALUES (?, ?::JSON)",
            rows,
        )

    context.log.info(f"Stored {len(rows)} regions in staging_businesses_by_region")
