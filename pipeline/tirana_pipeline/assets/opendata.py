"""Open Data Albania ingestion assets.

SCOPE NOTE (updated): The project now focuses exclusively on Tirana's
11 Municipal Units (Njësia Bashkiake Nr. 1-11).  The QKB business-count
datasets below are aggregated at Albanian *region* level (e.g. "Tiranë"
as a whole) and cannot be disaggregated to individual municipal units.
These assets are retained for reference but are excluded from the active
Dagster asset graph until unit-level POI data (e.g. OSM amenities or a
finer-grained QKB extract) is available.

To re-enable, uncomment the @asset decorators and wire the assets back
into the Definitions in __init__.py.
"""

import io
from pathlib import Path

import pandas as pd
import requests
from dagster import AssetExecutionContext

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


# ── DISABLED: region-level granularity is too coarse for municipal-unit scope ──
# Uncomment @asset decorators to re-enable once unit-level data is available.

# @asset(
#     group_name="ingestion",
#     description="Download businesses-by-region CSV from opendata.gov.al",
# )
def businesses_by_region(context: AssetExecutionContext) -> pd.DataFrame:
    """
    QKB dataset: number of registered businesses per Albanian region (2026).
    DISABLED: data is at region level (e.g. whole "Tiranë" region), not
    at Tirana Municipal Unit level. Cannot be spatially joined to the
    11 Municipal Unit polygons without disaggregation.
    """
    return _fetch_csv(BUSINESS_BY_REGION_URL, "businesses_by_region", context)


# @asset(
#     group_name="ingestion",
#     description="Download businesses-by-legal-form CSV from opendata.gov.al",
# )
def businesses_by_legal_form(context: AssetExecutionContext) -> pd.DataFrame:
    """QKB dataset: number of registered businesses per legal form (2026). DISABLED."""
    return _fetch_csv(BUSINESS_BY_LEGAL_FORM_URL, "businesses_by_legal_form", context)


# @asset(
#     group_name="storage",
#     description="Store businesses-by-region in MotherDuck for spatial join in Phase 2",
# )
def businesses_to_db(
    context: AssetExecutionContext,
    businesses_by_region: pd.DataFrame,
    db,
) -> None:
    """
    DISABLED: see module docstring. Will be re-enabled with unit-level POI data.
    """
    pass
