"""GTFS ingestion asset — downloads Tirana bus feed and stores stops + routes to MotherDuck."""

import io
import os
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from dagster import AssetExecutionContext, asset

from tirana_pipeline.resources import MotherDuckResource

GTFS_URL = "https://pt.tirana.al/gtfs/gtfs.zip"

# Use $DAGSTER_HOME/raw/gtfs when available (Docker / local dev),
# otherwise fall back to a temp dir that is always writable (CI).
_dagster_home = os.environ.get("DAGSTER_HOME", "")
RAW_DIR = (
    Path(_dagster_home) / "raw" / "gtfs"
    if _dagster_home
    else Path(os.environ.get("RUNNER_TEMP", "/tmp")) / "raw" / "gtfs"
)


@asset(group_name="ingestion", description="Download GTFS feed ZIP from Tirana transit portal")
def gtfs_raw(context: AssetExecutionContext) -> bytes:
    """Fetch the raw GTFS ZIP file and cache it to disk."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    cached = RAW_DIR / "gtfs.zip"

    context.log.info(f"Downloading GTFS feed from {GTFS_URL}")
    response = requests.get(GTFS_URL, timeout=60)
    response.raise_for_status()

    cached.write_bytes(response.content)
    context.log.info(
        f"Saved GTFS ZIP ({len(response.content) / 1024:.1f} KB) to {cached}"
    )
    return response.content


@asset(group_name="ingestion", description="Parse GTFS stops into a GeoDataFrame")
def gtfs_stops(context: AssetExecutionContext, gtfs_raw: bytes) -> gpd.GeoDataFrame:
    """Extract stops.txt from GTFS ZIP and return as a GeoDataFrame (EPSG:4326)."""
    with zipfile.ZipFile(io.BytesIO(gtfs_raw)) as zf:
        with zf.open("stops.txt") as f:
            stops_df = pd.read_csv(f)

    required = {"stop_id", "stop_name", "stop_lat", "stop_lon"}
    missing = required - set(stops_df.columns)
    if missing:
        raise ValueError(f"GTFS stops.txt missing columns: {missing}")

    gdf = gpd.GeoDataFrame(
        stops_df,
        geometry=gpd.points_from_xy(stops_df["stop_lon"], stops_df["stop_lat"]),
        crs="EPSG:4326",
    )
    context.log.info(f"Parsed {len(gdf)} stops from GTFS feed")
    return gdf


@asset(group_name="ingestion", description="Parse GTFS routes into a DataFrame")
def gtfs_routes(context: AssetExecutionContext, gtfs_raw: bytes) -> pd.DataFrame:
    """Extract routes.txt from GTFS ZIP."""
    with zipfile.ZipFile(io.BytesIO(gtfs_raw)) as zf:
        with zf.open("routes.txt") as f:
            routes_df = pd.read_csv(f)

    context.log.info(f"Parsed {len(routes_df)} routes from GTFS feed")
    return routes_df


@asset(
    group_name="storage",
    description="Write GTFS stops to MotherDuck (DuckDB spatial)",
)
def stops_to_motherduck(
    context: AssetExecutionContext,
    gtfs_stops: gpd.GeoDataFrame,
    db: MotherDuckResource,
) -> None:
    """Upsert GTFS stops into the MotherDuck `stops` table using DuckDB spatial."""
    # Reproject to UTM 34N for metric distance calculations
    stops_utm = gtfs_stops.to_crs("EPSG:32634")

    # Build a flat staging DataFrame with WKT strings.
    # DuckDB's executemany() fails to bind ST_GeomFromText(?) when mixed
    # integer/string params are present in the same tuple. The fix is to
    # register a DataFrame and cast WKT → GEOMETRY inside a SELECT.
    staging = pd.DataFrame([
        {
            "stop_id":   str(row.stop_id),
            "stop_name": str(row.stop_name),
            "wkt_wgs84": row.geometry.wkt,
            "wkt_utm":   stops_utm.loc[idx].geometry.wkt,
        }
        for idx, row in gtfs_stops.iterrows()
    ])

    with db.get_connection() as conn:
        conn.execute("DELETE FROM stops;")
        conn.register("_stops_staging", staging)
        conn.execute("""
            INSERT INTO stops (stop_id, stop_name, geom, geom_utm)
            SELECT
                stop_id,
                stop_name,
                ST_GeomFromText(wkt_wgs84),
                ST_GeomFromText(wkt_utm)
            FROM _stops_staging
            ON CONFLICT (stop_id) DO UPDATE SET
                stop_name = excluded.stop_name,
                geom      = excluded.geom,
                geom_utm  = excluded.geom_utm
        """)
        conn.unregister("_stops_staging")

    context.log.info(f"Upserted {len(staging)} stops into MotherDuck")


@asset(
    group_name="storage",
    description="Write GTFS routes to MotherDuck",
)
def routes_to_motherduck(
    context: AssetExecutionContext,
    gtfs_routes: pd.DataFrame,
    db: MotherDuckResource,
) -> None:
    """Upsert GTFS routes into the MotherDuck `routes` table.

    Maps routes.txt columns to the existing schema:
        route_id   -> route_id   (VARCHAR PRIMARY KEY)
        route_name -> route_name (VARCHAR)  -- prefers route_long_name, falls back to route_short_name
        route_type -> route_type (INTEGER)
        agency_id  -> agency_id  (VARCHAR)
        shape      -> NULL       (GEOMETRY, populated by a separate shapes asset if needed)
    """
    # Resolve route_name: prefer long name, fall back to short name
    if "route_long_name" in gtfs_routes.columns:
        route_name = gtfs_routes["route_long_name"].fillna(
            gtfs_routes.get("route_short_name", "")
        )
    elif "route_short_name" in gtfs_routes.columns:
        route_name = gtfs_routes["route_short_name"]
    else:
        route_name = pd.Series([""] * len(gtfs_routes))

    staging = pd.DataFrame({
        "route_id":   gtfs_routes["route_id"].astype(str),
        "route_name": route_name.astype(str),
        "route_type": gtfs_routes["route_type"].astype(int) if "route_type" in gtfs_routes.columns else 3,
        "agency_id":  gtfs_routes["agency_id"].astype(str) if "agency_id" in gtfs_routes.columns else "",
    })

    with db.get_connection() as conn:
        conn.execute("DELETE FROM routes;")
        conn.register("_routes_staging", staging)
        conn.execute("""
            INSERT INTO routes (route_id, route_name, route_type, agency_id, shape)
            SELECT
                route_id,
                route_name,
                route_type,
                agency_id,
                NULL AS shape
            FROM _routes_staging
            ON CONFLICT (route_id) DO UPDATE SET
                route_name = excluded.route_name,
                route_type = excluded.route_type,
                agency_id  = excluded.agency_id
        """)
        conn.unregister("_routes_staging")

    context.log.info(f"Upserted {len(staging)} routes into MotherDuck")
