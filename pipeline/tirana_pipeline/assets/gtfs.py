"""GTFS ingestion asset — downloads Tirana bus feed and stores stops + routes to PostGIS."""

import io
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from dagster import AssetExecutionContext, asset
from geoalchemy2 import Geometry, WKTElement
from shapely.geometry import Point
from sqlalchemy import text

GTFS_URL = "https://pt.tirana.al/gtfs/gtfs.zip"
RAW_DIR = Path("/data/raw/gtfs")


@asset(group_name="ingestion", description="Download GTFS feed ZIP from Tirana transit portal")
def gtfs_raw(context: AssetExecutionContext) -> bytes:
    """Fetch the raw GTFS ZIP file and cache it to disk."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    cached = RAW_DIR / "gtfs.zip"

    context.log.info(f"Downloading GTFS feed from {GTFS_URL}")
    response = requests.get(GTFS_URL, timeout=60)
    response.raise_for_status()

    cached.write_bytes(response.content)
    context.log.info(f"Saved GTFS ZIP ({len(response.content) / 1024:.1f} KB) to {cached}")
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
    description="Write GTFS stops to PostGIS table",
    deps=["gtfs_stops"],
)
def stops_to_postgis(
    context: AssetExecutionContext,
    gtfs_stops: gpd.GeoDataFrame,
    db: "DatabaseResource",  # type: ignore[name-defined]
) -> None:
    """Upsert GTFS stops into the PostGIS `stops` table."""
    engine = db.get_engine()

    # Reproject to UTM 34N for accurate distance calculations later
    stops_utm = gtfs_stops.to_crs("EPSG:32634")

    with engine.begin() as conn:
        conn.execute(
            text("""
                CREATE TABLE IF NOT EXISTS stops (
                    stop_id    TEXT PRIMARY KEY,
                    stop_name  TEXT,
                    geom       GEOMETRY(Point, 4326),
                    geom_utm   GEOMETRY(Point, 32634)
                );
                CREATE INDEX IF NOT EXISTS stops_geom_idx ON stops USING GIST (geom);
                CREATE INDEX IF NOT EXISTS stops_geom_utm_idx ON stops USING GIST (geom_utm);
            """)
        )

    rows = [
        {
            "stop_id": row.stop_id,
            "stop_name": row.stop_name,
            "geom": WKTElement(row.geometry.wkt, srid=4326),
            "geom_utm": WKTElement(stops_utm.loc[idx].geometry.wkt, srid=32634),
        }
        for idx, row in gtfs_stops.iterrows()
    ]

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO stops (stop_id, stop_name, geom, geom_utm)
                VALUES (:stop_id, :stop_name, ST_GeomFromText(:geom, 4326), ST_GeomFromText(:geom_utm, 32634))
                ON CONFLICT (stop_id) DO UPDATE
                    SET stop_name = EXCLUDED.stop_name,
                        geom      = EXCLUDED.geom,
                        geom_utm  = EXCLUDED.geom_utm;
            """),
            rows,
        )

    context.log.info(f"Upserted {len(rows)} stops into PostGIS")
