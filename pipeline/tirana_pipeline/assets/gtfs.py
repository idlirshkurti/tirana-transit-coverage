"""GTFS ingestion asset — downloads Tirana bus feed and stores stops + routes to MotherDuck."""

import io
import os
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from dagster import AssetExecutionContext, asset
from shapely.geometry import LineString, MultiLineString

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


@asset(group_name="ingestion", description="Parse GTFS trips into a DataFrame")
def gtfs_trips(context: AssetExecutionContext, gtfs_raw: bytes) -> pd.DataFrame:
    """Extract trips.txt from GTFS ZIP.

    Returns a DataFrame with at least route_id and shape_id columns,
    providing the many-to-many mapping between routes and shapes.
    """
    with zipfile.ZipFile(io.BytesIO(gtfs_raw)) as zf:
        names = zf.namelist()
        if "trips.txt" not in names:
            context.log.warning("trips.txt not found in GTFS ZIP — returning empty DataFrame")
            return pd.DataFrame(columns=["route_id", "shape_id"])
        with zf.open("trips.txt") as f:
            trips_df = pd.read_csv(f)

    required = {"route_id", "shape_id"}
    missing = required - set(trips_df.columns)
    if missing:
        context.log.warning(f"trips.txt missing columns {missing} — shape geometry will be NULL")
        for col in missing:
            trips_df[col] = None

    context.log.info(f"Parsed {len(trips_df)} trips from GTFS feed")
    return trips_df[["route_id", "shape_id"]].drop_duplicates()


@asset(group_name="ingestion", description="Parse GTFS shapes into a GeoDataFrame of route lines")
def gtfs_shapes(
    context: AssetExecutionContext,
    gtfs_raw: bytes,
    gtfs_trips: pd.DataFrame,
) -> gpd.GeoDataFrame:
    """Build per-route LineString/MultiLineString geometries from shapes.txt.

    Pipeline:
        shapes.txt  (shape_id, seq, lat, lon)
          -> sort by shape_pt_sequence
          -> build one LineString per shape_id
          -> join trips to map shape_id -> route_id
          -> dissolve into MultiLineString per route_id
    Returns a GeoDataFrame with columns [route_id, geometry] in EPSG:4326.
    """
    with zipfile.ZipFile(io.BytesIO(gtfs_raw)) as zf:
        names = zf.namelist()
        if "shapes.txt" not in names:
            context.log.warning("shapes.txt not found in GTFS ZIP — no route geometry available")
            return gpd.GeoDataFrame(columns=["route_id", "geometry"], geometry="geometry",
                                    crs="EPSG:4326")
        with zf.open("shapes.txt") as f:
            shapes_df = pd.read_csv(f)

    required = {"shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"}
    missing = required - set(shapes_df.columns)
    if missing:
        raise ValueError(f"shapes.txt missing required columns: {missing}")

    # Sort and build one LineString per shape_id
    shapes_df = shapes_df.sort_values(["shape_id", "shape_pt_sequence"])

    def _build_line(grp: pd.DataFrame) -> LineString | None:
        coords = list(zip(grp["shape_pt_lon"], grp["shape_pt_lat"]))
        if len(coords) < 2:
            return None
        return LineString(coords)

    shape_lines = (
        shapes_df.groupby("shape_id")
        .apply(_build_line, include_groups=False)
        .dropna()
        .reset_index()
        .rename(columns={0: "geometry"})
    )
    context.log.info(f"Built {len(shape_lines)} shape LineStrings")

    # Join trips to get route_id -> shape_id mapping
    trips_clean = gtfs_trips.dropna(subset=["shape_id"])
    if trips_clean.empty:
        context.log.warning("No valid shape_id values in trips — returning empty GeoDataFrame")
        return gpd.GeoDataFrame(columns=["route_id", "geometry"], geometry="geometry",
                                crs="EPSG:4326")

    merged = trips_clean.merge(shape_lines, on="shape_id", how="inner")

    # Dissolve: one MultiLineString per route_id
    def _dissolve_route(grp: pd.DataFrame):
        lines = [g for g in grp["geometry"] if g is not None]
        if not lines:
            return None
        if len(lines) == 1:
            return lines[0]
        return MultiLineString(lines)

    route_geoms = (
        merged.groupby("route_id")
        .apply(_dissolve_route, include_groups=False)
        .dropna()
        .reset_index()
        .rename(columns={0: "geometry"})
    )
    context.log.info(f"Dissolved into {len(route_geoms)} per-route geometries")

    gdf = gpd.GeoDataFrame(route_geoms, geometry="geometry", crs="EPSG:4326")
    return gdf


@asset(
    group_name="storage",
    description="Write GTFS stops to MotherDuck (DuckDB spatial)",
)
def stops_to_motherduck(
    context: AssetExecutionContext,
    gtfs_stops: gpd.GeoDataFrame,
    db: MotherDuckResource,
) -> None:
    """Upsert GTFS stops into the MotherDuck `stops` table using DuckDB spatial.

    Uses a staging-swap pattern for atomic writes:
    1. Write all new rows into `stops_staging` (schema clone of `stops`).
    2. Inside a single transaction: DROP live table, RENAME staging -> live.
    3. On any failure: ROLLBACK and clean up staging — previous data is preserved.
    """
    # Reproject to UTM 34N for metric distance calculations
    stops_utm = gtfs_stops.to_crs("EPSG:32634")

    # Build a flat staging DataFrame with WKT strings.
    # DuckDB's executemany() fails to bind ST_GeomFromText(?) when mixed
    # integer/string params are present in the same tuple. The fix is to
    # register a DataFrame and cast WKT -> GEOMETRY inside a SELECT.
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
        # Create empty staging table with same schema as stops
        conn.execute("CREATE OR REPLACE TABLE stops_staging AS SELECT * FROM stops WHERE 1=0;")
        conn.register("_stops_staging", staging)
        conn.execute("""
            INSERT INTO stops_staging (stop_id, stop_name, geom, geom_utm)
            SELECT
                stop_id,
                stop_name,
                ST_GeomFromText(wkt_wgs84),
                ST_GeomFromText(wkt_utm)
            FROM _stops_staging
        """)
        conn.unregister("_stops_staging")

        # Atomic swap: live table is never empty at any point
        conn.execute("BEGIN;")
        try:
            conn.execute("DROP TABLE stops;")
            conn.execute("ALTER TABLE stops_staging RENAME TO stops;")
            conn.execute("COMMIT;")
        except Exception:
            conn.execute("ROLLBACK;")
            conn.execute("DROP TABLE IF EXISTS stops_staging;")
            raise

    context.log.info(f"Atomically swapped {len(staging)} stops into MotherDuck")


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
        route_name -> route_name (VARCHAR)
            -- prefers route_long_name, falls back to route_short_name
        route_type -> route_type (INTEGER)
        agency_id  -> agency_id  (VARCHAR)
        shape      -> NULL       (GEOMETRY, populated by shapes_to_motherduck)
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

    route_type = (
        gtfs_routes["route_type"].astype(int)
        if "route_type" in gtfs_routes.columns
        else 3
    )
    agency_id = (
        gtfs_routes["agency_id"].astype(str)
        if "agency_id" in gtfs_routes.columns
        else ""
    )
    staging = pd.DataFrame({
        "route_id":   gtfs_routes["route_id"].astype(str),
        "route_name": route_name.astype(str),
        "route_type": route_type,
        "agency_id":  agency_id,
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


@asset(
    group_name="storage",
    description="Write per-route geometries to MotherDuck routes.shape column",
    deps=["routes_to_motherduck"],
)
def shapes_to_motherduck(
    context: AssetExecutionContext,
    gtfs_shapes: gpd.GeoDataFrame,
    db: MotherDuckResource,
) -> None:
    """Update routes.shape with MultiLineString geometries built from shapes.txt.

    Depends on routes_to_motherduck so rows exist before we UPDATE them.
    Only routes with a matching shape_id chain are updated; the rest remain NULL.
    """
    if gtfs_shapes.empty:
        context.log.warning("gtfs_shapes is empty — no route geometry to write")
        return

    staging = pd.DataFrame({
        "route_id": gtfs_shapes["route_id"].astype(str),
        "wkt":      gtfs_shapes["geometry"].apply(lambda g: g.wkt if g is not None else None),
    }).dropna(subset=["wkt"])

    if staging.empty:
        context.log.warning("All shape geometries are None — skipping update")
        return

    with db.get_connection() as conn:
        conn.register("_shapes_staging", staging)
        conn.execute("""
            UPDATE routes
            SET shape = ST_GeomFromText(s.wkt)
            FROM _shapes_staging s
            WHERE routes.route_id = s.route_id
        """)
        conn.unregister("_shapes_staging")

    context.log.info(f"Updated shape geometry for {len(staging)} routes in MotherDuck")
