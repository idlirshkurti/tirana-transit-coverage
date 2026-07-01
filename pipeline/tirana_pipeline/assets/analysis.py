"""Phase 2: Geospatial analysis — isochrones, demand join, coverage ratio, gap score.

All distance and area calculations use EPSG:32634 (UTM zone 34N).
Outputs a ranked GeoJSON to data/processed/gap_scores.geojson for Phase 3.
"""

import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
from dagster import AssetExecutionContext, asset
from shapely.geometry import mapping
from shapely.ops import unary_union

from tirana_pipeline.resources import MotherDuckResource

PROCESSED_DIR = Path(os.environ.get("DAGSTER_HOME", "/data")) / "processed"
EPSILON = 0.01  # avoid division-by-zero for fully unserved neighbourhoods
ISOCHRONE_RADIUS_M = 400  # walking buffer radius in metres


# ---------------------------------------------------------------------------
# 2.1  Walking isochrones
# ---------------------------------------------------------------------------

@asset(
    group_name="analysis",
    description=(
        f"Generate {ISOCHRONAL_RADIUS_M}m walking-buffer isochrones around each bus stop "
        "in UTM 34N (EPSG:32634)."
    ),
)
def stop_isochrones(
    context: AssetExecutionContext,
    db: MotherDuckResource,
) -> gpd.GeoDataFrame:
    """Load stops from MotherDuck and buffer them by ISOCHRONE_RADIUS_M metres.

    Returns a GeoDataFrame with columns [stop_id, geometry] in EPSG:32634.
    Each geometry is the 400m walking-buffer polygon around one stop.
    """
    with db.get_connection() as conn:
        stops_df = conn.execute(
            "SELECT stop_id, ST_AsText(geom_utm) AS wkt FROM stops;"
        ).df()

    if stops_df.empty:
        context.log.warning("No stops found in MotherDuck — returning empty isochrones")
        return gpd.GeoDataFrame(columns=["stop_id", "geometry"], geometry="geometry",
                                crs="EPSG:32634")

    from shapely import wkt as shapely_wkt
    stops_df["geometry"] = stops_df["wkt"].apply(shapely_wkt.loads)
    gdf = gpd.GeoDataFrame(stops_df[["stop_id", "geometry"]], geometry="geometry",
                           crs="EPSG:32634")

    gdf["geometry"] = gdf.geometry.buffer(ISOCHRONE_RADIUS_M)
    context.log.info(f"Generated {len(gdf)} isochrone buffers ({ISOCHRONE_RADIUS_M}m radius)")
    return gdf


# ---------------------------------------------------------------------------
# 2.2  Spatial join: demand onto neighbourhoods
# ---------------------------------------------------------------------------

@asset(
    group_name="analysis",
    description="Load neighbourhood polygons from MotherDuck as a GeoDataFrame (EPSG:32634).",
)
def neighbourhood_geodataframe(
    context: AssetExecutionContext,
    db: MotherDuckResource,
) -> gpd.GeoDataFrame:
    """Fetch neighbourhood polygons and reproject to UTM 34N for area calculations."""
    with db.get_connection() as conn:
        df = conn.execute(
            "SELECT neighbourhood_id, name, ST_AsText(geom) AS wkt FROM neighbourhoods;"
        ).df()

    if df.empty:
        raise RuntimeError("No neighbourhoods found in MotherDuck — run Phase 1 first.")

    from shapely import wkt as shapely_wkt
    df["geometry"] = df["wkt"].apply(shapely_wkt.loads)
    gdf = gpd.GeoDataFrame(df[["neighbourhood_id", "name", "geometry"]],
                           geometry="geometry", crs="EPSG:4326")
    gdf = gdf.to_crs("EPSG:32634")
    context.log.info(f"Loaded {len(gdf)} neighbourhood polygons (reprojected to EPSG:32634)")
    return gdf


@asset(
    group_name="analysis",
    description=(
        "Spatially join OSM POI counts onto neighbourhood polygons to produce "
        "proxy business_count and student_count per neighbourhood."
    ),
)
def neighbourhood_demand(
    context: AssetExecutionContext,
    neighbourhood_geodataframe: gpd.GeoDataFrame,
    db: MotherDuckResource,
) -> pd.DataFrame:
    """Approximate demand per neighbourhood using OSM amenity/POI density.

    Since QKB business data is aggregated at region level (whole Tirana),
    we use OSM amenity point counts spatially joined to each neighbourhood
    polygon as a proxy for business and education demand.

    Returns a DataFrame with columns:
        neighbourhood_id, business_count, student_count
    """
    try:
        import osmnx as ox
    except ImportError:
        raise ImportError("osmnx is required for neighbourhood_demand. Install it via pip.")

    context.log.info("Fetching OSM amenities for Tirana...")

    # Fetch amenity POIs within Tirana bounding box
    amenity_tags = {"amenity": True}
    try:
        pois = ox.features_from_place("Tirana, Albania", tags=amenity_tags)
        pois = pois[pois.geometry.geom_type == "Point"].copy()
        pois = pois[["geometry", "amenity"]].dropna(subset=["geometry"])
        pois = pois.set_crs("EPSG:4326", allow_override=True).to_crs("EPSG:32634")
        context.log.info(f"Fetched {len(pois)} OSM amenity POIs")
    except Exception as exc:
        context.log.warning(f"OSM amenity fetch failed ({exc}) — using zero demand fallback")
        result = neighbourhood_geodataframe[["neighbourhood_id"]].copy()
        result["business_count"] = 0.0
        result["student_count"] = 0.0
        return result

    # Spatially join POIs to neighbourhoods
    joined = gpd.sjoin(
        pois.reset_index(drop=True),
        neighbourhood_geodataframe[["neighbourhood_id", "geometry"]],
        how="inner",
        predicate="within",
    )

    # Education amenities proxy student demand
    education_types = {"school", "university", "college", "kindergarten", "language_school"}
    joined["is_education"] = joined["amenity"].isin(education_types)

    business_counts = (
        joined.groupby("neighbourhood_id").size().rename("business_count").reset_index()
    )
    student_counts = (
        joined[joined["is_education"]]
        .groupby("neighbourhood_id").size().rename("student_count").reset_index()
    )

    result = neighbourhood_geodataframe[["neighbourhood_id"]].merge(
        business_counts, on="neighbourhood_id", how="left"
    ).merge(
        student_counts, on="neighbourhood_id", how="left"
    ).fillna(0.0)

    context.log.info(
        f"Demand computed for {len(result)} neighbourhoods — "
        f"total POIs: {int(result['business_count'].sum())}, "
        f"education POIs: {int(result['student_count'].sum())}"
    )
    return result


# ---------------------------------------------------------------------------
# 2.3 + 2.4  Coverage ratio & gap score
# ---------------------------------------------------------------------------

@asset(
    group_name="analysis",
    description="Compute coverage ratio and gap score per neighbourhood; persist to MotherDuck.",
)
def coverage_scores(
    context: AssetExecutionContext,
    stop_isochrones: gpd.GeoDataFrame,
    neighbourhood_geodataframe: gpd.GeoDataFrame,
    neighbourhood_demand: pd.DataFrame,
    db: MotherDuckResource,
) -> gpd.GeoDataFrame:
    """Compute coverage_ratio and gap_score for each neighbourhood.

    coverage_ratio = ST_Area(intersection(neighbourhood, isochrone_union))
                     / ST_Area(neighbourhood)

    gap_score = (norm_business_density + norm_student_density)
                / (coverage_ratio + EPSILON)

    Persists results to MotherDuck `coverage_scores` table using
    the same atomic staging-swap pattern as Phase 1 storage assets.

    Returns a GeoDataFrame (neighbourhood polygons + scores) for GeoJSON export.
    """
    nb = neighbourhood_geodataframe.copy()

    # Guard: skip neighbourhoods with null/zero-area geometries
    nb = nb[nb.geometry.notna() & (nb.geometry.area > 0)].copy()
    if nb.empty:
        raise RuntimeError("All neighbourhood geometries are null or zero-area.")

    # Build union of all isochrones (the total 'served area')
    if stop_isochrones.empty:
        context.log.warning("No isochrones — coverage_ratio will be 0 for all neighbourhoods")
        isochrone_union = None
    else:
        isochrone_union = unary_union(stop_isochrones.geometry)
        context.log.info(
            f"Isochrone union area: {isochrone_union.area / 1e6:.2f} km²"
        )

    # 2.3 Coverage ratio
    def _coverage_ratio(neighbourhood_geom):
        if isochrone_union is None:
            return 0.0
        try:
            intersection = neighbourhood_geom.intersection(isochrone_union)
            return intersection.area / neighbourhood_geom.area
        except Exception:
            return 0.0

    nb["coverage_ratio"] = nb.geometry.apply(_coverage_ratio).clip(0.0, 1.0)

    # 2.4 Normalise demand + gap score
    nb = nb.merge(neighbourhood_demand, on="neighbourhood_id", how="left")
    nb["business_count"] = nb["business_count"].fillna(0.0)
    nb["student_count"] = nb["student_count"].fillna(0.0)

    nb_area_km2 = nb.geometry.area / 1e6  # km²
    nb["business_density"] = nb["business_count"] / nb_area_km2.replace(0, float("nan"))
    nb["student_density"] = nb["student_count"] / nb_area_km2.replace(0, float("nan"))
    nb["business_density"] = nb["business_density"].fillna(0.0)
    nb["student_density"] = nb["student_density"].fillna(0.0)

    def _minmax(series: pd.Series) -> pd.Series:
        rng = series.max() - series.min()
        return (series - series.min()) / rng if rng > 0 else pd.Series(0.0, index=series.index)

    nb["norm_business"] = _minmax(nb["business_density"])
    nb["norm_student"] = _minmax(nb["student_density"])
    nb["gap_score"] = (nb["norm_business"] + nb["norm_student"]) / (
        nb["coverage_ratio"] + EPSILON
    )
    nb = nb.sort_values("gap_score", ascending=False).reset_index(drop=True)

    context.log.info(
        f"Gap scores computed — top neighbourhood: "
        f"{nb.iloc[0]['name']} (gap_score={nb.iloc[0]['gap_score']:.3f})"
    )

    # Persist to MotherDuck using staging-swap
    records = nb[[
        "neighbourhood_id", "business_density", "student_density",
        "coverage_ratio", "gap_score",
    ]].copy()

    with db.get_connection() as conn:
        # Ensure table exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS coverage_scores (
                neighbourhood_id  VARCHAR PRIMARY KEY,
                business_density  DOUBLE,
                student_density   DOUBLE,
                coverage_ratio    DOUBLE,
                gap_score         DOUBLE,
                computed_at       TIMESTAMPTZ DEFAULT now()
            );
        """)

        conn.execute(
            "CREATE OR REPLACE TABLE coverage_scores_staging "
            "AS SELECT * FROM coverage_scores WHERE 1=0;"
        )
        conn.register("_cs_staging", records)
        conn.execute("""
            INSERT INTO coverage_scores_staging
                (neighbourhood_id, business_density, student_density, coverage_ratio, gap_score)
            SELECT neighbourhood_id, business_density, student_density, coverage_ratio, gap_score
            FROM _cs_staging;
        """)
        conn.unregister("_cs_staging")

        conn.execute("BEGIN;")
        try:
            conn.execute("DROP TABLE coverage_scores;")
            conn.execute("ALTER TABLE coverage_scores_staging RENAME TO coverage_scores;")
            conn.execute("COMMIT;")
        except Exception:
            conn.execute("ROLLBACK;")
            conn.execute("DROP TABLE IF EXISTS coverage_scores_staging;")
            raise

    context.log.info(f"Persisted {len(records)} coverage scores to MotherDuck")
    return nb


# ---------------------------------------------------------------------------
# 2.5  GeoJSON export
# ---------------------------------------------------------------------------

@asset(
    group_name="analysis",
    description="Export ranked gap scores as GeoJSON for Phase 3 (FastAPI + Leaflet).",
    deps=["coverage_scores"],
)
def gap_scores_geojson(
    context: AssetExecutionContext,
    coverage_scores: gpd.GeoDataFrame,
) -> None:
    """Write gap_scores.geojson to data/processed/ for consumption by Phase 3.

    Output columns: neighbourhood_id, name, coverage_ratio, gap_score,
    business_density, student_density, geometry (EPSG:4326 for web maps).
    """
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    output_path = PROCESSED_DIR / "gap_scores.geojson"

    export = coverage_scores[[
        "neighbourhood_id", "name", "coverage_ratio", "gap_score",
        "business_density", "student_density", "geometry",
    ]].copy()

    # Reproject back to WGS84 for web map consumption
    export = export.to_crs("EPSG:4326")

    export.to_file(output_path, driver="GeoJSON")
    context.log.info(f"Exported {len(export)} neighbourhoods to {output_path}")
