"""Phase 2: Geospatial analysis assets — isochrones, demand, coverage, gap scores.

All distance/area calculations are performed in EPSG:32634 (UTM zone 34N).
Final gap_scores.geojson is written to data/processed/ for Phase 3 consumption.

Asset dependency graph (all in group "analysis"):

    stops (MotherDuck)
        └─> walking_isochrones          (isochrones table + UTM union)
                └─> coverage_ratios     (coverage_ratio per neighbourhood)
                        └─> gap_scores  (ranked gap_score, written to MotherDuck + GeoJSON)

    neighbourhoods (MotherDuck)
        └─> neighbourhood_demand        (business/student counts per unit)
                └─> gap_scores
"""

from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
from dagster import AssetExecutionContext, asset
from shapely.geometry import mapping
from shapely.ops import unary_union

from tirana_pipeline.resources import MotherDuckResource

# ε prevents division-by-zero for completely unserved neighbourhoods
_EPSILON = 0.01
_ISOCHRONE_RADIUS_M = 400
_OUTPUT_DIR = Path("data/processed")


# ---------------------------------------------------------------------------
# 2.1  Walking isochrones
# ---------------------------------------------------------------------------

@asset(
    group_name="analysis",
    description=(
        f"Generate {_ISOCHRONE_RADIUS_M} m walking-buffer isochrones around every "
        "bus stop and persist to MotherDuck `isochrones` table."
    ),
)
def walking_isochrones(
    context: AssetExecutionContext,
    db: MotherDuckResource,
) -> gpd.GeoDataFrame:
    """Build 400 m Shapely buffer isochrones in UTM 34N around each stop.

    Steps:
    1. Read stops from MotherDuck (geom_utm already in EPSG:32634).
    2. Buffer each point by 400 m -> Polygon in UTM space.
    3. Persist to MotherDuck `isochrones` table (WKT, stored in UTM).
    4. Return a GeoDataFrame of all isochrone polygons (EPSG:32634).
    """
    with db.get_connection() as conn:
        stops_df = conn.execute(
            "SELECT stop_id, ST_AsText(geom_utm) AS wkt_utm FROM stops"
        ).df()

    if stops_df.empty:
        context.log.warning("No stops found in MotherDuck — skipping isochrone generation")
        return gpd.GeoDataFrame(
            columns=["stop_id", "radius_m", "geometry"], geometry="geometry", crs="EPSG:32634"
        )

    from shapely import wkt as shapely_wkt

    stops_gdf = gpd.GeoDataFrame(
        stops_df,
        geometry=stops_df["wkt_utm"].apply(shapely_wkt.loads),
        crs="EPSG:32634",
    )

    stops_gdf["geometry"] = stops_gdf.geometry.buffer(_ISOCHRONE_RADIUS_M)
    stops_gdf["radius_m"] = _ISOCHRONE_RADIUS_M
    isochrones_gdf = stops_gdf[["stop_id", "radius_m", "geometry"]].copy()

    context.log.info(
        f"Generated {len(isochrones_gdf)} isochrones "
        f"(radius={_ISOCHRONE_RADIUS_M} m, CRS=EPSG:32634)"
    )

    # Persist to MotherDuck using staging-swap for atomicity
    staging = pd.DataFrame({
        "stop_id":  isochrones_gdf["stop_id"],
        "radius_m": isochrones_gdf["radius_m"],
        "wkt":      isochrones_gdf["geometry"].apply(lambda g: g.wkt),
    })

    with db.get_connection() as conn:
        conn.execute(
            "CREATE OR REPLACE TABLE isochrones_staging "
            "AS SELECT * FROM isochrones WHERE 1=0;"
        )
        conn.register("_iso_staging", staging)
        conn.execute("""
            INSERT INTO isochrones_staging (stop_id, radius_m, geom)
            SELECT stop_id, radius_m, ST_GeomFromText(wkt)
            FROM _iso_staging
        """)
        conn.unregister("_iso_staging")
        conn.execute("BEGIN;")
        try:
            conn.execute("DELETE FROM isochrones;")
            conn.execute(
                "INSERT INTO isochrones SELECT * FROM isochrones_staging;"
            )
            conn.execute("COMMIT;")
        except Exception:
            conn.execute("ROLLBACK;")
            raise
        finally:
            conn.execute("DROP TABLE IF EXISTS isochrones_staging;")

    context.log.info(f"Persisted {len(staging)} isochrone rows to MotherDuck")
    return isochrones_gdf


# ---------------------------------------------------------------------------
# 2.2  Neighbourhood demand (business + student proxies)
# ---------------------------------------------------------------------------

@asset(
    group_name="analysis",
    description=(
        "Spatially disaggregate OSM POI density onto the 11 Municipal Unit polygons "
        "to derive business and student demand proxies."
    ),
)
def neighbourhood_demand(
    context: AssetExecutionContext,
    db: MotherDuckResource,
) -> gpd.GeoDataFrame:
    """Proxy demand per neighbourhood using OSM amenity POI counts.

    Region-level business data from opendata.gov.al is too coarse for the 11
    Municipal Units (see opendata.py module note). Instead we use OSM amenity
    POI counts as a business-activity proxy and OSM education POI counts as a
    student-activity proxy — both spatially joined directly to neighbourhood
    polygons, so no disaggregation approximation is needed.

    Falls back to uniform counts (1.0 per neighbourhood) if the OSM query
    returns no results, so downstream assets always have non-null demand.
    """
    import osmnx as ox

    with db.get_connection() as conn:
        nb_df = conn.execute(
            "SELECT neighbourhood_id, name, ST_AsText(geom) AS wkt FROM neighbourhoods"
        ).df()

    if nb_df.empty:
        context.log.warning("No neighbourhoods in MotherDuck — returning empty demand frame")
        return gpd.GeoDataFrame(
            columns=["neighbourhood_id", "name", "business_count", "student_count", "geometry"],
            geometry="geometry",
            crs="EPSG:4326",
        )

    from shapely import wkt as shapely_wkt

    nb_gdf = gpd.GeoDataFrame(
        nb_df,
        geometry=nb_df["wkt"].apply(shapely_wkt.loads),
        crs="EPSG:4326",
    )

    # Fetch OSM amenity POIs within the Tirana bounding box
    tirana_union = nb_gdf.geometry.unary_union
    minx, miny, maxx, maxy = tirana_union.bounds

    def _fetch_pois(tags: dict, label: str) -> gpd.GeoDataFrame:
        try:
            pois = ox.features_from_bbox(
                bbox=(miny, maxy, minx, maxx), tags=tags
            )
            pois = pois[pois.geometry.geom_type.isin(["Point", "Polygon", "MultiPolygon"])].copy()
            # Use centroid for area geometries so sjoin works cleanly
            pois["geometry"] = pois.geometry.centroid
            pois = pois.set_crs("EPSG:4326", allow_override=True)
            context.log.info(f"Fetched {len(pois)} OSM {label} POIs")
            return pois[["geometry"]].reset_index(drop=True)
        except Exception as exc:
            context.log.warning(f"OSM {label} fetch failed ({exc}) — using empty GeoDataFrame")
            return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs="EPSG:4326")

    business_pois = _fetch_pois(
        {"amenity": ["restaurant", "cafe", "bank", "pharmacy", "shop", "marketplace"]},
        "business",
    )
    education_pois = _fetch_pois(
        {"amenity": ["school", "university", "college", "kindergarten"]},
        "education",
    )

    def _count_pois_per_neighbourhood(
        nb: gpd.GeoDataFrame, pois: gpd.GeoDataFrame, col: str
    ) -> gpd.GeoDataFrame:
        if pois.empty:
            nb[col] = 1.0
            return nb
        joined = gpd.sjoin(pois, nb[["neighbourhood_id", "geometry"]], how="left", predicate="within")
        counts = joined.groupby("neighbourhood_id").size().rename(col)
        nb = nb.merge(counts, on="neighbourhood_id", how="left")
        nb[col] = nb[col].fillna(1.0)  # fallback: at least 1.0 so no zero demand
        return nb

    nb_gdf = _count_pois_per_neighbourhood(nb_gdf, business_pois, "business_count")
    nb_gdf = _count_pois_per_neighbourhood(nb_gdf, education_pois, "student_count")

    context.log.info(
        f"Demand computed for {len(nb_gdf)} neighbourhoods — "
        f"business_count sum={nb_gdf['business_count'].sum():.0f}, "
        f"student_count sum={nb_gdf['student_count'].sum():.0f}"
    )
    return nb_gdf[["neighbourhood_id", "name", "business_count", "student_count", "geometry"]]


# ---------------------------------------------------------------------------
# 2.3  Coverage ratio
# ---------------------------------------------------------------------------

@asset(
    group_name="analysis",
    description="Compute the fraction of each neighbourhood's area covered by the isochrone union.",
)
def coverage_ratios(
    context: AssetExecutionContext,
    walking_isochrones: gpd.GeoDataFrame,
    neighbourhood_demand: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Compute coverage_ratio = ST_Area(intersection) / ST_Area(neighbourhood) per polygon.

    Both layers are projected to EPSG:32634 (UTM 34N) for accurate area calculation.
    Edge cases handled:
    - Zero-area neighbourhood -> coverage_ratio = 0.0
    - Null/empty geometries   -> coverage_ratio = 0.0
    - No isochrones at all    -> coverage_ratio = 0.0 for all
    """
    nb_gdf = neighbourhood_demand.copy()

    if nb_gdf.empty:
        nb_gdf["coverage_ratio"] = 0.0
        return nb_gdf

    # Project both layers to UTM 34N for metric area calculations
    nb_utm = nb_gdf.to_crs("EPSG:32634")

    if walking_isochrones.empty:
        context.log.warning("No isochrones — coverage_ratio will be 0.0 for all neighbourhoods")
        nb_gdf["coverage_ratio"] = 0.0
        return nb_gdf

    # Build the union of all isochrone polygons once
    iso_union = unary_union(walking_isochrones.geometry)
    context.log.info(f"Isochrone union area: {iso_union.area / 1e6:.2f} km²")

    def _coverage(nb_geom):
        if nb_geom is None or nb_geom.is_empty:
            return 0.0
        area = nb_geom.area
        if area == 0:
            return 0.0
        intersection = nb_geom.intersection(iso_union)
        return min(intersection.area / area, 1.0)  # cap at 1.0 for floating-point safety

    nb_utm["coverage_ratio"] = nb_utm.geometry.apply(_coverage)
    nb_gdf["coverage_ratio"] = nb_utm["coverage_ratio"].values

    context.log.info(
        f"Coverage ratios computed — mean={nb_gdf['coverage_ratio'].mean():.3f}, "
        f"min={nb_gdf['coverage_ratio'].min():.3f}, "
        f"max={nb_gdf['coverage_ratio'].max():.3f}"
    )
    return nb_gdf


# ---------------------------------------------------------------------------
# 2.4  Gap score + persistence
# ---------------------------------------------------------------------------

@asset(
    group_name="analysis",
    description=(
        "Compute normalised gap scores per neighbourhood and persist to "
        "MotherDuck `coverage_scores` + data/processed/gap_scores.geojson."
    ),
)
def gap_scores(
    context: AssetExecutionContext,
    coverage_ratios: gpd.GeoDataFrame,
    db: MotherDuckResource,
) -> gpd.GeoDataFrame:
    """Compute and persist gap scores.

    gap_score = (norm_business_density + norm_student_density) / (coverage_ratio + ε)

    Normalisation: min-max per column so both density terms are on [0, 1].
    Neighbourhoods are ranked by gap_score descending (rank 1 = most underserved).
    """
    gdf = coverage_ratios.copy()

    if gdf.empty:
        context.log.warning("coverage_ratios is empty — no gap scores to compute")
        return gdf

    def _minmax(series: pd.Series) -> pd.Series:
        lo, hi = series.min(), series.max()
        if hi == lo:
            return pd.Series([0.5] * len(series), index=series.index)
        return (series - lo) / (hi - lo)

    gdf["norm_business"] = _minmax(gdf["business_count"])
    gdf["norm_student"] = _minmax(gdf["student_count"])
    gdf["gap_score"] = (
        (gdf["norm_business"] + gdf["norm_student"]) / (gdf["coverage_ratio"] + _EPSILON)
    )
    gdf = gdf.sort_values("gap_score", ascending=False).reset_index(drop=True)
    gdf["rank"] = gdf.index + 1

    context.log.info(
        f"Gap scores computed for {len(gdf)} neighbourhoods — "
        f"top underserved: {gdf.iloc[0]['name']} (score={gdf.iloc[0]['gap_score']:.3f})"
    )

    # ── Persist to MotherDuck coverage_scores (atomic swap) ──────────────────
    scores_df = gdf[[
        "neighbourhood_id", "norm_business", "norm_student", "coverage_ratio", "gap_score"
    ]].rename(columns={"norm_business": "business_density", "norm_student": "student_density"})

    with db.get_connection() as conn:
        conn.register("_scores", scores_df)
        conn.execute("BEGIN;")
        try:
            conn.execute("DELETE FROM coverage_scores;")
            conn.execute("""
                INSERT INTO coverage_scores
                    (neighbourhood_id, business_density, student_density,
                     coverage_ratio, gap_score, computed_at)
                SELECT
                    neighbourhood_id,
                    business_density,
                    student_density,
                    coverage_ratio,
                    gap_score,
                    now()
                FROM _scores
            """)
            conn.execute("COMMIT;")
        except Exception:
            conn.execute("ROLLBACK;")
            raise
        finally:
            conn.unregister("_scores")

    context.log.info(f"Persisted {len(scores_df)} gap score rows to MotherDuck coverage_scores")

    # ── Write GeoJSON for Phase 3 ─────────────────────────────────────────────
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = _OUTPUT_DIR / "gap_scores.geojson"

    geojson_gdf = gdf[[
        "neighbourhood_id", "name",
        "business_count", "student_count",
        "coverage_ratio", "gap_score", "rank",
        "geometry",
    ]].copy()
    # Ensure WGS84 for GeoJSON spec compliance
    if geojson_gdf.crs and geojson_gdf.crs.to_epsg() != 4326:
        geojson_gdf = geojson_gdf.to_crs("EPSG:4326")

    geojson_gdf.to_file(out_path, driver="GeoJSON")
    context.log.info(f"GeoJSON written to {out_path} ({out_path.stat().st_size / 1024:.1f} KB)")

    return gdf
