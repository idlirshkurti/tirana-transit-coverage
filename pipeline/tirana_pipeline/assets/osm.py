"""OSM ingestion asset — fetches Tirana Municipal Unit boundaries via osmnx.

Scope: Tirana's 11 Municipal Units ("Njësia Bashkiake Nr. 1-11") only.
Outer communes (Kashar, Dajt, Farkë, etc.) and Administrative Sub-units
(Bathore, Paskuqan, etc.) are excluded — they have near-zero bus service
and fall outside the transit equity study area.
"""

import geopandas as gpd
import pandas as pd
from dagster import AssetExecutionContext, asset
from shapely.geometry import MultiPolygon

from tirana_pipeline.resources import MotherDuckResource

# Only features whose OSM name starts with this prefix are kept.
MUNICIPAL_UNIT_PREFIX = "Njësia Bashkiake"
EXPECTED_UNIT_COUNT = 11


@asset(
    group_name="ingestion",
    description=(
        "Fetch the 11 Tirana Municipal Unit boundaries from OSM "
        "(Njësia Bashkiake Nr. 1-11, admin_level=9)."
    ),
)
def tirana_neighbourhoods(context: AssetExecutionContext) -> gpd.GeoDataFrame:
    """
    Pull admin_level=9 boundaries for Tirana from OSM and keep only the
    11 Municipal Units (Njësia Bashkiake).  Outer communes (Kashar, Dajt,
    Farkë, Kamëz, Vaqarr …) and Administrative Sub-units (Bathore,
    Paskuqan …) are filtered out — they have negligible bus coverage and
    are outside the project's study area.
    """
    import osmnx as ox

    context.log.info("Fetching Tirana admin_level=9 boundaries from OSM")

    tags = {"boundary": "administrative", "admin_level": "9"}
    try:
        gdf = ox.features_from_place("Tirana, Albania", tags=tags)
        gdf = gdf[["geometry", "name"]].dropna(subset=["geometry", "name"])
        gdf = gdf[gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"])].copy()
        context.log.info(f"Raw OSM result: {len(gdf)} admin_level=9 features")
    except Exception as e:
        context.log.warning(f"admin_level=9 query failed ({e}), trying level 10")
        tags["admin_level"] = "10"
        gdf = ox.features_from_place("Tirana, Albania", tags=tags)
        gdf = gdf[["geometry", "name"]].dropna(subset=["geometry", "name"])
        gdf = gdf[gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"])].copy()

    # ── Filter to Municipal Units only ────────────────────────────────────────────
    gdf = gdf[gdf["name"].str.startswith(MUNICIPAL_UNIT_PREFIX)].copy()
    gdf = gdf.reset_index(drop=True)
    gdf["neighbourhood_id"] = (gdf.index + 1).astype(str)
    gdf = gdf.set_crs("EPSG:4326", allow_override=True)

    n = len(gdf)
    context.log.info(
        f"Filtered to {n} Municipal Units "
        f"(expected {EXPECTED_UNIT_COUNT})"
    )
    if n != EXPECTED_UNIT_COUNT:
        context.log.warning(
            f"Expected {EXPECTED_UNIT_COUNT} municipal units but got {n}. "
            "OSM data may have changed — verify manually."
        )

    return gdf


@asset(
    group_name="storage",
    description="Write the 11 Municipal Unit boundaries to MotherDuck (DuckDB spatial)",
)
def neighbourhoods_to_motherduck(
    context: AssetExecutionContext,
    tirana_neighbourhoods: gpd.GeoDataFrame,
    db: MotherDuckResource,
) -> None:
    """Persist Municipal Unit polygons to MotherDuck `neighbourhoods` table.

    Uses a staging-swap pattern for atomic writes:
    1. Write all new rows into `neighbourhoods_staging` (schema clone).
    2. Inside a single transaction: DROP live table, RENAME staging -> live.
    3. On any failure: ROLLBACK and clean up staging — previous data is preserved.
    """
    # Normalise to MultiPolygon and collect WKT strings.
    records = []
    for _, row in tirana_neighbourhoods.iterrows():
        geom = row.geometry
        if geom.geom_type == "Polygon":
            geom = MultiPolygon([geom])
        records.append({
            "neighbourhood_id": str(row.neighbourhood_id),
            "name":             str(row["name"]),
            "wkt":              geom.wkt,
        })

    staging = pd.DataFrame(records)

    # DuckDB's executemany() fails to bind ST_GeomFromText(?) when mixed
    # parameter types are present in the same tuple. Fix: register a
    # DataFrame and cast WKT -> GEOMETRY inside a SELECT.
    with db.get_connection() as conn:
        # Create empty staging table with same schema as neighbourhoods
        conn.execute(
            "CREATE OR REPLACE TABLE neighbourhoods_staging "
            "AS SELECT * FROM neighbourhoods WHERE 1=0;"
        )
        conn.register("_nb_staging", staging)
        conn.execute("""
            INSERT INTO neighbourhoods_staging (neighbourhood_id, name, geom)
            SELECT
                neighbourhood_id,
                name,
                ST_GeomFromText(wkt)
            FROM _nb_staging
        """)
        conn.unregister("_nb_staging")

        # Atomic swap: live table is never empty at any point
        conn.execute("BEGIN;")
        try:
            conn.execute("DROP TABLE neighbourhoods;")
            conn.execute("ALTER TABLE neighbourhoods_staging RENAME TO neighbourhoods;")
            conn.execute("COMMIT;")
        except Exception:
            conn.execute("ROLLBACK;")
            conn.execute("DROP TABLE IF EXISTS neighbourhoods_staging;")
            raise

    context.log.info(f"Atomically swapped {len(staging)} Municipal Unit polygons into MotherDuck")
