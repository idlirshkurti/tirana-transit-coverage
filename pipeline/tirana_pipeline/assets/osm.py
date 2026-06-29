"""OSM ingestion asset — fetches Tirana Municipal Unit boundaries via osmnx.

Scope: Tirana's 11 Municipal Units ("Njësia Bashkiake Nr. 1-11") only.
Outer communes (Kashar, Dajt, Farkë, etc.) and Administrative Sub-units
(Bathore, Paskuqan, etc.) are excluded — they have near-zero bus service
and fall outside the transit equity study area.
"""

import geopandas as gpd
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

    # ── Filter to Municipal Units only ───────────────────────────────────────
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
    """Persist Municipal Unit polygons to MotherDuck `neighbourhoods` table."""
    rows = []
    for _, row in tirana_neighbourhoods.iterrows():
        geom = row.geometry
        if geom.geom_type == "Polygon":
            geom = MultiPolygon([geom])
        rows.append((
            str(row.neighbourhood_id),
            str(row["name"]),
            geom.wkt,
        ))

    with db.get_connection() as conn:
        conn.execute("DELETE FROM neighbourhoods;")
        conn.executemany(
            """
            INSERT INTO neighbourhoods (neighbourhood_id, name, geom)
            VALUES (
                ?,
                ?,
                ST_SetSRID(ST_GeomFromText(?), 4326)
            )
            ON CONFLICT (neighbourhood_id) DO UPDATE SET
                name = excluded.name,
                geom = excluded.geom
            """,
            rows,
        )

    context.log.info(f"Upserted {len(rows)} Municipal Unit polygons into MotherDuck")
