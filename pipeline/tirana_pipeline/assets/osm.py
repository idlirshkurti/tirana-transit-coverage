"""OSM ingestion asset — fetches Tirana neighbourhood boundaries via Overpass / osmnx."""

import geopandas as gpd
from dagster import AssetExecutionContext, asset
from shapely.geometry import MultiPolygon

from tirana_pipeline.resources import MotherDuckResource


@asset(group_name="ingestion", description="Fetch Tirana neighbourhood boundaries from OSM")
def tirana_neighbourhoods(context: AssetExecutionContext) -> gpd.GeoDataFrame:
    """
    Use osmnx to pull admin level 9/10 boundaries within Tirana municipality.
    Falls back to level 10 if level 9 returns no results.
    """
    import osmnx as ox

    context.log.info("Fetching Tirana admin boundaries from OSM")

    tags = {"boundary": "administrative", "admin_level": "9"}
    try:
        gdf = ox.features_from_place("Tirana, Albania", tags=tags)
        gdf = gdf[["geometry", "name"]].dropna(subset=["geometry", "name"])
        gdf = gdf[gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"])].copy()
        context.log.info(f"Found {len(gdf)} admin_level=9 boundaries")
    except Exception as e:
        context.log.warning(f"admin_level=9 query failed ({e}), trying level 10")
        tags["admin_level"] = "10"
        gdf = ox.features_from_place("Tirana, Albania", tags=tags)
        gdf = gdf[["geometry", "name"]].dropna(subset=["geometry", "name"])
        gdf = gdf[gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"])].copy()

    gdf = gdf.reset_index(drop=True)
    gdf["neighbourhood_id"] = gdf.index.astype(str)
    gdf = gdf.set_crs("EPSG:4326")

    context.log.info(f"Returning {len(gdf)} neighbourhood polygons")
    return gdf


@asset(
    group_name="storage",
    description="Write neighbourhood boundaries to MotherDuck (DuckDB spatial)",
)
def neighbourhoods_to_motherduck(
    context: AssetExecutionContext,
    tirana_neighbourhoods: gpd.GeoDataFrame,
    db: MotherDuckResource,
) -> None:
    """Persist neighbourhood polygons to MotherDuck `neighbourhoods` table."""
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
            VALUES (?, ?, ST_GeomFromText(?, 4326))
            ON CONFLICT (neighbourhood_id) DO UPDATE SET
                name = excluded.name,
                geom = excluded.geom
            """,
            rows,
        )

    context.log.info(f"Upserted {len(rows)} neighbourhoods into MotherDuck")
