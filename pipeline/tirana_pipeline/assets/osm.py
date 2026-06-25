"""OSM ingestion asset — fetches Tirana neighbourhood boundaries via Overpass / osmnx."""

import geopandas as gpd
from dagster import AssetExecutionContext, asset
from shapely.geometry import MultiPolygon
from sqlalchemy import text

from tirana_pipeline.resources import DatabaseResource


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
    description="Write neighbourhood boundaries to PostGIS",
)
def neighbourhoods_to_postgis(
    context: AssetExecutionContext,
    tirana_neighbourhoods: gpd.GeoDataFrame,
    db: DatabaseResource,
) -> None:
    """Persist neighbourhood polygons to PostGIS `neighbourhoods` table."""
    engine = db.get_engine()

    with engine.begin() as conn:
        conn.execute(
            text("""
                CREATE TABLE IF NOT EXISTS neighbourhoods (
                    neighbourhood_id  TEXT PRIMARY KEY,
                    name              TEXT,
                    geom              GEOMETRY(MultiPolygon, 4326)
                );
                CREATE INDEX IF NOT EXISTS neighbourhoods_geom_idx
                    ON neighbourhoods USING GIST (geom);
            """)
        )

    rows = []
    for _, row in tirana_neighbourhoods.iterrows():
        geom = row.geometry
        if geom.geom_type == "Polygon":
            geom = MultiPolygon([geom])
        rows.append({
            "neighbourhood_id": row.neighbourhood_id,
            "name": row["name"],
            "geom": geom.wkt,
        })

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO neighbourhoods (neighbourhood_id, name, geom)
                VALUES (
                    :neighbourhood_id,
                    :name,
                    ST_Multi(ST_GeomFromText(:geom, 4326))
                )
                ON CONFLICT (neighbourhood_id) DO UPDATE
                    SET name = EXCLUDED.name,
                        geom = EXCLUDED.geom;
            """),
            rows,
        )

    context.log.info(f"Upserted {len(rows)} neighbourhoods into PostGIS")
