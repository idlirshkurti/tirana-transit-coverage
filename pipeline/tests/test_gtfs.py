"""Unit tests for GTFS parsing assets."""

import io
import zipfile

import pandas as pd
import pytest
from dagster import build_asset_context

from tirana_pipeline.assets.gtfs import gtfs_routes, gtfs_stops


def _make_gtfs_zip(stops_csv: str, routes_csv: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("stops.txt", stops_csv)
        zf.writestr("routes.txt", routes_csv)
    return buf.getvalue()


STOPS_CSV = """stop_id,stop_name,stop_lat,stop_lon
1,Sheshi Skënderbej,41.3275,19.8187
2,Blloku,41.3200,19.8150
3,Kombinat,41.3100,19.8100
"""

ROUTES_CSV = """route_id,route_short_name,route_long_name,route_type
101,1,Kombinat - Sheshi,3
102,2,Blloku - Kombinat,3
"""


def test_gtfs_stops_returns_geodataframe() -> None:
    raw = _make_gtfs_zip(STOPS_CSV, ROUTES_CSV)
    with build_asset_context() as ctx:
        gdf = gtfs_stops(ctx, raw)
    assert len(gdf) == 3
    assert gdf.crs.to_epsg() == 4326
    assert "geometry" in gdf.columns


def test_gtfs_stops_geometry_points() -> None:
    raw = _make_gtfs_zip(STOPS_CSV, ROUTES_CSV)
    with build_asset_context() as ctx:
        gdf = gtfs_stops(ctx, raw)
    assert all(gdf.geometry.geom_type == "Point")


def test_gtfs_routes_returns_dataframe() -> None:
    raw = _make_gtfs_zip(STOPS_CSV, ROUTES_CSV)
    with build_asset_context() as ctx:
        df = gtfs_routes(ctx, raw)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert "route_id" in df.columns


def test_gtfs_stops_missing_columns_raises() -> None:
    bad_csv = "stop_id,stop_name\n1,Sheshi\n"
    raw = _make_gtfs_zip(bad_csv, ROUTES_CSV)
    with build_asset_context() as ctx:
        with pytest.raises(ValueError, match="missing columns"):
            gtfs_stops(ctx, raw)
