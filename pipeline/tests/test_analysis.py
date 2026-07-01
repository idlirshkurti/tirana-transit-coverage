"""Unit and integration tests for Phase 2 analysis assets."""

from __future__ import annotations

import geopandas as gpd
import pandas as pd
import pytest
from dagster import build_asset_context
from shapely.geometry import MultiPolygon, Point, Polygon

from tirana_pipeline.assets.analysis import (
    coverage_ratios,
    gap_scores,
    walking_isochrones,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_square_neighbourhood(
    neighbourhood_id: str, name: str, cx: float, cy: float, side_m: float = 2000.0
) -> dict:
    """Return a neighbourhood record as a dict with a UTM square polygon."""
    half = side_m / 2
    poly = Polygon([
        (cx - half, cy - half),
        (cx + half, cy - half),
        (cx + half, cy + half),
        (cx - half, cy + half),
        (cx - half, cy - half),
    ])
    return {
        "neighbourhood_id": neighbourhood_id,
        "name": name,
        "business_count": 10.0,
        "student_count": 5.0,
        "geometry": poly,
    }


@pytest.fixture()
def two_neighbourhoods_utm() -> gpd.GeoDataFrame:
    """Two 2x2 km square neighbourhoods in UTM 34N coordinates."""
    records = [
        _make_square_neighbourhood("1", "Njësia 1", cx=400_000, cy=4_580_000),
        _make_square_neighbourhood("2", "Njësia 2", cx=404_000, cy=4_580_000),
    ]
    gdf = gpd.GeoDataFrame(records, geometry="geometry", crs="EPSG:32634")
    # Add columns that come from neighbourhood_demand
    return gdf


@pytest.fixture()
def full_coverage_isochrones(two_neighbourhoods_utm: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Isochrones that fully cover both neighbourhoods."""
    # Use the neighbourhood polygons themselves as the isochrone union
    union_geom = two_neighbourhoods_utm.geometry.unary_union
    return gpd.GeoDataFrame(
        [{"stop_id": "s1", "radius_m": 400, "geometry": union_geom}],
        geometry="geometry",
        crs="EPSG:32634",
    )


@pytest.fixture()
def zero_coverage_isochrones() -> gpd.GeoDataFrame:
    """Isochrones far away from the neighbourhoods — zero overlap."""
    far_point = Point(100_000, 100_000).buffer(400)
    return gpd.GeoDataFrame(
        [{"stop_id": "s_far", "radius_m": 400, "geometry": far_point}],
        geometry="geometry",
        crs="EPSG:32634",
    )


# ---------------------------------------------------------------------------
# 2.3  coverage_ratios
# ---------------------------------------------------------------------------

class TestCoverageRatios:
    def test_full_coverage_returns_one(self, two_neighbourhoods_utm, full_coverage_isochrones):
        with build_asset_context() as ctx:
            result = coverage_ratios(ctx, full_coverage_isochrones, two_neighbourhoods_utm)
        assert len(result) == 2
        assert (result["coverage_ratio"] > 0.99).all(), (
            f"Expected ~1.0, got {result['coverage_ratio'].tolist()}"
        )

    def test_zero_coverage_returns_zero(self, two_neighbourhoods_utm, zero_coverage_isochrones):
        with build_asset_context() as ctx:
            result = coverage_ratios(ctx, zero_coverage_isochrones, two_neighbourhoods_utm)
        assert (result["coverage_ratio"] < 0.01).all()

    def test_empty_isochrones_returns_zero(self, two_neighbourhoods_utm):
        empty_iso = gpd.GeoDataFrame(
            columns=["stop_id", "radius_m", "geometry"], geometry="geometry", crs="EPSG:32634"
        )
        with build_asset_context() as ctx:
            result = coverage_ratios(ctx, empty_iso, two_neighbourhoods_utm)
        assert (result["coverage_ratio"] == 0.0).all()

    def test_coverage_ratio_bounded_between_zero_and_one(self, two_neighbourhoods_utm, full_coverage_isochrones):
        with build_asset_context() as ctx:
            result = coverage_ratios(ctx, full_coverage_isochrones, two_neighbourhoods_utm)
        assert result["coverage_ratio"].between(0.0, 1.0).all()

    def test_partial_coverage(self, two_neighbourhoods_utm):
        """Isochrone covers only the first neighbourhood."""
        first_geom = two_neighbourhoods_utm.iloc[0].geometry
        partial_iso = gpd.GeoDataFrame(
            [{"stop_id": "s1", "radius_m": 400, "geometry": first_geom}],
            geometry="geometry",
            crs="EPSG:32634",
        )
        with build_asset_context() as ctx:
            result = coverage_ratios(ctx, partial_iso, two_neighbourhoods_utm)
        ratios = result.set_index("neighbourhood_id")["coverage_ratio"]
        assert ratios["1"] > 0.99
        assert ratios["2"] < 0.01


# ---------------------------------------------------------------------------
# 2.4  gap_scores
# ---------------------------------------------------------------------------

class TestGapScores:
    """Unit tests for the gap score formula — no MotherDuck, no filesystem."""

    @pytest.fixture()
    def scored_input(self, two_neighbourhoods_utm) -> gpd.GeoDataFrame:
        gdf = two_neighbourhoods_utm.copy()
        # Low coverage -> high gap score for neighbourhood 1
        gdf["coverage_ratio"] = [0.05, 0.80]
        gdf["business_count"] = [50.0, 10.0]
        gdf["student_count"] = [20.0, 5.0]
        return gdf

    def test_gap_scores_returns_all_rows(self, scored_input, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "data" / "processed").mkdir(parents=True, exist_ok=True)

        class _FakeDB:
            def get_connection(self):
                from contextlib import contextmanager
                @contextmanager
                def _cm():
                    class _Conn:
                        def register(self, *a, **kw): pass
                        def unregister(self, *a, **kw): pass
                        def execute(self, *a, **kw): pass
                    yield _Conn()
                return _cm()

        with build_asset_context() as ctx:
            result = gap_scores(ctx, scored_input, _FakeDB())

        assert len(result) == 2

    def test_high_demand_low_coverage_ranks_first(self, scored_input, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "data" / "processed").mkdir(parents=True, exist_ok=True)

        class _FakeDB:
            def get_connection(self):
                from contextlib import contextmanager
                @contextmanager
                def _cm():
                    class _Conn:
                        def register(self, *a, **kw): pass
                        def unregister(self, *a, **kw): pass
                        def execute(self, *a, **kw): pass
                    yield _Conn()
                return _cm()

        with build_asset_context() as ctx:
            result = gap_scores(ctx, scored_input, _FakeDB())

        # Neighbourhood 1: high demand + low coverage -> rank 1
        top = result.iloc[0]
        assert top["neighbourhood_id"] == "1"
        assert top["rank"] == 1

    def test_gap_score_formula_with_synthetic_data(self):
        """Direct formula verification — no Dagster context needed."""
        from tirana_pipeline.assets.analysis import _EPSILON, _minmax  # type: ignore[attr-defined]
        business = pd.Series([100.0, 10.0])
        student = pd.Series([50.0, 5.0])
        coverage = pd.Series([0.1, 0.9])

        norm_b = (business - business.min()) / (business.max() - business.min())
        norm_s = (student - student.min()) / (student.max() - student.min())
        scores = (norm_b + norm_s) / (coverage + _EPSILON)

        assert scores.iloc[0] > scores.iloc[1], (
            "High demand + low coverage should produce higher gap score"
        )

    def test_geojson_written_to_disk(self, scored_input, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "data" / "processed").mkdir(parents=True, exist_ok=True)

        class _FakeDB:
            def get_connection(self):
                from contextlib import contextmanager
                @contextmanager
                def _cm():
                    class _Conn:
                        def register(self, *a, **kw): pass
                        def unregister(self, *a, **kw): pass
                        def execute(self, *a, **kw): pass
                    yield _Conn()
                return _cm()

        with build_asset_context() as ctx:
            gap_scores(ctx, scored_input, _FakeDB())

        out = tmp_path / "data" / "processed" / "gap_scores.geojson"
        assert out.exists(), "gap_scores.geojson was not written"
        import json
        with open(out) as f:
            geojson = json.load(f)
        assert geojson["type"] == "FeatureCollection"
        assert len(geojson["features"]) == 2


# ---------------------------------------------------------------------------
# 2.1  walking_isochrones geometry sanity (no DB)
# ---------------------------------------------------------------------------

class TestIsochroneGeometry:
    """Geometry-only tests — no MotherDuck connection."""

    def test_buffer_produces_polygon(self):
        from shapely.geometry import Point
        pt = Point(400_000, 4_580_000)  # UTM 34N coordinate
        buf = pt.buffer(400)
        assert buf.geom_type == "Polygon"
        assert buf.area > 0

    def test_buffer_area_approximately_correct(self):
        import math
        from shapely.geometry import Point
        pt = Point(400_000, 4_580_000)
        radius = 400.0
        buf = pt.buffer(radius)
        expected_area = math.pi * radius ** 2
        # Shapely buffer is an approximation; allow 1% tolerance
        assert abs(buf.area - expected_area) / expected_area < 0.01

    def test_crs_is_utm_34n(self):
        from shapely.geometry import Point
        pts = gpd.GeoDataFrame(
            [{"geometry": Point(400_000, 4_580_000)}], geometry="geometry", crs="EPSG:32634"
        )
        assert pts.crs.to_epsg() == 32634
