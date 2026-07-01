"""Unit and integration tests for Phase 2 analysis assets."""

import math

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point, Polygon

# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def _square_neighbourhood(cx: float, cy: float, half: float) -> Polygon:
    """Return a square polygon centred at (cx, cy) with given half-side in metres."""
    return Polygon([
        (cx - half, cy - half),
        (cx + half, cy - half),
        (cx + half, cy + half),
        (cx - half, cy + half),
    ])


@pytest.fixture()
def sample_neighbourhoods() -> gpd.GeoDataFrame:
    """Two 1 km² squares side-by-side in UTM 34N coordinates."""
    return gpd.GeoDataFrame(
        {
            "neighbourhood_id": ["n1", "n2"],
            "name": ["Alpha", "Beta"],
            "geometry": [
                _square_neighbourhood(0, 0, 500),    # n1: x in [-500, 500]
                _square_neighbourhood(1200, 0, 500), # n2: no overlap with n1
            ],
        },
        crs="EPSG:32634",
    )


@pytest.fixture()
def sample_stops() -> gpd.GeoDataFrame:
    """One stop at centre of n1, none near n2."""
    return gpd.GeoDataFrame(
        {"stop_id": ["s1"], "geometry": [Point(0, 0)]},
        crs="EPSG:32634",
    )


# ---------------------------------------------------------------------------
# 2.1  Isochrone generation
# ---------------------------------------------------------------------------

class TestIsochrones:
    def test_buffer_geometry_type(self, sample_stops):
        """Buffer of a Point must be a Polygon."""
        buffered = sample_stops.geometry.buffer(400)
        assert all(g.geom_type == "Polygon" for g in buffered)

    def test_buffer_radius(self, sample_stops):
        """Buffered area should be approximately pi * r^2."""
        r = 400
        buffered = sample_stops.geometry.buffer(r)
        expected_area = math.pi * r ** 2
        actual_area = buffered.iloc[0].area
        assert abs(actual_area - expected_area) / expected_area < 0.01

    def test_crs_preserved(self, sample_stops):
        """CRS must remain EPSG:32634 after buffering."""
        gdf = sample_stops.copy()
        gdf["geometry"] = gdf.geometry.buffer(400)
        assert gdf.crs.to_epsg() == 32634

    def test_empty_stops_returns_empty(self):
        """Empty input GeoDataFrame should yield empty isochrones gracefully."""
        empty = gpd.GeoDataFrame(columns=["stop_id", "geometry"], geometry="geometry",
                                 crs="EPSG:32634")
        buffered = empty.geometry.buffer(400)
        assert len(buffered) == 0


# ---------------------------------------------------------------------------
# 2.2  Coverage ratio
# ---------------------------------------------------------------------------

class TestCoverageRatio:
    def test_full_coverage(self, sample_neighbourhoods, sample_stops):
        """A 400m buffer overlaps the 500m-half-side square — ratio must be in (0, 1]."""
        from shapely.ops import unary_union
        isochrones = sample_stops.copy()
        isochrones["geometry"] = isochrones.geometry.buffer(400)
        union = unary_union(isochrones.geometry)

        nb = sample_neighbourhoods.copy()
        nb["coverage_ratio"] = nb.geometry.apply(
            lambda g: g.intersection(union).area / g.area
        )

        assert nb.loc[nb.neighbourhood_id == "n1", "coverage_ratio"].iloc[0] > 0
        assert nb.loc[nb.neighbourhood_id == "n1", "coverage_ratio"].iloc[0] <= 1.0

    def test_zero_coverage(self, sample_neighbourhoods, sample_stops):
        """n2 has no stop nearby — intersection area should be 0."""
        from shapely.ops import unary_union
        isochrones = sample_stops.copy()
        isochrones["geometry"] = isochrones.geometry.buffer(400)
        union = unary_union(isochrones.geometry)

        nb = sample_neighbourhoods.copy()
        ratio_n2 = nb.loc[nb.neighbourhood_id == "n2", "geometry"].iloc[0].intersection(union).area
        assert ratio_n2 == pytest.approx(0.0)

    def test_null_geometry_handled(self):
        """Neighbourhoods with null geometry should be filtered out, not raise."""
        nb = gpd.GeoDataFrame(
            {"neighbourhood_id": ["n_null"], "geometry": [None]},
            crs="EPSG:32634",
        )
        valid = nb[nb.geometry.notna() & nb.geometry.apply(
            lambda g: g is not None and g.area > 0
        )]
        assert len(valid) == 0


# ---------------------------------------------------------------------------
# 2.4  Gap score formula
# ---------------------------------------------------------------------------

class TestGapScore:
    EPSILON = 0.01

    def _gap_score(self, norm_biz, norm_stu, coverage):
        return (norm_biz + norm_stu) / (coverage + self.EPSILON)

    def test_high_demand_low_coverage_scores_high(self):
        score = self._gap_score(1.0, 1.0, 0.0)
        assert score == pytest.approx(2.0 / self.EPSILON)

    def test_zero_demand_scores_zero(self):
        score = self._gap_score(0.0, 0.0, 0.5)
        assert score == pytest.approx(0.0)

    def test_full_coverage_reduces_score(self):
        low = self._gap_score(0.5, 0.5, 1.0)
        high = self._gap_score(0.5, 0.5, 0.0)
        assert high > low

    def test_epsilon_prevents_division_by_zero(self):
        """gap_score must be finite even at zero coverage."""
        score = self._gap_score(1.0, 1.0, 0.0)
        assert math.isfinite(score)

    def test_minmax_normalisation(self):
        """Min-max normalisation should return values in [0, 1]."""
        series = pd.Series([10.0, 20.0, 30.0, 0.0])
        rng = series.max() - series.min()
        normalised = (series - series.min()) / rng
        assert normalised.min() == pytest.approx(0.0)
        assert normalised.max() == pytest.approx(1.0)

    def test_minmax_constant_series(self):
        """Constant series (range=0) should return all zeros, not NaN."""
        series = pd.Series([5.0, 5.0, 5.0])
        rng = series.max() - series.min()
        normalised = (
            (series - series.min()) / rng
            if rng > 0
            else pd.Series(0.0, index=series.index)
        )
        assert all(normalised == 0.0)


# ---------------------------------------------------------------------------
# 2.2  Spatial join (synthetic)
# ---------------------------------------------------------------------------

class TestSpatialJoin:
    def test_poi_within_neighbourhood(self, sample_neighbourhoods):
        """POI at (0,0) should join to n1 only."""
        pois = gpd.GeoDataFrame(
            {"amenity": ["cafe"], "geometry": [Point(0, 0)]},
            crs="EPSG:32634",
        )
        joined = gpd.sjoin(
            pois,
            sample_neighbourhoods[["neighbourhood_id", "geometry"]],
            how="inner",
            predicate="within",
        )
        assert len(joined) == 1
        assert joined.iloc[0]["neighbourhood_id"] == "n1"

    def test_poi_outside_all_neighbourhoods(self, sample_neighbourhoods):
        """POI far outside all neighbourhoods should return empty join."""
        pois = gpd.GeoDataFrame(
            {"amenity": ["cafe"], "geometry": [Point(99999, 99999)]},
            crs="EPSG:32634",
        )
        joined = gpd.sjoin(
            pois,
            sample_neighbourhoods[["neighbourhood_id", "geometry"]],
            how="inner",
            predicate="within",
        )
        assert len(joined) == 0

    def test_business_vs_education_split(self, sample_neighbourhoods):
        """Education amenities should be counted separately from total."""
        pois = gpd.GeoDataFrame(
            {
                "amenity": ["cafe", "school", "restaurant"],
                "geometry": [Point(0, 0), Point(10, 10), Point(20, 20)],
            },
            crs="EPSG:32634",
        )
        joined = gpd.sjoin(
            pois,
            sample_neighbourhoods[["neighbourhood_id", "geometry"]],
            how="inner",
            predicate="within",
        )
        education_types = {"school", "university", "college", "kindergarten", "language_school"}
        joined["is_education"] = joined["amenity"].isin(education_types)
        edu_count = joined[joined["is_education"]].shape[0]
        assert edu_count == 1  # only school
