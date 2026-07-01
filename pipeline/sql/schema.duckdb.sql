-- MotherDuck / DuckDB schema for tirana_transit database
-- Run once to initialise: connect to MotherDuck and execute this file.
-- The spatial extension is pre-installed on MotherDuck (LOAD spatial is called at runtime).

LOAD spatial;

-- Raw bus stop locations (WGS84 + UTM 34N for metric ops)
CREATE TABLE IF NOT EXISTS stops (
    stop_id    VARCHAR PRIMARY KEY,
    stop_name  VARCHAR,
    geom       GEOMETRY,  -- EPSG:4326  Point
    geom_utm   GEOMETRY   -- EPSG:32634 Point
);

-- Bus route metadata and shapes
CREATE TABLE IF NOT EXISTS routes (
    route_id    VARCHAR PRIMARY KEY,
    route_name  VARCHAR,
    route_type  INTEGER,
    agency_id   VARCHAR,
    shape       GEOMETRY  -- EPSG:4326 LineString
);

-- 400 m walking isochrones (buffers) per stop — stored in EPSG:32634 (UTM 34N)
CREATE TABLE IF NOT EXISTS isochrones (
    stop_id   VARCHAR,
    radius_m  INTEGER,
    geom      GEOMETRY,  -- EPSG:32634 Polygon
    PRIMARY KEY (stop_id, radius_m)
);

-- OSM admin-level neighbourhood polygons
CREATE TABLE IF NOT EXISTS neighbourhoods (
    neighbourhood_id  VARCHAR PRIMARY KEY,
    name              VARCHAR,
    geom              GEOMETRY  -- EPSG:4326 MultiPolygon
);

-- Staging: region-level business counts from opendata.gov.al
CREATE TABLE IF NOT EXISTS staging_businesses_by_region (
    region  VARCHAR,
    data    JSON
);

-- Phase 2: neighbourhood demand from OSM POI spatial join
CREATE TABLE IF NOT EXISTS neighbourhood_demand (
    neighbourhood_id  VARCHAR PRIMARY KEY REFERENCES neighbourhoods(neighbourhood_id),
    business_count    DOUBLE,
    student_count     DOUBLE,
    computed_at       TIMESTAMPTZ DEFAULT now()
);

-- Final computed gap scores per neighbourhood
CREATE TABLE IF NOT EXISTS coverage_scores (
    neighbourhood_id  VARCHAR PRIMARY KEY,
    business_density  DOUBLE,
    student_density   DOUBLE,
    coverage_ratio    DOUBLE,
    gap_score         DOUBLE,  -- = (norm_business_density + norm_student_density) / (coverage_ratio + ε)
    computed_at       TIMESTAMPTZ DEFAULT now()
);
