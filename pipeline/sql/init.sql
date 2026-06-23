-- Initialise PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- Core tables are created by Dagster assets on first materialisation.
-- This script just ensures the PostGIS extension is available.

SELECT PostGIS_Version();
