# Tirana Transit Coverage Gap & Equity Analysis

Geospatial analysis tool that identifies neighborhoods in Tirana with high economic/educational activity but poor bus transit coverage — built for city planners and transit advocates.

---

## Project Overview

The goal is to identify neighborhoods in Tirana where demand for transit (measured by business density and student enrollment) outstrips supply (bus stop coverage). The output is a ranked GeoJSON of neighborhoods with gap scores, served through a FastAPI backend and visualised in a Leaflet.js dashboard.

---

## Data Sources

| Source | What you get | Format |
|---|---|---|
| `pt.tirana.al/gtfs/gtfs.zip` | Bus stops (lat/lon), routes, shapes | GTFS (CSV inside ZIP) |
| `opendata.gov.al` business CSVs | Business counts by region (QKB, 2026) | CSV |
| Education datasets on `opendata.gov.al` | Student enrollment by region | CSV |
| OpenStreetMap (Overpass API) | Road network, neighborhoods, admin boundaries | GeoJSON |

---

## Architecture

```
┌─────────────────────────────────────────────┐
│            Data Ingestion (Dagster)          │
│  - GTFS fetcher   - opendata.gov.al API      │
│  - OSM Overpass   - Education CSVs           │
└────────────────┬────────────────────────────┘
                 │
┌────────────────▼────────────────────────────┐
│          Transform Layer (dbt/Python)        │
│  - Parse GTFS stops → GeoDataFrame           │
│  - Generate 400m walking isochrones          │
│  - Spatially join business & education data  │
│  - Compute coverage score per neighbourhood  │
└────────────────┬────────────────────────────┘
                 │
┌────────────────▼────────────────────────────┐
│           Analysis / Scoring                 │
│  - Coverage ratio = served pop / total       │
│  - Gap score = (business density +           │
│                student density) - coverage   │
│  - Rank neighbourhoods by gap score          │
└────────────────┬────────────────────────────┘
                 │
┌────────────────▼────────────────────────────┐
│          Interactive Dashboard               │
│  - FastAPI backend (GeoJSON endpoints)       │
│  - Leaflet.js map with heatmap overlay       │
│  - Choropleth: gap score by neighbourhood    │
│  - Filter by business type, school density   │
└─────────────────────────────────────────────┘
```

---

## Phased Build Plan

### Phase 1 — Data Pipeline (Week 1) ✅
1. Download and parse the GTFS feed with `gtfs_kit` / raw `pandas`
2. Extract `stops.txt` → GeoDataFrame with lat/lon
3. Fetch OSM neighbourhood boundaries for Tirana via Overpass API (`osmnx`)
4. Download business and education CSVs from `opendata.gov.al`
5. Build a Dagster pipeline with one asset per data source

### Phase 2 — Geospatial Analysis (Week 2)
1. Generate **400m walking isochrones** around each bus stop using `geopandas` buffers
2. Compute the **union of all isochrones** — the "served area"
3. **Spatial join** business counts and school enrollment to each neighbourhood polygon
4. Calculate a **gap score** per neighbourhood:
   ```
   gap = (business_density + student_density) / (coverage_ratio + ε)
   ```
5. Output a ranked GeoJSON of neighbourhoods with scores

### Phase 3 — API + Dashboard (Week 3)
1. Wrap analysis outputs in a **FastAPI** service: `/coverage`, `/gaps`, `/stops`
2. Build a **Leaflet.js** dashboard:
   - Choropleth map coloured by gap score
   - Clickable neighbourhoods showing stats
   - Bus stop markers + isochrone overlay toggle
   - Bar chart: top 10 underserved neighbourhoods
3. Dockerize and deploy to Kubernetes

---

## Repository Structure

```
tirana-transit-coverage/
├── README.md
├── docker-compose.yml
├── .github/workflows/
│   └── ci.yml
├── pipeline/                   # Dagster data pipeline
│   ├── pyproject.toml
│   ├── dagster.yaml
│   └── tirana_pipeline/
│       ├── __init__.py
│       ├── assets/
│       │   ├── gtfs.py
│       │   ├── opendata.py
│       │   ├── osm.py
│       │   └── analysis.py
│       └── resources.py
├── api/                        # FastAPI service (Phase 3)
│   ├── pyproject.toml
│   ├── Dockerfile
│   └── app/
│       ├── main.py
│       ├── routes/
│       └── models.py
├── dashboard/                  # Leaflet.js frontend (Phase 3)
│   ├── index.html
│   └── assets/
└── infra/                      # Kubernetes manifests
    ├── deployment.yaml
    └── configmap.yaml
```

---

## Database

PostgreSQL + PostGIS is used for geospatial storage and querying.

```sql
-- Core tables
stops           (stop_id, name, geom GEOMETRY(Point, 4326))
routes          (route_id, route_name, shape GEOMETRY(LineString))
isochrones      (stop_id, radius_m, geom GEOMETRY(Polygon))
neighbourhoods  (id, name, geom GEOMETRY(MultiPolygon))

-- Computed/scored
coverage_scores (neighbourhood_id, business_density,
                 student_density, coverage_ratio, gap_score,
                 computed_at TIMESTAMPTZ)
```

Raw downloads (GTFS ZIP, CSVs) are kept as files in `data/raw/`. Only transformed, queryable outputs go into PostGIS.

---

## Local Development

```bash
# Start all services
docker-compose up

# Run Dagster pipeline
cd pipeline
pip install -e .
dagster dev

# Materialise all assets
dagster asset materialize --select "*"
```

---

## Key Libraries

| Library | Purpose |
|---|---|
| `geopandas` | Vector analysis, spatial joins |
| `shapely` | Geometry operations (buffers for isochrones) |
| `osmnx` | OSM street network + admin boundaries |
| `gtfs_kit` | GTFS parsing |
| `dagster` | Pipeline orchestration |
| `sqlalchemy` + `geoalchemy2` | PostGIS ORM |
| `fastapi` | REST API (Phase 3) |

---

## Known Challenges

- **Isochrone accuracy**: Simple 400m buffers ignore road topology. Phase 2 will optionally upgrade to network-based isochrones via `osmnx` + `networkx`.
- **Data granularity**: Business CSVs from `opendata.gov.al` are aggregated by region, not street level. OSM building/POI density will be used to spatially disaggregate.
- **Coordinate systems**: All sources use WGS84 (EPSG:4326). Reproject to `EPSG:32634` (UTM zone 34N) before computing distances and areas.
