# Agent Instructions — tirana-transit-coverage

This file instructs AI agents (Copilot, Cursor, Claude, Perplexity, etc.) on how to work in this repository. Read it fully before making any changes.

---

## Project Purpose

This is a **geospatial transit equity analysis tool** for Tirana, Albania. It identifies neighbourhoods with high economic/educational activity but poor bus coverage, using:
- Tirana GTFS bus feed as the supply-side signal
- Albanian open government data (businesses, schools) as the demand-side signal
- PostGIS for all spatial computation
- Dagster for pipeline orchestration

The project is built in **three phases**. Only Phase 1 is implemented. Phases 2 and 3 are tracked as GitHub Issues #1 and #2.

---

## Repository Layout

```
tirana-transit-coverage/
├── AGENTS.md                        ← you are here
├── README.md                        ← full project plan and architecture
├── docker-compose.yml               ← local dev: PostGIS + Dagster
├── .github/workflows/ci.yml         ← lint + test on every push/PR
├── pipeline/                        ← Dagster pipeline (Phase 1 complete)
│   ├── pyproject.toml               ← dependencies and ruff config
│   ├── dagster.yaml                 ← Postgres-backed run/event storage
│   ├── Dockerfile
│   ├── sql/init.sql                 ← PostGIS extension bootstrap
│   ├── tirana_pipeline/
│   │   ├── __init__.py              ← Dagster Definitions entry point
│   │   ├── resources.py             ← DatabaseResource (SQLAlchemy)
│   │   └── assets/
│   │       ├── gtfs.py              ← GTFS ingestion + PostGIS write
│   │       ├── osm.py               ← OSM neighbourhood boundaries
│   │       └── opendata.py          ← opendata.gov.al business CSVs
│   └── tests/
│       └── test_gtfs.py             ← unit tests for GTFS parsing
├── api/                             ← FastAPI backend (Phase 3, not yet built)
├── dashboard/                       ← Leaflet.js frontend (Phase 3, not yet built)
└── infra/                           ← Kubernetes manifests (Phase 3, not yet built)
```

---

## Current State (Phase 1)

The following Dagster assets are implemented and tested:

| Asset | Group | What it does |
|---|---|---|
| `gtfs_raw` | ingestion | Downloads GTFS ZIP from `pt.tirana.al/gtfs/gtfs.zip` |
| `gtfs_stops` | ingestion | Parses `stops.txt` → GeoDataFrame (EPSG:4326) |
| `gtfs_routes` | ingestion | Parses `routes.txt` → DataFrame |
| `stops_to_postgis` | storage | Upserts stops into PostGIS with EPSG:4326 + EPSG:32634 geometries |
| `tirana_neighbourhoods` | ingestion | Fetches admin boundaries from OSM via `osmnx` |
| `neighbourhoods_to_postgis` | storage | Persists neighbourhood polygons to PostGIS |
| `businesses_by_region` | ingestion | Downloads QKB business-by-region CSV |
| `businesses_by_legal_form` | ingestion | Downloads QKB business-by-legal-form CSV |
| `businesses_to_db` | storage | Stages business data as JSONB in PostGIS |

---

## Rules for Making Changes

### General
- **Never push directly to `main`** — always create a feature branch and open a PR
- Branch naming: `feat/<short-description>`, `fix/<short-description>`, `chore/<short-description>`
- PR titles must follow Conventional Commits: `feat:`, `fix:`, `chore:`, `docs:`, `test:`
- Every PR must pass CI (ruff lint + pytest) before merging

### Python Style
- Python **3.11+** only
- Line length: **100 characters** (enforced by ruff)
- Linting: `ruff check .` from the `pipeline/` directory — fix all warnings before committing
- Type hints on all function signatures
- Docstrings on every asset and resource class
- No bare `except:` — always catch specific exceptions

### Dagster Assets
- Every new data source gets its **own asset file** in `pipeline/tirana_pipeline/assets/`
- Assets must have a `group_name` (either `"ingestion"` or `"storage"`) and a `description`
- Assets that write to the DB receive `db: DatabaseResource` as a parameter
- Log progress with `context.log.info(...)` — never use `print()`
- Cache raw downloads to `/data/raw/<source>/` with a fallback to the cache if the network request fails
- Register new asset modules in `tirana_pipeline/__init__.py` via `load_assets_from_modules`

### Database / PostGIS
- All tables are created by Dagster assets using `CREATE TABLE IF NOT EXISTS` — never write raw schema migrations outside of assets
- All distance and area calculations must use **EPSG:32634** (UTM zone 34N for Albania) — never compute distances in EPSG:4326
- Always create a GIST spatial index after creating a geometry column
- Use `ON CONFLICT ... DO UPDATE` (upsert) for all writes — never `INSERT` without conflict handling
- Never store credentials in code — use the `DATABASE_URL` environment variable

### Tests
- Every new asset must have at least one unit test in `pipeline/tests/`
- Unit tests must not require a live DB or network — mock or use synthetic data
- Use a `MockContext` class (see `test_gtfs.py` for the pattern) to satisfy the `AssetExecutionContext` parameter
- Run tests with: `cd pipeline && pytest tests/ -v`

### What NOT to do
- Do not add new Python dependencies without updating `pyproject.toml`
- Do not store raw data files (CSVs, ZIPs, GeoJSON, Parquet) in the repo — they are gitignored; use `/data/raw/` locally
- Do not hardcode the `DATABASE_URL`, GTFS URL, or `opendata.gov.al` URLs as magic strings outside of the asset files where they are used
- Do not use `geopandas` for distance/area calculations in EPSG:4326 — reproject first
- Do not implement Phase 2 or Phase 3 logic in Phase 1 asset files — keep concerns separated

---

## Local Development

```bash
# Clone
git clone https://github.com/idlirshkurti/tirana-transit-coverage
cd tirana-transit-coverage

# Start PostGIS + Dagster (Docker or Podman)
docker-compose up -d          # Docker
podman-compose up -d          # Podman (drop-in replacement)

# Install pipeline locally for tests
cd pipeline
pip install -e ".[dev]"

# Lint
ruff check .

# Test
pytest tests/ -v

# Open Dagster UI
open http://localhost:3000
```

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgresql://dev:dev@localhost:5432/tirana` | PostGIS connection string |
| `DAGSTER_HOME` | `/opt/dagster/dagster_home` | Dagster metadata directory |

Copy `.env.example` to `.env` for local overrides (`.env` is gitignored).

---

## What to Build Next

The next two phases are fully specced in GitHub Issues:

- **[Issue #1 — Phase 2: Geospatial Analysis & Coverage Gap Scoring](https://github.com/idlirshkurti/tirana-transit-coverage/issues/1)**
  - Isochrone generation (400m buffers → optional network-based)
  - Spatial join of demand data onto neighbourhoods
  - Coverage ratio + gap score computation
  - Output: `coverage_scores` table in PostGIS

- **[Issue #2 — Phase 3: FastAPI Backend + Leaflet.js Dashboard](https://github.com/idlirshkurti/tirana-transit-coverage/issues/2)**
  - REST API endpoints serving GeoJSON from PostGIS
  - Interactive choropleth map with isochrone toggle
  - Top-10 underserved neighbourhoods bar chart
  - Kubernetes deployment manifests

Always check the issue checklist and tick off tasks as you complete them.

---

## Data Sources Reference

| Source | URL | Format | Refresh |
|---|---|---|---|
| Tirana GTFS | `https://pt.tirana.al/gtfs/gtfs.zip` | GTFS ZIP | Static (re-download periodically) |
| Businesses by region | `https://opendata.gov.al/files/Dataset/service/a9b8b467-.../csv.csv` | CSV | Annual (QKB 2026) |
| Businesses by legal form | `https://opendata.gov.al/files/Dataset/service/96bac749-.../csv.csv` | CSV | Annual |
| OSM boundaries | Overpass API via `osmnx` | GeoJSON | On-demand |

---

## Key Architectural Decisions (ADRs)

1. **PostGIS over flat files for analysis** — spatial indexes make neighbourhood intersection queries orders of magnitude faster than in-memory geopandas once the dataset grows
2. **EPSG:32634 for all metric calculations** — UTM zone 34N is the correct projected CRS for Albania; EPSG:4326 degree-based distances are inaccurate
3. **Dagster for orchestration** — already in use in the owner's stack; assets model the dependency graph cleanly and provide observability out of the box
4. **JSONB staging for business data** — the Albanian open data CSVs have inconsistent column names across years; JSONB staging preserves the raw structure and lets Phase 2 normalise it
5. **Monorepo** — pipeline, API, and dashboard share data contracts and are deployed together; splitting would add coordination overhead at this scale
