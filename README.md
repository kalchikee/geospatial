# Chicago NDVI Monitoring Pipeline

An automated geospatial ETL system that ingests Sentinel-2 and Landsat 8/9 satellite imagery for Chicago, computes monthly NDVI, performs parcel-level zonal statistics, detects vegetation decline, and serves an interactive Leaflet map.

---

## System Architecture

```
┌────────────────────────────────────────────────────────────┐
│  External Data Sources                                      │
│  ┌─────────────────┐   ┌──────────────────────────────┐    │
│  │ ESA Copernicus  │   │ USGS Landsat on AWS (COG/S3) │    │
│  │ STAC API (S2)   │   │ LandsatLook STAC API          │    │
│  └────────┬────────┘   └──────────────┬───────────────┘    │
└───────────┼────────────────────────────┼───────────────────┘
            │                            │
            ▼                            ▼
┌───────────────────────────────────────────────────────────┐
│  Docker Compose Services                                   │
│                                                           │
│  ┌────────────────────────────────────────────────────┐   │
│  │  pipeline container (Python 3.12 + GDAL + cron)   │   │
│  │                                                    │   │
│  │  ingest/ ──► process/ ──► load/ ──► analysis/     │   │
│  │  sentinel2   cloud_mask   raster_loader zonal_stats│   │
│  │  landsat     reproject    vector_loader change_det │   │
│  │  parcels     ndvi                                  │   │
│  │              composite                             │   │
│  │                                                    │   │
│  │  pipeline.py (click CLI, cron-invoked monthly)    │   │
│  └────────────────────────┬───────────────────────────┘   │
│                            │                              │
│  ┌─────────────────────────▼──────────────────────────┐   │
│  │  PostGIS (PostgreSQL 16 + PostGIS 3.4)             │   │
│  │                                                    │   │
│  │  raw_imagery  processed_ndvi  parcels              │   │
│  │  parcel_summaries  change_detection                │   │
│  └──────────────┬─────────────────┬───────────────────┘   │
│                 │                 │                        │
│  ┌──────────────▼──┐   ┌──────────▼───────────────────┐   │
│  │  pg_tileserv    │   │  FastAPI (uvicorn)            │   │
│  │  :7800          │   │  :8000                        │   │
│  │  Vector tiles   │   │  /parcels/geojson             │   │
│  │  (change flags) │   │  /changes  /ndvi/stats        │   │
│  └──────────────┬──┘   └──────────┬───────────────────┘   │
│                 │                 │                        │
│  ┌──────────────▼─────────────────▼───────────────────┐   │
│  │  nginx :8080 → Leaflet frontend                    │   │
│  │  Time slider │ Layer toggles │ Parcel detail chart │   │
│  └────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────┘
```

---

## Database Schema

| Table | Description |
|---|---|
| `raw_imagery` | Scene metadata (source, date, cloud cover, STAC href) |
| `processed_ndvi` | Monthly NDVI rasters (tiled 256×256, EPSG:3435) |
| `parcels` | ~600k Cook County parcels in EPSG:3435 |
| `parcel_summaries` | Per-parcel NDVI stats per month per source |
| `change_detection` | Month-over-month NDVI delta flags |

Spatial indexes: GIST on all geometry columns. BRIN on `period_start` for fast time-range queries.

---

## Quick Start

### Prerequisites
- Docker Desktop (Windows/Mac/Linux)
- Git
- A free [Copernicus Data Space](https://dataspace.copernicus.eu/) account
- A free AWS account (for Landsat requester-pays bucket)

### 1. Clone and configure

```bash
git clone <repo-url> chicago-ndvi-pipeline
cd chicago-ndvi-pipeline
cp .env.example .env
# Edit .env and fill in CDSE credentials + AWS keys
```

### 2. Start services

```bash
docker-compose up -d
# Wait ~30s for PostGIS to initialize and run schema scripts
docker-compose ps   # all 5 services should be healthy/running
```

### 3. Load Chicago parcels (one-time)

```bash
docker exec chicago_pipeline python pipeline.py --load-parcels
# ~5–10 minutes to fetch ~600k parcels from Cook County API
```

### 4. Run the pipeline for a month

```bash
docker exec chicago_pipeline python pipeline.py --month 2024-07
# Runs: ingest → process → load → zonal stats → change detection
# Expect 30–90 min depending on scene count and machine speed
```

### 5. Open the web map

- **Leaflet frontend**: http://localhost:8080
- **FastAPI docs**: http://localhost:8000/docs
- **pg_tileserv**: http://localhost:7800

---

## Pipeline Phases

### Phase 1 — Ingestion
- **Sentinel-2**: Searches Copernicus STAC for Chicago scenes with ≤20% cloud cover. Downloads B04 (Red 10m), B08 (NIR 10m), SCL (cloud classification 20m) per scene.
- **Landsat 8/9**: Searches USGS LandsatLook STAC. Uses GDAL `/vsis3/` to read only the Chicago bounding window from each COG band — avoids full ~300MB downloads.
- **Parcels**: Fetches Cook County Assessor parcels from the Socrata open data API.

### Phase 2 — Processing
1. **Cloud mask**: S2 uses SCL (keep classes 4=vegetation, 5=bare soil). Landsat uses QA_PIXEL bits 3 (cloud) and 4 (shadow).
2. **Reproject**: All bands → EPSG:3435 (Illinois State Plane East).
3. **Clip**: Masked to Chicago bounding polygon.
4. **NDVI**: `(NIR − Red) / (NIR + Red)` with Landsat scale factor applied.
5. **Composite**: Monthly median stack across all valid scenes per source.

### Phase 3 — Database Loading
- Rasters: `raster2pgsql -I -C -M -t 256x256` piped to `psql`.
- Vectors: `geopandas.to_postgis()` via SQLAlchemy.

### Phase 4 — Analysis
- **Zonal statistics**: `rasterstats.zonal_stats()` computes mean/median/min/max/std per parcel.
- **Change detection**: Compares current vs prior month NDVI mean. Flags where delta < −0.1. Classifies severity: minor/moderate/severe.

### Phase 5 — Serving
- **pg_tileserv** auto-discovers `change_flags_view` and serves `.pbf` vector tiles.
- **FastAPI** serves GeoJSON for parcels, changes, and NDVI statistics.
- **Leaflet** renders choropleth parcels, change overlay, time slider, and per-parcel history charts.

### Phase 6 — Automation
Monthly cron job (inside pipeline container):
```
0 6 1 * * python pipeline.py --month $(date -d "last month" +%Y-%m)
```

---

## Performance Benchmarks (Chicago, July 2024 test run)

| Step | Duration | Data Volume |
|---|---|---|
| S2 scene search | ~5s | — |
| S2 band download (×8 scenes) | ~12 min | ~480 MB |
| S2 cloud mask + NDVI + composite | ~8 min | ~25 MB output |
| Landsat COG window reads (×4 scenes) | ~3 min | ~40 MB |
| Landsat NDVI + composite | ~3 min | ~15 MB output |
| raster2pgsql load | ~4 min | ~12 MB in PostGIS |
| Zonal statistics (×580k parcels) | ~18 min | ~1.2M rows |
| Change detection query | ~45s | ~580k rows |
| **Total** | **~50 min** | **~1.7 GB scratch** |

---

## Tech Stack

| Component | Technology |
|---|---|
| Language | Python 3.12 |
| Raster I/O | rasterio 1.3, GDAL 3.6 |
| Vector I/O | geopandas, fiona, shapely |
| STAC client | pystac-client |
| Zonal stats | rasterstats |
| Database | PostgreSQL 16 + PostGIS 3.4 |
| DB driver | psycopg2, SQLAlchemy 2.0 |
| Tile server | pg_tileserv |
| API | FastAPI + uvicorn |
| Frontend | Leaflet 1.9, Chart.js 4 |
| Container | Docker Compose |
| Scheduler | cron (inside container) |
| Logging | structlog (JSON) |

---

## Data Sources

- **Sentinel-2 L2A**: ESA Copernicus Data Space Ecosystem — free account required
- **Landsat 8/9 Collection 2 Level-2 SR**: USGS via AWS Open Data (requester-pays)
- **Cook County Parcels**: Cook County Assessor's Office via Socrata open data API

---

## Limitations & Scalability Notes

- **Parcel count**: ~580k Chicago parcels × monthly NDVI = ~7M rows/year in `parcel_summaries`. Partitioning by `period_start` YEAR would be appropriate beyond 3 years of data.
- **Raster storage**: PostGIS in-db rasters are convenient but not optimised for large mosaics. Production systems should consider storing rasters as files (S3 + COG) and using only PostGIS for vector data.
- **Sentinel-2 resolution**: 10m pixels are smaller than many parcels, providing good detail. Landsat at 30m may miss small parcels entirely (handled by `valid_pct` threshold).
- **Cloud coverage**: Monthly composites smooth over clouds but dense cloud cover months (e.g. January) may have fewer valid pixels. `valid_pct` in `parcel_summaries` indicates reliability.
- **Scaling**: For multiple cities, parameterise `config/settings.py` and run pipeline with `--city` flag. The schema supports multiple AOIs by adding a `city` column.
