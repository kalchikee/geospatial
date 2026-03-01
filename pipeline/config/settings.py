"""
Central configuration for the Chicago NDVI Monitoring Pipeline.
All environment-sensitive values are read from environment variables.
"""
import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Area of Interest — Chicago, IL
# ---------------------------------------------------------------------------
CHICAGO_BBOX = (-87.94, 41.64, -87.52, 42.02)  # (minx, miny, maxx, maxy) WGS84

# Approximate Chicago city boundary as a GeoJSON-style dict for rasterio clipping
CHICAGO_BBOX_GEOM = {
    "type": "Polygon",
    "coordinates": [[
        [-87.94, 41.64],
        [-87.52, 41.64],
        [-87.52, 42.02],
        [-87.94, 42.02],
        [-87.94, 41.64],
    ]],
}

# ---------------------------------------------------------------------------
# Coordinate Reference Systems
# ---------------------------------------------------------------------------
SOURCE_CRS = "EPSG:4326"       # WGS84 — input CRS for all downloads
TARGET_CRS = "EPSG:3435"       # Illinois State Plane East (ft) — storage CRS
TARGET_CRS_UTM = "EPSG:32616"  # UTM Zone 16N — intermediate metric operations

# ---------------------------------------------------------------------------
# Processing thresholds
# ---------------------------------------------------------------------------
NODATA = -9999.0
NDVI_CHANGE_THRESHOLD = -0.1   # delta below this triggers a flag
MAX_CLOUD_COVER = 80           # percent — scenes above this are skipped (cloud mask removes actual cloud pixels)

# Severity bands for NDVI decline
SEVERITY_MINOR = -0.1          # delta in [-0.2, -0.1)
SEVERITY_MODERATE = -0.2       # delta in [-0.3, -0.2)
SEVERITY_SEVERE = -0.3         # delta < -0.3

# ---------------------------------------------------------------------------
# Sentinel-2 — Element 84 Earth Search (AWS public COGs, no auth required)
# ---------------------------------------------------------------------------
STAC_ENDPOINT_S2 = "https://earth-search.aws.element84.com/v1"
STAC_COLLECTION_S2 = "sentinel-2-l2a"

# S2 band asset names as used by Element 84 Earth Search
S2_BAND_RED = "red"   # B04, 10m
S2_BAND_NIR = "nir"   # B08, 10m
S2_BAND_SCL = "scl"   # Scene Classification Layer, 20m

# SCL values considered clear (vegetation=4, bare soil=5)
S2_CLEAR_SCL_VALUES = [4, 5]

# ---------------------------------------------------------------------------
# Landsat 8/9 (USGS via AWS Open Data) — OPTIONAL
# ---------------------------------------------------------------------------
STAC_ENDPOINT_LS = "https://landsatlook.usgs.gov/stac-server"
STAC_COLLECTION_LS = "landsat-c2l2-sr"
LANDSAT_BUCKET = "usgs-landsat"

# True only when both AWS keys are non-empty
AWS_CONFIGURED = bool(
    os.environ.get("AWS_ACCESS_KEY_ID", "").strip()
    and os.environ.get("AWS_SECRET_ACCESS_KEY", "").strip()
)

# Landsat band asset names in the STAC items
LS_BAND_RED = "red"       # OLI Band 4
LS_BAND_NIR = "nir08"     # OLI Band 5
LS_BAND_QA = "qa_pixel"   # Quality Assessment band

# QA_PIXEL bit positions
LS_QA_CLOUD_BIT = 3
LS_QA_CLOUD_SHADOW_BIT = 4

# ---------------------------------------------------------------------------
# Spatial units — Chicago Community Areas (City of Chicago Open Data Portal)
# 77 community area polygons used as spatial analysis units.
# ---------------------------------------------------------------------------
COMMUNITY_AREAS_URL = (
    "https://data.cityofchicago.org/resource/igwz-8jzy.geojson"
    "?$limit=100"
)

# ---------------------------------------------------------------------------
# File system paths (inside Docker container)
# ---------------------------------------------------------------------------
SCRATCH_DIR = Path(os.environ.get("SCRATCH_DIR", "/tmp/rasters"))
SCRATCH_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgres://ndvi_user:password@localhost:5432/chicago_ndvi",
)
DB_HOST = os.environ.get("POSTGRES_HOST", "postgis")
DB_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
DB_NAME = os.environ.get("POSTGRES_DB", "chicago_ndvi")
DB_USER = os.environ.get("POSTGRES_USER", "ndvi_user")
DB_PASS = os.environ.get("POSTGRES_PASSWORD", "")
