"""
Landsat 8/9 Collection 2 Level-2 Surface Reflectance ingestion.

Data source: USGS LandsatLook STAC server + AWS Open Data (usgs-landsat bucket).

Download strategy: COG windowed reads via GDAL /vsis3/ virtual filesystem.
This reads only the Chicago spatial window from each band without downloading
the full ~300MB scene. Requires AWS credentials (even for public data, USGS
uses requester-pays billing; a free AWS account with billing enabled suffices).

If /vsis3/ fails (no AWS credentials), falls back to streaming download.
"""
import os
from calendar import monthrange
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import numpy as np
import rasterio
import rasterio.transform
import structlog
from rasterio.crs import CRS
from rasterio.enums import Resampling
from rasterio.transform import from_bounds
from pystac_client import Client
from shapely.geometry import box

from config.settings import (
    STAC_ENDPOINT_LS,
    STAC_COLLECTION_LS,
    CHICAGO_BBOX,
    MAX_CLOUD_COVER,
    SCRATCH_DIR,
    LS_BAND_RED,
    LS_BAND_NIR,
    LS_BAND_QA,
    SOURCE_CRS,
)
from utils.db import get_connection, get_cursor

log = structlog.get_logger(__name__)

# Target read resolution (metres) — native Landsat SR bands are 30m
READ_RESOLUTION = 30

# GDAL environment for COG windowed reads from S3
GDAL_COG_ENV = {
    "GDAL_HTTP_MERGE_CONSECUTIVE_REQUESTS": "YES",
    "GDAL_HTTP_MULTIPLEX": "YES",
    "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
    "CPL_VSIL_CURL_ALLOWED_EXTENSIONS": ".TIF,.tif,.ovr",
    "AWS_REQUEST_PAYER": "requester",
}


# ---------------------------------------------------------------------------
# STAC search
# ---------------------------------------------------------------------------

def search_scenes(year: int, month: int) -> list:
    """Search USGS LandsatLook STAC for L8/L9 scenes covering Chicago."""
    start = f"{year}-{month:02d}-01T00:00:00Z"
    _, last_day = monthrange(year, month)
    end = f"{year}-{month:02d}-{last_day:02d}T23:59:59Z"

    catalog = Client.open(STAC_ENDPOINT_LS)
    search = catalog.search(
        collections=[STAC_COLLECTION_LS],
        bbox=CHICAGO_BBOX,
        datetime=f"{start}/{end}",
        query={"eo:cloud_cover": {"lte": MAX_CLOUD_COVER}},
        max_items=30,
    )
    items = list(search.items())
    log.info("landsat_stac_search_complete", count=len(items), year=year, month=month)
    return items


# ---------------------------------------------------------------------------
# COG windowed read
# ---------------------------------------------------------------------------

def _s3_uri_from_href(href: str) -> str:
    """Convert an S3 https URL to a GDAL /vsis3/ path."""
    parsed = urlparse(href)
    if parsed.scheme in ("s3", ""):
        return f"/vsis3/{parsed.netloc}{parsed.path}"
    # https://usgs-landsat.s3.us-west-2.amazonaws.com/...
    if "s3.amazonaws.com" in parsed.netloc or "s3.us-west-2.amazonaws.com" in parsed.netloc:
        bucket = parsed.netloc.split(".")[0]
        return f"/vsis3/{bucket}{parsed.path}"
    # Fall back to vsicurl for non-S3 https
    return f"/vsicurl/{href}"


def _windowed_read(href: str, bbox_wgs84: tuple) -> Optional[tuple[np.ndarray, dict]]:
    """
    Read a spatial window from a COG at the given WGS84 bbox.
    Returns (array, profile) or None on failure.
    """
    vsi_path = _s3_uri_from_href(href)

    with rasterio.Env(**GDAL_COG_ENV):
        try:
            with rasterio.open(vsi_path) as src:
                # Reproject bbox to dataset CRS for windowing
                from rasterio.warp import transform_bounds
                window_bounds = transform_bounds(
                    SOURCE_CRS, src.crs,
                    *bbox_wgs84,
                )
                window = src.window(*window_bounds)
                data = src.read(1, window=window, boundless=True, fill_value=0)
                transform = src.window_transform(window)
                profile = src.profile.copy()
                profile.update({
                    "height": data.shape[0],
                    "width": data.shape[1],
                    "transform": transform,
                    "count": 1,
                })
                return data, profile
        except Exception as exc:
            log.warning("landsat_windowed_read_failed", href=href, error=str(exc))
            return None


def _stream_download(href: str, dest_path: Path) -> Optional[Path]:
    """Fallback: stream-download the full band file."""
    import requests
    if dest_path.exists():
        return dest_path
    try:
        with requests.get(href, stream=True, timeout=300) as r:
            r.raise_for_status()
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            with open(dest_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        log.info("landsat_band_downloaded", path=str(dest_path))
        return dest_path
    except Exception as exc:
        log.error("landsat_download_failed", href=href, error=str(exc))
        return None


# ---------------------------------------------------------------------------
# Scene extraction
# ---------------------------------------------------------------------------

def _platform(item) -> str:
    plat = item.properties.get("platform", "").lower()
    return "landsat8" if "8" in plat else "landsat9"


def extract_scene_window(item, scene_dir: Path) -> Optional[dict[str, Path]]:
    """
    Perform windowed reads of Red, NIR, and QA_PIXEL bands for Chicago bbox.
    Writes clipped GeoTIFFs to scene_dir. Returns {band: path} dict or None.
    """
    assets = item.assets
    band_map = {
        LS_BAND_RED: "B4",
        LS_BAND_NIR: "B5",
        LS_BAND_QA: "QA_PIXEL",
    }

    # Resolve asset hrefs (STAC items use lowercase keys)
    required_keys = {LS_BAND_RED, LS_BAND_NIR, LS_BAND_QA}
    available_keys = set(assets.keys())
    if not required_keys.issubset(available_keys):
        log.warning("landsat_missing_assets", scene=item.id,
                    missing=list(required_keys - available_keys))
        return None

    paths = {}
    scene_dir.mkdir(parents=True, exist_ok=True)

    for band_key in required_keys:
        href = assets[band_key].href
        dest_path = scene_dir / f"{item.id}_{band_key}.tif"

        result = _windowed_read(href, CHICAGO_BBOX)
        if result is None:
            # Fallback to full download
            full_path = scene_dir / f"{item.id}_{band_key}_full.tif"
            downloaded = _stream_download(href, full_path)
            if downloaded is None:
                return None
            # Re-read the window from the downloaded file
            result = _windowed_read(str(downloaded), CHICAGO_BBOX)
            if result is None:
                return None

        data, profile = result
        profile.update(driver="GTiff", compress="lzw", dtype=data.dtype)
        with rasterio.open(dest_path, "w", **profile) as dst:
            dst.write(data, 1)

        paths[band_key] = dest_path
        log.info("landsat_band_extracted", scene=item.id, band=band_key, path=str(dest_path))

    return paths


# ---------------------------------------------------------------------------
# Database metadata recording
# ---------------------------------------------------------------------------

def _record_scene(item, platform: str, local_paths: dict[str, Path]) -> None:
    acq_dt = item.datetime
    cloud = item.properties.get("eo:cloud_cover")
    bbox_coords = item.bbox
    bbox_wkt = (
        f"SRID=4326;POLYGON(("
        f"{bbox_coords[0]} {bbox_coords[1]},"
        f"{bbox_coords[2]} {bbox_coords[1]},"
        f"{bbox_coords[2]} {bbox_coords[3]},"
        f"{bbox_coords[0]} {bbox_coords[3]},"
        f"{bbox_coords[0]} {bbox_coords[1]}"
        f"))"
    )

    sql = """
        INSERT INTO raw_imagery
            (source, scene_id, acquisition_dt, cloud_cover, bbox, stac_href, local_path, processed)
        VALUES
            (%s, %s, %s, %s, ST_GeomFromEWKT(%s), %s, %s, FALSE)
        ON CONFLICT (scene_id) DO UPDATE
            SET processed = EXCLUDED.processed
    """
    with get_connection() as conn, get_cursor(conn) as cur:
        cur.execute(sql, (
            platform,
            item.id,
            acq_dt,
            cloud,
            bbox_wkt,
            item.get_self_href(),
            str(local_paths.get(LS_BAND_RED, "")),
        ))


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run(year: int, month: int) -> list[dict]:
    """
    Full Landsat 8/9 ingestion for a given year/month.

    1. Search LandsatLook STAC for Chicago scenes.
    2. Window-read Red, NIR, QA_PIXEL for each scene.
    3. Record metadata in raw_imagery.

    Returns list of dicts: {item, paths, platform} for downstream processing.
    """
    log.info("landsat_ingestion_start", year=year, month=month)
    items = search_scenes(year, month)

    results = []
    for item in items:
        platform = _platform(item)
        scene_dir = SCRATCH_DIR / platform / item.id
        paths = extract_scene_window(item, scene_dir)
        if paths is None:
            continue

        _record_scene(item, platform, paths)
        results.append({"item": item, "paths": paths, "platform": platform})
        log.info("landsat_scene_ingested", scene=item.id, platform=platform)

    log.info("landsat_ingestion_complete", scenes_ingested=len(results), year=year, month=month)
    return results
