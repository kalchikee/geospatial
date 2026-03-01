"""
Sentinel-2 L2A ingestion via Element 84 Earth Search STAC API.

No authentication required — data is served as public AWS COGs.

Download strategy: Only red (B04, 10m), nir (B08, 10m), and scl (SCL, 20m) are
fetched per scene. Files are written to SCRATCH_DIR and cleaned up after loading.
"""
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
import structlog
from pystac_client import Client

from config.settings import (
    STAC_ENDPOINT_S2,
    STAC_COLLECTION_S2,
    CHICAGO_BBOX,
    MAX_CLOUD_COVER,
    SCRATCH_DIR,
    S2_BAND_RED,
    S2_BAND_NIR,
    S2_BAND_SCL,
)
from utils.db import get_connection, get_cursor

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# STAC search
# ---------------------------------------------------------------------------

def search_scenes(year: int, month: int) -> list:
    """
    Search Element 84 Earth Search for Sentinel-2 L2A scenes covering Chicago
    for the given year/month with cloud cover <= MAX_CLOUD_COVER.

    Returns a list of pystac Item objects.
    """
    from calendar import monthrange

    start = f"{year}-{month:02d}-01T00:00:00Z"
    _, last_day = monthrange(year, month)
    end = f"{year}-{month:02d}-{last_day:02d}T23:59:59Z"

    catalog = Client.open(STAC_ENDPOINT_S2)
    search = catalog.search(
        collections=[STAC_COLLECTION_S2],
        bbox=CHICAGO_BBOX,
        datetime=f"{start}/{end}",
        max_items=100,
    )

    all_items = list(search.items())
    items = [
        it for it in all_items
        if (it.properties.get("eo:cloud_cover") or 100) <= MAX_CLOUD_COVER
    ]
    log.info("s2_stac_search_complete", count=len(items),
             total_found=len(all_items), year=year, month=month)
    return items


# ---------------------------------------------------------------------------
# Band download
# ---------------------------------------------------------------------------

def _download_asset(asset_href: str, dest_path: Path) -> Path:
    """Stream-download a single STAC asset to disk (no auth required)."""
    if dest_path.exists():
        log.debug("s2_asset_cached", path=str(dest_path))
        return dest_path

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(asset_href, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=65536):
                f.write(chunk)

    log.info("s2_asset_downloaded", path=str(dest_path),
             size_mb=round(dest_path.stat().st_size / 1e6, 1))
    return dest_path


def download_scene_bands(item, scene_dir: Path) -> Optional[dict]:
    """
    Download red, nir, and scl bands for a single STAC item.
    Returns dict of {band_name: local_path} or None if assets are missing.
    """
    assets = item.assets
    required = {S2_BAND_RED, S2_BAND_NIR, S2_BAND_SCL}
    missing = required - set(assets.keys())
    if missing:
        log.warning("s2_missing_assets", scene=item.id, missing=sorted(missing))
        return None

    paths = {}
    for band_key in required:
        href = assets[band_key].href
        ext = ".tif" if href.lower().endswith(".tif") else ".tif"
        dest = scene_dir / f"{item.id}_{band_key}{ext}"
        paths[band_key] = _download_asset(href, dest)

    return paths


# ---------------------------------------------------------------------------
# Database metadata recording
# ---------------------------------------------------------------------------

def _record_scene(item, local_paths: dict) -> None:
    """Insert or update scene metadata in raw_imagery."""
    acq_dt = item.datetime or datetime.now(timezone.utc)
    cloud = item.properties.get("eo:cloud_cover")
    bbox_coords = item.bbox  # [minx, miny, maxx, maxy]
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
            ('sentinel2', %s, %s, %s, ST_GeomFromEWKT(%s), %s, %s, FALSE)
        ON CONFLICT (scene_id) DO UPDATE
            SET processed = EXCLUDED.processed
    """
    with get_connection() as conn, get_cursor(conn) as cur:
        cur.execute(sql, (
            item.id,
            acq_dt,
            cloud,
            bbox_wkt,
            item.get_self_href(),
            str(local_paths.get(S2_BAND_RED, "")),
        ))


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run(year: int, month: int) -> list:
    """
    Full Sentinel-2 ingestion for a given year/month.

    1. Search Element 84 Earth Search for Chicago scenes.
    2. Download red, nir, scl bands for each scene.
    3. Record metadata in raw_imagery.

    Returns list of dicts: {item, paths} for downstream processing.
    """
    log.info("s2_ingestion_start", year=year, month=month)
    items = search_scenes(year, month)

    results = []
    for item in items:
        scene_dir = SCRATCH_DIR / "sentinel2" / item.id
        scene_dir.mkdir(parents=True, exist_ok=True)

        paths = download_scene_bands(item, scene_dir)
        if paths is None:
            continue

        _record_scene(item, paths)
        results.append({"item": item, "paths": paths})
        log.info("s2_scene_ingested", scene=item.id)

    log.info("s2_ingestion_complete", scenes_ingested=len(results), year=year, month=month)
    return results
