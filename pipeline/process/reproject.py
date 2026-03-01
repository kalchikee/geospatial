"""
Raster reprojection and clipping to the Chicago AOI.

Steps:
  1. Reproject source raster (any CRS) to TARGET_CRS (EPSG:3435).
  2. Clip to the Chicago bounding polygon.

Both operations are performed with rasterio in-memory where possible.
"""
from pathlib import Path
from typing import Optional

import numpy as np
import rasterio
import rasterio.mask
import rasterio.warp
import structlog
from rasterio.crs import CRS
from rasterio.warp import calculate_default_transform, reproject, Resampling

from config.settings import TARGET_CRS, CHICAGO_BBOX_GEOM, NODATA

log = structlog.get_logger(__name__)

TARGET_CRS_OBJ = CRS.from_string(TARGET_CRS)


def reproject_to_target(
    src_path: Path,
    dst_path: Path,
    nodata: float = NODATA,
    resampling: Resampling = Resampling.bilinear,
) -> Path:
    """
    Reproject a GeoTIFF to TARGET_CRS and write to dst_path.
    Returns dst_path.
    """
    with rasterio.open(src_path) as src:
        transform, width, height = calculate_default_transform(
            src.crs, TARGET_CRS_OBJ, src.width, src.height, *src.bounds
        )
        profile = src.profile.copy()
        profile.update(
            crs=TARGET_CRS_OBJ,
            transform=transform,
            width=width,
            height=height,
            nodata=nodata,
            dtype="float32",
            compress="lzw",
            driver="GTiff",
        )

        dst_path.parent.mkdir(parents=True, exist_ok=True)
        with rasterio.open(dst_path, "w", **profile) as dst:
            for band_idx in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, band_idx),
                    destination=rasterio.band(dst, band_idx),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=transform,
                    dst_crs=TARGET_CRS_OBJ,
                    resampling=resampling,
                    src_nodata=nodata,
                    dst_nodata=nodata,
                )

    log.info("reprojected", src=str(src_path), dst=str(dst_path), crs=TARGET_CRS)
    return dst_path


def clip_to_chicago(src_path: Path, dst_path: Path, nodata: float = NODATA) -> Path:
    """
    Clip a GeoTIFF to the Chicago bounding polygon.
    The input must already be in TARGET_CRS (or at least a projected CRS).
    Returns dst_path.
    """
    with rasterio.open(src_path) as src:
        # Transform the WGS84 bbox geom to the raster's CRS
        from rasterio.warp import transform_geom
        clip_geom = transform_geom("EPSG:4326", src.crs, CHICAGO_BBOX_GEOM)

        masked_data, masked_transform = rasterio.mask.mask(
            src,
            [clip_geom],
            crop=True,
            nodata=nodata,
            filled=True,
        )
        profile = src.profile.copy()
        profile.update(
            transform=masked_transform,
            height=masked_data.shape[1],
            width=masked_data.shape[2],
            nodata=nodata,
            compress="lzw",
            driver="GTiff",
        )

        dst_path.parent.mkdir(parents=True, exist_ok=True)
        with rasterio.open(dst_path, "w", **profile) as dst:
            dst.write(masked_data)

    log.info("clipped_to_chicago", src=str(src_path), dst=str(dst_path))
    return dst_path


def reproject_and_clip(src_path: Path, out_dir: Path, suffix: str = "") -> Path:
    """
    Convenience wrapper: reproject → clip, storing intermediate files in out_dir.
    Returns the final clipped path.
    """
    stem = src_path.stem + suffix
    reproj_path = out_dir / f"{stem}_reproj.tif"
    clipped_path = out_dir / f"{stem}_clipped.tif"

    reproject_to_target(src_path, reproj_path)
    clip_to_chicago(reproj_path, clipped_path)

    # Clean up intermediate reprojected file
    reproj_path.unlink(missing_ok=True)

    return clipped_path
