"""
Monthly median composite generator.

Takes a list of NDVI GeoTIFFs from the same month and source, stacks them,
and computes a per-pixel median ignoring nodata values.

The median composite:
- Suppresses remaining cloud/shadow artefacts that slipped past the cloud mask.
- Provides a stable, representative "greenness" for the month.

Output: a single Float32 GeoTIFF matching the spatial extent and CRS of the inputs.
All input rasters must be co-registered (same CRS, resolution, extent).
"""
from pathlib import Path
from typing import Optional

import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.warp import reproject
import structlog

from config.settings import NODATA

log = structlog.get_logger(__name__)


def _align_to_reference(src_path: Path, ref_profile: dict) -> np.ndarray:
    """
    Read and align a raster to the reference profile's transform/shape.
    Returns a 2D float32 array.
    """
    with rasterio.open(src_path) as src:
        if (src.width == ref_profile["width"]
                and src.height == ref_profile["height"]
                and src.transform == ref_profile["transform"]):
            data = src.read(1).astype(np.float32)
        else:
            data = np.empty(
                (ref_profile["height"], ref_profile["width"]), dtype=np.float32
            )
            reproject(
                source=rasterio.band(src, 1),
                destination=data,
                src_transform=src.transform,
                src_crs=src.crs,
                dst_transform=ref_profile["transform"],
                dst_crs=ref_profile["crs"],
                resampling=Resampling.bilinear,
                src_nodata=NODATA,
                dst_nodata=NODATA,
            )
    # Replace nodata with nan for median computation
    data[data == NODATA] = np.nan
    return data


def build_monthly_composite(
    ndvi_paths: list[Path],
    out_path: Path,
) -> Optional[Path]:
    """
    Build a monthly median composite from a list of NDVI GeoTIFF paths.

    Parameters
    ----------
    ndvi_paths : list[Path]
        Per-scene NDVI rasters (co-registered, same CRS & AOI).
    out_path : Path
        Destination path for the composite GeoTIFF.

    Returns
    -------
    Path or None
        out_path on success, None if no valid inputs.
    """
    if not ndvi_paths:
        log.warning("composite_no_inputs", out=str(out_path))
        return None

    log.info("composite_start", n_scenes=len(ndvi_paths), out=str(out_path))

    # Use first scene as spatial reference
    with rasterio.open(ndvi_paths[0]) as ref:
        ref_profile = ref.profile.copy()
        ref_transform = ref.transform
        ref_crs = ref.crs

    # Stack all scenes into a 3D array [scenes, rows, cols]
    arrays = []
    for p in ndvi_paths:
        try:
            arr = _align_to_reference(p, ref_profile)
            arrays.append(arr)
        except Exception as exc:
            log.warning("composite_scene_skip", path=str(p), error=str(exc))

    if not arrays:
        log.error("composite_all_scenes_failed", out=str(out_path))
        return None

    stack = np.stack(arrays, axis=0)  # [N, H, W]

    # Chunked median to stay within memory limits (avoids large int64 argsort)
    CHUNK = 256
    H, W = stack.shape[1], stack.shape[2]
    composite = np.full((H, W), NODATA, dtype=np.float32)
    all_nan_mask = np.zeros((H, W), dtype=bool)
    for r0 in range(0, H, CHUNK):
        r1 = min(r0 + CHUNK, H)
        slc = stack[:, r0:r1, :]
        all_nan_mask[r0:r1] = np.all(np.isnan(slc), axis=0)
        chunk = np.nanmedian(slc, axis=0).astype(np.float32)
        chunk[all_nan_mask[r0:r1]] = NODATA
        composite[r0:r1] = chunk

    valid_pct = float((~all_nan_mask).sum() / all_nan_mask.size * 100)
    log.info(
        "composite_complete",
        scenes_used=len(arrays),
        valid_pct=round(valid_pct, 1),
        ndvi_mean=float(np.nanmean(composite[composite != NODATA])),
        out=str(out_path),
    )

    # Write composite
    ref_profile.update(
        dtype="float32",
        count=1,
        nodata=NODATA,
        compress="lzw",
        driver="GTiff",
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(out_path, "w", **ref_profile) as dst:
        dst.write(composite, 1)

    return out_path
