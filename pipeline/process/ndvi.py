"""
NDVI computation: (NIR - Red) / (NIR + Red)

Accepts two co-registered, reprojected, cloud-masked GeoTIFF paths.
Output is a Float32 GeoTIFF clipped to [-1.0, 1.0] with nodata=-9999.

Handles divide-by-zero gracefully using numpy masked arrays.
"""
from pathlib import Path

import numpy as np
import rasterio
import structlog

from config.settings import NODATA

log = structlog.get_logger(__name__)


def compute_ndvi(red_path: Path, nir_path: Path, out_path: Path) -> Path:
    """
    Compute NDVI from co-registered Red and NIR GeoTIFFs.

    Parameters
    ----------
    red_path : Path
        Red band GeoTIFF (cloud-masked, reprojected, clipped).
    nir_path : Path
        NIR band GeoTIFF (cloud-masked, reprojected, clipped).
    out_path : Path
        Destination path for the NDVI GeoTIFF.

    Returns
    -------
    Path
        out_path on success.
    """
    with rasterio.open(red_path) as r_src, rasterio.open(nir_path) as n_src:
        red = r_src.read(1).astype(np.float32)
        nir = n_src.read(1).astype(np.float32)
        profile = r_src.profile.copy()
        nodata_val = r_src.nodata if r_src.nodata is not None else NODATA

    # Build validity mask: pixel is valid where neither band is nodata
    valid = (red != nodata_val) & (nir != nodata_val)

    # Replace nodata with nan for safe arithmetic
    red = np.where(valid, red, np.nan)
    nir = np.where(valid, nir, np.nan)

    # Scale integer DN to surface reflectance if values are large (Landsat uses scale 0.0000275)
    if np.nanmax(nir) > 2.0:
        # Landsat Collection 2 SR scale factor
        red = red * 0.0000275 - 0.2
        nir = nir * 0.0000275 - 0.2

    # Compute NDVI with masked arrays to handle divide-by-zero
    denom = nir + red
    with np.errstate(invalid="ignore", divide="ignore"):
        ndvi = np.where(
            (denom != 0) & valid,
            (nir - red) / denom,
            np.nan,
        )

    # Clip to physically valid NDVI range
    ndvi = np.clip(ndvi, -1.0, 1.0)

    # Replace nan with nodata sentinel
    ndvi_out = np.where(np.isnan(ndvi), NODATA, ndvi).astype(np.float32)

    valid_pixels = int(valid.sum())
    ndvi_valid = ndvi[valid]
    log.info(
        "ndvi_computed",
        valid_pixels=valid_pixels,
        ndvi_mean=float(np.nanmean(ndvi_valid)) if valid_pixels else None,
        ndvi_min=float(np.nanmin(ndvi_valid)) if valid_pixels else None,
        ndvi_max=float(np.nanmax(ndvi_valid)) if valid_pixels else None,
        out=str(out_path),
    )

    # Write output
    profile.update(
        dtype="float32",
        count=1,
        nodata=NODATA,
        compress="lzw",
        driver="GTiff",
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(out_path, "w", **profile) as dst:
        dst.write(ndvi_out, 1)

    return out_path
