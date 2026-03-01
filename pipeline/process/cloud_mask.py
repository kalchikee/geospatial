"""
Cloud masking for Sentinel-2 and Landsat imagery.

Sentinel-2: Uses the Scene Classification Layer (SCL).
  Clear pixels: SCL 4 (Vegetation) and 5 (Bare Soil).
  All other classes (cloud, shadow, water, snow, etc.) are masked.

Landsat: Uses the QA_PIXEL band (Collection 2 Level-2).
  Cloud bit:        bit 3
  Cloud shadow bit: bit 4
  Pixels with either bit set are masked.

Both functions return a boolean mask array where True = clear (keep).
"""
from pathlib import Path

import numpy as np
import rasterio
import structlog

from config.settings import S2_CLEAR_SCL_VALUES, LS_QA_CLOUD_BIT, LS_QA_CLOUD_SHADOW_BIT

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Sentinel-2 cloud mask
# ---------------------------------------------------------------------------

def sentinel2_clear_mask(scl_path: Path) -> np.ndarray:
    """
    Read SCL GeoTIFF and return a boolean clear-pixel mask.
    Shape matches the SCL raster (20m resolution).
    True = clear pixel, False = masked.
    """
    with rasterio.open(scl_path) as src:
        scl = src.read(1)

    mask = np.zeros(scl.shape, dtype=bool)
    for val in S2_CLEAR_SCL_VALUES:
        mask |= (scl == val)

    clear_pct = mask.sum() / mask.size * 100
    log.info("s2_cloud_mask_computed", clear_pct=round(clear_pct, 1), path=str(scl_path))
    return mask


# ---------------------------------------------------------------------------
# Landsat cloud mask
# ---------------------------------------------------------------------------

def landsat_clear_mask(qa_path: Path) -> np.ndarray:
    """
    Read QA_PIXEL GeoTIFF and return a boolean clear-pixel mask.
    True = clear pixel (no cloud or shadow).
    """
    with rasterio.open(qa_path) as src:
        qa = src.read(1).astype(np.uint16)

    cloud_bit = 1 << LS_QA_CLOUD_BIT
    shadow_bit = 1 << LS_QA_CLOUD_SHADOW_BIT
    mask = ((qa & cloud_bit) == 0) & ((qa & shadow_bit) == 0)

    clear_pct = mask.sum() / mask.size * 100
    log.info("landsat_cloud_mask_computed", clear_pct=round(clear_pct, 1), path=str(qa_path))
    return mask


# ---------------------------------------------------------------------------
# Apply mask to a band array
# ---------------------------------------------------------------------------

def apply_mask(band_array: np.ndarray, mask: np.ndarray, nodata: float = np.nan) -> np.ndarray:
    """
    Apply a boolean clear mask to a band array.
    Masked pixels (mask=False) are set to nodata.

    The mask may be a different resolution than band_array (e.g. SCL at 20m,
    bands at 10m). In that case the mask is upsampled with nearest-neighbour.
    """
    if mask.shape != band_array.shape:
        from skimage.transform import resize
        mask = resize(
            mask.astype(np.uint8),
            band_array.shape,
            order=0,
            preserve_range=True,
            anti_aliasing=False,
        ).astype(bool)

    result = band_array.astype(np.float32)
    result[~mask] = nodata
    return result
