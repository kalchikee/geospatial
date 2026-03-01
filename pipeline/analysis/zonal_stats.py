"""
Parcel-level NDVI zonal statistics using rasterstats.

For each Chicago parcel polygon, we compute:
  - mean, median, min, max, std — summary NDVI statistics
  - count — number of pixels
  - valid_pct — percentage of pixels that are not nodata

Results are inserted into parcel_summaries via the vector_loader.

Performance notes:
  - rasterstats processes all parcels in a single pass over the raster.
  - For ~600k Chicago parcels × ~1m-resolution NDVI, this takes ~10–20 min.
  - We run it once per source (Sentinel-2, Landsat) per month.
"""
from datetime import date
from pathlib import Path
from typing import Optional

import geopandas as gpd
import numpy as np
import structlog
from rasterstats import zonal_stats

from config.settings import NODATA, TARGET_CRS
from load.vector_loader import upsert_parcel_summaries
from utils.db import get_connection, get_cursor

log = structlog.get_logger(__name__)


def _load_parcels() -> gpd.GeoDataFrame:
    """Load all Chicago parcels from PostGIS into a GeoDataFrame."""
    from sqlalchemy import text
    from utils.db import get_engine
    engine = get_engine()
    gdf = gpd.read_postgis(
        "SELECT id, pin, geom FROM parcels",
        con=engine,
        geom_col="geom",
        crs=TARGET_CRS,
    )
    return gdf


def _severity(delta: float) -> Optional[str]:
    """Map NDVI delta to severity label."""
    if delta < -0.3:
        return "severe"
    if delta < -0.2:
        return "moderate"
    if delta < -0.1:
        return "minor"
    return None


def compute_zonal_stats(
    composite_tif: Path,
    source: str,
    period_start: date,
) -> int:
    """
    Compute NDVI zonal statistics for all Chicago parcels.

    Parameters
    ----------
    composite_tif : Path
        Monthly NDVI composite GeoTIFF (EPSG:3435, Float32, nodata=-9999).
    source : str
        'sentinel2', 'landsat8', or 'landsat9'.
    period_start : date
        First day of the processing month.

    Returns
    -------
    int
        Number of parcel records inserted.
    """
    log.info("zonal_stats_start", source=source, period=str(period_start), tif=str(composite_tif))

    parcels_gdf = _load_parcels()
    log.info("parcels_loaded_for_stats", count=len(parcels_gdf))

    # rasterstats expects geometry in same CRS as raster
    stats = zonal_stats(
        parcels_gdf,
        str(composite_tif),
        stats=["mean", "median", "min", "max", "std", "count", "nodata"],
        nodata=NODATA,
        all_touched=False,
        geojson_out=False,
    )

    records = []
    for parcel, stat in zip(parcels_gdf.itertuples(), stats):
        total = (stat.get("count") or 0) + (stat.get("nodata") or 0)
        valid_count = stat.get("count") or 0
        valid_pct = (valid_count / total * 100) if total > 0 else 0.0

        records.append({
            "parcel_id": parcel.id,
            "pin": parcel.pin,
            "ndvi_mean":   round(stat["mean"], 4) if stat.get("mean") is not None else None,
            "ndvi_median": round(stat["median"], 4) if stat.get("median") is not None else None,
            "ndvi_min":    round(stat["min"], 4) if stat.get("min") is not None else None,
            "ndvi_max":    round(stat["max"], 4) if stat.get("max") is not None else None,
            "ndvi_std":    round(stat["std"], 4) if stat.get("std") is not None else None,
            "pixel_count": valid_count,
            "valid_pct":   round(valid_pct, 2),
        })

    inserted = upsert_parcel_summaries(records, period_start, source)
    log.info("zonal_stats_complete", inserted=inserted, source=source, period=str(period_start))
    return inserted


def run(year: int, month: int, composite_tif: Path, source: str) -> int:
    """Entry point for pipeline orchestrator."""
    from datetime import date
    period_start = date(year, month, 1)
    return compute_zonal_stats(composite_tif, source, period_start)
