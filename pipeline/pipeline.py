"""
Chicago NDVI Monitoring Pipeline — Main Orchestrator

Usage:
    python pipeline.py --month 2024-07
    python pipeline.py --month 2024-07 --source sentinel2
    python pipeline.py --load-parcels          # one-time parcel load
    python pipeline.py --month 2024-07 --dry-run

The pipeline runs in the following order for each satellite source:
  1. Ingest: download scenes from STAC APIs
  2. Process: cloud mask → reproject → clip → NDVI → composite
  3. Load: raster2pgsql into PostGIS
  4. Analyze: zonal statistics per parcel
  5. Detect: change detection vs prior month
"""
import sys
import time
from calendar import monthrange
from datetime import date
from pathlib import Path

import click

from config.settings import SCRATCH_DIR, AWS_CONFIGURED
from utils.logging_config import configure_logging, get_logger

configure_logging("INFO")
log = get_logger("pipeline")

# Landsat sources are only included when AWS credentials are present
SOURCES = ["sentinel2"] + (["landsat8", "landsat9"] if AWS_CONFIGURED else [])


# ---------------------------------------------------------------------------
# Per-scene processing helpers
# ---------------------------------------------------------------------------

def _process_sentinel2_scene(scene: dict, out_dir: Path) -> Path | None:
    """Cloud-mask, reproject, clip, and compute NDVI for one S2 scene."""
    from process.cloud_mask import sentinel2_clear_mask, apply_mask
    from process.reproject import reproject_and_clip
    from process.ndvi import compute_ndvi
    from config.settings import S2_BAND_RED, S2_BAND_NIR, S2_BAND_SCL, NODATA

    item = scene["item"]
    paths = scene["paths"]

    # Cloud mask
    scl_mask = sentinel2_clear_mask(paths[S2_BAND_SCL])

    # Masked arrays → temp tifs
    masked_dir = out_dir / "masked" / item.id
    masked_dir.mkdir(parents=True, exist_ok=True)

    import rasterio
    import numpy as np

    def _write_masked(band_path: Path, suffix: str) -> Path:
        with rasterio.open(band_path) as src:
            data = src.read(1).astype("float32")
            profile = src.profile.copy()
        masked = apply_mask(data, scl_mask, nodata=NODATA)
        dest = masked_dir / f"{item.id}_{suffix}_masked.tif"
        profile.update(dtype="float32", nodata=NODATA, compress="lzw")
        with rasterio.open(dest, "w", **profile) as dst:
            dst.write(masked, 1)
        return dest

    red_masked = _write_masked(paths[S2_BAND_RED], "red")
    nir_masked = _write_masked(paths[S2_BAND_NIR], "nir")

    # Reproject + clip
    clip_dir = out_dir / "clipped" / item.id
    red_clipped = reproject_and_clip(red_masked, clip_dir, "_red")
    nir_clipped = reproject_and_clip(nir_masked, clip_dir, "_nir")

    # NDVI
    ndvi_path = out_dir / "ndvi" / f"{item.id}_ndvi.tif"
    ndvi_path.parent.mkdir(parents=True, exist_ok=True)
    result = compute_ndvi(red_clipped, nir_clipped, ndvi_path)

    # Clean up intermediates
    for p in [red_masked, nir_masked, red_clipped, nir_clipped]:
        p.unlink(missing_ok=True)

    return result


def _process_landsat_scene(scene: dict, out_dir: Path) -> Path | None:
    """Cloud-mask, reproject, clip, and compute NDVI for one Landsat scene."""
    from process.cloud_mask import landsat_clear_mask, apply_mask
    from process.reproject import reproject_and_clip
    from process.ndvi import compute_ndvi
    from config.settings import LS_BAND_RED, LS_BAND_NIR, LS_BAND_QA, NODATA

    item = scene["item"]
    paths = scene["paths"]

    qa_mask = landsat_clear_mask(paths[LS_BAND_QA])

    masked_dir = out_dir / "masked" / item.id
    masked_dir.mkdir(parents=True, exist_ok=True)

    import rasterio
    import numpy as np

    def _write_masked(band_path: Path, suffix: str) -> Path:
        with rasterio.open(band_path) as src:
            data = src.read(1).astype("float32")
            profile = src.profile.copy()
        masked = apply_mask(data, qa_mask, nodata=NODATA)
        dest = masked_dir / f"{item.id}_{suffix}_masked.tif"
        profile.update(dtype="float32", nodata=NODATA, compress="lzw")
        with rasterio.open(dest, "w", **profile) as dst:
            dst.write(masked, 1)
        return dest

    red_masked = _write_masked(paths[LS_BAND_RED], "red")
    nir_masked = _write_masked(paths[LS_BAND_NIR], "nir")

    clip_dir = out_dir / "clipped" / item.id
    red_clipped = reproject_and_clip(red_masked, clip_dir, "_red")
    nir_clipped = reproject_and_clip(nir_masked, clip_dir, "_nir")

    ndvi_path = out_dir / "ndvi" / f"{item.id}_ndvi.tif"
    ndvi_path.parent.mkdir(parents=True, exist_ok=True)
    result = compute_ndvi(red_clipped, nir_clipped, ndvi_path)

    for p in [red_masked, nir_masked, red_clipped, nir_clipped]:
        p.unlink(missing_ok=True)

    return result


# ---------------------------------------------------------------------------
# Per-source pipeline
# ---------------------------------------------------------------------------

def run_source_pipeline(
    year: int,
    month: int,
    source: str,
    dry_run: bool = False,
) -> dict:
    """
    Run the full pipeline for one satellite source for a given month.
    Returns a metrics dict.
    """
    t0 = time.time()
    metrics = {
        "source": source,
        "year": year,
        "month": month,
        "scenes_ingested": 0,
        "scenes_processed": 0,
        "composite_created": False,
        "raster_loaded": False,
        "parcels_computed": 0,
        "changes_detected": 0,
        "errors": [],
    }

    out_dir = SCRATCH_DIR / source / f"{year}-{month:02d}"
    out_dir.mkdir(parents=True, exist_ok=True)

    # --- 1. Ingest ---
    try:
        if source == "sentinel2":
            from ingest import sentinel2 as ingest_mod
            scenes = ingest_mod.run(year, month)
        else:
            from ingest import landsat as ingest_mod
            scenes = [s for s in ingest_mod.run(year, month)
                      if s["platform"] == source]
        metrics["scenes_ingested"] = len(scenes)
    except Exception as exc:
        log.error("ingest_failed", source=source, error=str(exc))
        metrics["errors"].append(f"ingest: {exc}")
        return metrics

    if not scenes:
        log.warning("no_scenes_found", source=source, year=year, month=month)
        return metrics

    if dry_run:
        log.info("dry_run_stop_after_ingest", source=source)
        return metrics

    # --- 2. Process (per scene) ---
    ndvi_paths = []
    for scene in scenes:
        try:
            if source == "sentinel2":
                ndvi_path = _process_sentinel2_scene(scene, out_dir)
            else:
                ndvi_path = _process_landsat_scene(scene, out_dir)

            if ndvi_path and ndvi_path.exists():
                ndvi_paths.append(ndvi_path)
                metrics["scenes_processed"] += 1
        except Exception as exc:
            scene_id = scene["item"].id
            log.error("scene_processing_failed", scene=scene_id, error=str(exc))
            metrics["errors"].append(f"process:{scene_id}: {exc}")

    if not ndvi_paths:
        log.error("no_ndvi_outputs", source=source)
        metrics["errors"].append("No NDVI outputs produced")
        return metrics

    # --- 3. Composite ---
    from process.composite import build_monthly_composite

    composite_path = out_dir / f"ndvi_composite_{source}_{year}-{month:02d}.tif"
    composite = build_monthly_composite(ndvi_paths, composite_path)
    if composite is None:
        metrics["errors"].append("Composite build failed")
        return metrics
    metrics["composite_created"] = True

    # --- 4. Load to PostGIS ---
    from load.raster_loader import load_raster_to_postgis, mark_scenes_processed

    _, last_day = monthrange(year, month)
    period_start = date(year, month, 1)
    period_end = date(year, month, last_day)

    loaded = load_raster_to_postgis(composite_path, source, period_start, period_end)
    metrics["raster_loaded"] = loaded

    scene_ids = [s["item"].id for s in scenes]
    mark_scenes_processed(scene_ids)

    # --- 5. Zonal statistics ---
    from analysis.zonal_stats import run as zonal_run
    try:
        parcels_computed = zonal_run(year, month, composite_path, source)
        metrics["parcels_computed"] = parcels_computed
    except Exception as exc:
        log.error("zonal_stats_failed", source=source, error=str(exc))
        metrics["errors"].append(f"zonal_stats: {exc}")

    # --- 6. Change detection ---
    from analysis.change_detection import run as change_run
    try:
        changes = change_run(year, month, source)
        metrics["changes_detected"] = changes
    except Exception as exc:
        log.error("change_detection_failed", source=source, error=str(exc))
        metrics["errors"].append(f"change_detection: {exc}")

    metrics["elapsed_seconds"] = round(time.time() - t0, 1)
    log.info("source_pipeline_complete", **metrics)
    return metrics


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@click.command()
@click.option("--month", required=False, help="Processing month as YYYY-MM")
@click.option(
    "--source",
    default="sentinel2",
    type=click.Choice(["all", "sentinel2", "landsat8", "landsat9"]),
    show_default=True,
    help="Satellite source. 'all' runs every configured source (Landsat requires AWS creds).",
)
@click.option("--load-parcels", is_flag=True, default=False,
              help="Load Cook County parcel data (run once or monthly refresh)")
@click.option("--dry-run", is_flag=True, default=False,
              help="Ingest metadata only; skip processing and loading")
def main(month: str, source: str, load_parcels: bool, dry_run: bool):
    """Chicago NDVI Monitoring Pipeline."""

    # Parcel load (idempotent, can be run standalone)
    if load_parcels:
        log.info("parcel_load_start")
        from ingest.parcels import run as parcel_run
        count = parcel_run()
        log.info("parcel_load_done", count=count)
        if not month:
            return

    if not month:
        click.echo("Error: --month is required unless using --load-parcels only.")
        sys.exit(1)

    try:
        year_val, month_val = int(month[:4]), int(month[5:7])
    except (ValueError, IndexError):
        click.echo(f"Error: --month must be in YYYY-MM format, got: {month}")
        sys.exit(1)

    if source == "all":
        sources_to_run = SOURCES
    else:
        if source in ("landsat8", "landsat9") and not AWS_CONFIGURED:
            click.echo(
                f"Error: {source} requires AWS credentials (AWS_ACCESS_KEY_ID / "
                "AWS_SECRET_ACCESS_KEY) in your .env file."
            )
            sys.exit(1)
        sources_to_run = [source]

    log.info("active_sources", sources=sources_to_run, aws_configured=AWS_CONFIGURED)
    all_metrics = []

    for src in sources_to_run:
        log.info("pipeline_start", source=src, month=month, dry_run=dry_run)
        m = run_source_pipeline(year_val, month_val, src, dry_run=dry_run)
        all_metrics.append(m)

    # Summary
    log.info(
        "pipeline_run_complete",
        month=month,
        sources=sources_to_run,
        total_scenes=sum(m["scenes_ingested"] for m in all_metrics),
        total_parcels=sum(m["parcels_computed"] for m in all_metrics),
        total_changes=sum(m["changes_detected"] for m in all_metrics),
        errors=[e for m in all_metrics for e in m["errors"]],
    )


if __name__ == "__main__":
    main()
