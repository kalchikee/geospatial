"""
Load NDVI composite rasters into PostGIS using raster2pgsql.

raster2pgsql converts a GeoTIFF into SQL INSERT statements.
We pipe its output directly into psql via subprocess, avoiding
the need to materialise a large SQL file on disk.

Flags used:
  -I   Create a GiST spatial index on the raster column.
  -C   Apply raster constraints (validates srid, resolution, etc.).
  -M   Vacuum the table after insert.
  -t   Tile size: 256x256 pixels per row.
  -a   Append mode (add rows to existing table).
  -s   Source SRID (overrides GeoTIFF embedded SRID if wrong).
"""
import subprocess
from calendar import monthrange
from datetime import date
from pathlib import Path

import structlog

from config.settings import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS, TARGET_CRS
from utils.db import get_connection, get_cursor

log = structlog.get_logger(__name__)

# Extract numeric SRID from "EPSG:3435"
TARGET_SRID = int(TARGET_CRS.split(":")[1])


def load_raster_to_postgis(
    tif_path: Path,
    source: str,
    period_start: date,
    period_end: date,
    table: str = "processed_ndvi",
    tile_size: str = "256x256",
) -> bool:
    """
    Load a composite NDVI GeoTIFF into the processed_ndvi table.

    Steps:
    1. Delete any existing row for (source, period_start).
    2. Run raster2pgsql | psql to insert tiled raster rows.
    3. Update the period_start/period_end columns on the inserted rows
       (raster2pgsql only fills the rast column; metadata columns are set via UPDATE).

    Returns True on success.
    """
    log.info("raster_load_start", tif=str(tif_path), source=source, period=str(period_start))

    # 1. Remove stale data
    with get_connection() as conn, get_cursor(conn) as cur:
        cur.execute(
            "DELETE FROM processed_ndvi WHERE source = %s AND period_start = %s",
            (source, period_start),
        )

    # 2. raster2pgsql → psql
    r2p_cmd = [
        "raster2pgsql",
        "-a",            # append to existing table
        "-I",            # create spatial index
        "-C",            # apply constraints
        "-M",            # vacuum after insert
        "-t", tile_size,
        f"-s", str(TARGET_SRID),
        str(tif_path),
        f"public.{table}",
    ]

    psql_cmd = [
        "psql",
        f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASS}",
    ]

    try:
        r2p = subprocess.Popen(r2p_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        psql = subprocess.Popen(
            psql_cmd, stdin=r2p.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        r2p.stdout.close()
        psql_out, psql_err = psql.communicate(timeout=600)
        r2p.wait()

        if r2p.returncode != 0:
            _, r2p_err = r2p.communicate()
            log.error("raster2pgsql_failed", returncode=r2p.returncode, stderr=r2p_err.decode())
            return False

        if psql.returncode != 0:
            log.error("psql_failed", returncode=psql.returncode, stderr=psql_err.decode())
            return False

    except subprocess.TimeoutExpired:
        log.error("raster_load_timeout", tif=str(tif_path))
        psql.kill()
        r2p.kill()
        return False

    # 3. Backfill metadata columns
    with get_connection() as conn, get_cursor(conn) as cur:
        cur.execute(
            """
            UPDATE processed_ndvi
            SET source = %s, period_start = %s, period_end = %s, nodata_value = -9999
            WHERE source IS NULL AND period_start IS NULL
            """,
            (source, period_start, period_end),
        )

    log.info("raster_load_complete", source=source, period=str(period_start))
    return True


def mark_scenes_processed(scene_ids: list[str]) -> None:
    """Set processed=TRUE for the given scene IDs in raw_imagery."""
    if not scene_ids:
        return
    with get_connection() as conn, get_cursor(conn) as cur:
        cur.execute(
            "UPDATE raw_imagery SET processed = TRUE WHERE scene_id = ANY(%s)",
            (scene_ids,),
        )
