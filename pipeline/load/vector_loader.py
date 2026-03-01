"""
Vector data loading utilities using geopandas + SQLAlchemy.

Used for bulk-inserting analysis results (parcel_summaries, change_detection)
back into PostGIS after computation.
"""
from datetime import date

import geopandas as gpd
import pandas as pd
import structlog

from utils.db import get_connection, get_cursor, get_engine

log = structlog.get_logger(__name__)


def upsert_parcel_summaries(records: list[dict], period_start: date, source: str) -> int:
    """
    Bulk insert parcel summary records.
    Uses DELETE + INSERT for simplicity (table is append-only by period).

    Parameters
    ----------
    records : list[dict]
        Each dict has keys: parcel_id, pin, ndvi_mean, ndvi_median,
        ndvi_min, ndvi_max, ndvi_std, pixel_count, valid_pct.
    period_start : date
    source : str

    Returns
    -------
    int
        Number of rows inserted.
    """
    if not records:
        return 0

    with get_connection() as conn, get_cursor(conn) as cur:
        # Remove existing rows for this period × source
        cur.execute(
            "DELETE FROM parcel_summaries WHERE period_start = %s AND source = %s",
            (period_start, source),
        )

        insert_sql = """
            INSERT INTO parcel_summaries
                (parcel_id, pin, source, period_start,
                 ndvi_mean, ndvi_median, ndvi_min, ndvi_max, ndvi_std,
                 pixel_count, valid_pct)
            VALUES
                (%(parcel_id)s, %(pin)s, %(source)s, %(period_start)s,
                 %(ndvi_mean)s, %(ndvi_median)s, %(ndvi_min)s, %(ndvi_max)s, %(ndvi_std)s,
                 %(pixel_count)s, %(valid_pct)s)
            ON CONFLICT (parcel_id, source, period_start) DO NOTHING
        """

        rows = [
            {**r, "source": source, "period_start": period_start}
            for r in records
        ]
        cur.executemany(insert_sql, rows)
        count = len(rows)

    log.info("parcel_summaries_loaded", count=count, source=source, period=str(period_start))
    return count


def upsert_change_detection(records: list[dict]) -> int:
    """
    Bulk insert change detection records.

    Parameters
    ----------
    records : list[dict]
        Keys: parcel_id, pin, source, period_current, period_prior,
              ndvi_current, ndvi_prior, ndvi_delta, flagged, severity.

    Returns
    -------
    int
        Number of rows inserted.
    """
    if not records:
        return 0

    insert_sql = """
        INSERT INTO change_detection
            (parcel_id, pin, source, period_current, period_prior,
             ndvi_current, ndvi_prior, ndvi_delta, flagged, severity)
        VALUES
            (%(parcel_id)s, %(pin)s, %(source)s, %(period_current)s, %(period_prior)s,
             %(ndvi_current)s, %(ndvi_prior)s, %(ndvi_delta)s, %(flagged)s, %(severity)s)
        ON CONFLICT (parcel_id, source, period_current) DO UPDATE SET
            ndvi_current = EXCLUDED.ndvi_current,
            ndvi_prior   = EXCLUDED.ndvi_prior,
            ndvi_delta   = EXCLUDED.ndvi_delta,
            flagged      = EXCLUDED.flagged,
            severity     = EXCLUDED.severity,
            detected_at  = NOW()
    """

    with get_connection() as conn, get_cursor(conn) as cur:
        cur.executemany(insert_sql, records)

    count = len(records)
    flagged = sum(1 for r in records if r.get("flagged"))
    log.info("change_detection_loaded", total=count, flagged=flagged)
    return count
