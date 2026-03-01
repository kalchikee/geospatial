"""
Month-over-month vegetation change detection.

For each parcel, compare the current month's NDVI mean against
the prior month's NDVI mean. Flag parcels where the delta falls
below the configured threshold (default: -0.1).

Severity bands:
  minor:    delta in [-0.2, -0.1)
  moderate: delta in [-0.3, -0.2)
  severe:   delta < -0.3

Results are written to the change_detection table.
"""
from datetime import date
from typing import Optional

import structlog

from config.settings import (
    NDVI_CHANGE_THRESHOLD,
    SEVERITY_MINOR,
    SEVERITY_MODERATE,
    SEVERITY_SEVERE,
)
from load.vector_loader import upsert_change_detection
from utils.db import get_connection, get_cursor

log = structlog.get_logger(__name__)


def _get_prior_month(year: int, month: int) -> date:
    """Return the first day of the prior month."""
    if month == 1:
        return date(year - 1, 12, 1)
    return date(year, month - 1, 1)


def _classify_severity(delta: float) -> Optional[str]:
    if delta < SEVERITY_SEVERE:
        return "severe"
    if delta < SEVERITY_MODERATE:
        return "moderate"
    if delta < SEVERITY_MINOR:
        return "minor"
    return None


def detect_changes(year: int, month: int, source: str) -> int:
    """
    Compare current vs prior month parcel NDVI and flag significant declines.

    Parameters
    ----------
    year, month : int
        Current processing period.
    source : str
        'sentinel2', 'landsat8', or 'landsat9'.

    Returns
    -------
    int
        Number of records inserted (flagged + unflagged).
    """
    period_current = date(year, month, 1)
    period_prior = _get_prior_month(year, month)

    log.info(
        "change_detection_start",
        source=source,
        current=str(period_current),
        prior=str(period_prior),
    )

    # Fetch both months' parcel summaries in a single joined query
    query = """
        SELECT
            curr.parcel_id,
            curr.pin,
            curr.ndvi_mean AS ndvi_current,
            prior.ndvi_mean AS ndvi_prior
        FROM parcel_summaries curr
        JOIN parcel_summaries prior
            ON curr.parcel_id = prior.parcel_id
            AND curr.source   = prior.source
        WHERE curr.source       = %s
          AND curr.period_start = %s
          AND prior.period_start = %s
          AND curr.ndvi_mean IS NOT NULL
          AND prior.ndvi_mean IS NOT NULL
    """

    with get_connection() as conn, get_cursor(conn) as cur:
        cur.execute(query, (source, period_current, period_prior))
        rows = cur.fetchall()

    log.info("change_detection_pairs_found", count=len(rows))

    records = []
    for row in rows:
        ndvi_curr = float(row["ndvi_current"])
        ndvi_prior = float(row["ndvi_prior"])
        delta = round(ndvi_curr - ndvi_prior, 4)
        flagged = delta < NDVI_CHANGE_THRESHOLD
        severity = _classify_severity(delta) if flagged else None

        records.append({
            "parcel_id":      row["parcel_id"],
            "pin":            row["pin"],
            "source":         source,
            "period_current": period_current,
            "period_prior":   period_prior,
            "ndvi_current":   ndvi_curr,
            "ndvi_prior":     ndvi_prior,
            "ndvi_delta":     delta,
            "flagged":        flagged,
            "severity":       severity,
        })

    inserted = upsert_change_detection(records)

    flagged_count = sum(1 for r in records if r["flagged"])
    log.info(
        "change_detection_complete",
        total=inserted,
        flagged=flagged_count,
        source=source,
        period=str(period_current),
    )
    return inserted


def run(year: int, month: int, source: str) -> int:
    """Entry point for pipeline orchestrator."""
    return detect_changes(year, month, source)
