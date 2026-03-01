"""
Chicago NDVI Monitoring Pipeline — FastAPI Backend

Endpoints:
  GET /health                              — liveness probe
  GET /periods                             — list available processing periods
  GET /ndvi/stats?period=2024-07           — citywide NDVI statistics for a period
  GET /parcels/geojson?period=2024-07&source=sentinel2
                                           — parcel NDVI summaries as GeoJSON
  GET /changes?period=2024-07&source=sentinel2&severity=severe
                                           — flagged parcels as GeoJSON
  GET /parcels/{pin}/history               — NDVI time series for a single parcel

All geometry responses are in EPSG:4326 (WGS84) for Leaflet compatibility.
"""
import os
from datetime import date
from typing import Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgres://ndvi_user:password@localhost:5432/chicago_ndvi",
)

app = FastAPI(
    title="Chicago NDVI Monitoring API",
    description="Serves parcel-level NDVI summaries and change detection results.",
    version="1.0.0",
)

# Allow all origins for the local Leaflet frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# DB helper
# ---------------------------------------------------------------------------

def _conn():
    """Return a new psycopg2 connection."""
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def _query(sql: str, params: tuple = ()) -> list[dict]:
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/periods")
def list_periods():
    """Return all available processing periods (month × source combinations)."""
    rows = _query(
        """
        SELECT DISTINCT period_start, source
        FROM parcel_summaries
        ORDER BY period_start DESC, source
        """
    )
    return [
        {"period": r["period_start"].strftime("%Y-%m"), "source": r["source"]}
        for r in rows
    ]


@app.get("/ndvi/stats")
def ndvi_stats(
    period: str = Query(..., description="YYYY-MM"),
    source: str = Query("sentinel2"),
):
    """Return citywide NDVI summary statistics for a processing period."""
    try:
        year, month = int(period[:4]), int(period[5:7])
        period_start = date(year, month, 1)
    except (ValueError, IndexError):
        raise HTTPException(400, "period must be YYYY-MM")

    rows = _query(
        """
        SELECT
            count(*)                    AS parcel_count,
            avg(ndvi_mean)              AS city_mean,
            percentile_cont(0.5)
                WITHIN GROUP (ORDER BY ndvi_mean) AS city_median,
            min(ndvi_mean)              AS city_min,
            max(ndvi_mean)              AS city_max,
            stddev(ndvi_mean)           AS city_std,
            sum(CASE WHEN ndvi_mean > 0.3 THEN 1 ELSE 0 END) AS high_veg_count,
            sum(CASE WHEN ndvi_mean < 0.1 THEN 1 ELSE 0 END) AS low_veg_count
        FROM parcel_summaries
        WHERE period_start = %s AND source = %s AND ndvi_mean IS NOT NULL
        """,
        (period_start, source),
    )

    if not rows:
        raise HTTPException(404, f"No data for period={period} source={source}")

    r = rows[0]
    return {
        "period": period,
        "source": source,
        "parcel_count":  int(r["parcel_count"]),
        "city_mean":     float(r["city_mean"]) if r["city_mean"] else None,
        "city_median":   float(r["city_median"]) if r["city_median"] else None,
        "city_min":      float(r["city_min"]) if r["city_min"] else None,
        "city_max":      float(r["city_max"]) if r["city_max"] else None,
        "city_std":      float(r["city_std"]) if r["city_std"] else None,
        "high_veg_count": int(r["high_veg_count"]),
        "low_veg_count":  int(r["low_veg_count"]),
    }


@app.get("/parcels/geojson")
def parcels_geojson(
    period: str = Query(..., description="YYYY-MM"),
    source: str = Query("sentinel2"),
    limit: int = Query(10000, le=50000),
):
    """
    Return parcel polygons with NDVI statistics as GeoJSON FeatureCollection.
    Geometry is reprojected to WGS84 for Leaflet.
    """
    try:
        year, month = int(period[:4]), int(period[5:7])
        period_start = date(year, month, 1)
    except (ValueError, IndexError):
        raise HTTPException(400, "period must be YYYY-MM")

    rows = _query(
        """
        SELECT
            p.pin,
            p.address,
            ps.ndvi_mean,
            ps.ndvi_median,
            ps.ndvi_std,
            ps.pixel_count,
            ps.valid_pct,
            ST_AsGeoJSON(ST_Transform(p.geom, 4326)) AS geometry
        FROM parcel_summaries ps
        JOIN parcels p ON p.id = ps.parcel_id
        WHERE ps.period_start = %s
          AND ps.source = %s
          AND ps.ndvi_mean IS NOT NULL
        LIMIT %s
        """,
        (period_start, source, limit),
    )

    features = [
        {
            "type": "Feature",
            "geometry": __import__("json").loads(r["geometry"]),
            "properties": {
                "pin":        r["pin"],
                "address":    r["address"],
                "ndvi_mean":  float(r["ndvi_mean"]) if r["ndvi_mean"] else None,
                "ndvi_median": float(r["ndvi_median"]) if r["ndvi_median"] else None,
                "ndvi_std":   float(r["ndvi_std"]) if r["ndvi_std"] else None,
                "pixel_count": r["pixel_count"],
                "valid_pct":  float(r["valid_pct"]) if r["valid_pct"] else None,
            },
        }
        for r in rows
    ]

    return {"type": "FeatureCollection", "features": features}


@app.get("/changes")
def changes_geojson(
    period: str = Query(..., description="YYYY-MM"),
    source: str = Query("sentinel2"),
    severity: Optional[str] = Query(None, description="minor | moderate | severe"),
):
    """Return flagged change-detection parcels as GeoJSON."""
    try:
        year, month = int(period[:4]), int(period[5:7])
        period_current = date(year, month, 1)
    except (ValueError, IndexError):
        raise HTTPException(400, "period must be YYYY-MM")

    severity_filter = "AND cd.severity = %s" if severity else ""
    params = [period_current, source]
    if severity:
        params.append(severity)

    rows = _query(
        f"""
        SELECT
            cd.pin,
            cd.ndvi_current,
            cd.ndvi_prior,
            cd.ndvi_delta,
            cd.severity,
            cd.period_prior,
            p.address,
            ST_AsGeoJSON(ST_Transform(p.geom, 4326)) AS geometry
        FROM change_detection cd
        JOIN parcels p ON p.id = cd.parcel_id
        WHERE cd.period_current = %s
          AND cd.source = %s
          AND cd.flagged = TRUE
          {severity_filter}
        ORDER BY cd.ndvi_delta ASC
        """,
        tuple(params),
    )

    features = [
        {
            "type": "Feature",
            "geometry": __import__("json").loads(r["geometry"]),
            "properties": {
                "pin":          r["pin"],
                "address":      r["address"],
                "ndvi_current": float(r["ndvi_current"]),
                "ndvi_prior":   float(r["ndvi_prior"]),
                "ndvi_delta":   float(r["ndvi_delta"]),
                "severity":     r["severity"],
                "period_prior": str(r["period_prior"]),
            },
        }
        for r in rows
    ]

    return {"type": "FeatureCollection", "features": features}


@app.get("/parcels/{pin}/history")
def parcel_history(pin: str, source: str = Query("sentinel2")):
    """Return the full NDVI time series for a single parcel PIN."""
    rows = _query(
        """
        SELECT period_start, ndvi_mean, ndvi_median, ndvi_std, pixel_count
        FROM parcel_summaries
        WHERE pin = %s AND source = %s AND ndvi_mean IS NOT NULL
        ORDER BY period_start ASC
        """,
        (pin, source),
    )

    if not rows:
        raise HTTPException(404, f"No data found for PIN {pin}")

    return {
        "pin": pin,
        "source": source,
        "history": [
            {
                "period":     r["period_start"].strftime("%Y-%m"),
                "ndvi_mean":  float(r["ndvi_mean"]),
                "ndvi_median": float(r["ndvi_median"]) if r["ndvi_median"] else None,
                "ndvi_std":   float(r["ndvi_std"]) if r["ndvi_std"] else None,
                "pixel_count": r["pixel_count"],
            }
            for r in rows
        ],
    }
