-- Spatial indexes (GIST)
CREATE INDEX IF NOT EXISTS raw_imagery_bbox_idx
    ON raw_imagery USING GIST(bbox);

CREATE INDEX IF NOT EXISTS parcels_geom_idx
    ON parcels USING GIST(geom);

CREATE INDEX IF NOT EXISTS processed_ndvi_rast_idx
    ON processed_ndvi USING GIST(ST_ConvexHull(rast));

-- Temporal indexes on parcel_summaries for time-slider queries
CREATE INDEX IF NOT EXISTS parcel_summaries_period_idx
    ON parcel_summaries (period_start, source);

CREATE INDEX IF NOT EXISTS parcel_summaries_pin_idx
    ON parcel_summaries (pin);

-- BRIN index for fast range scans on large time-series table
CREATE INDEX IF NOT EXISTS parcel_summaries_period_brin
    ON parcel_summaries USING BRIN(period_start);

-- Change detection indexes
CREATE INDEX IF NOT EXISTS change_detection_period_idx
    ON change_detection (period_current, source);

-- Partial index for active flags only — fast query for the dashboard
CREATE INDEX IF NOT EXISTS change_flags_active_idx
    ON change_detection (period_current)
    WHERE flagged = TRUE;

-- Source + scene time index for raw_imagery
CREATE INDEX IF NOT EXISTS raw_imagery_source_dt_idx
    ON raw_imagery (source, acquisition_dt DESC);
