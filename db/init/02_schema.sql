-- ================================================================
-- Raw imagery scene metadata
-- No raster data stored here — only download paths and STAC refs
-- ================================================================
CREATE TABLE IF NOT EXISTS raw_imagery (
    id              SERIAL PRIMARY KEY,
    source          TEXT NOT NULL CHECK (source IN ('sentinel2', 'landsat8', 'landsat9')),
    scene_id        TEXT NOT NULL UNIQUE,
    acquisition_dt  TIMESTAMPTZ NOT NULL,
    cloud_cover     NUMERIC(5,2),
    bbox            GEOMETRY(POLYGON, 4326),
    stac_href       TEXT,           -- STAC item self href
    local_path      TEXT,           -- scratch path inside container after download
    ingested_at     TIMESTAMPTZ DEFAULT NOW(),
    processed       BOOLEAN DEFAULT FALSE
);

-- ================================================================
-- Monthly NDVI rasters (one row per source per month after composite)
-- Tiled 256x256 by raster2pgsql -t 256x256
-- ================================================================
CREATE TABLE IF NOT EXISTS processed_ndvi (
    id              SERIAL PRIMARY KEY,
    source          TEXT NOT NULL CHECK (source IN ('sentinel2', 'landsat8', 'landsat9', 'composite')),
    period_start    DATE NOT NULL,   -- first day of the month
    period_end      DATE NOT NULL,   -- last day of the month
    rast            RASTER,
    nodata_value    NUMERIC DEFAULT -9999,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (source, period_start)
);

-- ================================================================
-- Chicago / Cook County parcels
-- Sourced from Cook County Assessor via Socrata API
-- Stored in EPSG:3435 (IL State Plane East) to match rasters
-- ================================================================
CREATE TABLE IF NOT EXISTS parcels (
    id              SERIAL PRIMARY KEY,
    pin             TEXT NOT NULL UNIQUE,    -- 14-digit Property Index Number
    address         TEXT,
    class_code      TEXT,
    geom            GEOMETRY(MULTIPOLYGON, 3435) NOT NULL,
    loaded_at       TIMESTAMPTZ DEFAULT NOW()
);

-- ================================================================
-- Parcel-level NDVI summary statistics
-- One row per parcel × source × month
-- ================================================================
CREATE TABLE IF NOT EXISTS parcel_summaries (
    id              SERIAL PRIMARY KEY,
    parcel_id       INTEGER REFERENCES parcels(id) ON DELETE CASCADE,
    pin             TEXT NOT NULL,
    source          TEXT NOT NULL,
    period_start    DATE NOT NULL,
    ndvi_mean       NUMERIC(6,4),
    ndvi_median     NUMERIC(6,4),
    ndvi_min        NUMERIC(6,4),
    ndvi_max        NUMERIC(6,4),
    ndvi_std        NUMERIC(6,4),
    pixel_count     INTEGER,
    valid_pct       NUMERIC(5,2),    -- percentage of non-nodata pixels
    computed_at     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (parcel_id, source, period_start)
);

-- ================================================================
-- Vegetation change detection flags
-- One row per parcel × source × month (current vs prior month)
-- ================================================================
CREATE TABLE IF NOT EXISTS change_detection (
    id              SERIAL PRIMARY KEY,
    parcel_id       INTEGER REFERENCES parcels(id) ON DELETE CASCADE,
    pin             TEXT NOT NULL,
    source          TEXT NOT NULL,
    period_current  DATE NOT NULL,
    period_prior    DATE NOT NULL,
    ndvi_current    NUMERIC(6,4),
    ndvi_prior      NUMERIC(6,4),
    ndvi_delta      NUMERIC(6,4),       -- current - prior (negative = decline)
    flagged         BOOLEAN NOT NULL,
    severity        TEXT CHECK (severity IN ('minor', 'moderate', 'severe')),
    detected_at     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (parcel_id, source, period_current)
);

-- ================================================================
-- pg_tileserv view: flagged parcels with geometry for tile serving
-- pg_tileserv auto-discovers this and serves vector tiles
-- ================================================================
CREATE OR REPLACE VIEW public.change_flags_view AS
SELECT
    cd.id,
    cd.pin,
    cd.source,
    cd.period_current,
    cd.ndvi_current,
    cd.ndvi_prior,
    cd.ndvi_delta,
    cd.severity,
    p.address,
    p.geom
FROM change_detection cd
JOIN parcels p ON p.id = cd.parcel_id
WHERE cd.flagged = TRUE;

COMMENT ON VIEW public.change_flags_view IS 'Parcels with significant NDVI decline — served as vector tiles by pg_tileserv';
