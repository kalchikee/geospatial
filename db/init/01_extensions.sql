-- Enable PostGIS core and raster extensions
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_raster;
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- Set raster out-of-db flag (rasters stored in-db for portability)
SET postgis.gdal_enabled_drivers = 'ENABLE_ALL';
ALTER DATABASE chicago_ndvi SET postgis.gdal_enabled_drivers TO 'ENABLE_ALL';
ALTER DATABASE chicago_ndvi SET postgis.enable_outdb_rasters TO FALSE;
