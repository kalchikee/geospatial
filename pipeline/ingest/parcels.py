"""
Chicago Community Area ingestion from the City of Chicago Open Data Portal.

Source: City of Chicago — Community Areas (Boundaries)
URL: https://data.cityofchicago.org/resource/igwz-8jzy.geojson

Using community areas (77 polygons) as the spatial analysis unit.
Each area is stored in EPSG:3435 (IL State Plane East) to match the raster CRS.
Uses upsert on community_area number to allow re-running safely.
"""
import geopandas as gpd
import requests
import structlog
from shapely.geometry import MultiPolygon, Polygon

from config.settings import TARGET_CRS
from utils.db import get_engine, get_connection, get_cursor

log = structlog.get_logger(__name__)

COMMUNITY_AREAS_URL = (
    "https://data.cityofchicago.org/resource/igwz-8jzy.geojson"
    "?$limit=100"
)


def fetch_parcels() -> gpd.GeoDataFrame:
    """
    Download Chicago community area boundaries from the City of Chicago Data Portal.
    Returns a GeoDataFrame in TARGET_CRS with columns:
      pin, address, class_code, geometry
    where pin = community area number (matches schema).
    """
    log.info("parcels_fetch_start")

    resp = requests.get(COMMUNITY_AREAS_URL, timeout=60)
    resp.raise_for_status()

    gdf = gpd.read_file(resp.text)

    if gdf.empty:
        raise RuntimeError("No community areas returned from Chicago Data Portal")

    # Normalize: map community area fields to the parcels schema columns
    # area_numbe = community area number (1–77), used as PIN
    if "area_numbe" in gdf.columns:
        gdf = gdf.rename(columns={"area_numbe": "pin"})
    elif "community_area" in gdf.columns:
        gdf = gdf.rename(columns={"community_area": "pin"})

    if "community" in gdf.columns:
        gdf = gdf.rename(columns={"community": "address"})

    gdf["pin"] = gdf["pin"].astype(str).str.zfill(2)
    gdf["class_code"] = "community_area"

    # Ensure geometry is set
    if "geometry" not in gdf.columns:
        raise RuntimeError("Community areas response missing geometry column")

    # Convert single Polygons to MultiPolygon for schema consistency
    gdf["geometry"] = gdf["geometry"].apply(
        lambda g: MultiPolygon([g]) if isinstance(g, Polygon) else g
    )

    # Reproject to storage CRS
    gdf = gdf.to_crs(TARGET_CRS)

    # Keep only needed columns
    keep = [c for c in ["pin", "address", "class_code", "geometry"] if c in gdf.columns]
    gdf = gdf[keep]
    gdf = gdf.dropna(subset=["pin", "geometry"])

    log.info("parcels_fetch_complete", total=len(gdf))
    return gdf


def load_parcels(gdf: gpd.GeoDataFrame) -> int:
    """
    Load community areas into the parcels table using upsert on PIN.
    Returns the number of rows inserted/updated.
    """
    engine = get_engine()

    gdf.to_postgis(
        "parcels_staging",
        engine,
        if_exists="replace",
        index=False,
        dtype={"geometry": "GEOMETRY(MULTIPOLYGON, 3435)"},
    )

    upsert_sql = """
        INSERT INTO parcels (pin, address, class_code, geom)
        SELECT pin, address, class_code, geometry
        FROM parcels_staging
        ON CONFLICT (pin) DO UPDATE SET
            address    = EXCLUDED.address,
            class_code = EXCLUDED.class_code,
            geom       = EXCLUDED.geom,
            loaded_at  = NOW();

        DROP TABLE IF EXISTS parcels_staging;
    """

    with get_connection() as conn, get_cursor(conn) as cur:
        cur.execute(upsert_sql)
        count = len(gdf)

    log.info("parcels_loaded", count=count)
    return count


def run() -> int:
    """Download and load Chicago community areas. Returns count."""
    gdf = fetch_parcels()
    return load_parcels(gdf)
