/**
 * mockdata.js — Synthetic NDVI data engine for the GitHub Pages demo.
 *
 * No backend required. All data is generated deterministically in JavaScript.
 *
 * Community boundaries: fetched once from City of Chicago open data (CORS-enabled).
 * NDVI values: derived from land-use knowledge + seasonal multipliers + deterministic noise.
 * 12 months of data for 3 satellite sources across all 77 Chicago community areas.
 */

// ---------------------------------------------------------------------------
// Periods: 2023-08 → 2024-07, three sources
// ---------------------------------------------------------------------------
const MONTH_STRINGS = [
  '2023-08','2023-09','2023-10','2023-11','2023-12',
  '2024-01','2024-02','2024-03','2024-04','2024-05','2024-06','2024-07',
];

const SOURCES = ['sentinel2', 'landsat8', 'landsat9'];

export const PERIODS = MONTH_STRINGS.flatMap(p =>
  SOURCES.map(s => ({ period: p, source: s }))
);

// ---------------------------------------------------------------------------
// Seasonal NDVI multipliers (Chicago latitude — deciduous vegetation)
// ---------------------------------------------------------------------------
const SEASONAL = {
  1: 0.62, 2: 0.65, 3: 0.75, 4: 0.85, 5: 0.95,
  6: 1.00, 7: 1.00, 8: 0.97, 9: 0.88, 10: 0.78,
  11: 0.68, 12: 0.62,
};

// ---------------------------------------------------------------------------
// Base NDVI values for each community area (1-indexed, community_area field).
// Values represent peak-summer NDVI means derived from land-use knowledge:
//   - Parks/lakefront:          0.38-0.45
//   - Suburban residential:     0.28-0.35
//   - Dense urban/mixed:        0.15-0.22
//   - Commercial/industrial:    0.06-0.15
// ---------------------------------------------------------------------------
const BASE_NDVI = {
   1: 0.28,  2: 0.27,  3: 0.21,  4: 0.27,  5: 0.28,  6: 0.20,  7: 0.42,
   8: 0.14,  9: 0.33, 10: 0.34, 11: 0.29, 12: 0.36, 13: 0.29, 14: 0.26,
  15: 0.27, 16: 0.25, 17: 0.28, 18: 0.27, 19: 0.25, 20: 0.23, 21: 0.22,
  22: 0.20, 23: 0.28, 24: 0.19, 25: 0.24, 26: 0.22, 27: 0.21, 28: 0.18,
  29: 0.22, 30: 0.23, 31: 0.22, 32: 0.07, 33: 0.15, 34: 0.20, 35: 0.22,
  36: 0.23, 37: 0.22, 38: 0.23, 39: 0.28, 40: 0.31, 41: 0.32, 42: 0.24,
  43: 0.26, 44: 0.26, 45: 0.27, 46: 0.19, 47: 0.14, 48: 0.26, 49: 0.26,
  50: 0.25, 51: 0.17, 52: 0.15, 53: 0.24, 54: 0.16, 55: 0.19, 56: 0.27,
  57: 0.26, 58: 0.24, 59: 0.24, 60: 0.23, 61: 0.22, 62: 0.25, 63: 0.24,
  64: 0.28, 65: 0.24, 66: 0.23, 67: 0.22, 68: 0.21, 69: 0.23, 70: 0.26,
  71: 0.24, 72: 0.35, 73: 0.26, 74: 0.34, 75: 0.34, 76: 0.20, 77: 0.26,
};

// ---------------------------------------------------------------------------
// Source calibration offsets (Landsat vs Sentinel-2 SR differences)
// ---------------------------------------------------------------------------
const SOURCE_OFFSET = { sentinel2: 0, landsat8: -0.018, landsat9: -0.012 };

// ---------------------------------------------------------------------------
// Areas with simulated vegetation decline (for change detection demo)
// ndviDecline = reduction applied from 2024-01 onwards
// ---------------------------------------------------------------------------
const DECLINE_AREAS = {
  47: { delta: -0.32, severity: 'severe' },    // Burnside — industrial redevelopment
  51: { delta: -0.31, severity: 'severe' },    // South Deering
  52: { delta: -0.29, severity: 'severe' },    // East Side
  54: { delta: -0.33, severity: 'severe' },    // Riverdale
  55: { delta: -0.30, severity: 'severe' },    // Hegewisch
  26: { delta: -0.22, severity: 'moderate' },  // West Garfield Park
  27: { delta: -0.21, severity: 'moderate' },  // East Garfield Park
  37: { delta: -0.20, severity: 'moderate' },  // Fuller Park
  38: { delta: -0.23, severity: 'moderate' },  // Grand Boulevard
  68: { delta: -0.22, severity: 'moderate' },  // Englewood
  29: { delta: -0.12, severity: 'minor' },     // North Lawndale
  30: { delta: -0.11, severity: 'minor' },     // South Lawndale
  33: { delta: -0.13, severity: 'minor' },     // Near South Side
  34: { delta: -0.10, severity: 'minor' },     // Armour Square
  67: { delta: -0.11, severity: 'minor' },     // West Englewood
};

// ---------------------------------------------------------------------------
// Deterministic noise: small but realistic variation per (area, month, source)
// Uses Math.sin to avoid true randomness (same value on every page load)
// ---------------------------------------------------------------------------
function noise(areaId, monthIdx, sourceIdx) {
  return Math.sin(areaId * 17.3 + monthIdx * 7.7 + sourceIdx * 3.1) * 0.018;
}

// ---------------------------------------------------------------------------
// Core NDVI value computation for a single area/period/source
// ---------------------------------------------------------------------------
function ndviValue(areaId, year, month, sourceIdx) {
  const base = BASE_NDVI[areaId] ?? 0.20;
  const seasonal = SEASONAL[month] ?? 1.0;
  const offset = SOURCE_OFFSET[SOURCES[sourceIdx]];
  const monthIdx = MONTH_STRINGS.indexOf(`${year}-${String(month).padStart(2,'0')}`);
  const n = noise(areaId, monthIdx, sourceIdx);

  let value = base * seasonal + offset + n;

  // Apply decline for affected areas in 2024 Q1+
  if (areaId in DECLINE_AREAS && year === 2024 && month >= 1) {
    const decay = DECLINE_AREAS[areaId].delta * Math.min(1, (month - 1) / 3 + 0.3);
    value += decay;
  }

  return Math.max(0, Math.min(0.9, value));
}

// ---------------------------------------------------------------------------
// Community GeoJSON fetch + cache
// ---------------------------------------------------------------------------
let _communityCache = null;

export async function getCommunityGeoJSON() {
  if (_communityCache) return _communityCache;

  const url =
    'https://data.cityofchicago.org/resource/igwz-8jzy.geojson' +
    '?$limit=100&$select=community_area,community,the_geom';

  try {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    _communityCache = await res.json();
  } catch (err) {
    console.warn('Chicago API unavailable, using fallback geometry:', err);
    _communityCache = _fallbackGeoJSON();
  }
  return _communityCache;
}

// ---------------------------------------------------------------------------
// Enrich GeoJSON with synthetic NDVI for a given period + source
// ---------------------------------------------------------------------------
export function getNdviGeoJSON(baseGeoJSON, period, source) {
  const [yearStr, monthStr] = period.split('-');
  const year = parseInt(yearStr);
  const month = parseInt(monthStr);
  const sourceIdx = SOURCES.indexOf(source);

  const features = (baseGeoJSON.features || []).map(f => {
    const areaId = parseInt(f.properties.community_area ?? f.properties.area_numbe ?? 0);
    const ndvi = ndviValue(areaId, year, month, sourceIdx);
    const std = Math.abs(noise(areaId, month, sourceIdx + 1)) * 0.03 + 0.01;

    return {
      ...f,
      properties: {
        ...f.properties,
        community_name: f.properties.community ?? f.properties.community_area ?? `Area ${areaId}`,
        community_number: areaId,
        ndvi_mean:   +ndvi.toFixed(4),
        ndvi_median: +(ndvi - noise(areaId, month, 0) * 0.005).toFixed(4),
        ndvi_std:    +std.toFixed(4),
        pixel_count: Math.floor(800 + areaId * 23),
        valid_pct:   +(92 + noise(areaId, month, 2) * 5).toFixed(1),
      },
    };
  });

  return { type: 'FeatureCollection', features };
}

// ---------------------------------------------------------------------------
// Change detection GeoJSON: current vs prior month
// ---------------------------------------------------------------------------
export function getChangesGeoJSON(baseGeoJSON, period, source, severityFilter) {
  const [yearStr, monthStr] = period.split('-');
  const year = parseInt(yearStr);
  const month = parseInt(monthStr);
  const sourceIdx = SOURCES.indexOf(source);

  // Prior month
  let priorYear = year, priorMonth = month - 1;
  if (priorMonth === 0) { priorMonth = 12; priorYear--; }

  const features = [];

  for (const f of (baseGeoJSON.features || [])) {
    const areaId = parseInt(f.properties.community_area ?? f.properties.area_numbe ?? 0);
    if (!(areaId in DECLINE_AREAS)) continue;

    const { severity } = DECLINE_AREAS[areaId];
    if (severityFilter && severity !== severityFilter) continue;

    const ndviCurr = ndviValue(areaId, year, month, sourceIdx);
    const ndviPrior = ndviValue(areaId, priorYear, priorMonth, sourceIdx);
    const delta = ndviCurr - ndviPrior;

    if (delta >= -0.05) continue; // not significant

    features.push({
      ...f,
      properties: {
        ...f.properties,
        community_name:  f.properties.community ?? `Area ${areaId}`,
        community_number: areaId,
        ndvi_current: +ndviCurr.toFixed(4),
        ndvi_prior:   +ndviPrior.toFixed(4),
        ndvi_delta:   +delta.toFixed(4),
        severity,
        period_prior: `${priorYear}-${String(priorMonth).padStart(2,'0')}`,
      },
    });
  }

  return { type: 'FeatureCollection', features };
}

// ---------------------------------------------------------------------------
// Citywide statistics for a period + source
// ---------------------------------------------------------------------------
export function getStats(period, source) {
  const [yearStr, monthStr] = period.split('-');
  const year = parseInt(yearStr);
  const month = parseInt(monthStr);
  const sourceIdx = SOURCES.indexOf(source);

  const values = Object.keys(BASE_NDVI).map(id =>
    ndviValue(parseInt(id), year, month, sourceIdx)
  );
  values.sort((a, b) => a - b);

  const mean   = values.reduce((s, v) => s + v, 0) / values.length;
  const median = values[Math.floor(values.length / 2)];
  const min    = values[0];
  const max    = values[values.length - 1];
  const std    = Math.sqrt(values.reduce((s, v) => s + (v - mean) ** 2, 0) / values.length);

  return {
    period,
    source,
    parcel_count:  77,
    city_mean:     +mean.toFixed(4),
    city_median:   +median.toFixed(4),
    city_min:      +min.toFixed(4),
    city_max:      +max.toFixed(4),
    city_std:      +std.toFixed(4),
    high_veg_count: values.filter(v => v > 0.3).length,
    low_veg_count:  values.filter(v => v < 0.1).length,
  };
}

// ---------------------------------------------------------------------------
// NDVI time series history for a single community area
// ---------------------------------------------------------------------------
export function getHistory(communityNumber, source) {
  const sourceIdx = SOURCES.indexOf(source);

  return MONTH_STRINGS.map(period => {
    const [yearStr, monthStr] = period.split('-');
    const year = parseInt(yearStr);
    const month = parseInt(monthStr);
    const ndvi = ndviValue(communityNumber, year, month, sourceIdx);
    return { period, ndvi_mean: +ndvi.toFixed(4) };
  });
}

// ---------------------------------------------------------------------------
// Fallback GeoJSON: minimal 5-area embedded dataset when API is unavailable
// ---------------------------------------------------------------------------
function _fallbackGeoJSON() {
  return {
    type: 'FeatureCollection',
    features: [
      { type: 'Feature', properties: { community_area: '32', community: 'LOOP' },
        geometry: { type: 'Polygon', coordinates: [[[-87.637,41.878],[-87.619,41.878],[-87.619,41.889],[-87.637,41.889],[-87.637,41.878]]] } },
      { type: 'Feature', properties: { community_area: '7', community: 'LINCOLN PARK' },
        geometry: { type: 'Polygon', coordinates: [[[-87.654,41.908],[-87.630,41.908],[-87.630,41.930],[-87.654,41.930],[-87.654,41.908]]] } },
      { type: 'Feature', properties: { community_area: '41', community: 'HYDE PARK' },
        geometry: { type: 'Polygon', coordinates: [[[-87.610,41.784],[-87.580,41.784],[-87.580,41.805],[-87.610,41.805],[-87.610,41.784]]] } },
      { type: 'Feature', properties: { community_area: '72', community: 'BEVERLY' },
        geometry: { type: 'Polygon', coordinates: [[[-87.680,41.695],[-87.649,41.695],[-87.649,41.718],[-87.680,41.718],[-87.680,41.695]]] } },
      { type: 'Feature', properties: { community_area: '47', community: 'BURNSIDE' },
        geometry: { type: 'Polygon', coordinates: [[[-87.617,41.716],[-87.601,41.716],[-87.601,41.726],[-87.617,41.726],[-87.617,41.716]]] } },
    ],
  };
}
