/**
 * Chicago NDVI Monitoring — GitHub Pages / Demo Version
 *
 * All data is provided by mockdata.js (synthetic, deterministic).
 * No backend, no Docker, no database required.
 *
 * Differences from the live version (frontend/js/app.js):
 *  - Imports from mockdata.js instead of calling FastAPI endpoints
 *  - Community areas (77 areas) used instead of ~600k parcels
 *  - Tooltip shows community name, not parcel PIN
 */

import { initTimeline, currentPeriod } from './timeline.js';
import {
  PERIODS,
  getCommunityGeoJSON,
  getNdviGeoJSON,
  getChangesGeoJSON,
  getStats,
  getHistory,
} from './mockdata.js';

// ---------------------------------------------------------------------------
// NDVI colour scale (low → high), matching legend gradient
// ---------------------------------------------------------------------------
function ndviColor(val) {
  if (val === null || val === undefined) return '#cccccc';
  if (val < 0.0)  return '#d73027';
  if (val < 0.1)  return '#f46d43';
  if (val < 0.2)  return '#fdae61';
  if (val < 0.3)  return '#fee090';
  if (val < 0.4)  return '#ffffbf';
  if (val < 0.5)  return '#a6d96a';
  if (val < 0.6)  return '#66bd63';
  if (val < 0.7)  return '#1a9850';
  return '#006837';
}

function severityColor(sev) {
  return { minor: '#f59e0b', moderate: '#ef4444', severe: '#7c3aed' }[sev] || '#888';
}

// ---------------------------------------------------------------------------
// Map initialisation
// ---------------------------------------------------------------------------
const map = L.map('map', {
  center: [41.83, -87.73],
  zoom: 11,
  preferCanvas: true,
});

L.tileLayer(
  'https://{s}.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}{r}.png',
  {
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/">CARTO</a>',
    subdomains: 'abcd',
    maxZoom: 19,
  }
).addTo(map);

// Labels on top of NDVI (renders above choropleth layer)
const labelsLayer = L.tileLayer(
  'https://{s}.basemaps.cartocdn.com/light_only_labels/{z}/{x}/{y}{r}.png',
  { subdomains: 'abcd', maxZoom: 19, pane: 'shadowPane' }
).addTo(map);

let ndviLayer = null;
let changesLayer = null;
let communityGeoJSON = null; // cached after first fetch

// ---------------------------------------------------------------------------
// GeoJSON layer factories
// ---------------------------------------------------------------------------
function buildNdviLayer(geojson) {
  return L.geoJSON(geojson, {
    style: feature => ({
      fillColor: ndviColor(feature.properties.ndvi_mean),
      fillOpacity: 0.72,
      color: '#ffffff',
      weight: 0.8,
      opacity: 0.6,
    }),
    onEachFeature(feature, layer) {
      const p = feature.properties;
      layer.bindTooltip(
        `<strong>${p.community_name}</strong><br/>NDVI: ${p.ndvi_mean?.toFixed(3) ?? '—'}`,
        { sticky: true }
      );
      layer.on('click', () =>
        showAreaDetail(p.community_number, p.community_name)
      );
    },
  });
}

function buildChangesLayer(geojson) {
  return L.geoJSON(geojson, {
    style: feature => ({
      fillColor: severityColor(feature.properties.severity),
      fillOpacity: 0.85,
      color: '#111',
      weight: 1.2,
    }),
    onEachFeature(feature, layer) {
      const p = feature.properties;
      layer.bindTooltip(
        `<strong>${p.community_name}</strong><br/>` +
        `Severity: <strong>${p.severity}</strong><br/>` +
        `NDVI Δ: ${p.ndvi_delta?.toFixed(3)}`,
        { sticky: true }
      );
      layer.on('click', () =>
        showAreaDetail(p.community_number, p.community_name)
      );
    },
  });
}

// ---------------------------------------------------------------------------
// Render — called on any control change or period change
// ---------------------------------------------------------------------------
async function renderLayers() {
  const period = currentPeriod();
  const source = document.getElementById('source-select').value;
  const showNdvi = document.getElementById('toggle-ndvi').checked;
  const showChanges = document.getElementById('toggle-changes').checked;
  const severity = document.getElementById('severity-select').value;

  if (ndviLayer)    { map.removeLayer(ndviLayer);    ndviLayer    = null; }
  if (changesLayer) { map.removeLayer(changesLayer); changesLayer = null; }

  if (!period || !communityGeoJSON) return;

  document.getElementById('period-label').textContent = period;
  const hudEl = document.getElementById('period-label-hud');
  if (hudEl) hudEl.textContent = period;

  if (showNdvi) {
    const gj = getNdviGeoJSON(communityGeoJSON, period, source);
    ndviLayer = buildNdviLayer(gj).addTo(map);
  }

  if (showChanges) {
    const gj = getChangesGeoJSON(communityGeoJSON, period, source, severity || null);
    changesLayer = buildChangesLayer(gj).addTo(map);
  }

  renderStats(getStats(period, source));
}

// ---------------------------------------------------------------------------
// Stats panel — metric card grid
// ---------------------------------------------------------------------------
function renderStats(stats) {
  const range = stats.city_min !== null
    ? `${stats.city_min.toFixed(2)} – ${stats.city_max.toFixed(2)}`
    : '—';

  document.getElementById('stats-content').innerHTML = `
    <div class="stats-grid">
      <div class="stat-card">
        <div class="stat-value">${stats.city_mean?.toFixed(3) ?? '—'}</div>
        <div class="stat-label">Mean NDVI</div>
      </div>
      <div class="stat-card">
        <div class="stat-value">${stats.city_median?.toFixed(3) ?? '—'}</div>
        <div class="stat-label">Median</div>
      </div>
      <div class="stat-card accent-green">
        <div class="stat-value">${stats.high_veg_count}</div>
        <div class="stat-label">High Veg &gt;0.3</div>
      </div>
      <div class="stat-card accent-red">
        <div class="stat-value">${stats.low_veg_count}</div>
        <div class="stat-label">Low Veg &lt;0.1</div>
      </div>
      <div class="stat-card" style="grid-column:1/-1">
        <div class="stat-value" style="font-size:14px">${range}</div>
        <div class="stat-label">NDVI Range</div>
      </div>
    </div>
  `;
}

// ---------------------------------------------------------------------------
// Community detail panel + history sparkline
// ---------------------------------------------------------------------------
let ndviChart = null;

function showAreaDetail(communityNumber, communityName) {
  const source = document.getElementById('source-select').value;
  const history = getHistory(communityNumber, source);

  const panel = document.getElementById('detail-panel');
  document.getElementById('detail-pin').textContent = communityName;
  document.getElementById('detail-address').textContent =
    `Community Area #${communityNumber} — ${source.replace('sentinel2','Sentinel-2').replace('landsat8','Landsat 8').replace('landsat9','Landsat 9')}`;
  panel.classList.remove('hidden');

  const labels = history.map(h => h.period);
  const values = history.map(h => h.ndvi_mean);

  if (ndviChart) ndviChart.destroy();
  ndviChart = new Chart(document.getElementById('ndvi-chart'), {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label: 'NDVI Mean',
        data: values,
        borderColor: '#22c55e',
        backgroundColor: 'rgba(34,197,94,0.12)',
        borderWidth: 2,
        pointRadius: 3,
        pointBackgroundColor: '#22c55e',
        tension: 0.35,
        fill: true,
      }],
    },
    options: {
      animation: false,
      responsive: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: 'rgba(15,23,42,.95)',
          borderColor: '#334155',
          borderWidth: 1,
          titleColor: '#94a3b8',
          bodyColor: '#f1f5f9',
          callbacks: { label: ctx => `NDVI: ${ctx.parsed.y.toFixed(3)}` },
        },
      },
      scales: {
        y: {
          min: -0.05,
          max: 0.75,
          ticks: { font: { size: 10 }, color: '#64748b' },
          grid: { color: 'rgba(51,65,85,.5)' },
          border: { color: '#334155' },
        },
        x: {
          ticks: { font: { size: 9 }, color: '#64748b', maxRotation: 45 },
          grid: { display: false },
          border: { color: '#334155' },
        },
      },
    },
  });
}

document.getElementById('close-detail').addEventListener('click', () => {
  document.getElementById('detail-panel').classList.add('hidden');
});

// ---------------------------------------------------------------------------
// Event bindings
// ---------------------------------------------------------------------------
document.getElementById('source-select').addEventListener('change', renderLayers);
document.getElementById('toggle-ndvi').addEventListener('change', renderLayers);
document.getElementById('toggle-changes').addEventListener('change', renderLayers);
document.getElementById('severity-select').addEventListener('change', renderLayers);

// ---------------------------------------------------------------------------
// Boot: fetch community boundaries once, then start the timeline
// ---------------------------------------------------------------------------
async function boot() {
  document.getElementById('stats-content').innerHTML = '<em>Loading boundaries…</em>';

  communityGeoJSON = await getCommunityGeoJSON();

  initTimeline(PERIODS, renderLayers);
}

boot();
