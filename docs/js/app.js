/**
 * Chicago NDVI Monitor — Leaflet Application
 * Dark GIS theme · Real Sentinel-2 data · Static pre-baked JSON (no backend required)
 */

import { initTimeline, currentPeriod } from './timeline.js';

// Pre-loaded history lookup (populated on boot)
let _history = {};

// ── NDVI colour ramp ─────────────────────────────────────────
function ndviColor(val) {
  if (val === null || val === undefined) return '#30363d';
  if (val < 0.0)  return '#d73027';
  if (val < 0.1)  return '#f46d43';
  if (val < 0.2)  return '#fdae61';
  if (val < 0.3)  return '#fee090';
  if (val < 0.4)  return '#a6d96a';
  if (val < 0.5)  return '#66bd63';
  if (val < 0.6)  return '#1a9850';
  return '#006837';
}

function ndviClass(val) {
  if (val === null || val === undefined) return '';
  if (val >= 0.3) return 'green';
  if (val >= 0.0) return 'orange';
  return 'red';
}

function severityColor(sev) {
  return { minor: '#f59e0b', moderate: '#ef4444', severe: '#a78bfa' }[sev] || '#8b949e';
}

// ── Loading state ────────────────────────────────────────────
const loader   = document.getElementById('loader');
const statusEl = document.getElementById('status-chip');
const hPeriod  = document.getElementById('header-period');
const hCount   = document.getElementById('header-count');

function setLoading(on) {
  if (on) {
    loader.classList.remove('hidden');
    statusEl.textContent = '⟳ LOADING';
    statusEl.classList.add('loading');
  } else {
    loader.classList.add('hidden');
    statusEl.textContent = '● LIVE';
    statusEl.classList.remove('loading');
  }
}

// ── Map ───────────────────────────────────────────────────────
const map = L.map('map', { center: [41.845, -87.68], zoom: 11, preferCanvas: true });

L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
  attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/attributions">CARTO</a>',
  subdomains: 'abcd',
  maxZoom: 20,
}).addTo(map);

let ndviLayer    = null;
let changesLayer = null;

// ── Layer factories ──────────────────────────────────────────
function buildNdviLayer(geojson) {
  return L.geoJSON(geojson, {
    style: f => ({
      fillColor:   ndviColor(f.properties.ndvi_mean),
      fillOpacity: 0.78,
      color:       'rgba(0,0,0,0.3)',
      weight:      0.6,
    }),
    onEachFeature(f, layer) {
      const p = f.properties, v = p.ndvi_mean, cls = ndviClass(v);
      layer.bindTooltip(
        `<span class="tt-name">${p.address || p.pin}</span>` +
        `<div class="tt-row"><span class="tt-key">NDVI Mean</span><span class="tt-val ${cls}">${v?.toFixed(4) ?? '—'}</span></div>` +
        `<div class="tt-row"><span class="tt-key">Pixels</span><span class="tt-val">${p.pixel_count?.toLocaleString() ?? '—'}</span></div>` +
        `<div class="tt-row"><span class="tt-key">Coverage</span><span class="tt-val">${p.valid_pct?.toFixed(1) ?? '—'}%</span></div>`,
        { sticky: true, opacity: 1 }
      );
      layer.on('mouseover', function () { this.setStyle({ fillOpacity: 0.95, weight: 1.5, color: '#e6edf3' }); });
      layer.on('mouseout',  function () { this.setStyle({ fillOpacity: 0.78, weight: 0.6, color: 'rgba(0,0,0,0.3)' }); });
      layer.on('click', () => showParcelDetail(p.pin, p.address, p));
    },
  });
}

function buildChangesLayer(geojson) {
  return L.geoJSON(geojson, {
    style: f => ({
      fillColor:   severityColor(f.properties.severity),
      fillOpacity: 0.55,
      color:       '#0d1117',
      weight:      1.2,
      dashArray:   '5 4',
    }),
    onEachFeature(f, layer) {
      const p = f.properties, sev = p.severity || '—';
      layer.bindTooltip(
        `<span class="tt-name">${p.address || p.pin}</span>` +
        `<div class="tt-row"><span class="tt-key">Severity</span><span class="tt-val">${sev.toUpperCase()}</span></div>` +
        `<div class="tt-row"><span class="tt-key">NDVI Δ</span><span class="tt-val red">${p.ndvi_delta?.toFixed(4)}</span></div>` +
        `<div class="tt-row"><span class="tt-key">Current NDVI</span><span class="tt-val">${p.ndvi_current?.toFixed(4)}</span></div>`,
        { sticky: true, opacity: 1 }
      );
      layer.on('click', () => showParcelDetail(p.pin, p.address, null));
    },
  });
}

// ── Static data loaders ──────────────────────────────────────
async function loadNdvi(period, source) {
  const r = await fetch(`./data/parcels-${period}-${source}.geojson`);
  if (!r.ok) throw new Error(`NDVI ${r.status}`);
  return r.json();
}

async function loadChanges(period, source, severity) {
  const r = await fetch(`./data/changes-${period}-${source}.geojson`);
  if (!r.ok) throw new Error(`Changes ${r.status}`);
  const fc = await r.json();
  if (severity) {
    fc.features = fc.features.filter(f => f.properties.severity === severity);
  }
  return fc;
}

async function loadStats(period, source) {
  const r = await fetch(`./data/stats-${period}-${source}.json`);
  if (!r.ok) return null;
  return r.json();
}

// ── Render layers ────────────────────────────────────────────
async function renderLayers() {
  const period   = currentPeriod();
  const source   = document.getElementById('source-select').value;
  const showNdvi = document.getElementById('toggle-ndvi').checked;
  const showChg  = document.getElementById('toggle-changes').checked;
  const sev      = document.getElementById('severity-select').value;

  if (ndviLayer)    { map.removeLayer(ndviLayer);    ndviLayer    = null; }
  if (changesLayer) { map.removeLayer(changesLayer); changesLayer = null; }
  if (!period) return;

  document.getElementById('period-label').textContent = period;
  hPeriod.textContent = period;

  setLoading(true);
  try {
    const [ndviData, chgData, statsData] = await Promise.all([
      showNdvi ? loadNdvi(period, source)         : Promise.resolve(null),
      showChg  ? loadChanges(period, source, sev) : Promise.resolve(null),
      loadStats(period, source),
    ]);

    if (ndviData && showNdvi) {
      ndviLayer = buildNdviLayer(ndviData).addTo(map);
      const n = ndviData.features?.length ?? 0;
      document.getElementById('ndvi-count').textContent = `${n} areas`;
      hCount.textContent = `${n}`;
    }
    if (chgData && showChg) {
      changesLayer = buildChangesLayer(chgData).addTo(map);
      document.getElementById('change-count').textContent = `${chgData.features?.length ?? 0} flagged`;
    }
    if (statsData) renderStats(statsData);

  } catch (err) {
    console.error('Layer load error:', err);
  } finally {
    setLoading(false);
  }
}

// ── Stats panel ──────────────────────────────────────────────
function renderStats(s) {
  const fmt = v => v?.toFixed(4) ?? '—';
  document.getElementById('stats-content').innerHTML = `
    <div class="stat-primary">
      <div class="stat-primary-label">City NDVI Mean</div>
      <div class="stat-primary-value">${fmt(s.city_mean)}</div>
      <div class="stat-primary-sub">${s.parcel_count?.toLocaleString()} community areas · sentinel-2</div>
    </div>
    <div class="stat-grid2">
      <div class="stat-mini"><div class="stat-mini-label">Median</div><div class="stat-mini-value">${fmt(s.city_median)}</div></div>
      <div class="stat-mini"><div class="stat-mini-label">Std Dev</div><div class="stat-mini-value">${fmt(s.city_std)}</div></div>
      <div class="stat-mini"><div class="stat-mini-label">Min</div><div class="stat-mini-value">${fmt(s.city_min)}</div></div>
      <div class="stat-mini"><div class="stat-mini-label">Max</div><div class="stat-mini-value">${fmt(s.city_max)}</div></div>
    </div>`;
}

// ── Detail panel ─────────────────────────────────────────────
let ndviChart = null;

function showParcelDetail(pin, address, props) {
  const panel = document.getElementById('detail-panel');
  document.getElementById('detail-pin').textContent     = address || `PIN ${pin}`;
  document.getElementById('detail-address').textContent = `Community Area · PIN ${pin}`;
  panel.classList.remove('hidden');

  if (props) {
    const px = props.pixel_count;
    document.getElementById('detail-stats').innerHTML = `
      <div class="dp-stat"><div class="dp-stat-label">NDVI</div><div class="dp-stat-value">${props.ndvi_mean?.toFixed(3) ?? '—'}</div></div>
      <div class="dp-stat"><div class="dp-stat-label">Pixels</div><div class="dp-stat-value">${px != null ? (px/1000).toFixed(0)+'k' : '—'}</div></div>
      <div class="dp-stat"><div class="dp-stat-label">Cover</div><div class="dp-stat-value">${props.valid_pct?.toFixed(0) ?? '—'}%</div></div>`;
  }

  const histData = _history[pin] ?? [];
  if (ndviChart) ndviChart.destroy();

  ndviChart = new Chart(document.getElementById('ndvi-chart'), {
    type: 'line',
    data: {
      labels: histData.map(h => h.period),
      datasets: [{
        data:                 histData.map(h => h.ndvi_mean),
        borderColor:          '#3fb950',
        backgroundColor:      'rgba(63,185,80,0.07)',
        borderWidth:          1.5,
        pointRadius:          4,
        pointBackgroundColor: '#3fb950',
        pointBorderColor:     '#0d1117',
        pointBorderWidth:     1.5,
        tension:              0.35,
        fill:                 true,
      }],
    },
    options: {
      animation: false,
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: 'rgba(22,27,34,0.97)',
          borderColor: '#30363d', borderWidth: 1,
          titleColor: '#8b949e', bodyColor: '#e6edf3',
          titleFont: { family: 'Inter', size: 10 },
          bodyFont:  { family: 'JetBrains Mono', size: 12 },
          callbacks: { label: ctx => ctx.parsed.y.toFixed(4) },
        },
      },
      scales: {
        y: { grid: { color: 'rgba(48,54,61,0.7)' }, ticks: { color: '#8b949e', font: { family: 'JetBrains Mono', size: 9 }, maxTicksLimit: 5 } },
        x: { grid: { display: false }, ticks: { color: '#8b949e', font: { family: 'Inter', size: 9 }, maxRotation: 0 } },
      },
    },
  });
}

document.getElementById('close-detail').addEventListener('click', () => {
  document.getElementById('detail-panel').classList.add('hidden');
});

document.getElementById('source-select').addEventListener('change', renderLayers);
document.getElementById('toggle-ndvi').addEventListener('change', renderLayers);
document.getElementById('toggle-changes').addEventListener('change', renderLayers);
document.getElementById('severity-select').addEventListener('change', renderLayers);

// ── Boot ─────────────────────────────────────────────────────
async function boot() {
  // Pre-load history for instant detail panel response
  try {
    const r = await fetch('./data/history.json');
    if (r.ok) _history = await r.json();
  } catch (e) { /* history panel will be empty but map still works */ }

  await initTimeline(renderLayers);
}

boot();
