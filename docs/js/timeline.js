/**
 * Timeline module — manages the period slider and period list.
 *
 * Fetches available periods from GET /periods and populates a range slider.
 * Exposes currentPeriod() for the main app layer to read.
 * Calls renderCallback() whenever the selected period changes.
 */

let _periods = [];   // [{period: "2024-07", source: "sentinel2"}, ...]
let _current = 0;    // slider index

export function currentPeriod() {
  return _periods[_current]?.period ?? null;
}

export async function initTimeline(apiBase, renderCallback) {
  const res = await fetch(`${apiBase}/periods`);
  if (!res.ok) {
    console.warn('Could not load periods from API');
    return;
  }

  const data = await res.json();

  // Deduplicate by period string (keep first occurrence per period)
  const seen = new Set();
  _periods = data.filter(d => {
    if (seen.has(d.period)) return false;
    seen.add(d.period);
    return true;
  }).reverse(); // chronological order

  const slider = document.getElementById('period-slider');
  slider.min = 0;
  slider.max = Math.max(0, _periods.length - 1);
  slider.value = _periods.length - 1;  // default: latest period
  _current = _periods.length - 1;

  _updateLabel();

  slider.addEventListener('input', () => {
    _current = parseInt(slider.value, 10);
    _updateLabel();
    renderCallback();
  });

  document.getElementById('btn-prev').addEventListener('click', () => {
    if (_current > 0) {
      _current--;
      slider.value = _current;
      _updateLabel();
      renderCallback();
    }
  });

  document.getElementById('btn-next').addEventListener('click', () => {
    if (_current < _periods.length - 1) {
      _current++;
      slider.value = _current;
      _updateLabel();
      renderCallback();
    }
  });

  // Trigger initial render
  renderCallback();
}

function _updateLabel() {
  const label = document.getElementById('period-label');
  label.textContent = currentPeriod() ?? '—';
}
