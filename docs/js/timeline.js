/**
 * Timeline module — static version for GitHub Pages demo.
 *
 * Instead of fetching /periods from the API, this accepts a pre-built
 * periods array directly. All other logic is identical to the live version.
 */

let _periods = [];
let _current = 0;

export function currentPeriod() {
  return _periods[_current]?.period ?? null;
}

/**
 * @param {Array<{period: string, source: string}>} periods - pre-built list
 * @param {Function} renderCallback - called on period change
 */
export function initTimeline(periods, renderCallback) {
  // Deduplicate by period string (one entry per month regardless of source)
  const seen = new Set();
  _periods = periods
    .filter(d => { if (seen.has(d.period)) return false; seen.add(d.period); return true; })
    .sort((a, b) => a.period.localeCompare(b.period)); // chronological

  const slider = document.getElementById('period-slider');
  slider.min = 0;
  slider.max = Math.max(0, _periods.length - 1);
  slider.value = _periods.length - 1;
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

  renderCallback();
}

function _updateLabel() {
  const label = document.getElementById('period-label');
  label.textContent = currentPeriod() ?? '—';
}
