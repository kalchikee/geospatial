/**
 * Timeline module — loads periods from static data/periods.json
 */

let _periods = [];
let _current = 0;

export function currentPeriod() {
  return _periods[_current]?.period ?? null;
}

export async function initTimeline(renderCallback) {
  const res = await fetch('./data/periods.json');
  if (!res.ok) { console.warn('Could not load periods'); return; }

  const data = await res.json();
  const seen = new Set();
  _periods = data.filter(d => {
    if (seen.has(d.period)) return false;
    seen.add(d.period);
    return true;
  }).reverse();

  const slider = document.getElementById('period-slider');
  slider.min   = 0;
  slider.max   = Math.max(0, _periods.length - 1);
  slider.value = _periods.length - 1;
  _current     = _periods.length - 1;
  _updateLabel();

  slider.addEventListener('input', () => {
    _current = parseInt(slider.value, 10);
    _updateLabel();
    renderCallback();
  });

  document.getElementById('btn-prev').addEventListener('click', () => {
    if (_current > 0) { _current--; slider.value = _current; _updateLabel(); renderCallback(); }
  });
  document.getElementById('btn-next').addEventListener('click', () => {
    if (_current < _periods.length - 1) { _current++; slider.value = _current; _updateLabel(); renderCallback(); }
  });

  renderCallback();
}

function _updateLabel() {
  document.getElementById('period-label').textContent = currentPeriod() ?? '—';
}
