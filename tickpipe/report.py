"""A self-contained HTML report of a live run against the historical run."""

from __future__ import annotations

import html
import json
from pathlib import Path

import polars as pl

from tickpipe.compare import Parity, sample
from tickpipe.signal import VALUES
from tickpipe.timefmt import clock

_TITLES = {
    "level_flow": ("Level flow", "Signed contracts summed over the strip, inside the window"),
    "slope_flow": ("Slope flow", "The same flows weighted from -1 at the front contract to +1 at the back"),
}


def _gap(joined: pl.DataFrame) -> pl.DataFrame:
    """Add the largest disagreement between the two runs on each tick (infinite when one side is missing)."""
    gaps = [(pl.col(v) - pl.col(f"{v}_live")).abs().fill_null(float("inf")) for v in VALUES]
    return joined.with_columns(pl.max_horizontal(gaps).alias("gap"))


def _chart_points(joined: pl.DataFrame, limit: int = 900) -> tuple[pl.DataFrame, int]:
    """Thin the ticks to at most `limit` points. A bucket shows its last tick,
    unless the runs disagree inside it: then it shows the worst tick, so a
    difference can never hide between two plotted points."""
    span = int(joined["ts"][-1] - joined["ts"][0]) + 1
    bucket = max(1_000_000_000, -(-span // limit))
    points = joined.group_by((pl.col("ts") // bucket).alias("bucket"), maintain_order=True).agg(
        pl.all().sort_by("gap", maintain_order=True).last()
    )
    return points, bucket


def _number(value: float | None, digits: int) -> str:
    return "missing" if value is None else format(value, f",.{digits}f")


def _tick_rows(frame: pl.DataFrame) -> str:
    rows = []
    for row in frame.iter_rows(named=True):
        same = "yes" if row["gap"] == 0 else "no"
        rows.append(
            "<tr>"
            f"<td>{clock(row['ts'])}</td><td>{html.escape(str(row['ticker']))}</td>"
            f"<td>{_number(row['level_flow_live'], 0)}</td><td>{_number(row['level_flow'], 0)}</td>"
            f"<td>{_number(row['slope_flow_live'], 2)}</td><td>{_number(row['slope_flow'], 2)}</td>"
            f"<td>{same}</td></tr>"
        )
    return "\n".join(rows)


def write(path: str | Path, parity: Parity, joined: pl.DataFrame, *, window_ns: int) -> Path:
    path = Path(path)
    joined = _gap(joined)
    differing = joined.filter(pl.col("gap") > 0)
    identical = joined.height - differing.height
    points, bucket = _chart_points(joined)
    first_ms = int(joined["ts"][0]) // 1_000_000
    window_full = first_ms + window_ns // 1_000_000

    data = {
        "t": (points["ts"] // 1_000_000).to_list(),
        "windowFull": window_full if window_full < int(joined["ts"][-1]) // 1_000_000 else None,
        "charts": [
            {
                "id": value,
                "title": _TITLES[value][0],
                "digits": 0 if value == "level_flow" else 2,
                "historical": points[value].to_list(),
                "live": points[f"{value}_live"].to_list(),
            }
            for value in VALUES
        ],
    }
    shown = differing.head(8) if differing.height else sample(joined)
    shown_title = "First ticks that differ" if differing.height else "Ticks from the middle of the run"
    window_s = window_ns / 1e9
    amount, unit = (window_s / 60, "minute") if window_s >= 60 else (window_s, "second")
    window_text = f"{amount:g} {unit}" + ("" if amount == 1 else "s")
    bucket_s = bucket / 1e9
    verdict = (
        "Live and historical are identical, tick for tick."
        if parity.identical
        else "Live and historical differ. The first differing ticks are listed below."
    )
    largest = max(parity.max_abs_diff.values(), default=0.0)
    fields = {
        "__VERDICT__": verdict,
        "__WINDOW__": window_text,
        "__IDENTICAL__": f"{identical:,}",
        "__COMPARED__": f"{joined.height:,}",
        "__HISTORICAL__": f"{parity.historical_rows:,}",
        "__LIVE__": f"{parity.live_rows:,}",
        "__ONLY_HISTORICAL__": f"{parity.only_historical:,}",
        "__ONLY_LIVE__": f"{parity.only_live:,}",
        "__DIFFERENT__": f"{differing.height - parity.only_historical - parity.only_live:,}",
        "__LARGEST__": f"{largest:g}",
        "__FROM__": clock(int(joined["ts"][0]), millis=False),
        "__TO__": clock(int(joined["ts"][-1]), millis=False),
        "__BUCKET__": f"{bucket_s:g}",
        "__SHOWN_TITLE__": shown_title,
        "__SHOWN_ROWS__": _tick_rows(shown),
        "__CHART_SECTIONS__": "\n".join(
            f'<section class="card chart"><h2>{_TITLES[v][0]}</h2><p>{_TITLES[v][1]}</p>'
            f'<div class="plot" data-chart="{v}"></div></section>'
            for v in VALUES
        ),
        "__DATA__": json.dumps(data, separators=(",", ":")),
    }
    page = _PAGE
    for key, value in fields.items():
        page = page.replace(key, value)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(page, encoding="utf-8")
    return path


_PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Live against historical</title>
<style>
:root {
  color-scheme: light;
  --page: #f9f9f7; --surface: #fcfcfb; --border: rgba(11,11,11,0.10);
  --text-primary: #0b0b0b; --text-secondary: #52514e; --muted: #898781;
  --grid: #e1e0d9; --axis: #c3c2b7;
  --series-1: #2a78d6; --series-2: #eb6834; --band: 0.38;
}
@media (prefers-color-scheme: dark) {
  :root:where(:not([data-theme="light"])) {
    color-scheme: dark;
    --page: #0d0d0d; --surface: #1a1a19; --border: rgba(255,255,255,0.10);
    --text-primary: #f0efec; --text-secondary: #c3c2b7; --muted: #898781;
    --grid: #2c2c2a; --axis: #383835;
    --series-1: #3987e5; --series-2: #d95926; --band: 0.6;
  }
}
:root[data-theme="dark"] {
  color-scheme: dark;
  --page: #0d0d0d; --surface: #1a1a19; --border: rgba(255,255,255,0.10);
  --text-primary: #f0efec; --text-secondary: #c3c2b7; --muted: #898781;
  --grid: #2c2c2a; --axis: #383835;
  --series-1: #3987e5; --series-2: #d95926; --band: 0.6;
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--page); color: var(--text-primary);
  font: 14px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif; }
main { max-width: 1040px; margin: 0 auto; padding: 32px 16px 56px; }
h1 { font-size: 22px; font-weight: 600; margin: 0 0 6px; }
h2 { font-size: 15px; font-weight: 600; margin: 0; }
.lede { color: var(--text-secondary); max-width: 72ch; margin: 0 0 24px; }
.card { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 16px; }
.tiles { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin-bottom: 12px; }
.hero { grid-column: 1 / -1; }
.label { color: var(--text-secondary); font-size: 13px; }
.value { font-size: 24px; font-weight: 600; line-height: 1.2; margin-top: 2px; }
.hero .value { font-size: 52px; line-height: 1.05; letter-spacing: -0.01em; margin: 4px 0; }
.note { color: var(--text-secondary); font-size: 13px; }
.legend { display: flex; flex-wrap: wrap; gap: 6px 24px; color: var(--text-secondary); font-size: 13px; margin: 20px 2px 10px; }
.key { display: inline-block; width: 22px; vertical-align: middle; margin-right: 8px; border-radius: 4px; }
.key.historical { height: 8px; background: var(--series-1); opacity: var(--band); }
.key.live { height: 2px; background: var(--series-2); }
.chart { margin-bottom: 12px; }
.chart p { color: var(--text-secondary); font-size: 13px; margin: 2px 0 10px; }
.plot { width: 100%; }
.plot svg { display: block; width: 100%; outline-offset: 4px; }
.plot text { font: 12px system-ui, -apple-system, "Segoe UI", sans-serif; fill: var(--muted); font-variant-numeric: tabular-nums; }
.plot text.mark { fill: var(--text-secondary); }
.grid { stroke: var(--grid); stroke-width: 1; }
.axis { stroke: var(--axis); stroke-width: 1; }
.band { fill: none; stroke: var(--series-1); stroke-width: 8; stroke-opacity: var(--band); stroke-linejoin: round; stroke-linecap: round; }
.line { fill: none; stroke: var(--series-2); stroke-width: 2; stroke-linejoin: round; stroke-linecap: round; }
.dot { stroke: var(--surface); stroke-width: 2; }
.dot.historical { fill: var(--series-1); }
.dot.live { fill: var(--series-2); }
.tip { position: fixed; display: none; pointer-events: none; z-index: 5; min-width: 190px;
  background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 8px 12px;
  box-shadow: 0 6px 20px rgba(0,0,0,0.14); font-size: 13px; }
.tip .when { color: var(--text-secondary); font-variant-numeric: tabular-nums; }
.tip .group { color: var(--text-secondary); margin-top: 6px; }
.tip .row { display: flex; align-items: center; gap: 8px; }
.tip .row strong { font-weight: 600; font-variant-numeric: tabular-nums; min-width: 78px; text-align: right; }
.tip .row span.name { color: var(--text-secondary); }
.tip .key { margin-right: 0; width: 14px; }
.scroll { overflow-x: auto; }
table { border-collapse: collapse; width: 100%; font-size: 13px; font-variant-numeric: tabular-nums; }
th, td { text-align: right; padding: 6px 10px; border-bottom: 1px solid var(--grid); white-space: nowrap; }
th { color: var(--text-secondary); font-weight: 500; }
th:first-child, td:first-child, .ticks th:nth-child(2), .ticks td:nth-child(2) { text-align: left; }
details { margin-top: 12px; }
summary { cursor: pointer; color: var(--text-secondary); }
.foot { color: var(--text-secondary); font-size: 13px; margin-top: 20px; max-width: 80ch; }
@media (max-width: 720px) { .tiles { grid-template-columns: repeat(2, minmax(0, 1fr)); } .hero .value { font-size: 48px; } }
</style>
</head>
<body>
<main>
  <h1>Trade flow imbalance: live against historical</h1>
  <p class="lede">__VERDICT__ One signal definition was run twice: as a batch over stored ticks, and over
  the same ticks streamed through sockets. Window: __WINDOW__. Ticks from __FROM__ to __TO__.</p>

  <div class="tiles">
    <div class="card hero">
      <div class="label">Ticks identical in both runs</div>
      <div class="value">__IDENTICAL__</div>
      <div class="note">of __COMPARED__ compared (historical __HISTORICAL__, live __LIVE__)</div>
    </div>
    <div class="card"><div class="label">Only in historical</div><div class="value">__ONLY_HISTORICAL__</div></div>
    <div class="card"><div class="label">Only in live</div><div class="value">__ONLY_LIVE__</div></div>
    <div class="card"><div class="label">Ticks with a different value</div><div class="value">__DIFFERENT__</div></div>
    <div class="card"><div class="label">Largest absolute difference</div><div class="value">__LARGEST__</div></div>
  </div>

  <div class="legend">
    <span><i class="key historical"></i>Historical: one batch run over stored ticks</span>
    <span><i class="key live"></i>Live: streaming run, as published on the signal socket</span>
  </div>
  __CHART_SECTIONS__

  <section class="card">
    <h2>__SHOWN_TITLE__</h2>
    <div class="scroll">
    <table class="ticks">
      <thead><tr><th>Time</th><th>Contract</th><th>Level, live</th><th>Level, historical</th>
      <th>Slope, live</th><th>Slope, historical</th><th>Same</th></tr></thead>
      <tbody>
__SHOWN_ROWS__
      </tbody>
    </table>
    </div>
    <details>
      <summary>Table view of the charts</summary>
      <div class="scroll"><table id="chart-table"></table></div>
    </details>
  </section>

  <p class="foot">Ticks are matched on time, contract and sequence number. A tick present in one run only counts
  as a difference, and values must be equal bit for bit. Each chart point is the last tick of an interval of
  __BUCKET__ s, or the tick with the largest disagreement when the runs differ inside that interval.</p>
</main>
<div class="tip" id="tip" role="status"></div>

<script>
const DATA = __DATA__;
const NS = "http://www.w3.org/2000/svg";
const T = DATA.t;
const SERIES = [["historical", "Historical"], ["live", "Live"]];

function node(name, attrs, parent) {
  const el = document.createElementNS(NS, name);
  for (const key in attrs) el.setAttribute(key, attrs[key]);
  if (parent) parent.appendChild(el);
  return el;
}
function html(name, className, text, parent) {
  const el = document.createElement(name);
  if (className) el.className = className;
  if (text !== undefined) el.textContent = text;
  if (parent) parent.appendChild(el);
  return el;
}
function clock(ms) { return new Date(ms).toISOString().slice(11, 19); }
function number(value, digits) {
  if (value === null) return "missing";
  return value.toLocaleString("en-US", { minimumFractionDigits: digits, maximumFractionDigits: digits });
}
function niceStep(span, target) {
  const raw = span / target, mag = Math.pow(10, Math.floor(Math.log10(raw))), n = raw / mag;
  return (n <= 1 ? 1 : n <= 2 ? 2 : n <= 5 ? 5 : 10) * mag;
}
function timeStep(span, target) {
  const steps = [1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 14400, 21600, 43200, 86400];
  for (const s of steps) if (span / (s * 1000) <= target) return s * 1000;
  return 86400000;
}

const charts = [];
let hovered = -1;

function draw(chart) {
  const host = chart.host;
  host.textContent = "";
  const width = Math.max(300, host.clientWidth), height = 250;
  const m = { top: 28, right: 16, bottom: 30, left: 54 };
  const svg = node("svg", { viewBox: `0 0 ${width} ${height}`, height: height, role: "img", tabindex: 0,
    "aria-label": chart.spec.title + ", historical and live. Use the left and right arrow keys to read values." }, host);
  const all = chart.spec.historical.concat(chart.spec.live).filter(v => v !== null);
  let lo = Math.min(0, ...all), hi = Math.max(0, ...all);
  if (lo === hi) hi = lo + 1;
  const step = niceStep(hi - lo, 4);
  lo = Math.floor(lo / step) * step; hi = Math.ceil(hi / step) * step;
  const t0 = T[0], t1 = T[T.length - 1] > T[0] ? T[T.length - 1] : T[0] + 1;
  const x = t => m.left + (t - t0) / (t1 - t0) * (width - m.left - m.right);
  const y = v => m.top + (hi - v) / (hi - lo) * (height - m.top - m.bottom);
  const compact = Math.max(Math.abs(lo), Math.abs(hi)) >= 10000;

  for (let v = lo; v <= hi + step / 2; v += step) {
    const value = Math.round(v / step) * step;
    node("line", { class: value === 0 ? "axis" : "grid", x1: m.left, x2: width - m.right, y1: y(value), y2: y(value) }, svg);
    const label = compact && value !== 0 ? (value / 1000).toLocaleString("en-US") + "K" : value.toLocaleString("en-US");
    node("text", { x: m.left - 8, y: y(value) + 4, "text-anchor": "end" }, svg).textContent = label;
  }
  const tick = timeStep(t1 - t0, Math.max(2, Math.floor((width - m.left - m.right) / 110)));
  for (let t = Math.ceil(t0 / tick) * tick; t <= t1; t += tick) {
    node("line", { class: "axis", x1: x(t), x2: x(t), y1: height - m.bottom, y2: height - m.bottom + 4 }, svg);
    node("text", { x: x(t), y: height - 8, "text-anchor": "middle" }, svg).textContent =
      tick % 60000 === 0 ? clock(t).slice(0, 5) : clock(t);
  }
  if (DATA.windowFull !== null) {
    const wx = x(DATA.windowFull);
    node("line", { class: "axis", x1: wx, x2: wx, y1: m.top - 20, y2: height - m.bottom }, svg);
    node("text", { class: "mark", x: wx + 6, y: m.top - 10 }, svg).textContent = "window full from " + clock(DATA.windowFull);
  }
  for (const [key] of SERIES) {
    let path = "", pen = false;
    chart.spec[key].forEach((v, i) => {
      if (v === null) { pen = false; return; }
      path += (pen ? "L" : "M") + x(T[i]).toFixed(1) + " " + y(v).toFixed(1);
      pen = true;
    });
    node("path", { class: key === "historical" ? "band" : "line", d: path }, svg);
  }
  chart.cross = node("line", { class: "axis", y1: m.top, y2: height - m.bottom, visibility: "hidden" }, svg);
  chart.dots = SERIES.map(([key]) => node("circle", { class: "dot " + key, r: 5, visibility: "hidden" }, svg));
  chart.x = x; chart.y = y; chart.svg = svg; chart.width = width;
  const hit = node("rect", { x: m.left, y: 0, width: width - m.left - m.right, height: height, fill: "transparent" }, svg);

  function nearest(clientX) {
    const box = svg.getBoundingClientRect();
    const px = (clientX - box.left) * (width / box.width);
    const t = t0 + (px - m.left) / (width - m.left - m.right) * (t1 - t0);
    let a = 0, b = T.length - 1;
    while (b - a > 1) { const mid = (a + b) >> 1; if (T[mid] < t) a = mid; else b = mid; }
    return Math.abs(T[a] - t) <= Math.abs(T[b] - t) ? a : b;
  }
  hit.addEventListener("pointermove", e => show(nearest(e.clientX), e.clientX, e.clientY));
  hit.addEventListener("pointerleave", () => { if (document.activeElement !== svg) hide(); });
  svg.addEventListener("focus", () => focusShow(chart, hovered < 0 ? T.length - 1 : hovered));
  svg.addEventListener("blur", hide);
  svg.addEventListener("keydown", e => {
    const jump = e.shiftKey ? 10 : 1;
    if (e.key === "ArrowLeft") focusShow(chart, Math.max(0, hovered - jump));
    else if (e.key === "ArrowRight") focusShow(chart, Math.min(T.length - 1, hovered + jump));
    else if (e.key === "Escape") hide();
    else return;
    e.preventDefault();
  });
}

function focusShow(chart, i) {
  const box = chart.svg.getBoundingClientRect();
  show(i, box.left + chart.x(T[i]) * (box.width / chart.width), box.top + 40);
}

function show(i, clientX, clientY) {
  hovered = i;
  const tip = document.getElementById("tip");
  tip.textContent = "";
  html("div", "when", clock(T[i]), tip);
  for (const chart of charts) {
    const cx = chart.x(T[i]);
    chart.cross.setAttribute("x1", cx); chart.cross.setAttribute("x2", cx);
    chart.cross.setAttribute("visibility", "visible");
    html("div", "group", chart.spec.title, tip);
    SERIES.forEach(([key, name], k) => {
      const v = chart.spec[key][i], dot = chart.dots[k];
      if (v === null) dot.setAttribute("visibility", "hidden");
      else { dot.setAttribute("cx", cx); dot.setAttribute("cy", chart.y(v)); dot.setAttribute("visibility", "visible"); }
      const row = html("div", "row", undefined, tip);
      html("i", "key " + key, undefined, row);
      html("strong", "", number(v, chart.spec.digits), row);
      html("span", "name", name, row);
    });
  }
  tip.style.display = "block";
  const w = tip.offsetWidth, h = tip.offsetHeight;
  const left = clientX + 16 + w > window.innerWidth ? clientX - 16 - w : clientX + 16;
  tip.style.left = Math.max(8, left) + "px";
  tip.style.top = Math.max(8, Math.min(window.innerHeight - h - 8, clientY - h / 2)) + "px";
}

function hide() {
  document.getElementById("tip").style.display = "none";
  for (const chart of charts) {
    chart.cross.setAttribute("visibility", "hidden");
    chart.dots.forEach(dot => dot.setAttribute("visibility", "hidden"));
  }
}

function table() {
  const el = document.getElementById("chart-table");
  const head = html("tr", "", undefined, html("thead", "", undefined, el));
  html("th", "", "Time", head);
  for (const spec of DATA.charts) for (const [, name] of SERIES) html("th", "", spec.title + ", " + name.toLowerCase(), head);
  const body = html("tbody", "", undefined, el);
  T.forEach((t, i) => {
    const row = html("tr", "", undefined, body);
    html("td", "", clock(t), row);
    for (const spec of DATA.charts) for (const [key] of SERIES) html("td", "", number(spec[key][i], spec.digits), row);
  });
}

for (const spec of DATA.charts) {
  charts.push({ spec: spec, host: document.querySelector('[data-chart="' + spec.id + '"]') });
}
charts.forEach(draw);
table();
// Redraw when a chart's box changes width (window resize, or a scrollbar appearing).
const observer = new ResizeObserver(entries => {
  for (const entry of entries) {
    const chart = charts.find(c => c.host === entry.target);
    if (chart && Math.max(300, chart.host.clientWidth) !== chart.width) { hide(); hovered = -1; draw(chart); }
  }
});
charts.forEach(chart => observer.observe(chart.host));
</script>
</body>
</html>
"""
