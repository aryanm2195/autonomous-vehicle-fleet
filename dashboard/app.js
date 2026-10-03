// AV Fleet dashboard - every number on this page comes from the backend API,
// which reads what the pipeline wrote to Redis/MongoDB. Nothing is hard-coded.
'use strict';

const STATUS_COLORS = {
  AVAILABLE: '#3fb950', IN_SERVICE: '#4ea1ff', IDLE: '#8b9aab',
  CHARGING: '#a371f7', MAINTENANCE: '#d29922', EMERGENCY: '#ff4d6d'
};
const TRAFFIC_COLORS = { LOW: '#3fb950', MODERATE: '#4ea1ff', HIGH: '#d29922', CONGESTED: '#f85149' };
const $ = (s) => document.querySelector(s);
const esc = (s) => String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const fmt = (n, d = 0) => (n === null || n === undefined || Number.isNaN(n)) ? '–' : Number(n).toLocaleString(undefined, { maximumFractionDigits: d, minimumFractionDigits: d });

let token = sessionStorage.getItem('token');
let role = sessionStorage.getItem('role');
let map, markers = new Map(), ws, wsRetry = 1000;
let lastPositions = [];

// ------------------------------------------------------------------ api
async function api(path, opts = {}) {
  const r = await fetch(path, { ...opts, headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + token, ...(opts.headers || {}) } });
  if (r.status === 401) { logout(); throw new Error('unauthorised'); }
  if (!r.ok) throw new Error((await r.json().catch(() => ({}))).detail || r.statusText);
  return r.json();
}

// ------------------------------------------------------------------ auth
$('#loginForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  $('#loginErr').textContent = '';
  try {
    const r = await fetch('/api/auth/login', { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: $('#u').value, password: $('#p').value }) });
    const j = await r.json();
    if (!r.ok) throw new Error(j.detail || 'login failed');
    token = j.access_token; role = j.role;
    sessionStorage.setItem('token', token); sessionStorage.setItem('role', role); sessionStorage.setItem('user', j.username);
    start();
  } catch (err) { $('#loginErr').textContent = err.message; }
});
function logout() {
  sessionStorage.clear(); token = null;
  if (ws) ws.close();
  $('#login').classList.remove('hidden');
}
$('#logout').addEventListener('click', logout);

// ------------------------------------------------------------------ map
function initMap() {
  $('#legend').innerHTML = Object.entries(STATUS_COLORS).map(([s, c]) => `<span style="--c:${c}">${s.replace('_', ' ')}</span>`).join('');
  if (window.L) {
    map = L.map('map', { preferCanvas: true }).setView([-37.8136, 144.9631], 13);
    // Esri "World Dark Gray" basemap: no API key needed, dark theme built in
    L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}', {
      maxZoom: 16, attribution: 'Tiles &copy; Esri &mdash; Esri, DeLorme, NAVTEQ' }).addTo(map);
    L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Reference/MapServer/tile/{z}/{y}/{x}', {
      maxZoom: 16 }).addTo(map);
  } else {
    // offline fallback: plain canvas scatter plot of positions
    $('#map').innerHTML = '<canvas id="mapCanvas" style="width:100%;height:100%"></canvas>';
  }
}
function drawPositions(pos) {
  lastPositions = pos;
  if (!window.L) return drawCanvasMap(pos);
  const seen = new Set();
  for (const p of pos) {
    seen.add(p.id);
    let m = markers.get(p.id);
    const color = STATUS_COLORS[p.s] || '#999';
    if (!m) {
      m = L.circleMarker([p.lat, p.lon], { radius: p.s === 'EMERGENCY' ? 7 : 4, weight: 1, color, fillColor: color, fillOpacity: .85 }).addTo(map);
      m.on('click', () => showVehicle(p.id));
      markers.set(p.id, m);
    } else {
      m.setLatLng([p.lat, p.lon]);
      m.setStyle({ color, fillColor: color, radius: p.s === 'EMERGENCY' ? 7 : 4 });
    }
    m.bindTooltip(`${p.id}<br>${p.s} · ${fmt(p.v, 1)} km/h · ${fmt(p.b, 1)}% · ${p.p} pax`);
  }
  for (const [id, m] of markers) if (!seen.has(id)) { map.removeLayer(m); markers.delete(id); }
}
function drawCanvasMap(pos) {
  const c = $('#mapCanvas'); if (!c) return;
  const w = c.width = c.clientWidth, h = c.height = c.clientHeight, g = c.getContext('2d');
  g.fillStyle = '#0b1016'; g.fillRect(0, 0, w, h);
  if (!pos.length) return;
  const lats = pos.map(p => p.lat), lons = pos.map(p => p.lon);
  const [a, b, cc, d] = [Math.min(...lats), Math.max(...lats), Math.min(...lons), Math.max(...lons)];
  for (const p of pos) {
    g.fillStyle = STATUS_COLORS[p.s] || '#999';
    g.beginPath(); g.arc(10 + (p.lon - cc) / (d - cc || 1) * (w - 20), 10 + (b - p.lat) / (b - a || 1) * (h - 20), 3, 0, 7); g.fill();
  }
}

// ------------------------------------------------------------------ summary / kpis
function bars(el, data, colors) {
  const total = Object.values(data).reduce((a, b) => a + b, 0) || 1;
  el.innerHTML = Object.entries(data).map(([k, v]) =>
    `<div class="row"><span>${esc(k.replace('_', ' '))}</span><div class="track"><div class="fill" style="width:${(v / total * 100).toFixed(1)}%;background:${colors[k] || '#4ea1ff'}"></div></div><span class="val">${fmt(v)}</span></div>`).join('');
}
function renderSummary(s) {
  if (!s) return;
  $('#k-active').textContent = fmt(s.active_vehicles);
  $('#k-mps').textContent = fmt(s.messages_per_second, 1);
  $('#k-bat').textContent = s.avg_battery_pct !== undefined ? fmt(s.avg_battery_pct, 1) + '%' : '–';
  $('#k-speed').textContent = s.avg_speed_kmh !== undefined ? fmt(s.avg_speed_kmh, 1) + ' km/h' : '–';
  $('#k-pax').textContent = fmt(s.passengers_on_board);
  if (s.status) bars($('#statusBars'), s.status, STATUS_COLORS);
  if (s.traffic) bars($('#trafficBars'), s.traffic, TRAFFIC_COLORS);
  if (s.hotspots && s.hotspots.length) {
    $('#hotspots').textContent = 'Demand hotspots (k-means): ' + s.hotspots.map(h => `${h.latitude.toFixed(3)},${h.longitude.toFixed(3)} (${h.vehicles})`).join(' · ');
  }
}

// ------------------------------------------------------------------ alerts
function alertItem(a, isNew) {
  const li = document.createElement('li');
  li.className = a.severity + (isNew ? ' new' : '');
  li.dataset.id = a.alert_id;
  const when = a.created_at ? new Date(a.created_at).toLocaleTimeString() : '';
  const lat = a.pipeline_latency_ms !== undefined && a.pipeline_latency_ms !== null ? ` · pipeline ${fmt(a.pipeline_latency_ms, 1)} ms` : '';
  const details = a.details ? Object.entries(a.details).slice(0, 3).map(([k, v]) => `${k}=${typeof v === 'number' ? fmt(v, 2) : v}`).join(', ') : '';
  li.innerHTML = `<div class="a-top"><span>${esc(a.event_type)}</span><span class="pill">${esc(a.severity)}</span></div>
    <div class="a-meta">${esc(a.vehicle_id)} · ${esc(when)}${esc(lat)} · ${esc((a.sources || []).join('+'))} · <b>${esc(a.status || 'OPEN')}</b></div>
    <div class="a-meta">${esc(details)}</div>
    ${role !== 'viewer' && a.status !== 'RESOLVED' ? `<div class="a-actions">${a.status !== 'ACKNOWLEDGED' ? '<button data-act="ack">Acknowledge</button>' : ''}<button class="ghost" data-act="resolve">Resolve</button></div>` : ''}`;
  li.querySelectorAll('button').forEach(b => b.addEventListener('click', async (e) => {
    e.stopPropagation();
    try { await api(`/api/alerts/${a.alert_id}/${b.dataset.act}`, { method: 'POST' }); loadAlerts(); } catch (err) { toast(err.message); }
  }));
  li.addEventListener('click', () => showVehicle(a.vehicle_id));
  return li;
}
async function loadAlerts() {
  const f = $('#alertFilter').value;
  const list = await api('/api/alerts?limit=100' + (f ? '&status=' + f : ''));
  const ul = $('#alerts'); ul.innerHTML = '';
  list.forEach(a => ul.appendChild(alertItem(a)));
  const st = await api('/api/alerts/stats');
  $('#k-open').textContent = fmt(st.by_status?.OPEN || 0);
  $('#k-lat').textContent = st.latency_ms ? fmt(st.latency_ms.p95, 1) + ' ms' : '–';
}
$('#alertFilter').addEventListener('change', loadAlerts);
function onLiveAlert(a) {
  const f = $('#alertFilter').value;
  if (!f || f === 'OPEN') {
    const ul = $('#alerts');
    ul.insertBefore(alertItem(a, true), ul.firstChild);
    while (ul.children.length > 150) ul.removeChild(ul.lastChild);
  }
  const k = $('#k-open'); k.textContent = fmt((parseInt(k.textContent.replace(/\D/g, '')) || 0) + 1);
  if (a.severity === 'CRITICAL') toast(`CRITICAL ${a.event_type} on ${a.vehicle_id}` + (a.pipeline_latency_ms ? ` (${fmt(a.pipeline_latency_ms, 1)} ms end-to-end)` : ''));
}
function toast(msg) {
  const t = $('#toast'); t.textContent = msg; t.classList.add('show');
  clearTimeout(toast._t); toast._t = setTimeout(() => t.classList.remove('show'), 5000);
}

// ------------------------------------------------------------------ pipeline strip
const STAGES = [
  ['simulator', 'Simulator', s => s?.stats ? `${fmt(s.stats.stats?.telemetry)} msgs · ${fmt(s.stats.vehicles)} veh` : ''],
  ['mqtt', 'MQTT', () => 'Mosquitto'],
  ['nodered', 'Node-RED', s => s?.stats ? `${fmt(s.stats.received?.telemetry)} in · ${fmt(s.stats.rejected)} rej · ${fmt(s.stats.duplicates)} dup` : ''],
  ['kafka', 'Kafka', (s, all) => all.nodered?.stats ? `${fmt(all.nodered.stats.kafka?.sent)} acked` : ''],
  ['telemetry-service', 'Telemetry svc', s => s?.stats ? `${fmt(s.stats.inserted)} stored · p95 ${fmt(s.stats.e2e_latency_ms?.p95, 0)}ms` : ''],
  ['alert-service', 'Alert svc', s => s?.stats ? `${fmt(s.stats.alerts_created)} alerts` : ''],
  ['analytics-service', 'Analytics svc', s => s?.stats ? `${fmt(s.stats.consumed)} consumed` : ''],
  ['mongodb', 'MongoDB', () => ''], ['redis', 'Redis', () => '']
];
async function loadPipeline() {
  try {
    const h = await api('/api/pipeline/health');
    $('#pipeline').innerHTML = STAGES.map(([k, name, num], i) =>
      `${i ? '<span class="arrow">→</span>' : ''}<div class="stage"><div class="name"><span class="dot ${h[k]?.up ? 'ok' : 'bad'}"></span>${name}</div><div class="num">${esc(num(h[k], h))}</div></div>`).join('');
  } catch (e) { /* shown as stale */ }
}

// ------------------------------------------------------------------ throughput chart
async function loadThroughput() {
  const pts = await api('/api/analytics/throughput');
  const c = $('#tput'), w = c.width = c.clientWidth, h = c.height = 170, g = c.getContext('2d');
  g.clearRect(0, 0, w, h);
  if (pts.length < 2) { g.fillStyle = '#8b9aab'; g.fillText('waiting for data from analytics-service…', 10, 20); return; }
  const max = Math.max(...pts.map(p => p.mps), 1) * 1.15;
  g.strokeStyle = '#2a3542'; g.fillStyle = '#8b9aab'; g.font = '11px system-ui';
  for (let i = 0; i <= 4; i++) { const y = h - 20 - (h - 30) * i / 4; g.beginPath(); g.moveTo(40, y); g.lineTo(w, y); g.stroke(); g.fillText(fmt(max * i / 4), 2, y + 4); }
  g.strokeStyle = '#4ea1ff'; g.lineWidth = 2; g.beginPath();
  pts.forEach((p, i) => { const x = 40 + (w - 45) * i / (pts.length - 1), y = h - 20 - (h - 30) * p.mps / max; i ? g.lineTo(x, y) : g.moveTo(x, y); });
  g.stroke();
  g.fillStyle = '#8b9aab'; g.fillText(new Date(pts[0].t).toLocaleTimeString(), 40, h - 4);
  const last = new Date(pts[pts.length - 1].t).toLocaleTimeString(); g.fillText(last, w - g.measureText(last).width - 2, h - 4);
}

// ------------------------------------------------------------------ vehicles table
Object.keys(STATUS_COLORS).forEach(s => $('#vehStatus').insertAdjacentHTML('beforeend', `<option>${s}</option>`));
async function loadVehicles() {
  const st = $('#vehStatus').value, q = $('#vehSearch').value.trim();
  let rows;
  if (q) { rows = lastPositions.filter(p => p.id.includes(q)).slice(0, 100).map(p => ({ vehicle_id: p.id, status: p.s, battery: p.b, speed_kmh: p.v, passengers: p.p })); }
  else { rows = (await api('/api/vehicles?limit=100' + (st ? '&status=' + st : ''))).items; }
  $('#vehTable tbody').innerHTML = rows.map(r => `<tr data-id="${esc(r.vehicle_id)}"><td>${esc(r.vehicle_id)}</td>
    <td><span class="st" style="color:${STATUS_COLORS[r.status] || '#999'}">${esc(r.status)}</span></td>
    <td>${fmt(r.battery, 1)}%</td><td>${fmt(r.speed_kmh, 1)} km/h</td><td>${esc(r.passengers)}</td>
    <td>${esc(r.traffic || '')}</td><td>${r.timestamp ? new Date(r.timestamp).toLocaleTimeString() : ''}</td></tr>`).join('');
  $('#vehTable tbody').querySelectorAll('tr').forEach(tr => tr.addEventListener('click', () => showVehicle(tr.dataset.id)));
}
$('#vehStatus').addEventListener('change', loadVehicles);
$('#vehSearch').addEventListener('input', loadVehicles);
async function showVehicle(id) {
  $('#ctlVehicle').value = id;
  try {
    const v = await api('/api/vehicles/' + encodeURIComponent(id));
    const m = markers.get(id);
    if (m && map) { map.setView(m.getLatLng(), 16); m.openTooltip(); }
    toast(`${id}: ${v.vehicle?.status} · ${v.location?.speed_kmh} km/h · battery ${v.battery?.percentage}% · lidar ${v.lidar?.status} · radar ${v.radar?.status} · camera ${v.camera?.status}`);
  } catch (e) { toast(e.message); }
}

// ------------------------------------------------------------------ demo control
document.querySelectorAll('#controlCard button').forEach(b => b.addEventListener('click', async () => {
  const body = { vehicle_id: $('#ctlVehicle').value.trim() || undefined };
  if (b.dataset.s) body.scenario = b.dataset.s;
  if (b.dataset.f) { body.failure = b.dataset.f; body.seconds = 10; body.count = 20; delete body.vehicle_id; }
  try { const r = await api('/api/simulator/trigger', { method: 'POST', body: JSON.stringify(body) });
    $('#ctlOut').textContent = JSON.stringify(r); } catch (e) { $('#ctlOut').textContent = 'Error: ' + e.message; }
}));

// ------------------------------------------------------------------ websocket
function connectWs() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  ws = new WebSocket(`${proto}://${location.host}/ws?token=${encodeURIComponent(token)}`);
  ws.onopen = () => { $('#conn').className = 'dot ok'; wsRetry = 1000; };
  ws.onclose = (e) => {
    $('#conn').className = 'dot bad';
    if (e.code === 4401) return logout();
    if (token) setTimeout(connectWs, wsRetry = Math.min(wsRetry * 2, 15000));
  };
  ws.onmessage = (e) => {
    const m = JSON.parse(e.data);
    if (m.type === 'snapshot') { renderSummary(m.summary); drawPositions(m.positions || []); }
    else if (m.type === 'alert') onLiveAlert(m.data);
  };
}

// ------------------------------------------------------------------ boot
let timers = [];
function start() {
  $('#login').classList.add('hidden');
  $('#whoami').textContent = `${sessionStorage.getItem('user') || ''} (${role})`;
  $('#controlCard').style.display = role === 'viewer' ? 'none' : '';
  if (!map) initMap();
  connectWs();
  loadAlerts(); loadPipeline(); loadThroughput(); loadVehicles();
  timers.forEach(clearInterval);
  timers = [setInterval(loadPipeline, 5000), setInterval(loadThroughput, 5000), setInterval(loadVehicles, 5000),
            setInterval(loadAlerts, 15000)];
}
if (token) start();
