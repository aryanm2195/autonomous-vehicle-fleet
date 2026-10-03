// ---------------------------------------------------------------------------
// Parse + schema validation (edge filtering)
// Output 1: valid message   Output 2: rejected -> dead-letter topic
// Mirrors schemas/telemetry.schema.json & event.schema.json with cheap
// hand-written checks (full JSON-Schema validation per message is too slow
// for thousands of messages per second in a single Node-RED instance).
// ---------------------------------------------------------------------------
const stats = global.get('stats');
const kind = msg.topic.endsWith('/events') ? 'event' : 'telemetry';
stats.received[kind]++;

function reject(reason) {
    stats.rejected++;
    stats.rejectReasons[reason] = (stats.rejectReasons[reason] || 0) + 1;
    const raw = typeof msg.payload === 'string' ? msg.payload : JSON.stringify(msg.payload);
    msg.kafkaTopic = env.get('KAFKA_TOPIC_DLQ') || 'vehicle.dlq';
    msg.kafkaKey = (msg.topic.split('/')[1] || 'unknown');
    msg.payload = {
        reason: reason,
        mqtt_topic: msg.topic,
        received_at: new Date().toISOString(),
        raw: raw ? raw.slice(0, 2000) : null
    };
    return [null, msg];
}

let d = msg.payload;
if (Buffer.isBuffer(d)) d = d.toString('utf8');
if (typeof d === 'string') {
    try { d = JSON.parse(d); } catch (e) { return reject('invalid_json'); }
}
if (d === null || typeof d !== 'object' || Array.isArray(d)) return reject('not_an_object');

const isNum = (v) => typeof v === 'number' && Number.isFinite(v);
const inRange = (v, lo, hi) => isNum(v) && v >= lo && v <= hi;
const optNum = (v, lo, hi) => v === null || v === undefined || inRange(v, lo, hi);

if (typeof d.vehicle_id !== 'string' || !/^vehicle-\d{5,6}$/.test(d.vehicle_id)) return reject('bad_vehicle_id');
if (msg.topic.split('/')[1] !== d.vehicle_id) return reject('topic_vehicle_mismatch');
if (typeof d.message_id !== 'string' || d.message_id.length < 8) return reject('missing_message_id');
const ts = Date.parse(d.timestamp);
if (!Number.isFinite(ts)) return reject('bad_timestamp');
const now = Date.now();
if (ts > now + 5000) return reject('timestamp_in_future');
if (now - ts > 24 * 3600 * 1000) return reject('timestamp_too_old');

if (kind === 'event') {
    const TYPES = ['COLLISION_WARNING', 'OBSTACLE_DETECTED', 'SUDDEN_BRAKING', 'LOW_BATTERY', 'CRITICAL_BATTERY',
        'SENSOR_FAILURE', 'ABNORMAL_SPEED', 'ROUTE_DEVIATION', 'EMERGENCY_STOP'];
    const SEV = ['INFO', 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'];
    if (!TYPES.includes(d.event_type)) return reject('unknown_event_type');
    if (!SEV.includes(d.severity)) return reject('bad_severity');
} else {
    const L = d.location, B = d.battery, V = d.vehicle;
    if (!L || typeof L !== 'object') return reject('missing_location');
    if (!inRange(L.latitude, -90, 90) || !inRange(L.longitude, -180, 180)) return reject('bad_coordinates');
    if (!inRange(L.speed_kmh, 0, 400)) return reject('bad_speed');
    if (!inRange(L.heading, 0, 360)) return reject('bad_heading');
    if (!B || !inRange(B.percentage, 0, 100) || typeof B.charging !== 'boolean') return reject('bad_battery');
    if (!V || !['AVAILABLE', 'IN_SERVICE', 'IDLE', 'CHARGING', 'MAINTENANCE', 'EMERGENCY'].includes(V.status)) return reject('bad_vehicle_status');
    if (!Number.isInteger(V.passenger_count) || V.passenger_count < 0 || V.passenger_count > 6) return reject('bad_passenger_count');
    if (!d.lidar || !optNum(d.lidar.distance_m, 0, 300)) return reject('bad_lidar');
    if (!d.radar || !optNum(d.radar.distance_m, 0, 300)) return reject('bad_radar');
    if (!d.camera || !optNum(d.camera.confidence, 0, 1)) return reject('bad_camera');
    if (!d.traffic || !['LOW', 'MODERATE', 'HIGH', 'CONGESTED'].includes(d.traffic.status)) return reject('bad_traffic');
}
msg.kind = kind;
msg.payload = d;
msg.generatedAt = ts;
return [msg, null];
