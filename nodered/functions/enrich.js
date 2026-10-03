// ---------------------------------------------------------------------------
// Filtering + enrichment ("processing pipeline" from the project plan §7)
//  * drops simulator-only debugging fields
//  * adds ingestion metadata (received time, edge latency, pipeline stage)
//  * derives a priority flag so critical telemetry can be spotted downstream
//  * chooses the Kafka topic and partition key (vehicle_id => per-vehicle ordering)
// ---------------------------------------------------------------------------
const stats = global.get('stats');
const d = msg.payload;
const now = Date.now();
const edgeLatency = now - msg.generatedAt;

if (d.simulation) delete d.simulation.malformed_kind;
d.ingest = {
    received_at: new Date(now).toISOString(),
    edge_latency_ms: edgeLatency,
    gateway: env.get('HOSTNAME') || 'node-red',
    mqtt_topic: msg.topic
};

// reservoir of recent edge latencies for /stats percentiles
const lat = stats.latency[msg.kind];
lat.push(edgeLatency);
if (lat.length > 20000) lat.splice(0, lat.length - 20000);

if (msg.kind === 'event') {
    d.ingest.priority = (d.severity === 'CRITICAL' || d.severity === 'HIGH') ? 'HIGH' : 'NORMAL';
    msg.kafkaTopic = env.get('KAFKA_TOPIC_EVENTS') || 'vehicle.events';
    stats.forwarded.event++;
} else {
    const s = d.safety || {};
    const critical = s.emergency_braking === true ||
        (typeof s.time_to_collision_s === 'number' && s.time_to_collision_s < 3) ||
        d.vehicle.status === 'EMERGENCY';
    d.location.speed_mps = Math.round(d.location.speed_kmh / 3.6 * 100) / 100;   // unit conversion
    d.ingest.priority = critical ? 'HIGH' : 'NORMAL';
    msg.kafkaTopic = env.get('KAFKA_TOPIC_TELEMETRY') || 'vehicle.telemetry';
    stats.forwarded.telemetry++;
}
msg.kafkaKey = d.vehicle_id;
return msg;
