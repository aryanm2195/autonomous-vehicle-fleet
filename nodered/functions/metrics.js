// GET /metrics  -> Prometheus text format;  GET /stats -> JSON (used by the load-test harness)
const s = global.get('stats');
function pct(arr, p) {
    if (!arr.length) return null;
    const a = arr.slice().sort((x, y) => x - y);
    return a[Math.min(a.length - 1, Math.floor(p / 100 * a.length))];
}
if (msg.req.path.endsWith('/stats')) {
    if (msg.req.query.reset === '1') { s.latency.telemetry.length = 0; s.latency.event.length = 0; }
    msg.payload = {
        uptime_s: (Date.now() - s.startedAt) / 1000,
        received: s.received, forwarded: s.forwarded, rejected: s.rejected, reject_reasons: s.rejectReasons,
        duplicates: s.duplicates, out_of_order: s.outOfOrder, sequence_gaps: s.sequenceGaps, kafka: s.kafka,
        edge_latency_ms: {
            telemetry: { samples: s.latency.telemetry.length, p50: pct(s.latency.telemetry, 50), p95: pct(s.latency.telemetry, 95), p99: pct(s.latency.telemetry, 99) },
            event: { samples: s.latency.event.length, p50: pct(s.latency.event, 50), p95: pct(s.latency.event, 95), p99: pct(s.latency.event, 99) }
        }
    };
    msg.headers = { 'Content-Type': 'application/json' };
    return msg;
}
const L = [];
const c = (name, help, v, labels) => { L.push(`# HELP ${name} ${help}`, `# TYPE ${name} counter`); for (const [lab, val] of (labels || [['', v]])) L.push(`${name}${lab} ${val}`); };
const g = (name, help, v) => { L.push(`# HELP ${name} ${help}`, `# TYPE ${name} gauge`, `${name} ${v}`); };
c('nodered_messages_received_total', 'MQTT messages received', null, [['{kind="telemetry"}', s.received.telemetry], ['{kind="event"}', s.received.event]]);
c('nodered_messages_forwarded_total', 'Messages forwarded to Kafka', null, [['{kind="telemetry"}', s.forwarded.telemetry], ['{kind="event"}', s.forwarded.event]]);
c('nodered_messages_rejected_total', 'Messages rejected by validation', null,
  Object.keys(s.rejectReasons).length ? Object.entries(s.rejectReasons).map(([k, v]) => [`{reason="${k}"}`, v]) : [['{reason="none"}', 0]]);
c('nodered_duplicates_total', 'Duplicate messages dropped', s.duplicates);
c('nodered_out_of_order_total', 'Out-of-order telemetry messages', s.outOfOrder);
c('nodered_kafka_sent_total', 'Messages acknowledged by Kafka', s.kafka.sent);
c('nodered_kafka_errors_total', 'Kafka send errors', s.kafka.errors);
c('nodered_kafka_dropped_total', 'Messages dropped because the Kafka buffer overflowed', s.kafka.dropped);
g('nodered_kafka_connected', 'Kafka producer connected', s.kafka.connected ? 1 : 0);
const p = pct(s.latency.telemetry, 95);
g('nodered_edge_latency_p95_ms', 'p95 vehicle->Node-RED latency (recent window)', p === null ? 'NaN' : p);
msg.payload = L.join('\n') + '\n';
msg.headers = { 'Content-Type': 'text/plain; version=0.0.4' };
return msg;
