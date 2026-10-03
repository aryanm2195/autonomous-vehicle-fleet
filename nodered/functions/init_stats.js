// Manual "reset counters" button (the counters themselves are created in settings.js)
const s = global.get('stats');
s.startedAt = Date.now();
s.received.telemetry = s.received.event = 0;
s.forwarded.telemetry = s.forwarded.event = 0;
s.rejected = 0; s.rejectReasons = {};
s.duplicates = s.outOfOrder = s.sequenceGaps = 0;
s.latency.telemetry.length = 0; s.latency.event.length = 0;
node.status({ fill: 'blue', shape: 'dot', text: 'reset ' + new Date().toISOString() });
return null;
