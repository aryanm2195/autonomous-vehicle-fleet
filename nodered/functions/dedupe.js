// ---------------------------------------------------------------------------
// De-duplication + sequence tracking
// A bounded "seen" set of message_ids (two rotating generations, so memory is
// capped) drops duplicates caused by QoS-1 redelivery, store-and-forward
// replays or faulty senders. Sequence gaps / reordering are counted, not dropped.
// ---------------------------------------------------------------------------
const stats = global.get('stats');
const MAX = Number(env.get('DEDUPE_WINDOW') || 200000);
let cur = context.get('cur'), old = context.get('old');
if (!cur) { cur = new Set(); old = new Set(); context.set('cur', cur); context.set('old', old); }
const id = msg.payload.message_id;
if (cur.has(id) || old.has(id)) {
    stats.duplicates++;
    return null;
}
cur.add(id);
if (cur.size >= MAX) {                 // rotate generations
    context.set('old', cur);
    context.set('cur', new Set());
}
if (msg.kind === 'telemetry') {
    let seqs = context.get('seqs');
    if (!seqs) { seqs = new Map(); context.set('seqs', seqs); }
    const last = seqs.get(msg.payload.vehicle_id);
    const s = msg.payload.sequence;
    if (last !== undefined && Number.isInteger(s)) {
        if (s <= last) stats.outOfOrder++;
        else if (s > last + 1) stats.sequenceGaps += (s - last - 1);
    }
    if (Number.isInteger(s) && (last === undefined || s > last)) seqs.set(msg.payload.vehicle_id, s);
}
return msg;
