// ---------------------------------------------------------------------------
// Kafka producer (kafkajs) with micro-batching
// Messages are buffered and flushed every KAFKA_LINGER_MS or KAFKA_BATCH_SIZE
// messages, whichever comes first. If Kafka is unavailable the buffer keeps
// messages (bounded by KAFKA_MAX_BUFFER, oldest dropped and counted) and
// retries - Node-RED keeps accepting MQTT traffic meanwhile.
// ---------------------------------------------------------------------------
const st = context.get('state');
if (!st) return null;
st.buffer.push({ topic: msg.kafkaTopic, key: msg.kafkaKey, value: JSON.stringify(msg.payload) });
if (st.buffer.length > st.maxBuffer) {
    const drop = st.buffer.length - st.maxBuffer;
    st.buffer.splice(0, drop);
    global.get('stats').kafka.dropped += drop;
}
if (st.buffer.length >= st.batchSize && !st.flushing) st.flush();
return null;
