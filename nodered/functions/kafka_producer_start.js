// On Start: create the kafkajs producer and the flush loop
const stats = global.get('stats');
const enabled = String(env.get('KAFKA_ENABLED') || 'true').toLowerCase() !== 'false';
const st = {
    buffer: [], flushing: false, connected: false, producer: null,
    batchSize: Number(env.get('KAFKA_BATCH_SIZE') || 500),
    maxBuffer: Number(env.get('KAFKA_MAX_BUFFER') || 200000),
    linger: Number(env.get('KAFKA_LINGER_MS') || 20)
};
context.set('state', st);
stats.kafka.enabled = enabled;

st.flush = async function () {
    if (st.flushing || st.buffer.length === 0) return;
    if (!enabled) {                                    // edge-only benchmark mode
        stats.kafka.discardedDisabled += st.buffer.length;
        st.buffer.length = 0;
        return;
    }
    if (!st.connected) return;
    st.flushing = true;
    const batch = st.buffer.splice(0, Math.max(st.batchSize, 2000));
    const byTopic = {};
    for (const m of batch) (byTopic[m.topic] = byTopic[m.topic] || []).push({ key: m.key, value: m.value });
    const topicMessages = Object.keys(byTopic).map(t => ({ topic: t, messages: byTopic[t] }));
    const t0 = Date.now();
    try {
        await st.producer.sendBatch({ topicMessages, acks: -1, timeout: 10000 });
        stats.kafka.sent += batch.length;
        stats.kafka.batches++;
        stats.kafka.lastBatchMs = Date.now() - t0;
    } catch (e) {
        stats.kafka.errors++;
        stats.kafka.lastError = String(e.message || e);
        st.buffer.unshift(...batch);                  // keep order, retry later
        node.status({ fill: 'red', shape: 'ring', text: 'kafka error: ' + stats.kafka.lastError.slice(0, 40) });
    } finally {
        st.flushing = false;
    }
};

st.timer = setInterval(() => {
    st.flush();
    node.status({ fill: st.connected ? 'green' : (enabled ? 'yellow' : 'grey'), shape: 'dot',
        text: (enabled ? (st.connected ? 'connected' : 'connecting') : 'kafka disabled') +
            ' | sent ' + stats.kafka.sent + ' | buf ' + st.buffer.length });
}, st.linger);

if (enabled) {
    const { Kafka, logLevel } = global.get('kafkajs');
    const kafka = new Kafka({
        clientId: 'node-red-' + (env.get('HOSTNAME') || 'edge'),
        brokers: String(env.get('KAFKA_BROKER') || 'kafka:9092').split(','),
        logLevel: logLevel.WARN,
        retry: { initialRetryTime: 300, retries: 1000000 }
    });
    st.producer = kafka.producer({ allowAutoTopicCreation: false, idempotent: false, maxInFlightRequests: 5 });
    const connect = async () => {
        try {
            await st.producer.connect();
            st.connected = true;
            stats.kafka.connected = true;
            node.log('Kafka producer connected to ' + env.get('KAFKA_BROKER'));
        } catch (e) {
            stats.kafka.lastError = String(e.message || e);
            node.warn('Kafka connect failed, retrying in 3s: ' + stats.kafka.lastError);
            setTimeout(connect, 3000);
        }
    };
    st.producer.on(st.producer.events.DISCONNECT, () => { st.connected = false; stats.kafka.connected = false; setTimeout(connect, 3000); });
    connect();
}
