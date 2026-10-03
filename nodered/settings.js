// Node-RED settings for the ingestion / edge-processing tier
function loadKafkajs() {
    try { return require('kafkajs'); }
    catch (e) { return require('/usr/src/node-red/node_modules/kafkajs'); }
}

module.exports = {
    flowFile: 'flows.json',
    flowFilePretty: true,
    uiPort: process.env.PORT || 1880,
    // Protect the editor in anything other than a local demo
    adminAuth: process.env.NODERED_ADMIN_PASSWORD_HASH ? {
        type: 'credentials',
        users: [{ username: process.env.NODERED_ADMIN_USER || 'admin',
                  password: process.env.NODERED_ADMIN_PASSWORD_HASH, permissions: '*' }]
    } : undefined,
    credentialSecret: process.env.NODERED_CREDENTIAL_SECRET || false,
    diagnostics: { enabled: false },
    runtimeState: { enabled: false },
    logging: { console: { level: process.env.NODERED_LOG_LEVEL || 'info', metrics: false, audit: false } },
    functionExternalModules: false,
    functionTimeout: 0,
    functionGlobalContext: {
        kafkajs: loadKafkajs(),
        // shared in-memory counters (exposed on GET /metrics and GET /stats)
        stats: {
            startedAt: Date.now(),
            received: { telemetry: 0, event: 0 },
            forwarded: { telemetry: 0, event: 0 },
            rejected: 0, rejectReasons: {},
            duplicates: 0, outOfOrder: 0, sequenceGaps: 0,
            latency: { telemetry: [], event: [] },
            kafka: { enabled: true, connected: false, sent: 0, batches: 0, errors: 0, dropped: 0,
                     discardedDisabled: 0, lastBatchMs: 0, lastError: null }
        }
    },
    editorTheme: { projects: { enabled: false }, page: { title: 'AV Fleet - Node-RED ingestion' } }
};
