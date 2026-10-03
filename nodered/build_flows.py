"""Assemble nodered/flows.json from the readable JS sources in nodered/functions/.

    python nodered/build_flows.py
"""
import json
import pathlib

HERE = pathlib.Path(__file__).parent
F = lambda name: (HERE / "functions" / name).read_text()
TAB = "tab_fleet"
BROKER = "mqtt_broker"

nodes = [
    {"id": TAB, "type": "tab", "label": "Fleet ingestion pipeline", "disabled": False,
     "info": "Virtual vehicles (software simulator) -> MQTT -> **validate -> de-duplicate -> filter/enrich** -> Kafka.\n"
             "Rejected messages go to the dead-letter topic. Horizontal scaling: run more Node-RED replicas; "
             "MQTT shared subscriptions ($share/nodered/...) load-balance vehicles across them."},
    {"id": BROKER, "type": "mqtt-broker", "name": "Mosquitto", "broker": "${MQTT_BROKER}", "port": "${MQTT_PORT}",
     "clientid": "", "autoConnect": True, "usetls": False, "protocolVersion": "5", "keepalive": "30",
     "cleansession": True, "autoUnsubscribe": True, "birthTopic": "", "closeTopic": "", "willTopic": "",
     "credentials": {"user": "${MQTT_USERNAME}", "password": "${MQTT_PASSWORD}"}},

    # ---- ingest --------------------------------------------------------------
    {"id": "mqtt_telemetry", "type": "mqtt in", "z": TAB, "name": "Vehicle telemetry (shared sub)",
     "topic": "$share/nodered/fleet/+/telemetry", "qos": "0", "datatype": "utf8", "broker": BROKER,
     "nl": False, "rap": True, "rh": 0, "inputs": 0, "x": 170, "y": 120, "wires": [["fn_validate"]]},
    {"id": "mqtt_events", "type": "mqtt in", "z": TAB, "name": "Vehicle events QoS1 (shared sub)",
     "topic": "$share/nodered/fleet/+/events", "qos": "1", "datatype": "utf8", "broker": BROKER,
     "nl": False, "rap": True, "rh": 0, "inputs": 0, "x": 180, "y": 200, "wires": [["fn_validate"]]},

    {"id": "fn_validate", "type": "function", "z": TAB, "name": "Parse + validate schema", "func": F("validate.js"),
     "outputs": 2, "timeout": 0, "noerr": 0, "initialize": "", "finalize": "", "libs": [],
     "x": 440, "y": 160, "wires": [["fn_dedupe"], ["fn_kafka"]],
     "outputLabels": ["valid", "rejected -> DLQ"]},
    {"id": "fn_dedupe", "type": "function", "z": TAB, "name": "De-duplicate (message_id)", "func": F("dedupe.js"),
     "outputs": 1, "timeout": 0, "noerr": 0, "initialize": "", "finalize": "", "libs": [],
     "x": 680, "y": 140, "wires": [["fn_enrich"]]},
    {"id": "fn_enrich", "type": "function", "z": TAB, "name": "Filter + enrich + route", "func": F("enrich.js"),
     "outputs": 1, "timeout": 0, "noerr": 0, "initialize": "", "finalize": "", "libs": [],
     "x": 910, "y": 140, "wires": [["fn_kafka", "sw_events"]]},
    {"id": "fn_kafka", "type": "function", "z": TAB, "name": "Kafka producer (batched)", "func": F("kafka_producer.js"),
     "outputs": 0, "timeout": 0, "noerr": 0, "initialize": F("kafka_producer_start.js"),
     "finalize": F("kafka_producer_stop.js"), "libs": [], "x": 1150, "y": 160, "wires": []},

    # ---- visibility of emergency events in the editor -----------------------
    {"id": "sw_events", "type": "switch", "z": TAB, "name": "critical events only", "property": "payload.severity",
     "propertyType": "msg", "rules": [{"t": "eq", "v": "CRITICAL", "vt": "str"}], "checkall": "true",
     "repair": False, "outputs": 1, "x": 1140, "y": 240, "wires": [["dbg_events"]]},
    {"id": "dbg_events", "type": "debug", "z": TAB, "name": "CRITICAL events", "active": True, "tosidebar": True,
     "console": False, "tostatus": True, "complete": "payload", "targetType": "msg",
     "statusVal": "payload.event_type", "statusType": "msg", "x": 1350, "y": 240, "wires": []},

    # ---- observability -------------------------------------------------------
    {"id": "http_metrics", "type": "http in", "z": TAB, "name": "", "url": "/metrics", "method": "get",
     "upload": False, "swaggerDoc": "", "x": 160, "y": 340, "wires": [["fn_metrics"]]},
    {"id": "http_stats", "type": "http in", "z": TAB, "name": "", "url": "/stats", "method": "get",
     "upload": False, "swaggerDoc": "", "x": 150, "y": 400, "wires": [["fn_metrics"]]},
    {"id": "fn_metrics", "type": "function", "z": TAB, "name": "Prometheus / JSON stats", "func": F("metrics.js"),
     "outputs": 1, "timeout": 0, "noerr": 0, "initialize": "", "finalize": "", "libs": [],
     "x": 400, "y": 370, "wires": [["http_resp"]]},
    {"id": "http_resp", "type": "http response", "z": TAB, "name": "", "statusCode": "", "headers": {},
     "x": 610, "y": 370, "wires": []},
    {"id": "inj_reset", "type": "inject", "z": TAB, "name": "Reset counters", "props": [], "repeat": "",
     "crontab": "", "once": False, "onceDelay": 0.1, "topic": "", "x": 170, "y": 470, "wires": [["fn_reset"]]},
    {"id": "fn_reset", "type": "function", "z": TAB, "name": "reset stats", "func": F("init_stats.js"),
     "outputs": 1, "timeout": 0, "noerr": 0, "initialize": "", "finalize": "", "libs": [],
     "x": 370, "y": 470, "wires": [[]]},
    {"id": "comment1", "type": "comment", "z": TAB, "name": "SOFTWARE SIMULATION: all telemetry comes from virtual vehicles",
     "info": "No physical vehicles or sensors. See README.", "x": 290, "y": 60, "wires": []},
]

(HERE / "flows.json").write_text(json.dumps(nodes, indent=2))
# credentials are provided via env substitution (${MQTT_USERNAME}) - Node-RED reads them from flows_cred.json
creds = {BROKER: {"user": "${MQTT_USERNAME}", "password": "${MQTT_PASSWORD}"}}
(HERE / "flows_cred.json").write_text(json.dumps(creds, indent=2))
print(f"wrote {len(nodes)} nodes to flows.json")
