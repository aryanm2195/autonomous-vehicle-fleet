"""MQTT publishing layer.

Two connection models are supported:

* ``per_vehicle`` – every virtual vehicle owns its own MQTT session (client id
  = vehicle id, own Last-Will-and-Testament). This is the most faithful model
  of an IoT fleet and is used in demo mode.
* ``shared``      – vehicles are multiplexed over a pool of gateway sessions
  (``VEHICLES_PER_CONNECTION`` each). This mirrors a telematics gateway / edge
  aggregator and lets one process simulate thousands of vehicles without
  thousands of TCP sockets and threads.

Each link implements store-and-forward: while disconnected, messages go into a
bounded buffer and are flushed on reconnect (oldest dropped first when full).
"""
from __future__ import annotations

import collections
import logging
import socket
import ssl
import threading
import time

import paho.mqtt.client as mqtt

from . import metrics as M

log = logging.getLogger("fleetsim.mqtt")


class MqttLink:
    def __init__(self, client_id: str, cfg, lwt_topic: str | None = None):
        self.client_id = client_id
        self.cfg = cfg
        self.connected = threading.Event()
        self.forced_offline_until = 0.0
        self.buffer: collections.deque = collections.deque(maxlen=cfg.offline_buffer_size)
        self._lock = threading.Lock()
        self.lwt_topic = lwt_topic
        c = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id=client_id,
                        protocol=mqtt.MQTTv311, clean_session=True)
        if cfg.mqtt_username:
            c.username_pw_set(cfg.mqtt_username, cfg.mqtt_password)
        if cfg.mqtt_tls:
            c.tls_set(ca_certs=cfg.mqtt_ca_cert, cert_reqs=ssl.CERT_REQUIRED)
        if lwt_topic:
            c.will_set(lwt_topic, payload='{"state":"offline","reason":"unexpected_disconnect"}', qos=1, retain=True)
        c.max_inflight_messages_set(1000)
        c.max_queued_messages_set(0)
        c.reconnect_delay_set(min_delay=1, max_delay=10)
        c.on_connect = self._on_connect
        c.on_disconnect = self._on_disconnect
        self.client = c

    # paho callbacks run in the paho network thread
    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code == 0 or getattr(reason_code, "value", 1) == 0:
            try:  # small frames: disable Nagle so telemetry is not held back waiting for ACKs
                client.socket().setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except Exception:
                pass
            self.connected.set()
            M.CONNECTED.inc()
            if self.lwt_topic:
                client.publish(self.lwt_topic, '{"state":"online"}', qos=1, retain=True)
            self._flush()
        else:
            log.warning("%s connect refused: %s", self.client_id, reason_code)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties=None):
        if self.connected.is_set():
            M.CONNECTED.dec()
        self.connected.clear()
        log.info("%s disconnected (%s)", self.client_id, reason_code)

    def start(self):
        self.client.connect_async(self.cfg.mqtt_broker, self.cfg.mqtt_port, keepalive=30)
        self.client.loop_start()

    def stop(self):
        try:
            if self.lwt_topic and self.connected.is_set():
                self.client.publish(self.lwt_topic, '{"state":"offline","reason":"shutdown"}', qos=1, retain=True)
            self.client.disconnect()
        finally:
            self.client.loop_stop()

    def publish(self, topic: str, payload: bytes, qos: int = 0) -> None:
        if not self.connected.is_set() or time.time() < self.forced_offline_until:
            with self._lock:
                full = len(self.buffer) == self.buffer.maxlen
                self.buffer.append((topic, payload, qos))  # deque drops the oldest when full
            if full:
                M.BUFFER_DROPPED.inc()
            else:
                M.BUFFERED.inc()
            return
        info = self.client.publish(topic, payload, qos=qos)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            M.PUBLISH_ERRORS.inc()
            with self._lock:
                self.buffer.append((topic, payload, qos))
            M.BUFFERED.inc()

    def _flush(self):
        with self._lock:
            items = list(self.buffer)
            self.buffer.clear()
        if items:
            log.info("%s flushing %d buffered messages", self.client_id, len(items))
        for topic, payload, qos in items:
            self.client.publish(topic, payload, qos=qos)
            M.BUFFER_FLUSHED.inc()
        if items:
            M.BUFFERED.dec(len(items))

    # ---- failure simulation ----------------------------------------------
    def simulate_disconnect(self, seconds: float):
        """Drop the TCP session (without LWT suppression) and stay offline for ``seconds``."""
        self.forced_offline_until = time.time() + seconds
        self.client.disconnect()
        # a deliberate disconnect ends paho's network loop; restart it after the outage

        def _reconnect():
            try:
                self.client.loop_stop()
                self.client.connect_async(self.cfg.mqtt_broker, self.cfg.mqtt_port, keepalive=30)
                self.client.loop_start()
                log.info("%s reconnecting after simulated outage", self.client_id)
            except Exception as exc:
                log.warning("%s reconnect failed: %s", self.client_id, exc)
        threading.Timer(seconds, _reconnect).start()


class FleetPublisher:
    def __init__(self, cfg, vehicle_ids: list[str]):
        self.cfg = cfg
        self.prefix = cfg.mqtt_topic_prefix
        self.links: list[MqttLink] = []
        self.link_for: dict[str, MqttLink] = {}
        if cfg.mqtt_connection_mode == "per_vehicle":
            if len(vehicle_ids) > 1000:
                log.warning("per_vehicle mode with %d vehicles opens %d sockets/threads; "
                            "consider MQTT_CONNECTION_MODE=shared", len(vehicle_ids), len(vehicle_ids))
            for vid in vehicle_ids:
                link = MqttLink(vid, cfg, lwt_topic=f"{self.prefix}/{vid}/status")
                self.links.append(link)
                self.link_for[vid] = link
        else:
            per = max(1, cfg.vehicles_per_connection)
            n = (len(vehicle_ids) + per - 1) // per
            base = f"sim-gw-{cfg.vehicle_id_offset}-{int(time.time()) % 100000}"
            for i in range(n):
                link = MqttLink(f"{base}-{i:03d}", cfg, lwt_topic=f"{self.prefix}/_gateways/{base}-{i:03d}/status")
                self.links.append(link)
            for idx, vid in enumerate(vehicle_ids):
                self.link_for[vid] = self.links[idx // per]

    def start(self, wait: float = 15.0) -> int:
        for link in self.links:
            link.start()
        deadline = time.time() + wait
        for link in self.links:
            link.connected.wait(max(0.0, deadline - time.time()))
        up = sum(1 for l in self.links if l.connected.is_set())
        log.info("MQTT: %d/%d connections up to %s:%s (%s mode)", up, len(self.links),
                 self.cfg.mqtt_broker, self.cfg.mqtt_port, self.cfg.mqtt_connection_mode)
        return up

    def stop(self):
        for link in self.links:
            link.stop()

    def telemetry(self, vid: str, payload: bytes):
        self.link_for[vid].publish(f"{self.prefix}/{vid}/telemetry", payload, self.cfg.mqtt_telemetry_qos)
        M.MESSAGES.labels("telemetry").inc()

    def event(self, vid: str, payload: bytes):
        self.link_for[vid].publish(f"{self.prefix}/{vid}/events", payload, self.cfg.mqtt_event_qos)
        M.MESSAGES.labels("event").inc()

    def raw(self, vid: str, kind: str, payload: bytes, label: str):
        self.link_for[vid].publish(f"{self.prefix}/{vid}/{kind}", payload, 0)
        M.MESSAGES.labels(label).inc()

    def simulate_disconnect(self, seconds: float, fraction: float = 1.0):
        n = max(1, int(len(self.links) * fraction))
        for link in self.links[:n]:
            link.simulate_disconnect(seconds)
        return n
