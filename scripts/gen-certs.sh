#!/usr/bin/env bash
# Creates a private CA + server certificate for MQTT over TLS (port 8883) and a
# client certificate for mutual TLS. Then uncomment the TLS listener in mosquitto.conf,
# mount ./mosquitto/certs, and set MQTT_TLS=true / MQTT_CA_CERT for the simulator.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p mosquitto/certs && cd mosquitto/certs
openssl req -x509 -new -nodes -newkey rsa:2048 -keyout ca.key -out ca.crt -days 365 -subj "/CN=av-fleet-ca"
openssl req -new -nodes -newkey rsa:2048 -keyout server.key -out server.csr -subj "/CN=mosquitto"
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 365
openssl req -new -nodes -newkey rsa:2048 -keyout simulator.key -out simulator.csr -subj "/CN=simulator"
openssl x509 -req -in simulator.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out simulator.crt -days 365
rm -f *.csr
echo "certificates written to mosquitto/certs"
