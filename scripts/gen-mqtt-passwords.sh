#!/usr/bin/env bash
# Regenerate mosquitto/config/passwd from the passwords in .env
set -euo pipefail
cd "$(dirname "$0")/.."
source .env
docker run --rm -v "$PWD/mosquitto/config:/m" eclipse-mosquitto:2.0.20 sh -c "
  rm -f /m/passwd && touch /m/passwd &&
  mosquitto_passwd -b /m/passwd ${MQTT_SIM_USERNAME} '${MQTT_SIM_PASSWORD}' &&
  mosquitto_passwd -b /m/passwd ${MQTT_NODERED_USERNAME} '${MQTT_NODERED_PASSWORD}' &&
  mosquitto_passwd -b /m/passwd admin 'admin-dev-password'"
echo "mosquitto/config/passwd regenerated"
