FROM nodered/node-red:4.0
USER root
WORKDIR /usr/src/node-red
RUN npm install --no-audit --no-fund --omit=dev kafkajs@2.2.4
COPY settings.js /data/settings.js
COPY flows.json /data/flows.json
COPY flows_cred.json /data/flows_cred.json
RUN chown -R node-red:root /data
USER node-red
EXPOSE 1880
