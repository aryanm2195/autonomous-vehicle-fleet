# Convenience targets (Docker Desktop / docker compose v2)
.PHONY: up demo down logs ps test verify loadtest loadtest-quick chaos flows clean

up:            ## start the full stack (simulator in MODE from .env)
	docker compose up -d --build

demo:          ## lecturer demo: 20 vehicles + scripted events
	MODE=demo VEHICLE_COUNT=20 docker compose up -d --build
	@echo "Dashboard: http://localhost:8000  (operator / operator123)"
	@echo "Node-RED:  http://localhost:1880   Grafana: http://localhost:3000   Prometheus: http://localhost:9090"

down:
	docker compose down

clean:         ## stop and delete all data volumes
	docker compose down -v

logs:
	docker compose logs -f --tail=100 simulator alert-service

ps:
	docker compose ps

test:          ## unit tests (no containers needed)
	cd simulator && python -m pytest -q
	cd services && python -m pytest -q
	cd backend && python -m pytest -q

verify:        ## check every function of the running stack (PASS/FAIL)
	python scripts/verify_all.py

loadtest:      ## progressive load test against the running stack
	docker compose stop simulator
	python scripts/loadtest.py --stages 10,100,1000,5000,10000 --duration 60 --docker-stats

loadtest-quick:
	docker compose stop simulator
	python scripts/loadtest.py --stages 10,100,1000 --duration 30 --docker-stats

chaos:         ## failure scenarios (simulator must be running in docker)
	python scripts/chaos.py sensor_failure abnormal_behaviour malformed duplicate mqtt_disconnect \
	  kafka_consumer_failure database_unavailable redis_unavailable microservice_crash

flows:         ## rebuild nodered/flows.json from nodered/functions/*.js
	python nodered/build_flows.py
