.PHONY: up down test smoke logs dlq query

up:        ## Build and start everything
	docker compose up -d --build

down:      ## Stop everything and delete data
	docker compose down -v

test:      ## Unit tests on a local Flink mini-cluster
	cd pipeline && mvn -B verify

smoke:     ## End-to-end check against the running stack
	./scripts/smoke-test.sh

logs:      ## Follow the Flink job logs
	docker compose logs -f flink-jobmanager flink-taskmanager

dlq:       ## Show the last 5 rejected messages and why
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
	  --bootstrap-server kafka:9092 --topic patient-events-dlq --from-beginning --max-messages 5

query:     ## Abnormal vitals by department, as the read-only analyst user
	curl -s -u analyst:analystpass -H 'Content-Type: application/json' \
	  -d '{"sql":"SELECT department, COUNT(*) AS abnormal FROM patient_events WHERE abnormal = true GROUP BY department ORDER BY abnormal DESC"}' \
	  http://localhost:8099/query/sql | python3 -m json.tool
