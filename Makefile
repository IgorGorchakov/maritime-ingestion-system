# Maritime Intelligence Platform — developer entrypoints.

MVNW := ./mvnw

.DEFAULT_GOAL := help

.PHONY: help up down logs build test clean \
        run-ingestion run-enricher run-detection run-storage run-api \
        run-all stop-ingestion stop-enricher stop-detection stop-storage stop-api stop-all

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

## ---- Infrastructure ----
up: ## Start infra (Kafka, Schema Registry, Postgres, Prometheus :9090, Grafana :3000)
	docker compose up -d

down: ## Stop infra and remove containers
	docker compose down

logs: ## Tail infra logs
	docker compose logs -f

## ---- Build / test ----
build: ## Compile + install all modules (skip tests)
	$(MVNW) clean install -DskipTests

test: ## Full verify: unit + integration tests across all modules
	$(MVNW) verify

clean: ## Remove all build output
	$(MVNW) clean

## ---- Run services ----
run-ingestion: ## Run ingestion service (:8081)
	$(MVNW) -pl maritime-ingestion spring-boot:run -Dmaven.test.skip=true

run-enricher: ## Run enricher service (:8082)
	$(MVNW) -pl maritime-enricher spring-boot:run -Dmaven.test.skip=true

run-detection: ## Run detection service (:8086)
	$(MVNW) -pl maritime-detection spring-boot:run -Dmaven.test.skip=true

run-storage: ## Run storage service (:8083)
	$(MVNW) -pl maritime-storage spring-boot:run -Dmaven.test.skip=true

run-api: ## Run API service (:8084)
	$(MVNW) -pl maritime-api spring-boot:run -Dmaven.test.skip=true

run-all: ## Start all 5 Spring Boot services in background (logs → /tmp/*.log)
	$(MVNW) -pl maritime-ingestion spring-boot:run -Dmaven.test.skip=true > /tmp/ingestion.log 2>&1 &
	$(MVNW) -pl maritime-enricher  spring-boot:run -Dmaven.test.skip=true > /tmp/enricher.log  2>&1 &
	$(MVNW) -pl maritime-detection spring-boot:run -Dmaven.test.skip=true > /tmp/detection.log 2>&1 &
	$(MVNW) -pl maritime-storage   spring-boot:run -Dmaven.test.skip=true > /tmp/storage.log   2>&1 &
	$(MVNW) -pl maritime-api       spring-boot:run -Dmaven.test.skip=true > /tmp/api.log       2>&1 &
	@echo "All 5 services starting. Logs: /tmp/{ingestion,enricher,detection,storage,api}.log"

## ---- Stop individual services by port (safe — won't kill sibling services) ----
stop-ingestion: ## Kill ingestion service on :8081
	-lsof -ti:8081 | xargs kill 2>/dev/null || true

stop-enricher: ## Kill enricher service on :8082
	-lsof -ti:8082 | xargs kill 2>/dev/null || true

stop-detection: ## Kill detection service on :8086
	-lsof -ti:8086 | xargs kill 2>/dev/null || true

stop-storage: ## Kill storage service on :8083
	-lsof -ti:8083 | xargs kill 2>/dev/null || true

stop-api: ## Kill API service on :8084
	-lsof -ti:8084 | xargs kill 2>/dev/null || true

stop-all: stop-ingestion stop-enricher stop-detection stop-storage stop-api ## Stop all 5 Spring Boot services

## ---- Spark batch jobs ----
spark-build: ## Build the shaded Spark fat JAR
	$(MVNW) -pl maritime-spark package -DskipTests

spark-daily: ## Run DailyVesselAggregatesJob locally
	$(MVNW) -pl maritime-spark exec:java -Plocal \
		-Dexec.mainClass=com.maritime.spark.jobs.DailyVesselAggregatesJob

spark-risk: ## Run RiskRollupJob locally
	$(MVNW) -pl maritime-spark exec:java -Plocal \
		-Dexec.mainClass=com.maritime.spark.jobs.RiskRollupJob

spark-hotspot: ## Run LoiteringHotspotJob locally
	$(MVNW) -pl maritime-spark exec:java -Plocal \
		-Dexec.mainClass=com.maritime.spark.jobs.LoiteringHotspotJob
