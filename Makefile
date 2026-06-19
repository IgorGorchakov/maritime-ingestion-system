# Maritime Intelligence Platform — developer entrypoints.

MVNW := ./mvnw

.DEFAULT_GOAL := help

.PHONY: help up down logs build test clean \
        run-ingestion run-enricher run-detection run-storage run-api

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
	$(MVNW) -pl maritime-ingestion spring-boot:run

run-enricher: ## Run enricher service (:8082)
	$(MVNW) -pl maritime-enricher spring-boot:run

run-detection: ## Run detection service (:8086)
	$(MVNW) -pl maritime-detection spring-boot:run

run-storage: ## Run storage service (:8083)
	$(MVNW) -pl maritime-storage spring-boot:run

run-api: ## Run API service (:8084)
	$(MVNW) -pl maritime-api spring-boot:run

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
