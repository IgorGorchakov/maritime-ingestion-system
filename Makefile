# Maritime Intelligence Platform — developer entrypoints.
# One-command stack control + build/test so a reviewer never has to memorise flags.
# Uses the committed Maven wrapper (./mvnw) for a reproducible toolchain.

MVNW := ./mvnw

.DEFAULT_GOAL := help

.PHONY: help up down logs build test clean \
        run-ingestion run-streaming run-storage run-gateway

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

## ---- Infrastructure (Docker Compose) ----
up: ## Start infra (Kafka, LocalStack, Postgres/PostGIS) in the background
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

## ---- Run services (each in its own terminal) ----
run-ingestion: ## Run ingestion service (:8081)
	$(MVNW) -pl maritime-ingestion spring-boot:run

run-streaming: ## Run streaming/enrichment service (:8082)
	$(MVNW) -pl maritime-streaming spring-boot:run

run-storage: ## Run storage service (:8083)
	$(MVNW) -pl maritime-storage spring-boot:run

run-gateway: ## Run gateway/API service (:8084)
	$(MVNW) -pl maritime-gateway spring-boot:run
