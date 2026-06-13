# Maritime Intelligence Platform - Learning Project

This project simulates a simplified version of the Maritime Intelligence Platform described in the job description. It demonstrates the core architecture using Java, Spring Boot, Kafka, and Postgres — and runs entirely on localhost with no cloud dependency.

## Architecture Overview

1.  **Ingestion Service**: Simulates AIS vessel data feeds and pushes them to a Kafka topic (`maritime.ais.raw`).
2.  **Streaming Service**: Consumes raw vessel events from Kafka, enriches them with geospatial data (checking against restricted zones), calculates a risk score, and pushes the enriched data to another Kafka topic (`maritime.enriched`).
3.  **Storage Service**: Consumes enriched events from Kafka, saves "hot" data (latest state per vessel) to Postgres and "cold" data to a partitioned Parquet archive on the local filesystem. Provides a REST API to query vessel risk data. The storage tiers sit behind `VesselStateStore` / `ColdTierWriter` interfaces, so the backing store is swappable.
4.  **Gateway Service**: Aggregates data from the Storage Service and provides a unified API endpoint for frontend consumption.

## Prerequisites

-   Java 17+
-   Maven 3.8+
-   Docker & Docker Compose

## How to Run

### 1. Start Infrastructure
```bash
docker-compose up -d
```
This starts Kafka, Zookeeper, Schema Registry, and PostgreSQL.

### 2. Build the Project
```bash
mvn clean install -DskipTests
```

### 3. Run Services
Run each service in a separate terminal:

```bash
# Ingestion Service (Port 8081)
cd maritime-ingestion && mvn spring-boot:run

# Streaming Service (Port 8082)
cd ../maritime-streaming && mvn spring-boot:run

# Storage Service (Port 8083)
cd ../maritime-storage && mvn spring-boot:run

# Gateway Service (Port 8084)
cd ../maritime-gateway && mvn spring-boot:run
```

### 4. Trigger Data Simulation
Start the AIS simulator:
```bash
curl -X POST http://localhost:8081/api/v1/simulate/start
```

You should see logs in the Streaming and Storage service consoles indicating data processing.

### 5. Query Data
Retrieve vessel intelligence via the Gateway:
```bash
curl http://localhost:8084/api/v1/intelligence/123456789
```

### 6. Stop Simulation
```bash
curl -X POST http://localhost:8081/api/v1/simulate/stop
```

## Key Concepts Demonstrated

-   **Maven Multi-Module Project**: Shared code in `maritime-common`.
-   **Kafka Streaming**: Producer/Consumer pattern with topic partitioning.
-   **Geospatial Processing**: Point-in-polygon checks using JTS.
-   **Tiered Storage**: Postgres hot tier (latest state per vessel) + local Parquet cold tier, decoupled behind storage-port interfaces.
-   **Microservices Communication**: REST API aggregation via Gateway.