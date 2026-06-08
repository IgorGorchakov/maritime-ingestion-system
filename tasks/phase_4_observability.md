# Phase 4: Observability — Micrometer + Prometheus + Grafana + Structured Logs

> Part of the senior-MVP roadmap in `PLAN.md`. Sequence: 0 → 1 → 2 → 3 → **4** → … → 8.
> Status: ☑ Done — verified live 2026-06-08 (counters increment under load; correlation id traced ingestion→streaming→storage).

## Objective
Make the platform measurable and debuggable: metrics on every service, dashboards, and correlation-id tracing across services.

## JD competency proven
Monitoring, troubleshooting, bottleneck elimination.

## Why (rationale to internalize)
- **RED method** (Rate, Errors, Duration) — you cannot claim to "optimize bottlenecks" without first measuring them.
- A **correlation id** carried in Kafka headers + MDC makes a single vessel greppable across all four services (there is no HTTP call chain to carry trace context for async Kafka hops).
- Structured JSON logs replace `System.out`/`printStackTrace` so logs are queryable.

## Steps
1. Add `spring-boot-starter-actuator` + `micrometer-registry-prometheus` to ALL services (currently missing in streaming + storage).
2. Custom metrics: `events_ingested_total`, `events_enriched_total`, `events_quarantined_total`, `risk_level_total{level}`, `dlq_total`, plus processing-latency `Timer`s.
3. `logback-spring.xml` with JSON encoder; replace every `System.out.println` / `printStackTrace` with SLF4J.
4. Correlation-id filter/interceptor: generate at ingestion, propagate via Kafka header, bind to MDC in each consumer.
5. Add `prometheus` + `grafana` to `docker-compose.yml`; provision a starter dashboard.

## Deliverables
- Actuator + Prometheus metrics everywhere; Grafana dashboards; JSON logs; correlation id end-to-end.

## Verification
- `/actuator/prometheus` counters increment under load.
- Grafana shows risk-level mix + p99 processing latency.
- One MMSI's correlation id is found in ingestion, streaming, and storage logs.
