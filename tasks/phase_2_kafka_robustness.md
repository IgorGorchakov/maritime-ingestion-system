# Phase 2: Kafka Robustness — Idempotent Producer, Manual Acks, DLQ

> Part of the senior-MVP roadmap in `PLAN.md`. Sequence: 0 → 1 → **2** → … → 8.
> Status: ☐ Not started

## Objective
Make the streaming layer production-grade: no message loss, no head-of-line blocking on poison messages, explicit Kafka configuration in code.

## JD competency proven
Implementing and operating Kafka streaming solutions.

## Why (rationale to internalize)
- **Decision: at-least-once + idempotent producer + DLQ**, NOT full exactly-once — EOS is impossible across non-transactional S3/DynamoDB sinks.
- `enable.idempotence=true` + `acks=all`: producer PID/sequence numbers dedupe retries (no dupes from a retried send).
- `AckMode.MANUAL_IMMEDIATE`: commit the offset only *after* the side-effect succeeds → at-least-once.
- `DefaultErrorHandler` + `DeadLetterPublishingRecoverer`: a poison record goes to `<topic>.DLT` instead of blocking the partition forever.

## Steps
1. Add `config/` packages: `KafkaProducerConfig`, `KafkaConsumerConfig`, `TopicConfig` (explicit topics, `partitions=3`).
2. Producer: `enable.idempotence=true`, `acks=all`, sane retries.
3. Consumer containers: `AckMode.MANUAL_IMMEDIATE`; listeners take `Acknowledgment` and ack after success.
4. Register `DefaultErrorHandler` with backoff + `DeadLetterPublishingRecoverer`; declare `*.DLT` topics.

## Deliverables
- Explicit Kafka `@Configuration` per service (replaces pure properties auto-config).
- DLT topics; idempotent producer; manual-ack listeners.

## Verification
- A malformed/poison record lands in `<topic>.DLT`; the pipeline keeps processing subsequent records.
- Kill the streaming service mid-batch → on restart, no records lost (offsets weren't pre-committed).

## Risks / constraints
- Hard prerequisite for Phase 3 (quality gate) and Phase 6 (stateful detection).
