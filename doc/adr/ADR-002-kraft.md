# ADR-002 — Kafka KRaft single-node (apache/kafka)

- Status: accepted
- Date: 2026-10-19
- Deciders: CTO

## Context
`tz_v2.md` fixes Kafka **KRaft** (ZooKeeper excluded) and a single broker.
The earlier draft used `bitnami/kafka`; Bitnami restricted free Docker-Hub images
(~2025), breaking `docker pull`.

## Decision
Use the official `apache/kafka:3.7.0` image in KRaft mode with a combined
controller+broker node:
- `KAFKA_PROCESS_ROLES=controller,broker`, `KAFKA_CONTROLLER_QUORUM_VOTERS=0@kafka:9093`;
- internal listener `PLAINTEXT://kafka:9092`, external `localhost:29092`,
  controller `:9093`;
- heap tuned down (`KAFKA_HEAP_OPTS=-Xmx512m`) to avoid OOM on 8 GB hosts;
- topic creation is a best-effort sidecar container (`kafka-topics`) that runs
  `kafka/topics.sh`, so the main compose `up` flow stays unblocked.

## Consequences
- One less container/state (no ZooKeeper), modern control protocol.
- KRaft requires the separate controller port and roles (pitfall §4.3) — both set.
- Official image pulls reliably; no registry dependency.
- 4 canonical topics + 3 DLQ are created/ensured idempotently at start.
