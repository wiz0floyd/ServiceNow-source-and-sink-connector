# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working in this repository.

## Project Purpose

A pair of Kafka Connect connectors (source + sink) that bridge a customer's ServiceNow instance Hermes Kafka cluster with Confluent Cloud (primary target). Requirements doc: `Hermes_External_Kafka_Connector_Requirements_v01.md`.

Distribution target: **Confluent Hub top tier (Verified-Gold-equivalent)**. License: **Apache 2.0** (pending legal review).

## Build & Test

Java 11+, Maven 3.x. Stack: `kafka-clients` + `connect-api` (provided scope) + JUnit 5 + Mockito.

```bash
# Build (skip tests)
mvn package -DskipTests

# Run all tests
mvn test

# Run a single test class
mvn test -Dtest=HermesSinkTaskTest

# Run a single test method
mvn test -Dtest=HermesSinkTaskTest#putPreservesHeaders

# Build the Confluent Cloud deployment zip
mvn package
# → target/hermes-kafka-connector-0.1.0-SNAPSHOT-package.zip
```

On Windows without `mvn` on PATH, run via WSL: `wsl mvn test`

## Architecture

### Connector Pair

**Sink connector** — standard Kafka Connect sink. Reads from Confluent Cloud, writes to a single load-balanced Hermes endpoint (ports 4000–4050). Fails loud if target topic doesn't exist — no auto-create.

**Source connector** — custom shape. Internally runs **two embedded `KafkaConsumer` clients**, one per Hermes peer cluster (ports 4100–4150 and 4200–4250). Each cluster has independent offset tracking. Source-partition keys carry the cluster identifier so Connect's offset store can disambiguate them. Precedent: Confluent Replicator / MM2 `MirrorSourceConnector`.

### Authentication: mTLS + KIP-519

Hermes supports **mTLS only** — no SASL/SCRAM, no OAUTHBEARER. The connector must implement a custom `SslEngineFactory` (KIP-519) that loads the keystore from config strings (PEM or base64 PKCS12) rather than from a file path. This is mandatory because Confluent Cloud secrets are injected as strings, not files.

One mTLS cert covers both peer clusters per customer. Each connector instance manages its own cert lifecycle (N connectors = N lifecycles). Cert provisioning is manual via the instance PKI page.

### Bootstrap Endpoint Derivation

Customers provide their ServiceNow instance URL only. The connector derives Hermes bootstrap addresses from `<instanceURL>` + known port conventions — no manual broker address input.

### Data Plane

- **Serialization (MVP):** Pass-through bytes. Both ends use `ByteArrayConverter`. No Schema Registry in MVP.
- **Headers:** Preserved verbatim.
- **Sink partitioning (MVP):** Hash by key.
- **Topic rename (source):** Configurable via regex rules — evaluate `RegexRouter` SMT before building first-class config.

### Delivery Semantics

End-to-end **at-least-once**. Idempotency/dedup is the customer's responsibility. Sink defaults: `enable.idempotence=true`, `acks=all`. No exactly-once promise to/from Hermes.

## Key Open Items (Build Phase)

These are unresolved and must be decided before/during implementation:

- Offset coordination model between the two source peer clusters
- Idempotent producer compatibility with Hermes broker version
- Exact Confluent Cloud cert injection mechanism
- Hermes Kafka protocol version and client library compatibility
- DLQ strategy and config surface
- JMX metrics naming conventions
- Test matrix scope required for Confluent Hub listing
- SMT-based vs first-class config for regex rename rules

## Out of Scope for MVP

Do not implement: automated cert rotation, Schema Registry / schema-aware mode, configurable sink partitioning beyond hash-by-key, exactly-once semantics, topic auto-creation, Confluent Platform self-hosted as primary target.
