# Hermes ↔ External Kafka Connector Pair — Requirements (v0.1)

**Status:** Workshop output, ready for engineering review
**Date:** May 15, 2026
**Owner:** Adam Celli (PEx)

---

## Executive Summary

- Pair of Kafka Connect connectors (source + sink) bridging a customer's ServiceNow instance Hermes cluster with their external Kafka. Complementary to Stream Connect.
- Sink-led MVP attacks the Hermes-as-destination gap not solved by Cluster Linking; source ships in the pair as a Connect-native alternative.
- Confluent Cloud is the primary target. mTLS-only auth requires in-memory keystore loading via a custom `SslEngineFactory` (KIP-519) to work around CCloud's lack of filesystem mount.
- Pass-through bytes for MVP; schema-aware mode and configurable partitioning on the roadmap.
- Top-tier Confluent Hub listing is the distribution goal. Apache 2.0 license working assumption pending legal review. Engineering-owned from day one. No fixed GA date.

---

## 1. Positioning

A pair of Kafka Connect connectors (one source, one sink) bridging a customer's ServiceNow instance Hermes cluster with their external Kafka cluster. Complementary to Stream Connect — Stream Connect handles ServiceNow ↔ Hermes; these connectors handle Hermes ↔ external Kafka. Distribution target is Confluent Hub.

## 2. MVP Scope

Pair, **sink-led**. Sink attacks the unambiguous gap (Hermes-as-destination, not solved by Cluster Linking). Source ships in the pair as a Connect-native alternative to existing replication tools with different cost/UX trade-offs.

## 3. Target Deployment

- **Primary:** Confluent Cloud (Custom Connector)
- **Not the MVP target:** Confluent Platform self-hosted — existing tools (Replicator, MM2) are viable there
- Other Connect workers (MSK Connect, OSS Connect) inherit Cloud-compatible design; not explicitly tested in MVP

## 4. Authentication

mTLS only — Hermes does not support SASL/SCRAM or OAUTHBEARER today.

- One mTLS cert covers both peer clusters per customer
- Per-connection cert lifecycle (N connectors = N cert lifecycles)
- Cert provisioning is manual via the instance PKI page (automated rotation is roadmap)
- Confluent Cloud cannot mount keystore files. Connector must load keystore material from config strings (PEM or base64 PKCS12), via a custom `ssl.engine.factory.class` hook (introduced by KIP-519 in Apache Kafka 2.6 — version to confirm in build phase)
- Exact CCloud secret-injection mechanism (Cluster Linking secrets, Custom Connector secrets, secret provider) is a build-phase decision

## 5. Hermes Cluster Topology

Publicly addressable, gated only by mTLS. No PrivateLink/VPC peering required.

### Sink direction (writes into Hermes)

- Single load-balanced endpoint, ports 4000-4050
- LB routes to whichever cluster is currently active (4100 or 4200 behind it)
- Sink treats this as one logical bootstrap

### Source direction (reads from Hermes)

- Two peer clusters exposed directly: ports 4100-4150 and 4200-4250
- Same topic names on both; offsets are **independent** (not synchronized)
- Normal operation: writes hit only the active cluster; standby is idle
- Duplicates occur only during the failover edge case (write in flight when active switches). Steady-state is effectively exactly-once-from-Hermes; failover-window duplicates are bounded exception

## 6. Connector Shape

**Sink:** standard Connect sink connector. One Hermes target endpoint (LB).

**Source:** custom Connect source connector with internal dual-consumer. One connector instance manages two embedded `KafkaConsumer` clients (one per peer cluster), emits `SourceRecord`s, framework writes to external Kafka destination. Source-partition keys carry cluster identifier so per-cluster offsets disambiguate. Pattern precedented by Confluent Replicator and MM2's `MirrorSourceConnector` (both run single-cluster per instance today; framework imposes no constraint on dual-cluster in one task).

## 7. Topic Handling

- Topic discovery via `AdminClient.listTopics()`. Hermes cert scoping enforces per-customer namespace; no additional filtering needed
- Hermes topic naming conventions documented in ServiceNow product docs (referenced at build phase)
- **Sink:** fails loud if target Hermes topic doesn't exist. No auto-create (current 30-topic limit, future 960-partition limit, and auto-create implies the ServiceNow side isn't wired up anyway)
- **Source:** destination topic naming on external side is configurable via regex rename rules. Likely via stock `RegexRouter` SMT with guided-setup defaults

## 8. Data Plane

- **Serialization:** pass-through bytes for MVP. No Schema Registry coupling. `ByteArrayConverter` on both ends
- **Headers:** all record headers preserved verbatim. Stream Connect inbound accepts all headers
- **Sink partitioning:** hash by key for MVP. Other modes (preserve source partition, custom partitioner) are roadmap
- **Schema-aware mode:** roadmap. Will interop with ServiceNow's existing Confluent SR sync for Avro on the ServiceNow side

## 9. Delivery Semantics

End-to-end at-least-once. **Idempotency / dedup is the customer's responsibility on consumption.**

- **Sink:** `enable.idempotence=true`, `acks=all` as working defaults (Hermes engineering arb pending)
- **Source:** Connect framework offset commits via the offset storage topic; independent per peer cluster
- **No exactly-once** promise to/from Hermes

## 10. User Experience

- Customer provides **instance URL only**
- Connector derives Hermes bootstrap endpoints from URL + known port conventions
- Guided setup walks customer through: mTLS cert generation, keystore/secret injection into Confluent Cloud, topic selection via `AdminClient`, regex rename rules
- All failures (missing topic, cert issues, unreachable Hermes) surface as loud, clear config errors

## 11. Distribution & Quality

- **Confluent Hub tier:** top tier (Verified-Gold-equivalent — current program naming to verify in build phase). Comprehensive test matrix, security review, support contract, ongoing maintenance
- **License:** Apache 2.0 working assumption, pending ServiceNow legal review
- **Ownership:** Stream Connect or D&A engineering owns from day one. Voice-of-customer + requirements ownership stays with PEx. Personal prototype possible in parallel for design de-risking, off the critical path
- **GA target:** none. Ship when ready

## 12. Out of Scope for MVP (Explicit Roadmap)

- Automated mTLS cert rotation
- Schema-aware mode (Schema Registry integration)
- Configurable sink partitioning beyond hash-by-key
- Confluent Platform self-hosted as primary support target
- Exactly-once delivery semantics
- Topic auto-creation in either direction

## 13. Open Items (Build Phase)

- Offset coordination model between peer source clusters
- Idempotent producer compatibility with Hermes (engineering arb)
- Exact Confluent Cloud cert injection mechanism (Cluster Linking secrets vs Custom Connector secrets vs secret provider integration)
- Hermes Kafka protocol version and client library compatibility range
- DLQ strategy and config surface (stock Connect DLQ for sinks; source-side error handling)
- JMX metrics naming conventions
- Test matrix scope for top-tier Hub listing
- Official connector branding / naming
- Current Confluent Hub tier program names and exact certification gates
- SMT-based vs first-class config for regex rename (implementation choice, same UX)
- Hermes topic naming convention specifics (from ServiceNow product docs)
