# Handoff: Confluent Cloud Custom Connector Deployment

## Status

**END-TO-END VERIFIED** — connector `clcc-w7oj20j` (`sn-hermes-371c696c`) is RUNNING on `lkc-pgnjyq5` (2026-05-24). Test message produced to `sink_test` on Confluent Cloud, found at `snc.hermes1.sn_streamconnect.SSLtest` partition=0 offset=7 on Hermes (~20s latency).

**Root cause of PROVISIONING hang: missing ServiceLoader descriptors** (now fixed). See fixed files below.

Also fixed: `InterruptedException` swallowed in `HermesSinkConnector.validateTopicExists()`. Port arrays expanded from 4 to 8 entries.

**Fixed files:**
```
src/main/resources/META-INF/services/org.apache.kafka.connect.sink.SinkConnector
  → com.servicenow.kafka.connect.hermes.HermesSinkConnector

src/main/resources/META-INF/services/org.apache.kafka.connect.source.SourceConnector
  → com.servicenow.kafka.connect.hermes.HermesSourceConnector
```

---

## Egress Port Count — Important Finding

Deploying with `confluent.custom.connection.endpoints = hermes1.service-now.com:4000,...,4050` (51 ports) caused Confluent Cloud to hang in PROVISIONING for 40+ minutes and never reach RUNNING. Reducing to 8 ports (4000–4007) resulted in a clean ~6-minute provisioning.

**Current PEM Tool config (working):** 8 ports (4000–4007).  
**Production note:** If Hermes broker metadata returns addresses on ports outside 4000–4007, those connections will be dropped by Confluent Cloud egress policy. Monitor for connection errors and expand the range if needed — but be aware that each additional block of ~50 ports adds significant provisioning time.

The endpoint format is `hostname:port1,port2,...` (single hostname, comma-separated ports). Full `host:port` pairs per entry is rejected by the API.

---

## Build Workflow

```powershell
# Add Maven to PATH (not on PATH by default)
$env:PATH += ";C:\tools\maven\apache-maven-3.9.6\bin"

# Build
cd C:\dev\ServiceNow-source-and-sink-connector
mvn package

# Output:
# → target/components/packages/servicenow-hermes-kafka-connector-0.1.0.zip
```

---

## Confluent Cloud Sandbox State

| Resource | Value |
|---|---|
| Environment | `env-3dor1o` |
| Cluster | `lkc-pgnjyq5` (Dedicated, GCP us-east1) |
| Active connector | `clcc-w7oj20j` (`sn-hermes-371c696c`) — **RUNNING** |
| Plugin (correct fixed JAR) | `ccp-gjd0nn` |
| Old plugins (broken, do not reuse) | `ccp-j0m1ww`, `ccp-10738v` |
| Kafka API key | `JBQOQ5YHAGKXMIZB` (in `deploy.conf`) |

---

## Port Ranges (Hermes Kafka)

| Direction | Bootstrap ports (first 8) | Full range (egress allowlist) |
|---|---|---|
| Sink (producer → Hermes LB) | 4000–4007 | 4000–4050 |
| Source cluster 1 (consumer) | 4100–4107 | 4100–4150 |
| Source cluster 2 (consumer) | 4200–4207 | 4200–4250 |

Bootstrap strings only need to reach one broker to fetch cluster metadata. Data-plane connections then go to whatever addresses the broker returns — which may be anywhere in the full range. The egress allowlist must cover the full range.

---

## Open Design Question: Remove Bootstrap Derivation

The connector currently derives bootstrap addresses from the instance name (`myinstance` → `myinstance.service-now.com:4000,...`). The derivation logic is in `HermesBootstrapBuilder.java`.

**Alternative**: replace with a direct config property. Operators can copy-paste the bootstrap string from the ServiceNow Hermes dashboard. This removes one place where the connector has to know internal Hermes port conventions, and avoids bugs when conventions change.

Affected if we switch: `HermesBootstrapBuilder`, `HermesConnectorConfig.InstanceNameValidator`, `HermesSourceConfig` (override properties could become the primary config), `HermesSourceConnector.validateClustersReachable`, `HermesSinkConnector.validateTopicExists`, `HermesBootstrapBuilderTest`.

---

## Lessons Learned

- **No logs during PROVISIONING** = worker hasn't started yet, not a runtime error — check plugin/JAR structure and ServiceLoader descriptors
- **`--plugin-id` skips upload**: the flag means "reuse this artifact" not "update this plugin" — deploy without it to force a fresh upload
- **Lemon clusters**: if a Dedicated cluster throws immediate control plane errors before PROVISIONING, delete and create fresh — same config may work on new hardware
- **`confluent connect cluster update` is destructive**: replaces full config, not merges — always use `--config-file` with the complete config or redeploy via `sn-confluent deploy`
- **Egress endpoint format**: `hostname:port1,port2,port3` (single host, comma-separated ports — not repeated host:port pairs; full `host:port` repeated pairs are rejected by the API)
- **51 egress ports → 40+ min PROVISIONING**: each port adds setup overhead in Confluent Cloud's network plumbing; 8 ports provisions in ~6 min
- **Bootstrap vs egress**: connector bootstrap strings only need to cover the 4–8 brokers that actually exist; the egress allowlist must cover the full theoretical port range because broker metadata can return any address in that range
- **Hermes consumer group ACL**: group IDs must be prefixed `snc.<instance>.` or Hermes returns `GroupAuthorizationException`
- **kafka-python + Confluent Cloud**: requires `ssl_context=ssl.create_default_context()` — without it, the producer silently times out on metadata fetch
- **sink_test topic must be pre-created**: Confluent Cloud does not auto-create topics; connector runs RUNNING even without the topic existing, but no messages are forwarded
- **E2E latency ~20s**: message produced to Confluent Cloud → forwarded by sink connector → available on Hermes in approximately 20 seconds under test conditions
