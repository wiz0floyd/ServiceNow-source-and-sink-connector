# Handoff: Confluent Cloud Custom Connector Deployment

## Status

**Service descriptor fix: DONE** (branch `worktree-fix-provisioning`, PR in progress)

The JAR was missing Kafka Connect ServiceLoader descriptor files. Without them, the Confluent Cloud plugin scanner hangs in PROVISIONING indefinitely with no error logged.

**Fixed files added:**
```
src/main/resources/META-INF/services/org.apache.kafka.connect.sink.SinkConnector
  → com.servicenow.kafka.connect.hermes.HermesSinkConnector

src/main/resources/META-INF/services/org.apache.kafka.connect.source.SourceConnector
  → com.servicenow.kafka.connect.hermes.HermesSourceConnector
```

Also fixed in the same branch: `InterruptedException` swallowed in `HermesSinkConnector.validateTopicExists()` (interrupt flag was not restored, causing potential worker hang on shutdown). Port arrays expanded from 4 to 8 entries (covering the realistic 4–8 broker range per cluster).

---

## Deployment Blockers — Fix These Before Redeploying

Two bugs in `C:\dev\PEM Tool\sn_confluent\deploy\main.py` will silently break the connector after the JAR fix lands. See `C:\dev\PEM Tool\HANDOFF.md` for full detail.

### Blocker 1 — `--plugin-id ccp-j0m1ww` skips the JAR upload

Lines 589–607 of `deploy/main.py`: passing `--plugin-id` causes the deploy tool to reuse the existing plugin artifact without uploading the fixed JAR. The connector will remain stuck in PROVISIONING even after the fix.

**For the next deploy: run without `--plugin-id`:**
```bash
cd "C:\dev\PEM Tool"
sn-confluent deploy --cloud gcp --pem-dir . --no-wizard
```

Record the new plugin ID printed after upload (`ccp-XXXXXXXX`) and save it in `deploy.conf` as `plugin_id` for future runs. Do not pass it on the command line — put it in the config so the tool can eventually be updated to upload-and-update rather than create-new.

### Blocker 2 — Egress allowlist only covers 4 ports

Line 355 of `deploy/main.py` hardcodes:
```python
"confluent.custom.connection.endpoints": f"{instance_name}.service-now.com:4000,4001,4002,4003",
```

Hermes sink cluster spans ports **4000–4050**. Broker metadata responses return addresses on ports 4004–4050. Confluent Cloud's network policy will drop those connections silently. The connector will appear to start but produce zero messages.

Fix (in `deploy/main.py` line 355):
```python
"confluent.custom.connection.endpoints": (
    f"{instance_name}.service-now.com:"
    + ",".join(str(p) for p in range(4000, 4051))
),
```

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
| Active connector | `clcc-k8qxqkg` (`sn-hermes-371c696c`) — stuck in PROVISIONING, needs rebuild + redeploy |
| Old plugin (broken, do not reuse) | `ccp-j0m1ww` |
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
- **Egress endpoint format**: `hostname:port1,port2,port3` (single host, comma-separated ports — not repeated host:port pairs)
- **Bootstrap vs egress**: connector bootstrap strings only need to cover the 4–8 brokers that actually exist; the egress allowlist must cover the full theoretical port range because broker metadata can return any address in that range
