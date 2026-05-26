# Quick Start: Hermes Source Connector on Confluent Cloud

Deploy the source connector to stream records from a ServiceNow Hermes Kafka topic into a Confluent Cloud topic over mTLS.

## Prerequisites

- ServiceNow instance with the **Hermes Messaging Service** activated and a Hermes source topic with live data
- mTLS certificate generated via the **Instance PKI Certificate Generator** on your ServiceNow instance (produces a PKCS12 keystore and truststore)
- Confluent Cloud account with an existing cluster and a destination topic created in advance
- Confluent Cloud egress rules allowing outbound TCP to both Hermes peer clusters:
  - `<instance>.service-now.com:4100-4150` (peer cluster 1)
  - `<instance>.service-now.com:4200-4250` (peer cluster 2)
- Confluent CLI installed and authenticated (`confluent login`) — needed only for the CLI path

## Architecture note: dual-cluster consumer

The source connector runs **two embedded `KafkaConsumer` instances**, one connected to each Hermes peer cluster (ports 4100–4150 and 4200–4250). Both consumers read the same Hermes topic and forward records to the same Confluent Cloud destination topic. Offsets are tracked independently per cluster. This is intentional: Hermes peer clusters are independent, and the connector deduplicates at the Hermes level — you may see duplicates only during Hermes failover events.

`tasks.max` is effectively **1 per connector instance** — both embedded consumers live inside a single task. For higher throughput, deploy additional connector instances (each instance manages its own dual-consumer pair).

## Step 1 — Prepare your mTLS credentials

The connector loads keystore and truststore material from connector config strings, not files. Encode both files to single-line base64 before proceeding.

```bash
# Linux / macOS — the -A flag suppresses line breaks; breaks cause parsing failures
openssl base64 -A -in keystore.p12 -out keystore.b64
openssl base64 -A -in truststore.p12 -out truststore.b64
```

```powershell
# Windows PowerShell
[Convert]::ToBase64String([IO.File]::ReadAllBytes("keystore.p12")) | Out-File -NoNewline keystore.b64
[Convert]::ToBase64String([IO.File]::ReadAllBytes("truststore.p12")) | Out-File -NoNewline truststore.b64
```

> **Important:** The base64 string must have no newlines. Verify with:
> ```bash
> wc -l keystore.b64   # must print 0 or 1 (one line, no trailing newline)
> ```

## Step 2 — Confirm the Hermes source topic exists on both clusters

The connector reads from the same topic name on both peer clusters simultaneously. The topic must exist on **both** clusters before the connector starts.

The expected topic name format is:

```
snc.<instance>.<namespace>.<topic>
# Example:
snc.myinstance.sn_streamconnect.orders
```

## Step 3 — Choose a consumer group ID

The connector uses a single `hermes.consumer.group.id` against both Hermes peer clusters. Hermes ACLs require the group ID to be prefixed with `snc.<instance>.` — **the connector applies this prefix automatically** if the value you provide does not already start with it.

For example:
- You set `hermes.consumer.group.id=hermes-connect-source`
- The connector submits `snc.myinstance.hermes-connect-source` to the Hermes brokers

You can also set the full prefixed form directly; the connector will not double-prefix it.

## Step 4 — Understand `auto.offset.reset`

The connector uses `auto.offset.reset=earliest` by default. **On first deployment, the connector will replay all records still within Hermes retention.** After the connector commits its first offsets, subsequent restarts resume from the last committed position.

If your downstream application is not idempotent, be prepared for historical records to appear in the Confluent Cloud destination topic on the first run. The connector's offset tracking (via the Connect offset storage topic) ensures records are not replayed after the first run unless the offset storage is cleared.

## Step 5 — Deploy the connector

### Option A: Confluent Cloud Console

1. In your Confluent Cloud cluster, go to **Connectors** and click **Add connector**.
2. Click **Add connector plugin**.
3. Fill in the plugin details:
   - **Connector plugin name:** `ServiceNow Hermes Source`
   - **Connector class:** `com.servicenow.kafka.connect.hermes.HermesSourceConnector`
   - **Connector type:** Source
4. Click **Select connector archive** and upload `target/components/packages/servicenow-hermes-kafka-connector-0.1.0.zip`.
5. In the **Sensitive properties** field, paste exactly:
   ```
   hermes.ssl.keystore.b64,hermes.ssl.keystore.password,hermes.ssl.truststore.b64,hermes.ssl.truststore.password
   ```
6. Check the responsibility acknowledgment and click **Submit**.
7. Select the newly uploaded plugin and click **Get started**.
8. Choose your API key option and click **Continue**.
9. Enter the configuration properties. Minimum required properties:

| Property | Example value | Notes |
|----------|--------------|-------|
| `name` | `hermes-source-connector` | Connector instance name |
| `connector.class` | `com.servicenow.kafka.connect.hermes.HermesSourceConnector` | |
| `tasks.max` | `1` | Effective maximum is 1 per instance; see architecture note above |
| `key.converter` | `org.apache.kafka.connect.converters.ByteArrayConverter` | Pass-through bytes |
| `value.converter` | `org.apache.kafka.connect.converters.ByteArrayConverter` | Pass-through bytes |
| `hermes.instance.name` | `myinstance` | Bare instance name only — no protocol, no `.service-now.com` |
| `hermes.source.topic` | `snc.myinstance.sn_streamconnect.orders` | Full Hermes topic name — must exist on both peer clusters |
| `hermes.consumer.group.id` | `hermes-connect-source` | Auto-prefixed to `snc.myinstance.hermes-connect-source` by the connector |
| `hermes.destination.topic` | `my-confluent-destination-topic` | Confluent Cloud topic to write records to |
| `hermes.ssl.keystore.b64` | *(paste base64 string)* | **Sensitive** — encrypted at rest |
| `hermes.ssl.keystore.password` | `changeit` | **Sensitive** — encrypted at rest |
| `hermes.ssl.truststore.b64` | *(paste base64 string)* | **Sensitive** — encrypted at rest |
| `hermes.ssl.truststore.password` | `changeit` | **Sensitive** — encrypted at rest |

10. Under **Networking**, add both Hermes peer cluster egress endpoints:
    ```
    <instance>.service-now.com:4100-4150:TCP
    <instance>.service-now.com:4200-4250:TCP
    ```
    Confluent Cloud allows multiple endpoints separated by semicolons in some UI flows. Enter them as two separate entries if the UI requires it.
11. Set **Tasks** to `1` and click **Continue**.
12. Review the summary and click **Launch connector**.

### Option B: Confluent CLI

First register the plugin (one-time per organization; skip if already registered for the sink):

```bash
confluent connect custom-plugin create hermes-source-connector \
  --plugin-file target/components/packages/servicenow-hermes-kafka-connector-0.1.0.zip \
  --connector-type source \
  --connector-class com.servicenow.kafka.connect.hermes.HermesSourceConnector \
  --sensitive-properties hermes.ssl.keystore.b64,hermes.ssl.keystore.password,hermes.ssl.truststore.b64,hermes.ssl.truststore.password \
  --cloud aws
```

Copy the sample config and fill in your values:

```bash
cp config/quickstart-hermes-source.properties my-source-connector.properties
# edit my-source-connector.properties, then:
confluent connect cluster create \
  --config-file my-source-connector.properties \
  --cluster <your-cluster-id>
```

See `config/quickstart-hermes-source.properties` for the full annotated template.

## Step 6 — Verify

1. Confirm the connector card shows **Running** in Confluent Cloud.
2. Produce a test record to your Hermes source topic using a Kafka console producer pointed at the Hermes bootstrap endpoint.
3. Consume from the Confluent Cloud destination topic to confirm the record arrived:
   ```bash
   confluent kafka topic consume my-confluent-destination-topic \
     --cluster <cluster-id> \
     --from-beginning
   ```
4. Produce a second record and confirm it also appears — both peer-cluster consumers are operational when records flow consistently.

If the connector stays in **Provisioning** for more than two minutes, see [Troubleshooting](troubleshooting.md).

## Egress networking reference

The source connector connects directly to both Hermes peer clusters (not through the load balancer used by the sink). Declare both egress rules:

| Endpoint | Purpose |
|----------|---------|
| `<instance>.service-now.com:4100-4150:TCP` | Hermes peer cluster 1 |
| `<instance>.service-now.com:4200-4250:TCP` | Hermes peer cluster 2 |

Replace `<instance>` with your actual instance name (e.g., `myinstance`).

## Optional consumer tuning

| Property | Default | Description |
|----------|---------|-------------|
| `hermes.consumer.max.poll.records` | `500` | Maximum records returned per `poll()` call per embedded consumer. Increase for higher throughput. |
