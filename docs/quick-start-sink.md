# Quick Start: Hermes Sink Connector on Confluent Cloud

Deploy the sink connector to stream records from a Confluent Cloud topic into a ServiceNow Hermes Kafka topic over mTLS.

## Prerequisites

- ServiceNow instance with the **Hermes Messaging Service** activated and at least one Hermes topic created
- mTLS certificate generated via the **Instance PKI Certificate Generator** on your ServiceNow instance (produces a PKCS12 keystore and truststore)
- Confluent Cloud account with an existing cluster and a topic you want to sink from
- Confluent Cloud egress rule allowing outbound TCP to `<instance>.service-now.com:4000-4050` (the Hermes load-balanced write endpoint)
- Confluent CLI installed and authenticated (`confluent login`) — needed only for the CLI path

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

Store the resulting strings securely (a password manager or secrets vault). You will paste them into Confluent Cloud as sensitive properties in Step 3.

> **Important:** The base64 string must have no newlines. Verify with:
> ```bash
> wc -l keystore.b64   # must print 0 or 1 (one line, no trailing newline)
> ```

## Step 2 — Confirm the Hermes topic exists

The connector fails at startup if the target Hermes topic does not exist — it never auto-creates topics.

Verify the topic exists on your instance using the Hermes admin UI or your ServiceNow admin's confirmation. The expected topic name format is:

```
snc.<instance>.<namespace>.<topic>
# Example:
snc.myinstance.sn_streamconnect.orders
```

If the topic does not exist, create it through the ServiceNow Hermes topic management UI before proceeding.

## Step 3 — Deploy the connector

### Option A: Confluent Cloud Console

1. In your Confluent Cloud cluster, go to **Connectors** and click **Add connector**.
2. Click **Add connector plugin**.
3. Fill in the plugin details:
   - **Connector plugin name:** `ServiceNow Hermes Sink`
   - **Connector class:** `com.servicenow.kafka.connect.hermes.HermesSinkConnector`
   - **Connector type:** Sink
4. Click **Select connector archive** and upload `target/components/packages/servicenow-hermes-kafka-connector-0.1.0.zip`.
5. In the **Sensitive properties** field, paste exactly:
   ```
   hermes.ssl.keystore.b64,hermes.ssl.keystore.password,hermes.ssl.truststore.b64,hermes.ssl.truststore.password
   ```
6. Check the responsibility acknowledgment and click **Submit**.
7. Select the newly uploaded plugin and click **Get started**.
8. Choose your API key option and click **Continue**.
9. Enter the configuration properties (key/value pairs or JSON). Minimum required properties:

| Property | Example value | Notes |
|----------|--------------|-------|
| `name` | `hermes-sink` | Connector instance name |
| `connector.class` | `com.servicenow.kafka.connect.hermes.HermesSinkConnector` | |
| `tasks.max` | `1` | |
| `topics` | `my-confluent-topic` | Confluent Cloud topic(s) to read from |
| `key.converter` | `org.apache.kafka.connect.converters.ByteArrayConverter` | Pass-through bytes |
| `value.converter` | `org.apache.kafka.connect.converters.ByteArrayConverter` | Pass-through bytes |
| `hermes.instance.name` | `myinstance` | Bare instance name only — no protocol, no `.service-now.com` |
| `hermes.topic` | `snc.myinstance.sn_streamconnect.orders` | Full Hermes topic name — must exist |
| `hermes.ssl.keystore.b64` | *(paste base64 string)* | **Sensitive** — encrypted at rest |
| `hermes.ssl.keystore.password` | `changeit` | **Sensitive** — encrypted at rest |
| `hermes.ssl.truststore.b64` | *(paste base64 string)* | **Sensitive** — encrypted at rest |
| `hermes.ssl.truststore.password` | `changeit` | **Sensitive** — encrypted at rest |

10. Under **Networking**, add egress endpoints:
    ```
    <instance>.service-now.com:4000-4050:TCP
    ```
    Replace `<instance>` with your actual instance name.
11. Set **Tasks** to `1` and click **Continue**.
12. Review the summary and click **Launch connector**.

### Option B: Confluent CLI

First register the plugin (one-time per organization):

```bash
confluent connect custom-plugin create hermes-sink-connector \
  --plugin-file target/components/packages/servicenow-hermes-kafka-connector-0.1.0.zip \
  --connector-type sink \
  --connector-class com.servicenow.kafka.connect.hermes.HermesSinkConnector \
  --sensitive-properties hermes.ssl.keystore.b64,hermes.ssl.keystore.password,hermes.ssl.truststore.b64,hermes.ssl.truststore.password \
  --cloud aws
```

Then create a connector instance using the sample config from `config/quickstart-hermes-sink.properties` (edit the placeholder values first):

```bash
confluent connect cluster create \
  --config-file config/quickstart-hermes-sink.properties \
  --cluster <your-cluster-id>
```

## Step 4 — Verify

1. In the Confluent Cloud Console, confirm the connector card shows **Running** (not Provisioning or Failed).
2. Produce a test record to your Confluent Cloud source topic:
   ```bash
   confluent kafka topic produce my-confluent-topic \
     --cluster <cluster-id> \
     --value "hello from confluent"
   ```
3. Consume from the Hermes topic using a Kafka console consumer pointed at the Hermes bootstrap endpoint to confirm the record arrived.

If the connector stays in **Provisioning** for more than two minutes, see [Troubleshooting](troubleshooting.md).

## Egress networking reference

The Hermes write endpoint is a load balancer fronting ports 4000–4050. Confluent Cloud requires you to declare egress rules per-port or as a range. Use the following rule:

```
<instance>.service-now.com:4000-4050:TCP
```

Confluent automatically applies a single-level wildcard to the leftmost domain label, so the rule covers `myinstance.service-now.com` without further configuration.

## Optional producer tuning

| Property | Default | Description |
|----------|---------|-------------|
| `hermes.producer.acks` | `all` | Kafka producer acks. `all` provides maximum durability. |
| `hermes.producer.retries` | `2147483647` | Producer retry count. Default keeps retrying indefinitely with idempotent producer. |
