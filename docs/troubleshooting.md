# Troubleshooting Guide

This guide covers the most common failure modes for both the Hermes sink and source connectors. Log messages quoted here are taken directly from the connector source.

---

## Connector stuck in PROVISIONING

**Symptom:** The connector card in Confluent Cloud stays in a `PROVISIONING` state for more than two to three minutes.

**Causes:**

1. **Config validation failure** — the sink connector validates that the target Hermes topic exists at startup. If `HermesSinkConnector.validateTopicExists()` throws, the connector fails to start and may cycle through Provisioning → Failed. Check the Connect worker logs for:
   ```
   Hermes topic '<name>' does not exist.
   ```
   or:
   ```
   Failed to verify Hermes topic '<name>' — could not connect to <bootstrap>. Check instance name, network access, and SSL credentials.
   ```

2. **Egress firewall not configured** — the connector worker cannot reach the Hermes bootstrap endpoint. Verify that the Confluent Cloud egress endpoints are correctly declared:
   - Sink: `<instance>.service-now.com:4000-4050:TCP`
   - Source: `<instance>.service-now.com:4100-4150:TCP` and `<instance>.service-now.com:4200-4250:TCP`

3. **Config property typo** — a missing required property or a malformed value causes the `ConfigDef` validation to throw at startup. Check that `hermes.instance.name` contains only the bare instance name (no `https://`, no `.service-now.com`).

**Action:** Check **Connector logs** in Confluent Cloud (connector detail page → Logs tab) for the specific error. Fix the identified issue and restart or recreate the connector.

---

## SSL handshake failure at connect time

**Symptom:** The connector enters a FAILED state or logs repeated errors containing `SSLHandshakeException`, `SSL_ERROR`, or similar. The connector never successfully connects to a Hermes broker.

**Causes and checks:**

1. **Base64 string contains line breaks** — the most common cause. When encoding the keystore with `base64` without the `-A` flag, the output contains newlines every 76 characters. The connector's `InMemorySslEngineFactory` will fail to decode this with:
   ```
   Value is not valid base64: Illegal base64 character a
   ```
   or silently produce a corrupted keystore, leading to:
   ```
   Failed to load PKCS12 keystore — check content and password.
   ```
   Fix: re-encode using `openssl base64 -A -in keystore.p12` (note the `-A` flag) and update the connector config.

2. **Wrong password** — the keystore password in `hermes.ssl.keystore.password` does not match the PKCS12 file. The connector will log:
   ```
   Failed to initialize SSL context from in-memory keystores: ...
   ```
   Fix: verify the password matches what was set when the certificate was generated via Instance PKI Certificate Generator. Re-generate the certificate if the password is unknown.

3. **Expired certificate** — the certificate's `NotAfter` date has passed. The connector logs a warning 30 days before expiry:
   ```
   InMemorySslEngineFactory: mTLS certificate expires in N day(s) on <date>. Rotate the certificate via the Hermes Instance PKI Certificate Generator.
   ```
   After expiry, TLS handshakes fail immediately. See the **Certificate Rotation** section below.

4. **Wrong keystore/truststore swapped** — ensure `hermes.ssl.keystore.b64` contains the keystore (holds the private key) and `hermes.ssl.truststore.b64` contains the truststore (holds the CA cert). Swapping them causes a handshake failure at the key manager initialization step.

---

## Topic does not exist on Hermes (sink)

**Symptom:** Connector fails to start with:
```
Hermes topic '<name>' does not exist. Create the topic via the Hermes Messaging Service
(All > Hermes Messaging Service > Topics) before starting this connector.
Available topics: N found.
```

**Cause:** The value in `hermes.topic` does not match any topic visible to the mTLS certificate used by this connector. This can happen because:

1. **Topic not created** — the Hermes topic has not been created yet. Create it through the ServiceNow Hermes admin UI before starting the connector.

2. **Wrong topic name format** — Hermes topic names follow a namespaced format. The expected pattern is:
   ```
   snc.<instance>.<namespace>.<topic>
   ```
   Example: `snc.myinstance.sn_streamconnect.orders`. A common mistake is omitting the `snc.` prefix or the namespace segment.

3. **Wrong instance name** — if `hermes.instance.name` is wrong, the connector derives the wrong bootstrap addresses and connects to the wrong cluster (or fails to connect entirely).

4. **Certificate scope** — each mTLS certificate is scoped to a specific instance. If the certificate belongs to a different instance than `hermes.instance.name`, the cert ACLs will not allow listing or accessing the intended topics.

---

## Consumer group ACL rejection (source)

**Symptom:** Source connector task fails with an error containing `GroupAuthorizationException` in the Connect worker logs.

**Cause:** Hermes enforces that all consumer group IDs begin with `snc.<instance>.`. If a consumer connects with a non-prefixed group ID, Hermes rejects the request.

**Expected behavior:** The connector auto-prefixes the group ID. When the connector starts successfully, you will see a log line like:
```
Auto-prefixing consumer group.id with Hermes ACL prefix: hermes-connect-source → snc.myinstance.hermes-connect-source
```

**If you still see `GroupAuthorizationException`:**

- Confirm `hermes.instance.name` is set to the correct bare instance name. The prefix applied is `snc.<hermes.instance.name>.` — an incorrect instance name produces an incorrect prefix.
- If you set `hermes.consumer.group.id` to a full prefixed value (e.g., `snc.myinstance.hermes-connect-source`), the connector will use it as-is without double-prefixing. Verify that the prefixed value exactly matches the instance name.

---

## No records flowing (source)

**Symptom:** The source connector is RUNNING, but no records appear in the Confluent Cloud destination topic.

**Checks in order:**

1. **`auto.offset.reset` is `earliest`** (current default in the connector source) — the connector will start from the earliest available offset on first deployment. If you deployed and the topic had no new messages, wait for new messages to arrive in the Hermes source topic.

   > Note: the connector code sets `AUTO_OFFSET_RESET_CONFIG` to `"earliest"` in `buildConsumerProperties()`. This means a fresh connector will replay historical records if they are still within Hermes retention. If you are seeing too many records replaying, this is expected behavior on first deployment.

2. **Both Hermes peer clusters reachable** — the connector runs two embedded consumers. If one cluster is unreachable (egress rule missing for one port range), that consumer fails silently with:
   ```
   Transient error polling cluster=<N>: ... — will retry on next poll()
   ```
   Records from the reachable cluster will still flow, but throughput is halved. Check that egress rules cover both ranges:
   - `<instance>.service-now.com:4100-4150:TCP`
   - `<instance>.service-now.com:4200-4250:TCP`

3. **Source Hermes topic has no new messages** — verify that the source application is actively producing to the Hermes topic by checking message counts in the Hermes admin UI.

4. **Destination topic does not exist in Confluent Cloud** — the connector will attempt to produce to `hermes.destination.topic`. If the topic does not exist and the Confluent Cloud service account lacks `CREATE` permission, the producer will fail. Create the destination topic in Confluent Cloud before starting the connector.

---

## High consumer lag (source)

**Symptom:** Consumer group lag on the Hermes source topic is growing; the connector is not keeping up with production rate.

**Actions:**

1. **Increase `hermes.consumer.max.poll.records`** — the default is 500 records per `poll()` call per embedded consumer. Increase this value to process more records per poll cycle. The setting applies independently to each of the two embedded consumers.

2. **Deploy additional connector instances** — the source connector always runs exactly one task per instance, and that task manages both embedded consumers. `tasks.max` has no effect beyond 1. To scale horizontally, deploy a second connector instance with a different `hermes.consumer.group.id`. Each instance will consume independently, and your downstream application is responsible for deduplication.

   > Deploying multiple source connector instances targeting the same group ID is **not** a valid scale strategy — Kafka consumer group semantics partition the topic across the group members, which may cause each instance to see only a subset of partitions.

---

## Connector fails after certificate expiry

**Symptom:** A previously healthy connector begins logging TLS handshake failures. The Connect worker logs may show `SSLHandshakeException` or `CertificateExpiredException`. If the connector was running within 30 days of expiry, you may have seen the advance warning:
```
InMemorySslEngineFactory: mTLS certificate expires in N day(s) on <date>. Rotate the certificate via the Hermes Instance PKI Certificate Generator.
```

**Action:** See the **Certificate Rotation** section below.

---

## Certificate Rotation

### Detecting upcoming expiry

The connector logs a `WARN` level message at startup (and on each `configure()` call) when any certificate in the keystore expires within 30 days:

```
InMemorySslEngineFactory: mTLS certificate expires in N day(s) on <date>.
Rotate the certificate via the Hermes Instance PKI Certificate Generator.
```

Monitor your Connect worker logs for this message. If you use Confluent Cloud log streaming, create an alert on `mTLS certificate expires`.

### Rotation procedure

1. **Generate a new certificate** on your ServiceNow instance:
   - Navigate to **All > Hermes Messaging Service > Instance PKI Certificate Generator**
   - Generate a new certificate and download the new `keystore.p12` and `truststore.p12` files

2. **Encode the new files to base64:**
   ```bash
   # Linux / macOS
   openssl base64 -A -in keystore.p12 -out keystore_new.b64
   openssl base64 -A -in truststore.p12 -out truststore_new.b64
   ```

3. **Update the connector configuration** in Confluent Cloud:
   - In the Confluent Cloud Console, open the connector and click **Edit configuration**
   - Update the following four sensitive properties with the new base64 strings:
     - `hermes.ssl.keystore.b64`
     - `hermes.ssl.keystore.password`
     - `hermes.ssl.truststore.b64`
     - `hermes.ssl.truststore.password`
   - Save the configuration

   Alternatively, use the Confluent Cloud REST API:
   ```bash
   curl -X PUT \
     https://api.confluent.cloud/connect/v1/environments/<env-id>/clusters/<cluster-id>/connectors/<connector-name>/config \
     -H "Authorization: Basic <base64-encoded-key:secret>" \
     -H "Content-Type: application/json" \
     -d '{
       "hermes.ssl.keystore.b64": "<new-base64-keystore>",
       "hermes.ssl.keystore.password": "<new-password>",
       "hermes.ssl.truststore.b64": "<new-base64-truststore>",
       "hermes.ssl.truststore.password": "<new-password>"
     }'
   ```

4. **Connector restarts automatically** — Confluent Cloud restarts the connector task when the configuration is updated. The `InMemorySslEngineFactory.shouldBeRebuilt()` method detects the changed SSL properties and rebuilds the SSL context with the new certificate. The connector resumes processing without a gap in offset tracking.

5. **Verify** — confirm the connector returns to RUNNING status and that the advance-warning log message no longer appears.
