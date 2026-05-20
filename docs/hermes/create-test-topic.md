# Create a test topic in Hermes using the Kafka client

Source: https://www.servicenow.com/docs/r/servicenow-platform/multi-instance-framework-hermes/create-hermes-topic.html
Retrieved: 2026-05-20

## Topic naming format

```
snc.<instance_name>.<namespace>.<app_id>.<topic_name>
```

- `<instance_name>` — your instance name (case-sensitive)
- `<namespace>` — namespace of the domain (optional)
- `<app_id>` — application ID, e.g. `sn_logstoanalytics` or `sn_streamconnect`
- `<topic_name>` — unique topic name (case-sensitive)
- Full name is case-sensitive, limited to 200 characters.

## Bootstrap server format (SINK / PRODUCER direction, ports 4000–4003)

```
<instance_name>.service-now.com:4000,<instance_name>.service-now.com:4001,<instance_name>.service-now.com:4002,<instance_name>.service-now.com:4003
```

## Create topic command (Unix)

```bash
./bin/kafka-topics.sh --create \
  --topic snc.<instance_name>.<namespace>.sn_<app_id>.<topic_name> \
  --command-config ./config/producer.properties \
  --bootstrap-server <instance_name>.service-now.com:4000,<instance_name>.service-now.com:4001,<instance_name>.service-now.com:4002,<instance_name>.service-now.com:4003
```
