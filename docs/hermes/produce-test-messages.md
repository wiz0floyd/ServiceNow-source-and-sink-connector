# Produce test messages to a Hermes topic using the Kafka client

Source: https://www.servicenow.com/docs/r/servicenow-platform/multi-instance-framework-hermes/produce-messages-hermes.html
Retrieved: 2026-05-20

## SSL configuration (producer.properties)

```
security.protocol=SSL
ssl.truststore.password=<truststore password>
ssl.truststore.location=<path to truststore.p12>
ssl.truststore.type=PKCS12
ssl.keystore.password=<keystore password>
ssl.keystore.location=<path to keystore.p12>
ssl.keystore.type=PKCS12
ssl.key.password=<keystore password>
```

## Produce command (Unix)

```bash
./bin/kafka-console-producer.sh \
  --topic snc.<instance_name>.<namespace>.sn_<app_id>.<topic_name> \
  --producer.config ./config/producer.properties \
  --bootstrap-server <instance_name>.service-now.com:4000,<instance_name>.service-now.com:4001,<instance_name>.service-now.com:4002,<instance_name>.service-now.com:4003
```

## Produce command (Windows)

```
bin\windows\kafka-console-producer.bat --topic snc.<instance_name>.<namespace>.sn_<app_id>.<topic_name> --producer.config config\producer.properties --bootstrap-server <instance_name>.service-now.com:4000,<instance_name>.service-now.com:4001,<instance_name>.service-now.com:4002,<instance_name>.service-now.com:4003
```
