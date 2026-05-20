# Guided setup for Kafka consumers (LES / Hermes)

Source: https://www.servicenow.com/docs/r/platform-security/les-guided-setup-kafka.html
Retrieved: 2026-05-20

## Bootstrap addresses

**Producer (SINK direction):**
```
<instance_name>.service-now.com:4000,<instance_name>.service-now.com:4001,<instance_name>.service-now.com:4002,<instance_name>.service-now.com:4003
```
Port range: 4000–4050

**Consumer cluster 1 (SOURCE direction):**
```
<instance_name>.service-now.com:4100,<instance_name>.service-now.com:4101,<instance_name>.service-now.com:4102,<instance_name>.service-now.com:4103
```
Port range: 4100–4150

**Consumer cluster 2 (SOURCE direction):**
```
<instance_name>.service-now.com:4200,<instance_name>.service-now.com:4201,<instance_name>.service-now.com:4202,<instance_name>.service-now.com:4203
```
Port range: 4200–4250

## Key notes

- Two separate consumer processes are required (one per Hermes cluster) with the **same Consumer Group ID**.
- Topic prefix when accessed externally: prepend `snc.<instance_name>.` to the topic name.
- Disable schemas if required: `key.converter.schemas.enable=false`, `value.converter.schemas.enable=false`
- Keystores and truststores must be accessible to each consumer process.
- Hermes uses a pair of Kafka clusters for failover — if one goes down, data is produced to the other.
