# Changelog

All notable changes are documented here. Format: [Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-05-25

### Added
- Initial release: HermesSinkConnector and HermesSourceConnector
- mTLS-only authentication via KIP-519 InMemorySslEngineFactory (PKCS12 from base64 config)
- Dual-cluster source topology with independent per-cluster offset tracking
- At-least-once delivery guarantee; idempotent producer on sink
- Header preservation (verbatim byte pass-through)
- Topic existence validation at startup (fail-fast)
- Confluent Hub package (kafka-connect-maven-plugin)

### Offset key format
Source offset keys use the shape `{cluster: "1"|"2", topic: string, partition: int}`. This format is stable across versions — upgrades from 0.1.x will resume from stored offsets without replay.

### Config key stability
All config keys (`hermes.*`) are stable. No deprecated aliases exist in 0.1.0.
