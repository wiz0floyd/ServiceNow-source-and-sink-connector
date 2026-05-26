package com.servicenow.kafka.connect.hermes;

import com.servicenow.kafka.connect.hermes.ssl.InMemorySslEngineFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class HermesSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(HermesSinkTask.class);

    private KafkaProducer<byte[], byte[]> producer;
    private String hermesTopic;
    private ErrantRecordReporter errantRecordReporter;
    private final AtomicReference<Throwable> sendException = new AtomicReference<>();
    private volatile boolean firstRecordLogged = false;
    private final HermesSinkTaskMetrics metrics = new HermesSinkTaskMetrics();

    @Override
    public String version() {
        return HermesSinkConnector.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        HermesConnectorConfig config = new HermesConnectorConfig(props);
        hermesTopic = config.getHermesTopic();
        String bootstrap = config.getSinkBootstrapOverride().isEmpty()
            ? HermesBootstrapBuilder.buildSinkBootstrap(config.getInstanceName())
            : config.getSinkBootstrapOverride();
        log.info("HermesSinkTask starting — bootstrap: {}, topic: {}", bootstrap, hermesTopic);
        producer = createProducer(buildProducerProperties(config, bootstrap));
        errantRecordReporter = context.errantRecordReporter();
        int taskId = Integer.parseInt(props.getOrDefault("task.id", "0"));
        metrics.register(config.getInstanceName(), taskId);
        log.info("HermesSinkTask producer initialized");
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) return;

        // Fail fast if a previous async send failed without a reporter to absorb it.
        // getAndSet is atomic — avoids a TOCTOU window where a concurrent callback
        // fires between get() and set(null) and its exception gets cleared.
        Throwable prior = sendException.getAndSet(null);
        if (prior != null) {
            throw new ConnectException("Prior async send failed: " + prior.getMessage(), prior);
        }

        log.debug("HermesSinkTask put() batch: {} record(s)", records.size());

        for (SinkRecord record : records) {
            ProducerRecord<byte[], byte[]> pr = toProducerRecord(record);
            producer.send(pr, (metadata, exception) -> {
                if (exception == null) {
                    metrics.recordForwarded(1);
                    return;
                }
                metrics.recordFailed();
                if (errantRecordReporter != null) {
                    errantRecordReporter.report(record, exception);
                } else {
                    // Store for flush() to surface; first error wins
                    sendException.compareAndSet(null, exception);
                }
            });
        }

        // Log first record milestone once
        if (!firstRecordLogged) {
            firstRecordLogged = true;
            log.info("HermesSinkTask: first record batch sent to Hermes topic '{}'", hermesTopic);
        }
    }

    @Override
    public void flush(Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> currentOffsets) {
        producer.flush();
        Throwable t = sendException.getAndSet(null);
        if (t != null) {
            throw new ConnectException("Failed to deliver record(s) to Hermes topic '" + hermesTopic + "'", t);
        }
        log.debug("HermesSinkTask flush() complete for {} partition(s)", currentOffsets.size());
    }

    @Override
    public Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> preCommit(
            Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> currentOffsets) {
        log.debug("HermesSinkTask committing offsets for {} partition(s)", currentOffsets.size());
        return super.preCommit(currentOffsets);
    }

    @Override
    public void stop() {
        if (producer != null) {
            log.info("HermesSinkTask stopping — closing producer");
            producer.close(Duration.ofSeconds(30));
            producer = null;
        }
        metrics.unregister();
    }

    // ---- Record conversion ----

    private ProducerRecord<byte[], byte[]> toProducerRecord(SinkRecord record) {
        byte[] key = record.key() != null ? (byte[]) record.key() : null;
        byte[] value = record.value() != null ? (byte[]) record.value() : null;

        // Partition null → default partitioner uses hash-by-key (non-null key) or sticky (null key)
        ProducerRecord<byte[], byte[]> pr = new ProducerRecord<>(
            hermesTopic,
            null,             // partition: let partitioner decide
            record.timestamp(),
            key,
            value
        );

        // Preserve all headers verbatim
        if (record.headers() != null) {
            for (org.apache.kafka.connect.header.Header h : record.headers()) {
                pr.headers().add(h.key(), h.value() instanceof byte[] ? (byte[]) h.value() : null);
            }
        }

        return pr;
    }

    // ---- Properties builders (package-private for testing) ----

    static Map<String, Object> buildProducerProperties(HermesConnectorConfig config, String bootstrap) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, config.getProducerAcks());
        props.put(ProducerConfig.RETRIES_CONFIG, config.getProducerRetries());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        // TODO: wire batching configs once HermesConnectorConfig adds these methods
        props.put(ProducerConfig.LINGER_MS_CONFIG, config.getProducerLingerMs());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, config.getProducerBatchSize());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.getProducerCompressionType());
        // Max in-flight requests per connection: 5 is the safe limit for idempotent producers
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);

        if (config.isSslEnabled()) {
            // SSL — in-memory keystore via KIP-519. Values pass through as Password
            // objects so they remain masked if the KafkaProducer dumps its config.
            props.put("security.protocol", "SSL");
            props.put("ssl.engine.factory.class", InMemorySslEngineFactory.class.getName());
            props.put(InMemorySslEngineFactory.KEYSTORE_B64_CONFIG, config.getKeystoreB64());
            props.put(InMemorySslEngineFactory.KEYSTORE_PASSWORD_CONFIG, config.getKeystorePassword());
            props.put(InMemorySslEngineFactory.TRUSTSTORE_B64_CONFIG, config.getTruststoreB64());
            props.put(InMemorySslEngineFactory.TRUSTSTORE_PASSWORD_CONFIG, config.getTruststorePassword());
            props.put(InMemorySslEngineFactory.CERT_EXPIRY_WARN_DAYS_CONFIG, config.getCertExpiryWarnDays());
        }

        return props;
    }

    // Package-private for testing — allows injection of mock KafkaProducer
    KafkaProducer<byte[], byte[]> createProducer(Map<String, Object> producerProps) {
        return new KafkaProducer<>(producerProps);
    }

    // Package-private setter for testing
    void setProducer(KafkaProducer<byte[], byte[]> producer) {
        this.producer = producer;
    }

    void setHermesTopic(String topic) {
        this.hermesTopic = topic;
    }

    // Package-private setter for testing
    void setErrantRecordReporter(ErrantRecordReporter reporter) {
        this.errantRecordReporter = reporter;
    }
}
