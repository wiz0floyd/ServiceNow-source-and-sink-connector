package com.servicenow.kafka.connect.hermes;

import com.servicenow.kafka.connect.hermes.ssl.InMemorySslEngineFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class HermesSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(HermesSinkTask.class);

    private KafkaProducer<byte[], byte[]> producer;
    private String hermesTopic;

    @Override
    public String version() {
        return HermesSinkConnector.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        HermesConnectorConfig config = new HermesConnectorConfig(props);
        hermesTopic = config.getHermesTopic();
        String bootstrap = HermesBootstrapBuilder.buildSinkBootstrap(config.getInstanceName());
        log.info("HermesSinkTask starting — bootstrap: {}, topic: {}", bootstrap, hermesTopic);
        producer = createProducer(buildProducerProperties(config, bootstrap));
        log.info("HermesSinkTask producer initialized");
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        for (SinkRecord record : records) {
            ProducerRecord<byte[], byte[]> pr = toProducerRecord(record);
            try {
                producer.send(pr).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ConnectException("Interrupted while sending to Hermes topic '" + hermesTopic + "'", e);
            } catch (ExecutionException e) {
                log.error("Failed to send record to Hermes topic '{}': {}", hermesTopic, e.getCause().getMessage(), e.getCause());
                throw new ConnectException("Failed to deliver record to Hermes topic '" + hermesTopic + "'", e.getCause());
            }
        }
    }

    @Override
    public void flush(Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> currentOffsets) {
        // Ensure all in-flight sends complete before Connect commits offsets
        producer.flush();
    }

    @Override
    public void stop() {
        if (producer != null) {
            log.info("HermesSinkTask stopping — closing producer");
            producer.close(Duration.ofSeconds(30));
            producer = null;
        }
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
        // Max in-flight requests per connection: 5 is the safe limit for idempotent producers
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);

        // SSL — in-memory keystore via KIP-519. Values pass through as Password
        // objects so they remain masked if the KafkaProducer dumps its config.
        props.put("security.protocol", "SSL");
        props.put("ssl.engine.factory.class", InMemorySslEngineFactory.class.getName());
        props.put(InMemorySslEngineFactory.KEYSTORE_B64_CONFIG, config.getKeystoreB64());
        props.put(InMemorySslEngineFactory.KEYSTORE_PASSWORD_CONFIG, config.getKeystorePassword());
        props.put(InMemorySslEngineFactory.TRUSTSTORE_B64_CONFIG, config.getTruststoreB64());
        props.put(InMemorySslEngineFactory.TRUSTSTORE_PASSWORD_CONFIG, config.getTruststorePassword());

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
}
