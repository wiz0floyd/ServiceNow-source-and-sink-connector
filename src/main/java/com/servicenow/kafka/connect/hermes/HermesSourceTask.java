package com.servicenow.kafka.connect.hermes;

import com.servicenow.kafka.connect.hermes.ssl.InMemorySslEngineFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HermesSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(HermesSourceTask.class);

    static final String CLUSTER_1 = "1";
    static final String CLUSTER_2 = "2";

    private volatile boolean stopping = false;
    private volatile boolean firstRecordLogged = false;
    private KafkaConsumer<byte[], byte[]> consumer1;
    private KafkaConsumer<byte[], byte[]> consumer2;
    private String hermesTopic;
    private String confluentTopic;
    private Duration pollTimeout = Duration.ofMillis(100);

    // Tracks offsets confirmed by the Connect worker via commitRecord().
    private final ConcurrentHashMap<Map<String, Object>, Map<String, Object>> committedOffsets =
        new ConcurrentHashMap<>();
    private final HermesSourceTaskMetrics metrics = new HermesSourceTaskMetrics();

    @Override
    public String version() {
        return HermesSourceConnector.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        HermesSourceConfig config = new HermesSourceConfig(props);
        hermesTopic = config.getSourceTopic();
        confluentTopic = config.getDestinationTopic();
        pollTimeout = Duration.ofMillis(config.getPollTimeoutMs());

        String bootstrap1 = config.getCluster1BootstrapOverride().isEmpty()
            ? HermesBootstrapBuilder.buildSourceCluster1Bootstrap(config.getInstanceName())
            : config.getCluster1BootstrapOverride();
        String bootstrap2 = config.getCluster2BootstrapOverride().isEmpty()
            ? HermesBootstrapBuilder.buildSourceCluster2Bootstrap(config.getInstanceName())
            : config.getCluster2BootstrapOverride();

        log.info("HermesSourceTask starting — hermesTopic: {}, confluentTopic: {}, cluster1: {}, cluster2: {}",
            hermesTopic, confluentTopic, bootstrap1, bootstrap2);

        consumer1 = createConsumer1(buildConsumerProperties(config, bootstrap1));
        consumer2 = createConsumer2(buildConsumerProperties(config, bootstrap2));

        subscribeWithOffsetRestore(consumer1, CLUSTER_1);
        subscribeWithOffsetRestore(consumer2, CLUSTER_2);

        metrics.register(config.getInstanceName(), 0);
        log.info("HermesSourceTask consumers initialized for both source clusters");
    }

    @Override
    public List<SourceRecord> poll() {
        List<SourceRecord> results = new ArrayList<>();
        int c1Count = drainConsumer(consumer1, CLUSTER_1, results, pollTimeout);
        Duration c2Timeout = c1Count > 0 ? Duration.ZERO : pollTimeout;
        drainConsumer(consumer2, CLUSTER_2, results, c2Timeout);
        return results.isEmpty() ? null : results;
    }

    @Override
    public void stop() {
        log.info("HermesSourceTask stopping");
        stopping = true;
        // Wakeup unblocks any in-progress consumer.poll() before close() is called.
        if (consumer1 != null) consumer1.wakeup();
        if (consumer2 != null) consumer2.wakeup();
        closeConsumer(consumer1, CLUSTER_1);
        consumer1 = null;
        closeConsumer(consumer2, CLUSTER_2);
        consumer2 = null;
        metrics.unregister();
    }

    @Override
    public void commit() throws InterruptedException {
        log.debug("HermesSourceTask commit() — {} offset(s) confirmed by worker", committedOffsets.size());
    }

    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) {
        committedOffsets.put((Map<String, Object>) record.sourcePartition(), (Map<String, Object>) record.sourceOffset());
    }

    // ---- Subscription & offset restore ----

    private void subscribeWithOffsetRestore(KafkaConsumer<byte[], byte[]> consumer, String clusterId) {
        consumer.subscribe(Collections.singletonList(hermesTopic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                for (TopicPartition tp : partitions) {
                    Map<String, Object> partKey = sourcePartitionKey(clusterId, tp.topic(), tp.partition());
                    Map<String, Object> stored = context.offsetStorageReader().offset(partKey);
                    if (stored != null && stored.containsKey("offset")) {
                        long offset = ((Number) stored.get("offset")).longValue();
                        try {
                            consumer.seek(tp, offset);
                            log.debug("Restored offset {} for cluster={} partition={}", offset, clusterId, tp);
                        } catch (Exception e) {
                            // Stored offset may have been evicted by log retention; fall back to
                            // auto.offset.reset (earliest). Records between the lost offset and the
                            // new start position will be replayed.
                            log.warn("Stored offset {} for cluster={} partition={} is out of range or invalid; "
                                + "falling back to auto.offset.reset. Cause: {}", offset, clusterId, tp, e.getMessage());
                        }
                    }
                }
            }

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.warn("Partitions revoked for cluster={}: {}. In-flight unconfirmed offsets may be replayed "
                    + "after reassignment.", clusterId, partitions);
            }
        });
    }

    // ---- Record draining & conversion ----

    private int drainConsumer(KafkaConsumer<byte[], byte[]> consumer, String clusterId, List<SourceRecord> out, Duration timeout) {
        ConsumerRecords<byte[], byte[]> records;
        try {
            records = consumer.poll(timeout);
        } catch (WakeupException e) {
            // Wakeup is expected when stop() is called; also treat spurious wakeups as non-fatal.
            if (!stopping) {
                log.debug("Spurious WakeupException for cluster={}, ignoring", clusterId);
            }
            return 0;
        } catch (InterruptException e) {
            // Kafka's wrapper for InterruptedException — restore the interrupt flag and bail.
            Thread.currentThread().interrupt();
            return 0;
        } catch (RetriableException e) {
            log.warn("Transient error polling cluster={}: {} — will retry on next poll()", clusterId, e.getMessage());
            return 0;
        } catch (KafkaException e) {
            // Non-retriable: authentication failure, protocol error, etc. — fatal for the task.
            throw new ConnectException("Unrecoverable error polling cluster=" + clusterId, e);
        }

        if (!records.isEmpty() && !firstRecordLogged) {
            firstRecordLogged = true;
            log.info("HermesSourceTask first record received from cluster={}", clusterId);
        }
        log.debug("HermesSourceTask polled {} record(s) from cluster={}", records.count(), clusterId);

        int count = records.count();
        if (count > 0) {
            if (CLUSTER_1.equals(clusterId)) metrics.recordPollCluster1(count);
            else metrics.recordPollCluster2(count);
        }

        for (ConsumerRecord<byte[], byte[]> r : records) {
            out.add(toSourceRecord(r, clusterId));
        }
        return count;
    }

    private SourceRecord toSourceRecord(ConsumerRecord<byte[], byte[]> r, String clusterId) {
        Map<String, Object> srcPartition = sourcePartitionKey(clusterId, r.topic(), r.partition());
        Map<String, Object> srcOffset = new HashMap<>();
        // Store the next offset to read so a restart resumes from the correct position.
        srcOffset.put("offset", r.offset() + 1L);

        SourceRecord sr = new SourceRecord(
            srcPartition,
            srcOffset,
            confluentTopic,
            null,
            Schema.OPTIONAL_BYTES_SCHEMA,
            r.key(),
            Schema.OPTIONAL_BYTES_SCHEMA,
            r.value(),
            r.timestamp()
        );

        if (r.headers() != null) {
            for (org.apache.kafka.common.header.Header h : r.headers()) {
                sr.headers().addBytes(h.key(), h.value());
            }
        }

        return sr;
    }

    // Hermes ACLs require group IDs prefixed with "snc.<instance>." — auto-apply if missing.
    static String resolveGroupId(HermesSourceConfig config) {
        String groupId = config.getGroupId();
        String prefix = "snc." + config.getInstanceName() + ".";
        if (groupId.startsWith(prefix)) {
            return groupId;
        }
        String resolved = prefix + groupId;
        log.info("Auto-prefixing consumer group.id with Hermes ACL prefix: {} → {}", groupId, resolved);
        return resolved;
    }

    static Map<String, Object> sourcePartitionKey(String clusterId, String topic, int partition) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("cluster", clusterId);
        m.put("topic", topic);
        m.put("partition", partition);
        return m;
    }

    // ---- Properties builder ----

    static Map<String, Object> buildConsumerProperties(HermesSourceConfig config, String bootstrap) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, resolveGroupId(config));
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.getMaxPollRecords());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getAutoOffsetReset());
        // Connect manages offsets via its own offset store; the consumer must not auto-commit.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        if (config.isSslEnabled()) {
            // SSL — in-memory keystore via KIP-519. Password objects keep secrets masked
            // if the KafkaConsumer dumps its config.
            props.put("security.protocol", "SSL");
            props.put("ssl.engine.factory.class", InMemorySslEngineFactory.class.getName());
            props.put(InMemorySslEngineFactory.KEYSTORE_B64_CONFIG, config.getKeystoreB64());
            props.put(InMemorySslEngineFactory.KEYSTORE_PASSWORD_CONFIG, config.getKeystorePassword());
            props.put(InMemorySslEngineFactory.TRUSTSTORE_B64_CONFIG, config.getTruststoreB64());
            props.put(InMemorySslEngineFactory.TRUSTSTORE_PASSWORD_CONFIG, config.getTruststorePassword());
        }
        // else: PLAINTEXT — for local/Docker E2E testing only

        return props;
    }

    private void closeConsumer(KafkaConsumer<byte[], byte[]> consumer, String clusterId) {
        if (consumer != null) {
            try {
                consumer.close(Duration.ofSeconds(30));
            } catch (Exception e) {
                log.warn("Error closing consumer for cluster {}: {}", clusterId, e.getMessage());
            }
        }
    }

    // ---- Package-private hooks for tests ----

    KafkaConsumer<byte[], byte[]> createConsumer1(Map<String, Object> props) {
        return new KafkaConsumer<>(props);
    }

    KafkaConsumer<byte[], byte[]> createConsumer2(Map<String, Object> props) {
        return new KafkaConsumer<>(props);
    }

    void setConsumer1(KafkaConsumer<byte[], byte[]> consumer) {
        this.consumer1 = consumer;
    }

    void setConsumer2(KafkaConsumer<byte[], byte[]> consumer) {
        this.consumer2 = consumer;
    }

    void setHermesTopic(String topic) {
        this.hermesTopic = topic;
    }

    void setDestinationTopic(String topic) {
        this.confluentTopic = topic;
    }

    // Package-private — for tests only.
    Map<Map<String, Object>, Map<String, Object>> getCommittedOffsets() {
        return committedOffsets;
    }
}
