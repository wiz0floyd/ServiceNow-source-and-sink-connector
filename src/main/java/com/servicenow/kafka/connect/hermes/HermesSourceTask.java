package com.servicenow.kafka.connect.hermes;

import com.servicenow.kafka.connect.hermes.ssl.InMemorySslEngineFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HermesSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(HermesSourceTask.class);

    static final String CLUSTER_1 = "1";
    static final String CLUSTER_2 = "2";

    private KafkaConsumer<byte[], byte[]> consumer1;
    private KafkaConsumer<byte[], byte[]> consumer2;
    private String hermesTopic;
    private String confluentTopic;

    @Override
    public String version() {
        return HermesSourceConnector.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        HermesSourceConfig config = new HermesSourceConfig(props);
        hermesTopic = config.getSourceTopic();
        confluentTopic = config.getConfluentTopic();

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

        log.info("HermesSourceTask consumers initialized for both source clusters");
    }

    @Override
    public List<SourceRecord> poll() {
        List<SourceRecord> results = new ArrayList<>();
        drainConsumer(consumer1, CLUSTER_1, results);
        drainConsumer(consumer2, CLUSTER_2, results);
        return results;
    }

    @Override
    public void stop() {
        log.info("HermesSourceTask stopping");
        closeConsumer(consumer1, CLUSTER_1);
        consumer1 = null;
        closeConsumer(consumer2, CLUSTER_2);
        consumer2 = null;
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
                        consumer.seek(tp, offset);
                        log.debug("Restored offset {} for cluster={} partition={}", offset, clusterId, tp);
                    }
                }
            }

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // No-op: Connect manages offset commits via commitRecord / offset storage.
            }
        });
    }

    // ---- Record draining & conversion ----

    private void drainConsumer(KafkaConsumer<byte[], byte[]> consumer, String clusterId, List<SourceRecord> out) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord<byte[], byte[]> r : records) {
            out.add(toSourceRecord(r, clusterId));
        }
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
        Map<String, Object> m = new HashMap<>();
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
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
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

    void setConfluentTopic(String topic) {
        this.confluentTopic = topic;
    }
}
