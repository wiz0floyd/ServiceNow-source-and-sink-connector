package com.servicenow.kafka.connect.hermes;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Tag("integration")
@Testcontainers
class HermesSourceConnectorIT {

    @Container
    static final KafkaContainer cluster1 = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @Container
    static final KafkaContainer cluster2 = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    static final String HERMES_TOPIC = "snc.it-instance.sn_streamconnect.it-topic";
    static final String DEST_TOPIC = "cc-it-topic";
    static final int RECORDS_PER_CLUSTER = 3;

    private HermesSourceTask task;

    @BeforeEach
    void setUp() throws Exception {
        createTopic(cluster1, HERMES_TOPIC);
        createTopic(cluster2, HERMES_TOPIC);

        produce(cluster1, HERMES_TOPIC, RECORDS_PER_CLUSTER);
        produce(cluster2, HERMES_TOPIC, RECORDS_PER_CLUSTER);

        OffsetStorageReader mockOffsetReader = mock(OffsetStorageReader.class);
        when(mockOffsetReader.offset(any())).thenReturn(null);

        SourceTaskContext mockContext = mock(SourceTaskContext.class);
        when(mockContext.offsetStorageReader()).thenReturn(mockOffsetReader);

        task = new HermesSourceTask();
        task.initialize(mockContext);
        task.start(taskProps());
    }

    @AfterEach
    void tearDown() {
        task.stop();
    }

    @Test
    void pollReturnsBytesFromBothClusters() throws Exception {
        List<SourceRecord> allRecords = new ArrayList<>();
        // Poll multiple times to collect records from both embedded consumers
        for (int i = 0; i < 20 && allRecords.size() < RECORDS_PER_CLUSTER * 2; i++) {
            List<SourceRecord> batch = task.poll();
            if (batch != null) allRecords.addAll(batch);
            Thread.sleep(100);
        }

        assertEquals(RECORDS_PER_CLUSTER * 2, allRecords.size(),
            "Expected " + RECORDS_PER_CLUSTER + " records from each cluster, got: " + allRecords.size());

        // Every record should be routed to the destination topic
        for (SourceRecord r : allRecords) {
            assertEquals(DEST_TOPIC, r.topic());
        }

        // Both cluster IDs must appear
        long cluster1Count = allRecords.stream()
            .filter(r -> "1".equals(r.sourcePartition().get("cluster"))).count();
        long cluster2Count = allRecords.stream()
            .filter(r -> "2".equals(r.sourcePartition().get("cluster"))).count();
        assertEquals(RECORDS_PER_CLUSTER, cluster1Count,
            "Expected " + RECORDS_PER_CLUSTER + " records from cluster 1");
        assertEquals(RECORDS_PER_CLUSTER, cluster2Count,
            "Expected " + RECORDS_PER_CLUSTER + " records from cluster 2");
    }

    // ---- Helpers ----

    private Map<String, String> taskProps() {
        Map<String, String> p = new HashMap<>();
        p.put(HermesSourceConfig.HERMES_INSTANCE_NAME_CONFIG, "it-instance");
        p.put(HermesSourceConfig.HERMES_SOURCE_TOPIC_CONFIG, HERMES_TOPIC);
        p.put(HermesSourceConfig.HERMES_CONSUMER_GROUP_ID_CONFIG, "snc.it-instance.it-group");
        p.put(HermesSourceConfig.HERMES_DESTINATION_TOPIC_CONFIG, DEST_TOPIC);
        p.put(HermesSourceConfig.HERMES_SOURCE_CLUSTER1_BOOTSTRAP_OVERRIDE_CONFIG, cluster1.getBootstrapServers());
        p.put(HermesSourceConfig.HERMES_SOURCE_CLUSTER2_BOOTSTRAP_OVERRIDE_CONFIG, cluster2.getBootstrapServers());
        p.put(HermesSourceConfig.HERMES_SSL_ENABLED_CONFIG, "false");
        p.put(HermesSourceConfig.HERMES_CONSUMER_AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(HermesSourceConfig.HERMES_CONSUMER_POLL_TIMEOUT_MS_CONFIG, "500");
        return p;
    }

    private static void createTopic(KafkaContainer kafka, String topic) throws Exception {
        try (AdminClient admin = AdminClient.create(
                Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
        }
    }

    private static void produce(KafkaContainer kafka, String topic, int count) throws Exception {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                producer.send(new ProducerRecord<>(topic, ("key-" + i).getBytes(), ("val-" + i).getBytes())).get();
            }
        }
    }
}
