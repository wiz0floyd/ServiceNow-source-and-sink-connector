package com.servicenow.kafka.connect.hermes;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Tag("integration")
@Testcontainers
class HermesSinkConnectorIT {

    @Container
    static final KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    static final String TOPIC = "hermes-it-sink-topic";

    private HermesSinkTask task;

    @BeforeEach
    void setUp() throws Exception {
        // Pre-create the topic so the connector doesn't fail at startup
        try (org.apache.kafka.clients.admin.AdminClient admin = org.apache.kafka.clients.admin.AdminClient.create(
                Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new org.apache.kafka.clients.admin.NewTopic(TOPIC, 1, (short) 1)))
                .all().get();
        }

        SinkTaskContext mockContext = mock(SinkTaskContext.class);
        when(mockContext.errantRecordReporter()).thenReturn(null);

        task = new HermesSinkTask();
        task.initialize(mockContext);
        task.start(taskProps());
    }

    @AfterEach
    void tearDown() {
        task.stop();
    }

    @Test
    void putDeliversBytesToHermesBroker() throws Exception {
        List<SinkRecord> records = List.of(
            new SinkRecord(TOPIC, 0, null, "key1".getBytes(), null, "value1".getBytes(), 0),
            new SinkRecord(TOPIC, 0, null, "key2".getBytes(), null, "value2".getBytes(), 1),
            new SinkRecord(TOPIC, 0, null, "key3".getBytes(), null, "value3".getBytes(), 2)
        );

        task.put(records);
        task.flush(Collections.emptyMap());

        // Consume from the broker and assert at least one record arrived
        try (KafkaConsumer<byte[], byte[]> consumer = buildConsumer()) {
            consumer.subscribe(List.of(TOPIC));
            ConsumerRecords<byte[], byte[]> polled = ConsumerRecords.empty();
            for (int i = 0; i < 10 && polled.isEmpty(); i++) {
                polled = consumer.poll(Duration.ofMillis(500));
            }
            assertFalse(polled.isEmpty(), "Expected records on Hermes broker but got none");
        }
    }

    // ---- Helpers ----

    private Map<String, String> taskProps() {
        Map<String, String> p = new HashMap<>();
        p.put(HermesConnectorConfig.HERMES_INSTANCE_NAME_CONFIG, "it-instance");
        p.put(HermesConnectorConfig.HERMES_TOPIC_CONFIG, TOPIC);
        p.put(HermesConnectorConfig.HERMES_SINK_BOOTSTRAP_OVERRIDE_CONFIG, kafka.getBootstrapServers());
        p.put(HermesConnectorConfig.HERMES_SSL_ENABLED_CONFIG, "false");
        // SSL fields not needed when ssl.enabled=false
        p.put(HermesConnectorConfig.HERMES_SSL_KEYSTORE_B64_CONFIG, "");
        p.put(HermesConnectorConfig.HERMES_SSL_KEYSTORE_PASSWORD_CONFIG, "");
        p.put(HermesConnectorConfig.HERMES_SSL_TRUSTSTORE_B64_CONFIG, "");
        p.put(HermesConnectorConfig.HERMES_SSL_TRUSTSTORE_PASSWORD_CONFIG, "");
        return p;
    }

    private KafkaConsumer<byte[], byte[]> buildConsumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-sink-verifier");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }
}
