package com.servicenow.kafka.connect.hermes;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class HermesSourceTaskTest {

    @Mock KafkaConsumer<byte[], byte[]> mockConsumer1;
    @Mock KafkaConsumer<byte[], byte[]> mockConsumer2;
    @Mock SourceTaskContext mockContext;
    @Mock OffsetStorageReader mockOffsetReader;

    private HermesSourceTask task;
    static final String HERMES_TOPIC = "snc.myinstance.sn_streamconnect.test-topic";
    static final String CONFLUENT_TOPIC = "my-cc-topic";

    @BeforeEach
    void setUp() {
        task = new HermesSourceTask();
        task.initialize(mockContext);
        task.setConsumer1(mockConsumer1);
        task.setConsumer2(mockConsumer2);
        task.setHermesTopic(HERMES_TOPIC);
        task.setConfluentTopic(CONFLUENT_TOPIC);
    }

    @Test
    void pollReturnsNullWhenBothConsumersReturnNoRecords() {
        when(mockConsumer1.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());
        when(mockConsumer2.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());

        assertNull(task.poll());
    }

    @Test
    void pollReturnsRecordsFromConsumer1WithCluster1PartitionKey() {
        when(mockConsumer1.poll(any(Duration.class))).thenReturn(
            makeRecords(HERMES_TOPIC, 0, 10L, "k".getBytes(), "v".getBytes()));
        when(mockConsumer2.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());

        List<SourceRecord> results = task.poll();
        assertEquals(1, results.size());
        Map<String, ?> sourcePartition = results.get(0).sourcePartition();
        assertEquals("1", sourcePartition.get("cluster"));
        assertEquals(HERMES_TOPIC, sourcePartition.get("topic"));
        assertEquals(0, sourcePartition.get("partition"));
    }

    @Test
    void pollReturnsRecordsFromConsumer2WithCluster2PartitionKey() {
        when(mockConsumer1.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());
        when(mockConsumer2.poll(any(Duration.class))).thenReturn(
            makeRecords(HERMES_TOPIC, 0, 10L, "k".getBytes(), "v".getBytes()));

        List<SourceRecord> results = task.poll();
        assertEquals(1, results.size());
        assertEquals("2", results.get(0).sourcePartition().get("cluster"));
    }

    @Test
    void pollCombinesRecordsFromBothConsumers() {
        when(mockConsumer1.poll(any(Duration.class))).thenReturn(
            makeMultipleRecords(HERMES_TOPIC, 0, 0L, 2));
        when(mockConsumer2.poll(any(Duration.class))).thenReturn(
            makeMultipleRecords(HERMES_TOPIC, 0, 0L, 3));

        List<SourceRecord> results = task.poll();
        assertEquals(5, results.size());
    }

    @Test
    void pollSetsCorrectSourceOffset() {
        when(mockConsumer1.poll(any(Duration.class))).thenReturn(
            makeRecords(HERMES_TOPIC, 0, 42L, "k".getBytes(), "v".getBytes()));
        when(mockConsumer2.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());

        List<SourceRecord> results = task.poll();
        assertEquals(43L, results.get(0).sourceOffset().get("offset"));
    }

    @Test
    void pollSetsConfluentTopicOnAllRecords() {
        when(mockConsumer1.poll(any(Duration.class))).thenReturn(
            makeRecords(HERMES_TOPIC, 0, 0L, "k".getBytes(), "v".getBytes()));
        when(mockConsumer2.poll(any(Duration.class))).thenReturn(
            makeRecords(HERMES_TOPIC, 0, 0L, "k".getBytes(), "v".getBytes()));

        List<SourceRecord> results = task.poll();
        assertEquals(2, results.size());
        for (SourceRecord r : results) {
            assertEquals(CONFLUENT_TOPIC, r.topic());
        }
    }

    @Test
    void pollPreservesKeyAndValue() {
        byte[] key = "my-key".getBytes();
        byte[] value = "my-value".getBytes();
        when(mockConsumer1.poll(any(Duration.class))).thenReturn(
            makeRecords(HERMES_TOPIC, 0, 0L, key, value));
        when(mockConsumer2.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());

        List<SourceRecord> results = task.poll();
        assertArrayEquals(key, (byte[]) results.get(0).key());
        assertArrayEquals(value, (byte[]) results.get(0).value());
    }

    @Test
    void pollPreservesHeaders() {
        byte[] key = "k".getBytes();
        byte[] value = "v".getBytes();
        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader("ce-type", "incident.created".getBytes()));

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
            HERMES_TOPIC, 0, 0L, 1000L, TimestampType.CREATE_TIME,
            key.length, value.length, key, value, headers, Optional.empty());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> data = new HashMap<>();
        data.put(new TopicPartition(HERMES_TOPIC, 0), List.of(record));

        when(mockConsumer1.poll(any(Duration.class))).thenReturn(new ConsumerRecords<>(data));
        when(mockConsumer2.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());

        List<SourceRecord> results = task.poll();
        assertEquals(1, results.size());
        assertNotNull(results.get(0).headers().lastWithName("ce-type"));
        assertArrayEquals(
            "incident.created".getBytes(),
            (byte[]) results.get(0).headers().lastWithName("ce-type").value());
    }

    @Test
    void pollSetsNullPartitionForCCDefaultPartitioner() {
        when(mockConsumer1.poll(any(Duration.class))).thenReturn(
            makeRecords(HERMES_TOPIC, 0, 0L, "k".getBytes(), "v".getBytes()));
        when(mockConsumer2.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());

        List<SourceRecord> results = task.poll();
        assertNull(results.get(0).kafkaPartition());
    }

    @Test
    void stopClosesBothConsumers() {
        task.stop();
        verify(mockConsumer1).close(any(Duration.class));
        verify(mockConsumer2).close(any(Duration.class));
    }

    @Test
    void stopIsIdempotentAfterFirstCall() {
        task.stop();
        assertDoesNotThrow(() -> task.stop());
    }

    @Test
    void versionIsNonNull() {
        assertNotNull(task.version());
    }

    @Test
    void sourcePartitionKeyHasCorrectShape() {
        Map<String, Object> key = HermesSourceTask.sourcePartitionKey("1", "my-topic", 3);
        assertEquals("1", key.get("cluster"));
        assertEquals("my-topic", key.get("topic"));
        assertEquals(3, key.get("partition"));
    }

    @Test
    void buildConsumerPropertiesHasAutoCommitDisabled() {
        HermesSourceConfig config = new HermesSourceConfig(minimalConfigProps());
        Map<String, Object> props = HermesSourceTask.buildConsumerProperties(config, "bootstrap:9092");
        assertEquals("false", props.get("enable.auto.commit"));
    }

    @Test
    void buildConsumerPropertiesHasEarliestReset() {
        HermesSourceConfig config = new HermesSourceConfig(minimalConfigProps());
        Map<String, Object> props = HermesSourceTask.buildConsumerProperties(config, "bootstrap:9092");
        assertEquals("earliest", props.get("auto.offset.reset"));
    }

    @Test
    void resolveGroupIdAddsPrefixWhenMissing() {
        HermesSourceConfig config = new HermesSourceConfig(minimalConfigProps());
        assertEquals("snc.myinstance.test-group", HermesSourceTask.resolveGroupId(config));
    }

    @Test
    void resolveGroupIdDoesNotDoublePrefixWhenAlreadyPresent() {
        Map<String, String> props = minimalConfigProps();
        props.put(HermesSourceConfig.HERMES_CONSUMER_GROUP_ID_CONFIG, "snc.myinstance.test-group");
        HermesSourceConfig config = new HermesSourceConfig(props);
        assertEquals("snc.myinstance.test-group", HermesSourceTask.resolveGroupId(config));
    }

    @Test
    void buildConsumerPropertiesGroupIdHasHermesPrefix() {
        HermesSourceConfig config = new HermesSourceConfig(minimalConfigProps());
        Map<String, Object> props = HermesSourceTask.buildConsumerProperties(config, "bootstrap:9092");
        assertEquals("snc.myinstance.test-group", props.get("group.id"));
    }

    // ---- Helpers ----

    private ConsumerRecords<byte[], byte[]> makeRecords(String topic, int partition, long offset, byte[] key, byte[] value) {
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(topic, partition, offset, key, value);
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> data = new HashMap<>();
        data.put(new TopicPartition(topic, partition), List.of(record));
        return new ConsumerRecords<>(data);
    }

    private ConsumerRecords<byte[], byte[]> makeMultipleRecords(String topic, int partition, long startOffset, int count) {
        java.util.List<ConsumerRecord<byte[], byte[]>> list = new java.util.ArrayList<>();
        for (int i = 0; i < count; i++) {
            list.add(new ConsumerRecord<>(topic, partition, startOffset + i,
                ("k" + i).getBytes(), ("v" + i).getBytes()));
        }
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> data = new HashMap<>();
        data.put(new TopicPartition(topic, partition), list);
        return new ConsumerRecords<>(data);
    }

    private Map<String, String> minimalConfigProps() {
        Map<String, String> p = new HashMap<>();
        p.put(HermesSourceConfig.HERMES_INSTANCE_NAME_CONFIG, "myinstance");
        p.put(HermesSourceConfig.HERMES_SOURCE_TOPIC_CONFIG, HERMES_TOPIC);
        p.put(HermesSourceConfig.HERMES_CONSUMER_GROUP_ID_CONFIG, "test-group");
        p.put(HermesSourceConfig.CONFLUENT_TOPIC_CONFIG, CONFLUENT_TOPIC);
        p.put(HermesSourceConfig.HERMES_SSL_KEYSTORE_B64_CONFIG, "dGVzdA==");
        p.put(HermesSourceConfig.HERMES_SSL_KEYSTORE_PASSWORD_CONFIG, "pass");
        p.put(HermesSourceConfig.HERMES_SSL_TRUSTSTORE_B64_CONFIG, "dGVzdA==");
        p.put(HermesSourceConfig.HERMES_SSL_TRUSTSTORE_PASSWORD_CONFIG, "pass");
        return p;
    }
}
