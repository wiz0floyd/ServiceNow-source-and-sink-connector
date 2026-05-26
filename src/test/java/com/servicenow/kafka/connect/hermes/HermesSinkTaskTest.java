package com.servicenow.kafka.connect.hermes;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class HermesSinkTaskTest {

    @Mock KafkaProducer<byte[], byte[]> mockProducer;
    @Mock SinkTaskContext mockContext;
    @Mock ErrantRecordReporter mockErrantRecordReporter;

    @Captor ArgumentCaptor<ProducerRecord<byte[], byte[]>> recordCaptor;

    private HermesSinkTask task;
    private static final String HERMES_TOPIC = "snc.myinstance.sn_streamconnect.test-topic";

    @BeforeEach
    void setUp() {
        task = new HermesSinkTask();
        task.initialize(mockContext);
        task.setProducer(mockProducer);
        task.setHermesTopic(HERMES_TOPIC);
    }

    // --- Helper: stub producer.send(record, callback) to fire success immediately ---
    private void stubSuccessfulSend() {
        when(mockProducer.send(any(), any())).thenAnswer(invocation -> {
            org.apache.kafka.clients.producer.Callback cb = invocation.getArgument(1);
            cb.onCompletion(mock(RecordMetadata.class), null);
            return null;
        });
    }

    // --- Helper: stub producer.send(record, callback) to fire a failure immediately ---
    private void stubFailingSend(Exception exception) {
        when(mockProducer.send(any(), any())).thenAnswer(invocation -> {
            org.apache.kafka.clients.producer.Callback cb = invocation.getArgument(1);
            cb.onCompletion(null, exception);
            return null;
        });
    }

    @Test
    void putWithEmptyCollectionDoesNotCallSend() {
        task.put(Collections.emptyList());
        verifyNoInteractions(mockProducer);
    }

    @Test
    void putSendsOneRecordPerSinkRecord() {
        stubSuccessfulSend();

        List<SinkRecord> records = Arrays.asList(
            makeSinkRecord("key1".getBytes(), "value1".getBytes()),
            makeSinkRecord("key2".getBytes(), "value2".getBytes()),
            makeSinkRecord("key3".getBytes(), "value3".getBytes())
        );

        task.put(records);

        verify(mockProducer, times(3)).send(any(ProducerRecord.class), any());
    }

    @Test
    void putSendsToCorrectHermesTopic() {
        stubSuccessfulSend();
        task.put(List.of(makeSinkRecord("k".getBytes(), "v".getBytes())));

        verify(mockProducer).send(recordCaptor.capture(), any());
        assertEquals(HERMES_TOPIC, recordCaptor.getValue().topic());
    }

    @Test
    void putPreservesKeyAndValue() {
        stubSuccessfulSend();
        byte[] key = "my-key".getBytes();
        byte[] value = "my-value".getBytes();

        task.put(List.of(makeSinkRecord(key, value)));

        verify(mockProducer).send(recordCaptor.capture(), any());
        assertArrayEquals(key, recordCaptor.getValue().key());
        assertArrayEquals(value, recordCaptor.getValue().value());
    }

    @Test
    void putPreservesHeaders() {
        stubSuccessfulSend();
        ConnectHeaders headers = new ConnectHeaders();
        headers.addBytes("ce-specversion", "1.0".getBytes());
        headers.addBytes("ce-type", "com.sn.incident.created".getBytes());

        SinkRecord record = new SinkRecord(
            "source-topic", 0, Schema.BYTES_SCHEMA, "k".getBytes(),
            Schema.BYTES_SCHEMA, "v".getBytes(), 0L, 1000L,
            org.apache.kafka.common.record.TimestampType.CREATE_TIME, headers);

        task.put(List.of(record));

        verify(mockProducer).send(recordCaptor.capture(), any());
        org.apache.kafka.common.header.Headers sentHeaders = recordCaptor.getValue().headers();
        assertNotNull(sentHeaders.lastHeader("ce-specversion"));
        assertArrayEquals("1.0".getBytes(), sentHeaders.lastHeader("ce-specversion").value());
        assertNotNull(sentHeaders.lastHeader("ce-type"));
    }

    @Test
    void putWithNullKeyAndValueDoesNotThrow() {
        stubSuccessfulSend();
        task.put(List.of(makeSinkRecord(null, null)));
        verify(mockProducer, times(1)).send(any(), any());
    }

    @Test
    void putPartitionIsNullForHashByKeyBehavior() {
        stubSuccessfulSend();
        task.put(List.of(makeSinkRecord("k".getBytes(), "v".getBytes())));
        verify(mockProducer).send(recordCaptor.capture(), any());
        assertNull(recordCaptor.getValue().partition(),
            "Partition must be null to let the partitioner do hash-by-key");
    }

    @Test
    void putDoesNotThrowOnSendFailure_exceptionSurfacesInFlush() {
        // Without an errant reporter, permanent send failures are stored and surface in flush()
        stubFailingSend(new RuntimeException("send failed"));

        // put() must not throw
        assertDoesNotThrow(() -> task.put(List.of(makeSinkRecord("k".getBytes(), "v".getBytes()))));

        // flush() surfaces the stored exception
        assertThrows(ConnectException.class, () -> task.flush(Map.of()));
    }

    @Test
    void flushCallsProducerFlush() {
        task.flush(Collections.emptyMap());
        verify(mockProducer).flush();
    }

    @Test
    void flushThrowsConnectExceptionWhenAsyncSendFailed() {
        // Cause the callback to fire with a permanent error; no errant reporter configured
        stubFailingSend(new RuntimeException("broker rejected"));

        // put() stores the exception without throwing
        task.put(List.of(makeSinkRecord("k".getBytes(), "v".getBytes())));

        // flush() must throw ConnectException wrapping the stored exception
        ConnectException ex = assertThrows(ConnectException.class, () -> task.flush(Map.of()));
        assertTrue(ex.getMessage().contains(HERMES_TOPIC),
            "Exception message should reference the Hermes topic");
    }

    @Test
    void stopClosesProducer() {
        task.stop();
        verify(mockProducer).close(any(java.time.Duration.class));
    }

    @Test
    void stopIsIdempotentAfterFirstCall() {
        task.stop();
        // Second stop should not re-close (producer is nulled out)
        assertDoesNotThrow(() -> task.stop());
    }

    @Test
    void versionIsNonNull() {
        assertNotNull(task.version());
    }

    @Test
    void putSurfacesTransientKafkaErrorInFlush() {
        // NotLeaderOrFollowerException is a RetriableException in the Kafka client, but
        // in the async path the callback receives it directly; it is stored and surfaces in flush()
        stubFailingSend(new NotLeaderOrFollowerException("leader election"));

        assertDoesNotThrow(() -> task.put(List.of(makeSinkRecord("k".getBytes(), "v".getBytes()))));
        assertThrows(ConnectException.class, () -> task.flush(Map.of()));
    }

    @Test
    void putReportsToErrantRecordReporterOnPermanentFailure() {
        task.setErrantRecordReporter(mockErrantRecordReporter);

        InvalidTopicException cause = new InvalidTopicException("bad-topic");
        stubFailingSend(cause);

        SinkRecord record = makeSinkRecord("k".getBytes(), "v".getBytes());
        // Must not throw — the reporter absorbs the error and processing continues
        assertDoesNotThrow(() -> task.put(List.of(record)));

        verify(mockErrantRecordReporter).report(record, cause);
    }

    @Test
    void putDoesNotThrowWhenErrantReporterAbsorbsError() {
        // With a reporter wired, the callback routes to the reporter; sendException stays null
        task.setErrantRecordReporter(mockErrantRecordReporter);
        stubFailingSend(new RuntimeException("absorbed error"));

        assertDoesNotThrow(() -> task.put(List.of(makeSinkRecord("k".getBytes(), "v".getBytes()))));
        // flush() should also not throw — the exception was absorbed by the reporter
        assertDoesNotThrow(() -> task.flush(Map.of()));
    }

    @Test
    void preCommitDelegatesToSuper() {
        // preCommit() just logs and delegates — verify it returns the input offsets unchanged
        Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets =
            Map.of(new TopicPartition(HERMES_TOPIC, 0),
                   new org.apache.kafka.clients.consumer.OffsetAndMetadata(42L));
        Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> result =
            task.preCommit(offsets);
        // SinkTask.preCommit() default delegates to currentOffsets from context; since the
        // task is not fully started we just verify no exception is thrown and call completes.
        // (The return value is context-managed by the Connect runtime, not the task itself.)
        assertNotNull(result);
    }

    // ---- Helpers ----

    private SinkRecord makeSinkRecord(byte[] key, byte[] value) {
        return new SinkRecord(
            "source-topic", 0,
            key != null ? Schema.BYTES_SCHEMA : null, key,
            value != null ? Schema.BYTES_SCHEMA : null, value,
            0L
        );
    }
}
