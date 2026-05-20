package com.servicenow.kafka.connect.hermes;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class HermesSinkTaskTest {

    @Mock KafkaProducer<byte[], byte[]> mockProducer;
    @Mock SinkTaskContext mockContext;
    @Mock Future<RecordMetadata> mockFuture;

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

    @Test
    void putWithEmptyCollectionDoesNotCallSend() {
        task.put(Collections.emptyList());
        verifyNoInteractions(mockProducer);
    }

    @Test
    void putSendsOneRecordPerSinkRecord() throws Exception {
        when(mockProducer.send(any())).thenReturn(mockFuture);
        when(mockFuture.get()).thenReturn(null);

        List<SinkRecord> records = Arrays.asList(
            makeSinkRecord("key1".getBytes(), "value1".getBytes()),
            makeSinkRecord("key2".getBytes(), "value2".getBytes()),
            makeSinkRecord("key3".getBytes(), "value3".getBytes())
        );

        task.put(records);

        verify(mockProducer, times(3)).send(any(ProducerRecord.class));
    }

    @Test
    void putSendsToCorrectHermesTopic() throws Exception {
        when(mockProducer.send(any())).thenReturn(mockFuture);
        when(mockFuture.get()).thenReturn(null);
        task.put(List.of(makeSinkRecord("k".getBytes(), "v".getBytes())));

        verify(mockProducer).send(recordCaptor.capture());
        assertEquals(HERMES_TOPIC, recordCaptor.getValue().topic());
    }

    @Test
    void putPreservesKeyAndValue() throws Exception {
        when(mockProducer.send(any())).thenReturn(mockFuture);
        when(mockFuture.get()).thenReturn(null);
        byte[] key = "my-key".getBytes();
        byte[] value = "my-value".getBytes();

        task.put(List.of(makeSinkRecord(key, value)));

        verify(mockProducer).send(recordCaptor.capture());
        assertArrayEquals(key, recordCaptor.getValue().key());
        assertArrayEquals(value, recordCaptor.getValue().value());
    }

    @Test
    void putPreservesHeaders() throws Exception {
        when(mockProducer.send(any())).thenReturn(mockFuture);
        when(mockFuture.get()).thenReturn(null);
        ConnectHeaders headers = new ConnectHeaders();
        headers.addBytes("ce-specversion", "1.0".getBytes());
        headers.addBytes("ce-type", "com.sn.incident.created".getBytes());

        SinkRecord record = new SinkRecord(
            "source-topic", 0, Schema.BYTES_SCHEMA, "k".getBytes(),
            Schema.BYTES_SCHEMA, "v".getBytes(), 0L, 1000L,
            org.apache.kafka.common.record.TimestampType.CREATE_TIME, headers);

        task.put(List.of(record));

        verify(mockProducer).send(recordCaptor.capture());
        org.apache.kafka.common.header.Headers sentHeaders = recordCaptor.getValue().headers();
        assertNotNull(sentHeaders.lastHeader("ce-specversion"));
        assertArrayEquals("1.0".getBytes(), sentHeaders.lastHeader("ce-specversion").value());
        assertNotNull(sentHeaders.lastHeader("ce-type"));
    }

    @Test
    void putWithNullKeyAndValueDoesNotThrow() throws Exception {
        when(mockProducer.send(any())).thenReturn(mockFuture);
        when(mockFuture.get()).thenReturn(null);
        task.put(List.of(makeSinkRecord(null, null)));
        verify(mockProducer, times(1)).send(any());
    }

    @Test
    void putPartitionIsNullForHashByKeyBehavior() throws Exception {
        when(mockProducer.send(any())).thenReturn(mockFuture);
        when(mockFuture.get()).thenReturn(null);
        task.put(List.of(makeSinkRecord("k".getBytes(), "v".getBytes())));
        verify(mockProducer).send(recordCaptor.capture());
        assertNull(recordCaptor.getValue().partition(),
            "Partition must be null to let the partitioner do hash-by-key");
    }

    @Test
    void putThrowsConnectExceptionOnSendFailure() throws Exception {
        when(mockProducer.send(any())).thenReturn(mockFuture);
        when(mockFuture.get()).thenThrow(new ExecutionException("broker error", new RuntimeException("send failed")));

        assertThrows(ConnectException.class,
            () -> task.put(List.of(makeSinkRecord("k".getBytes(), "v".getBytes()))));
    }

    @Test
    void flushCallsProducerFlush() {
        task.flush(Collections.emptyMap());
        verify(mockProducer).flush();
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
