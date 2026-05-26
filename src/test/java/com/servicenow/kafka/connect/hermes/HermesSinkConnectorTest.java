package com.servicenow.kafka.connect.hermes;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class HermesSinkConnectorTest {

    @Mock AdminClient mockAdminClient;
    @Mock ListTopicsResult mockListTopicsResult;
    @Mock KafkaFuture<Set<String>> mockTopicsFuture;

    private HermesSinkConnector connector;

    @BeforeEach
    void setUp() {
        connector = new HermesSinkConnector() {
            @Override
            AdminClient createAdminClient(Map<String, Object> props) {
                return mockAdminClient;
            }
        };
    }

    @Test
    void configReturnsNonNullConfigDef() {
        ConfigDef def = connector.config();
        assertNotNull(def);
        // Verify key configs are defined
        assertTrue(def.configKeys().containsKey(HermesConnectorConfig.HERMES_INSTANCE_NAME_CONFIG));
        assertTrue(def.configKeys().containsKey(HermesConnectorConfig.HERMES_TOPIC_CONFIG));
    }

    @Test
    void taskConfigsReturnsCorrectCount() throws Exception {
        String targetTopic = "snc.myinstance.sn_streamconnect.test-topic";
        setupAdminClientWithTopics(Set.of(targetTopic));

        connector.start(validProps(targetTopic));

        List<Map<String, String>> configs = connector.taskConfigs(3);
        assertEquals(3, configs.size());
    }

    @Test
    void taskConfigsAreIdentical() throws Exception {
        String targetTopic = "snc.myinstance.sn_streamconnect.test-topic";
        setupAdminClientWithTopics(Set.of(targetTopic));
        connector.start(validProps(targetTopic));

        List<Map<String, String>> configs = connector.taskConfigs(2);
        assertEquals(configs.get(0), configs.get(1));
    }

    @Test
    void taskClassIsHermesSinkTask() {
        assertEquals(HermesSinkTask.class, connector.taskClass());
    }

    @Test
    void startThrowsWhenTopicNotFound() throws Exception {
        String targetTopic = "snc.myinstance.sn_streamconnect.nonexistent";
        setupAdminClientWithTopics(Set.of("some.other.topic"));

        ConnectException ex = assertThrows(ConnectException.class,
            () -> connector.start(validProps(targetTopic)));

        assertTrue(ex.getMessage().contains(targetTopic),
            "Exception must mention the missing topic name: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("does not exist") || ex.getMessage().contains("not found"),
            "Exception must say topic does not exist: " + ex.getMessage());
    }

    @Test
    void startThrowsConnectExceptionOnAdminFailure() throws Exception {
        when(mockAdminClient.listTopics()).thenReturn(mockListTopicsResult);
        when(mockListTopicsResult.names()).thenReturn(mockTopicsFuture);
        when(mockTopicsFuture.get(anyLong(), any(TimeUnit.class)))
            .thenThrow(new java.util.concurrent.ExecutionException("connection refused", new RuntimeException()));

        ConnectException ex = assertThrows(ConnectException.class,
            () -> connector.start(validProps("snc.myinstance.sn_streamconnect.any")));

        assertTrue(ex.getMessage().contains("connect") || ex.getMessage().contains("verify"),
            "Exception message should indicate connection failure: " + ex.getMessage());
    }

    @Test
    void startThrowsConnectExceptionAndRestoresInterruptFlagOnInterruption() throws Exception {
        when(mockAdminClient.listTopics()).thenReturn(mockListTopicsResult);
        when(mockListTopicsResult.names()).thenReturn(mockTopicsFuture);
        when(mockTopicsFuture.get(anyLong(), any(TimeUnit.class)))
            .thenThrow(new InterruptedException("simulated interrupt"));

        try {
            assertThrows(ConnectException.class,
                () -> connector.start(validProps("snc.myinstance.sn_streamconnect.any")));
            assertTrue(Thread.currentThread().isInterrupted(),
                "start() must restore the interrupt flag after catching InterruptedException");
        } finally {
            // Clear the interrupt flag so it does not bleed into subsequent tests.
            Thread.interrupted();
        }
    }

    @Test
    void stopDoesNotThrow() throws Exception {
        String targetTopic = "snc.myinstance.sn_streamconnect.test-topic";
        setupAdminClientWithTopics(Set.of(targetTopic));
        connector.start(validProps(targetTopic));
        assertDoesNotThrow(() -> connector.stop());
    }

    @Test
    void versionIsNonNull() {
        assertNotNull(connector.version());
        assertFalse(connector.version().isEmpty());
    }

    // ---- Helpers ----

    private Map<String, String> validProps(String hermesTopic) {
        Map<String, String> props = new HashMap<>();
        props.put(HermesConnectorConfig.HERMES_INSTANCE_NAME_CONFIG, "myinstance");
        props.put(HermesConnectorConfig.HERMES_TOPIC_CONFIG, hermesTopic);
        props.put(HermesConnectorConfig.HERMES_SSL_KEYSTORE_B64_CONFIG, "dGVzdA==");
        props.put(HermesConnectorConfig.HERMES_SSL_KEYSTORE_PASSWORD_CONFIG, "pass");
        props.put(HermesConnectorConfig.HERMES_SSL_TRUSTSTORE_B64_CONFIG, "dGVzdA==");
        props.put(HermesConnectorConfig.HERMES_SSL_TRUSTSTORE_PASSWORD_CONFIG, "pass");
        return props;
    }

    private void setupAdminClientWithTopics(Set<String> topics) throws Exception {
        when(mockAdminClient.listTopics()).thenReturn(mockListTopicsResult);
        when(mockListTopicsResult.names()).thenReturn(mockTopicsFuture);
        when(mockTopicsFuture.get(anyLong(), any(TimeUnit.class))).thenReturn(topics);
    }
}
