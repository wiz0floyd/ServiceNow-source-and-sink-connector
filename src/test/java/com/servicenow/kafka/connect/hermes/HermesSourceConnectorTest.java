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
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class HermesSourceConnectorTest {

    @Mock AdminClient mockAdminClient;
    @Mock ListTopicsResult mockListTopicsResult;
    @Mock KafkaFuture<Set<String>> mockTopicsFuture;

    private HermesSourceConnector connector;
    private static final String SOURCE_TOPIC = "snc.myinstance.sn_streamconnect.test-topic";

    @BeforeEach
    void setUp() {
        connector = new HermesSourceConnector() {
            @Override
            AdminClient createAdminClient(Map<String, Object> props) {
                return mockAdminClient;
            }
        };
    }

    @Test
    void taskClassIsHermesSourceTask() {
        assertEquals(HermesSourceTask.class, connector.taskClass());
    }

    @Test
    void taskConfigsAlwaysReturnsOneConfig() throws Exception {
        setupAdminClientWithTopics(Set.of(SOURCE_TOPIC));
        connector.start(validProps());

        List<Map<String, String>> configs = connector.taskConfigs(5);
        assertEquals(1, configs.size());
    }

    @Test
    void taskConfigsWarnsWhenMaxTasksGreaterThanOne() throws Exception {
        setupAdminClientWithTopics(Set.of(SOURCE_TOPIC));
        connector.start(validProps());

        // Even with maxTasks=3, the connector must return exactly one task config
        // (warning is emitted to the log but not testable here without log capture)
        List<Map<String, String>> configs = connector.taskConfigs(3);
        assertEquals(1, configs.size());
    }

    @Test
    void taskConfigsContainsOriginalProps() throws Exception {
        setupAdminClientWithTopics(Set.of(SOURCE_TOPIC));
        Map<String, String> props = validProps();
        connector.start(props);

        Map<String, String> taskConfig = connector.taskConfigs(1).get(0);
        for (Map.Entry<String, String> entry : props.entrySet()) {
            assertEquals(entry.getValue(), taskConfig.get(entry.getKey()),
                "Task config must contain original property: " + entry.getKey());
        }
    }

    @Test
    void startThrowsConnectExceptionAndRestoresInterruptFlagOnInterruption() throws Exception {
        when(mockAdminClient.listTopics()).thenReturn(mockListTopicsResult);
        when(mockListTopicsResult.names()).thenReturn(mockTopicsFuture);
        when(mockTopicsFuture.get(anyLong(), any(TimeUnit.class)))
            .thenThrow(new InterruptedException("simulated interrupt"));

        try {
            assertThrows(ConnectException.class,
                () -> connector.start(validProps()));
            assertTrue(Thread.currentThread().isInterrupted(),
                "start() must restore the interrupt flag after catching InterruptedException");
        } finally {
            // Clear the interrupt flag so it does not bleed into subsequent tests.
            Thread.interrupted();
        }
    }

    @Test
    void stopDoesNotThrow() {
        assertDoesNotThrow(() -> connector.stop());
    }

    @Test
    void versionIsNonNull() {
        assertNotNull(connector.version());
        assertFalse(connector.version().isEmpty());
    }

    @Test
    void configReturnsNonNull() {
        ConfigDef def = new HermesSourceConnector().config();
        assertNotNull(def);
        assertTrue(def.configKeys().containsKey(HermesSourceConfig.HERMES_INSTANCE_NAME_CONFIG));
        assertTrue(def.configKeys().containsKey(HermesSourceConfig.HERMES_SOURCE_TOPIC_CONFIG));
    }

    @Test
    void buildAdminPropertiesContainsBootstrap() {
        HermesSourceConfig config = new HermesSourceConfig(validProps());
        Map<String, Object> props = HermesSourceConnector.buildAdminProperties(config, "myhost:4100");
        assertEquals("myhost:4100", props.get("bootstrap.servers"));
    }

    @Test
    void addSslPropertiesSetsSecurityProtocol() {
        HermesSourceConfig config = new HermesSourceConfig(validProps());
        Map<String, Object> props = new HashMap<>();
        HermesSourceConnector.addSslProperties(props, config);
        assertEquals("SSL", props.get("security.protocol"));
    }

    @Test
    void validateClustersReachableThrowsWhenTopicMissingOnCluster1() throws Exception {
        // First call (cluster 1) returns no matching topics
        setupAdminClientWithTopics(Set.of("some.other.topic"));

        ConnectException ex = assertThrows(ConnectException.class,
            () -> connector.start(validProps()));
        assertTrue(ex.getMessage().contains(SOURCE_TOPIC),
            "Exception must mention the missing topic name: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("cluster 1"),
            "Exception must indicate cluster 1: " + ex.getMessage());
    }

    @Test
    void validateClustersReachableThrowsWhenTopicMissingOnCluster2() throws Exception {
        // Cluster 1 has the topic, cluster 2 does not
        when(mockAdminClient.listTopics()).thenReturn(mockListTopicsResult);
        when(mockListTopicsResult.names()).thenReturn(mockTopicsFuture);
        when(mockTopicsFuture.get(anyLong(), any(TimeUnit.class)))
            .thenReturn(Set.of(SOURCE_TOPIC))      // cluster 1
            .thenReturn(Set.of("other.topic"));    // cluster 2

        ConnectException ex = assertThrows(ConnectException.class,
            () -> connector.start(validProps()));
        assertTrue(ex.getMessage().contains(SOURCE_TOPIC),
            "Exception must mention the missing topic name: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("cluster 2"),
            "Exception must indicate cluster 2: " + ex.getMessage());
    }

    @Test
    void validateClustersReachableSucceedsWhenTopicExistsBothClusters() throws Exception {
        setupAdminClientWithTopics(Set.of(SOURCE_TOPIC));
        assertDoesNotThrow(() -> connector.start(validProps()));
    }

    // ---- Helpers ----

    private Map<String, String> validProps() {
        Map<String, String> props = new HashMap<>();
        props.put(HermesSourceConfig.HERMES_INSTANCE_NAME_CONFIG, "myinstance");
        props.put(HermesSourceConfig.HERMES_SOURCE_TOPIC_CONFIG, SOURCE_TOPIC);
        props.put(HermesSourceConfig.HERMES_CONSUMER_GROUP_ID_CONFIG, "test-group");
        props.put(HermesSourceConfig.HERMES_DESTINATION_TOPIC_CONFIG, "my-cc-topic");
        props.put(HermesSourceConfig.HERMES_SSL_KEYSTORE_B64_CONFIG, "dGVzdA==");
        props.put(HermesSourceConfig.HERMES_SSL_KEYSTORE_PASSWORD_CONFIG, "pass");
        props.put(HermesSourceConfig.HERMES_SSL_TRUSTSTORE_B64_CONFIG, "dGVzdA==");
        props.put(HermesSourceConfig.HERMES_SSL_TRUSTSTORE_PASSWORD_CONFIG, "pass");
        return props;
    }

    private void setupAdminClientWithTopics(Set<String> topics) throws Exception {
        when(mockAdminClient.listTopics()).thenReturn(mockListTopicsResult);
        when(mockListTopicsResult.names()).thenReturn(mockTopicsFuture);
        when(mockTopicsFuture.get(anyLong(), any(TimeUnit.class))).thenReturn(topics);
    }
}
