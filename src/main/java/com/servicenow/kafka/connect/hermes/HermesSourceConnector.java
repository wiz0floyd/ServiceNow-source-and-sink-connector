package com.servicenow.kafka.connect.hermes;

import com.servicenow.kafka.connect.hermes.ssl.InMemorySslEngineFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class HermesSourceConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(HermesSourceConnector.class);

    static final String VERSION = "0.1.0-SNAPSHOT";

    private Map<String, String> props;

    @Override
    public String version() {
        return VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting HermesSourceConnector");
        HermesSourceConfig config = new HermesSourceConfig(props);
        validateClustersReachable(config);
        this.props = Collections.unmodifiableMap(new HashMap<>(props));
        log.info("HermesSourceConnector started — Hermes instance: {}, source topic: {}, destination: {}",
            config.getInstanceName(), config.getSourceTopic(), config.getConfluentTopic());
    }

    @Override
    public Class<? extends Task> taskClass() {
        return HermesSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // Dual-cluster state lives inside a single task (one embedded consumer per Hermes
        // peer cluster, with independent offset tracking). To scale horizontally, deploy
        // additional connector instances rather than increasing tasks.max.
        List<Map<String, String>> configs = new ArrayList<>(1);
        configs.add(props);
        return configs;
    }

    @Override
    public void stop() {
        log.info("HermesSourceConnector stopped");
    }

    @Override
    public ConfigDef config() {
        return HermesSourceConfig.CONFIG_DEF;
    }

    // ---- Cluster validation ----

    void validateClustersReachable(HermesSourceConfig config) {
        String sourceTopic = config.getSourceTopic();
        String bootstrap1 = config.getCluster1BootstrapOverride().isEmpty()
            ? HermesBootstrapBuilder.buildSourceCluster1Bootstrap(config.getInstanceName())
            : config.getCluster1BootstrapOverride();
        String bootstrap2 = config.getCluster2BootstrapOverride().isEmpty()
            ? HermesBootstrapBuilder.buildSourceCluster2Bootstrap(config.getInstanceName())
            : config.getCluster2BootstrapOverride();

        validateTopicOnCluster(config, bootstrap1, sourceTopic, "1");
        validateTopicOnCluster(config, bootstrap2, sourceTopic, "2");
    }

    private void validateTopicOnCluster(HermesSourceConfig config, String bootstrap, String topic, String clusterLabel) {
        log.info("Validating Hermes topic '{}' exists on source cluster {} at {}", topic, clusterLabel, bootstrap);
        Map<String, Object> adminProps = buildAdminProperties(config, bootstrap);
        try (AdminClient admin = createAdminClient(adminProps)) {
            Set<String> topics = admin.listTopics().names().get(30, TimeUnit.SECONDS);
            if (!topics.contains(topic)) {
                throw new ConnectException(
                    "Hermes topic '" + topic + "' does not exist on source cluster " + clusterLabel + ". " +
                    "Create the topic via the Hermes Messaging Service (All > Hermes Messaging Service > Topics) " +
                    "before starting this connector. Available topics on cluster " + clusterLabel + ": " +
                    topics.size() + " found.");
            }
            log.info("Confirmed Hermes topic '{}' exists on source cluster {}", topic, clusterLabel);
        } catch (ConnectException e) {
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConnectException(
                "Interrupted while verifying Hermes topic '" + topic + "' on source cluster " + clusterLabel, e);
        } catch (Exception e) {
            throw new ConnectException(
                "Failed to verify Hermes topic '" + topic + "' on source cluster " + clusterLabel +
                " — could not connect to " + bootstrap +
                ". Check instance name, network access, and SSL credentials. Cause: " + e.getMessage(), e);
        }
    }

    static Map<String, Object> buildAdminProperties(HermesSourceConfig config, String bootstrap) {
        Map<String, Object> props = new HashMap<>();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000");
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "25000");
        addSslProperties(props, config);
        return props;
    }

    static void addSslProperties(Map<String, Object> props, HermesSourceConfig config) {
        if (!config.isSslEnabled()) {
            return;
        }
        // Values pass through as Password objects so Kafka client-side config logging
        // never prints them in plaintext.
        props.put("security.protocol", "SSL");
        props.put("ssl.engine.factory.class", InMemorySslEngineFactory.class.getName());
        props.put(InMemorySslEngineFactory.KEYSTORE_B64_CONFIG, config.getKeystoreB64());
        props.put(InMemorySslEngineFactory.KEYSTORE_PASSWORD_CONFIG, config.getKeystorePassword());
        props.put(InMemorySslEngineFactory.TRUSTSTORE_B64_CONFIG, config.getTruststoreB64());
        props.put(InMemorySslEngineFactory.TRUSTSTORE_PASSWORD_CONFIG, config.getTruststorePassword());
    }

    // Package-private for testing — allows injection of mock AdminClient
    AdminClient createAdminClient(Map<String, Object> adminProps) {
        return AdminClient.create(adminProps);
    }
}
