package com.servicenow.kafka.connect.hermes;

import com.servicenow.kafka.connect.hermes.ssl.InMemorySslEngineFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class HermesSinkConnector extends SinkConnector {

    private static final Logger log = LoggerFactory.getLogger(HermesSinkConnector.class);

    static final String VERSION = Version.get();

    private Map<String, String> props;

    @Override
    public String version() {
        return VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting HermesSinkConnector");
        HermesConnectorConfig config = new HermesConnectorConfig(props);
        validateTopicExists(config);
        this.props = Collections.unmodifiableMap(new HashMap<>(props));
        log.info("HermesSinkConnector started — Hermes instance: {}, target topic: {}",
            config.getInstanceName(), config.getHermesTopic());
    }

    @Override
    public Class<? extends Task> taskClass() {
        return HermesSinkTask.class;
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        Config result = super.validate(connectorConfigs);
        HermesConnectorConfig.addSslValidationErrors(connectorConfigs, result);
        return result;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // Each task gets a copy of the connector config with its own task.id injected
        // so JMX MBean ObjectNames are unique per task.
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> taskConfig = new HashMap<>(props);
            taskConfig.put("task.id", String.valueOf(i));
            configs.add(Collections.unmodifiableMap(taskConfig));
        }
        return configs;
    }

    @Override
    public void stop() {
        log.info("HermesSinkConnector stopped");
    }

    @Override
    public ConfigDef config() {
        return HermesConnectorConfig.CONFIG_DEF;
    }

    // ---- Topic validation ----

    void validateTopicExists(HermesConnectorConfig config) {
        String hermesTopic = config.getHermesTopic();
        String bootstrap = config.getSinkBootstrapOverride().isEmpty()
            ? HermesBootstrapBuilder.buildSinkBootstrap(config.getInstanceName())
            : config.getSinkBootstrapOverride();
        log.info("Validating Hermes topic '{}' exists at {}", hermesTopic, bootstrap);

        Map<String, Object> adminProps = buildAdminProperties(config, bootstrap);
        try (AdminClient admin = createAdminClient(adminProps)) {
            Set<String> topics = admin.listTopics().names().get(10, TimeUnit.SECONDS);
            if (!topics.contains(hermesTopic)) {
                throw new ConnectException(
                    "Hermes topic '" + hermesTopic + "' does not exist. " +
                    "Create the topic via the Hermes Messaging Service (All > Hermes Messaging Service > Topics) " +
                    "before starting this connector. Available topics: " + topics.size() + " found.");
            }
            log.info("Confirmed Hermes topic '{}' exists", hermesTopic);
        } catch (ConnectException e) {
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConnectException(
                "Interrupted while verifying Hermes topic '" + hermesTopic + "'", e);
        } catch (Exception e) {
            throw new ConnectException(
                "Failed to verify Hermes topic '" + hermesTopic + "' — could not connect to " +
                bootstrap + ". Check instance name, network access, and SSL credentials. Cause: " +
                e.getMessage(), e);
        }
    }

    static Map<String, Object> buildAdminProperties(HermesConnectorConfig config, String bootstrap) {
        Map<String, Object> props = new HashMap<>();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "8000");
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000");
        addSslProperties(props, config);
        return props;
    }

    static void addSslProperties(Map<String, Object> props, HermesConnectorConfig config) {
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
