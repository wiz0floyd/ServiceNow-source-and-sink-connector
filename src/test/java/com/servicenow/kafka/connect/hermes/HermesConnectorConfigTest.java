package com.servicenow.kafka.connect.hermes;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HermesConnectorConfigTest {

    static Map<String, String> validProps() {
        Map<String, String> props = new HashMap<>();
        props.put(HermesConnectorConfig.HERMES_INSTANCE_NAME_CONFIG, "myinstance");
        props.put(HermesConnectorConfig.HERMES_TOPIC_CONFIG, "snc.myinstance.sn_streamconnect.test-topic");
        props.put(HermesConnectorConfig.HERMES_SSL_KEYSTORE_B64_CONFIG, "dGVzdGtleXN0b3Jl");
        props.put(HermesConnectorConfig.HERMES_SSL_KEYSTORE_PASSWORD_CONFIG, "keystorepass");
        props.put(HermesConnectorConfig.HERMES_SSL_TRUSTSTORE_B64_CONFIG, "dGVzdHRydXN0c3RvcmU=");
        props.put(HermesConnectorConfig.HERMES_SSL_TRUSTSTORE_PASSWORD_CONFIG, "truststorepass");
        return props;
    }

    @Test
    void validConfigParsesWithoutError() {
        assertDoesNotThrow(() -> new HermesConnectorConfig(validProps()));
    }

    @Test
    void getterValuesMatchInput() {
        HermesConnectorConfig config = new HermesConnectorConfig(validProps());
        assertEquals("myinstance", config.getInstanceName());
        assertEquals("snc.myinstance.sn_streamconnect.test-topic", config.getHermesTopic());
        assertEquals("all", config.getProducerAcks());
        assertEquals(Integer.MAX_VALUE, config.getProducerRetries());
    }

    @Test
    void missingInstanceNameThrows() {
        Map<String, String> props = validProps();
        props.remove(HermesConnectorConfig.HERMES_INSTANCE_NAME_CONFIG);
        ConfigException ex = assertThrows(ConfigException.class, () -> new HermesConnectorConfig(props));
        assertTrue(ex.getMessage().contains(HermesConnectorConfig.HERMES_INSTANCE_NAME_CONFIG),
            "Exception should mention the missing field name");
    }

    @Test
    void missingHermesTopicThrows() {
        Map<String, String> props = validProps();
        props.remove(HermesConnectorConfig.HERMES_TOPIC_CONFIG);
        ConfigException ex = assertThrows(ConfigException.class, () -> new HermesConnectorConfig(props));
        assertTrue(ex.getMessage().contains(HermesConnectorConfig.HERMES_TOPIC_CONFIG));
    }

    @Test
    void missingKeystoreThrows() {
        Map<String, String> props = validProps();
        props.remove(HermesConnectorConfig.HERMES_SSL_KEYSTORE_B64_CONFIG);
        assertThrows(ConfigException.class, () -> new HermesConnectorConfig(props));
    }

    @Test
    void instanceNameWithProtocolThrows() {
        Map<String, String> props = validProps();
        props.put(HermesConnectorConfig.HERMES_INSTANCE_NAME_CONFIG, "https://myinstance.service-now.com");
        ConfigException ex = assertThrows(ConfigException.class, () -> new HermesConnectorConfig(props));
        assertTrue(ex.getMessage().contains("bare instance name") || ex.getMessage().contains("://"));
    }

    @Test
    void instanceNameWithPathThrows() {
        Map<String, String> props = validProps();
        props.put(HermesConnectorConfig.HERMES_INSTANCE_NAME_CONFIG, "myinstance/path");
        assertThrows(ConfigException.class, () -> new HermesConnectorConfig(props));
    }

    @Test
    void passwordFieldsAreNotExposedInToString() {
        HermesConnectorConfig config = new HermesConnectorConfig(validProps());
        String str = config.toString();
        // AbstractConfig masks PASSWORD type values with [hidden]
        assertFalse(str.contains("keystorepass"), "Keystore password must be masked in toString()");
        assertFalse(str.contains("truststorepass"), "Truststore password must be masked in toString()");
    }

    @Test
    void sensitivePropertiesListMatchesAllPasswordFields() {
        // SENSITIVE_PROPERTIES drives the --sensitive-properties value at upload time.
        // Every Type.PASSWORD config defined above must appear in this list.
        assertEquals(4, HermesConnectorConfig.SENSITIVE_PROPERTIES.size());
        assertTrue(HermesConnectorConfig.SENSITIVE_PROPERTIES.contains(
            HermesConnectorConfig.HERMES_SSL_KEYSTORE_B64_CONFIG));
        assertTrue(HermesConnectorConfig.SENSITIVE_PROPERTIES.contains(
            HermesConnectorConfig.HERMES_SSL_KEYSTORE_PASSWORD_CONFIG));
        assertTrue(HermesConnectorConfig.SENSITIVE_PROPERTIES.contains(
            HermesConnectorConfig.HERMES_SSL_TRUSTSTORE_B64_CONFIG));
        assertTrue(HermesConnectorConfig.SENSITIVE_PROPERTIES.contains(
            HermesConnectorConfig.HERMES_SSL_TRUSTSTORE_PASSWORD_CONFIG));
    }

    @Test
    void sensitivePropertiesCsvIsCommaSeparated() {
        String csv = HermesConnectorConfig.SENSITIVE_PROPERTIES_CSV;
        assertEquals(3, csv.chars().filter(c -> c == ',').count(),
            "CSV should contain exactly 3 commas for 4 properties");
        assertFalse(csv.contains(" "), "CSV must not contain spaces (CLI flag is comma-only)");
    }

    @Test
    void passwordGettersReturnPasswordType() {
        HermesConnectorConfig config = new HermesConnectorConfig(validProps());
        // Returns Password (not raw String) so values stay masked when passed through
        // to producer/admin config maps that may be logged by Kafka clients.
        assertEquals("[hidden]", config.getKeystoreB64().toString());
        assertEquals("[hidden]", config.getKeystorePassword().toString());
        assertEquals("[hidden]", config.getTruststoreB64().toString());
        assertEquals("[hidden]", config.getTruststorePassword().toString());
        // .value() still returns the raw plaintext when needed
        assertEquals("dGVzdGtleXN0b3Jl", config.getKeystoreB64().value());
    }

    @Test
    void customAcksAndRetriesAreRespected() {
        Map<String, String> props = validProps();
        props.put(HermesConnectorConfig.HERMES_PRODUCER_ACKS_CONFIG, "1");
        props.put(HermesConnectorConfig.HERMES_PRODUCER_RETRIES_CONFIG, "5");
        HermesConnectorConfig config = new HermesConnectorConfig(props);
        assertEquals("1", config.getProducerAcks());
        assertEquals(5, config.getProducerRetries());
    }

    @Test
    void customLingerMsIsRespected() {
        Map<String, String> props = validProps();
        props.put(HermesConnectorConfig.HERMES_PRODUCER_LINGER_MS_CONFIG, "10");
        HermesConnectorConfig config = new HermesConnectorConfig(props);
        assertEquals(10, config.getProducerLingerMs());
    }

    @Test
    void defaultLingerMsIsFive() {
        HermesConnectorConfig config = new HermesConnectorConfig(validProps());
        assertEquals(5, config.getProducerLingerMs());
    }

    @Test
    void compressionTypeValidatorRejectsInvalidValue() {
        Map<String, String> props = validProps();
        props.put(HermesConnectorConfig.HERMES_PRODUCER_COMPRESSION_TYPE_CONFIG, "invalid");
        assertThrows(ConfigException.class, () -> new HermesConnectorConfig(props));
    }

    @Test
    void acksValidatorRejectsInvalidValue() {
        Map<String, String> props = validProps();
        props.put(HermesConnectorConfig.HERMES_PRODUCER_ACKS_CONFIG, "-1");
        assertThrows(ConfigException.class, () -> new HermesConnectorConfig(props));
    }
}
