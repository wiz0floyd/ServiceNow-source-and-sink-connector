package com.servicenow.kafka.connect.hermes;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HermesSourceConfigTest {

    static Map<String, String> validProps() {
        Map<String, String> props = new HashMap<>();
        props.put(HermesSourceConfig.HERMES_INSTANCE_NAME_CONFIG, "myinstance");
        props.put(HermesSourceConfig.HERMES_SOURCE_TOPIC_CONFIG, "snc.myinstance.sn_streamconnect.test-topic");
        props.put(HermesSourceConfig.HERMES_CONSUMER_GROUP_ID_CONFIG, "hermes-source-connector-grp");
        props.put(HermesSourceConfig.CONFLUENT_TOPIC_CONFIG, "cc.dest-topic");
        props.put(HermesSourceConfig.HERMES_SSL_KEYSTORE_B64_CONFIG, "dGVzdGtleXN0b3Jl");
        props.put(HermesSourceConfig.HERMES_SSL_KEYSTORE_PASSWORD_CONFIG, "keystorepass");
        props.put(HermesSourceConfig.HERMES_SSL_TRUSTSTORE_B64_CONFIG, "dGVzdHRydXN0c3RvcmU=");
        props.put(HermesSourceConfig.HERMES_SSL_TRUSTSTORE_PASSWORD_CONFIG, "truststorepass");
        return props;
    }

    @Test
    void validConfigParsesWithoutError() {
        assertDoesNotThrow(() -> new HermesSourceConfig(validProps()));
    }

    @Test
    void getterValuesMatchInput() {
        HermesSourceConfig config = new HermesSourceConfig(validProps());
        assertEquals("myinstance", config.getInstanceName());
        assertEquals("snc.myinstance.sn_streamconnect.test-topic", config.getSourceTopic());
        assertEquals("hermes-source-connector-grp", config.getGroupId());
        assertEquals("cc.dest-topic", config.getConfluentTopic());
    }

    @Test
    void missingInstanceNameThrows() {
        Map<String, String> props = validProps();
        props.remove(HermesSourceConfig.HERMES_INSTANCE_NAME_CONFIG);
        ConfigException ex = assertThrows(ConfigException.class, () -> new HermesSourceConfig(props));
        assertTrue(ex.getMessage().contains(HermesSourceConfig.HERMES_INSTANCE_NAME_CONFIG));
    }

    @Test
    void missingSourceTopicThrows() {
        Map<String, String> props = validProps();
        props.remove(HermesSourceConfig.HERMES_SOURCE_TOPIC_CONFIG);
        ConfigException ex = assertThrows(ConfigException.class, () -> new HermesSourceConfig(props));
        assertTrue(ex.getMessage().contains(HermesSourceConfig.HERMES_SOURCE_TOPIC_CONFIG));
    }

    @Test
    void missingGroupIdThrows() {
        Map<String, String> props = validProps();
        props.remove(HermesSourceConfig.HERMES_CONSUMER_GROUP_ID_CONFIG);
        ConfigException ex = assertThrows(ConfigException.class, () -> new HermesSourceConfig(props));
        assertTrue(ex.getMessage().contains(HermesSourceConfig.HERMES_CONSUMER_GROUP_ID_CONFIG));
    }

    @Test
    void missingConfluentTopicThrows() {
        Map<String, String> props = validProps();
        props.remove(HermesSourceConfig.CONFLUENT_TOPIC_CONFIG);
        ConfigException ex = assertThrows(ConfigException.class, () -> new HermesSourceConfig(props));
        assertTrue(ex.getMessage().contains(HermesSourceConfig.CONFLUENT_TOPIC_CONFIG));
    }

    @Test
    void missingKeystoreThrowsWhenSslEnabled() {
        Map<String, String> props = validProps();
        props.remove(HermesSourceConfig.HERMES_SSL_KEYSTORE_B64_CONFIG);
        ConfigException ex = assertThrows(ConfigException.class, () -> new HermesSourceConfig(props));
        assertTrue(ex.getMessage().contains(HermesSourceConfig.HERMES_SSL_KEYSTORE_B64_CONFIG));
    }

    @Test
    void missingKeystorePasswordThrowsWhenSslEnabled() {
        Map<String, String> props = validProps();
        props.remove(HermesSourceConfig.HERMES_SSL_KEYSTORE_PASSWORD_CONFIG);
        ConfigException ex = assertThrows(ConfigException.class, () -> new HermesSourceConfig(props));
        assertTrue(ex.getMessage().contains(HermesSourceConfig.HERMES_SSL_KEYSTORE_PASSWORD_CONFIG));
    }

    @Test
    void missingTruststoreThrowsWhenSslEnabled() {
        Map<String, String> props = validProps();
        props.remove(HermesSourceConfig.HERMES_SSL_TRUSTSTORE_B64_CONFIG);
        ConfigException ex = assertThrows(ConfigException.class, () -> new HermesSourceConfig(props));
        assertTrue(ex.getMessage().contains(HermesSourceConfig.HERMES_SSL_TRUSTSTORE_B64_CONFIG));
    }

    @Test
    void missingTruststorePasswordThrowsWhenSslEnabled() {
        Map<String, String> props = validProps();
        props.remove(HermesSourceConfig.HERMES_SSL_TRUSTSTORE_PASSWORD_CONFIG);
        ConfigException ex = assertThrows(ConfigException.class, () -> new HermesSourceConfig(props));
        assertTrue(ex.getMessage().contains(HermesSourceConfig.HERMES_SSL_TRUSTSTORE_PASSWORD_CONFIG));
    }

    @Test
    void sslFieldsNotRequiredWhenSslDisabled() {
        Map<String, String> props = new HashMap<>();
        props.put(HermesSourceConfig.HERMES_INSTANCE_NAME_CONFIG, "myinstance");
        props.put(HermesSourceConfig.HERMES_SOURCE_TOPIC_CONFIG, "snc.myinstance.sn_streamconnect.test-topic");
        props.put(HermesSourceConfig.HERMES_CONSUMER_GROUP_ID_CONFIG, "hermes-source-connector-grp");
        props.put(HermesSourceConfig.CONFLUENT_TOPIC_CONFIG, "cc.dest-topic");
        props.put(HermesSourceConfig.HERMES_SSL_ENABLED_CONFIG, "false");
        assertDoesNotThrow(() -> new HermesSourceConfig(props));
    }

    @Test
    void defaultMaxPollRecordsIs500() {
        HermesSourceConfig config = new HermesSourceConfig(validProps());
        assertEquals(500, config.getMaxPollRecords());
    }

    @Test
    void customMaxPollRecordsIsRespected() {
        Map<String, String> props = validProps();
        props.put(HermesSourceConfig.HERMES_CONSUMER_MAX_POLL_RECORDS_CONFIG, "100");
        HermesSourceConfig config = new HermesSourceConfig(props);
        assertEquals(100, config.getMaxPollRecords());
    }

    @Test
    void maxPollRecordsBelowMinThrows() {
        Map<String, String> props = validProps();
        props.put(HermesSourceConfig.HERMES_CONSUMER_MAX_POLL_RECORDS_CONFIG, "0");
        assertThrows(ConfigException.class, () -> new HermesSourceConfig(props));
    }

    @Test
    void instanceNameWithProtocolThrows() {
        Map<String, String> props = validProps();
        props.put(HermesSourceConfig.HERMES_INSTANCE_NAME_CONFIG, "https://myinstance.service-now.com");
        ConfigException ex = assertThrows(ConfigException.class, () -> new HermesSourceConfig(props));
        assertTrue(ex.getMessage().contains("bare instance name") || ex.getMessage().contains("://"));
    }

    @Test
    void instanceNameWithPathThrows() {
        Map<String, String> props = validProps();
        props.put(HermesSourceConfig.HERMES_INSTANCE_NAME_CONFIG, "myinstance/path");
        assertThrows(ConfigException.class, () -> new HermesSourceConfig(props));
    }

    @Test
    void passwordFieldsAreNotExposedInToString() {
        HermesSourceConfig config = new HermesSourceConfig(validProps());
        String str = config.toString();
        assertFalse(str.contains("keystorepass"), "Keystore password must be masked in toString()");
        assertFalse(str.contains("truststorepass"), "Truststore password must be masked in toString()");
    }

    @Test
    void passwordGettersReturnPasswordType() {
        HermesSourceConfig config = new HermesSourceConfig(validProps());
        assertEquals("[hidden]", config.getKeystoreB64().toString());
        assertEquals("[hidden]", config.getKeystorePassword().toString());
        assertEquals("[hidden]", config.getTruststoreB64().toString());
        assertEquals("[hidden]", config.getTruststorePassword().toString());
        assertEquals("dGVzdGtleXN0b3Jl", config.getKeystoreB64().value());
    }

    @Test
    void sensitivePropertiesContainsAllFourSslKeys() {
        assertEquals(4, HermesSourceConfig.SENSITIVE_PROPERTIES.size());
        assertTrue(HermesSourceConfig.SENSITIVE_PROPERTIES.contains(
            HermesSourceConfig.HERMES_SSL_KEYSTORE_B64_CONFIG));
        assertTrue(HermesSourceConfig.SENSITIVE_PROPERTIES.contains(
            HermesSourceConfig.HERMES_SSL_KEYSTORE_PASSWORD_CONFIG));
        assertTrue(HermesSourceConfig.SENSITIVE_PROPERTIES.contains(
            HermesSourceConfig.HERMES_SSL_TRUSTSTORE_B64_CONFIG));
        assertTrue(HermesSourceConfig.SENSITIVE_PROPERTIES.contains(
            HermesSourceConfig.HERMES_SSL_TRUSTSTORE_PASSWORD_CONFIG));
    }

    @Test
    void sensitivePropertiesCsvContainsAllFourSslKeys() {
        String csv = HermesSourceConfig.SENSITIVE_PROPERTIES_CSV;
        assertTrue(csv.contains(HermesSourceConfig.HERMES_SSL_KEYSTORE_B64_CONFIG));
        assertTrue(csv.contains(HermesSourceConfig.HERMES_SSL_KEYSTORE_PASSWORD_CONFIG));
        assertTrue(csv.contains(HermesSourceConfig.HERMES_SSL_TRUSTSTORE_B64_CONFIG));
        assertTrue(csv.contains(HermesSourceConfig.HERMES_SSL_TRUSTSTORE_PASSWORD_CONFIG));
        assertEquals(3, csv.chars().filter(c -> c == ',').count(),
            "CSV should contain exactly 3 commas for 4 properties");
        assertFalse(csv.contains(" "), "CSV must not contain spaces (CLI flag is comma-only)");
    }
}
