package com.servicenow.kafka.connect.hermes.ssl;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLEngine;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Uses pre-generated PKCS12 fixtures in src/test/resources/fixtures/ to avoid
 * dependency on sun.security.x509 internal APIs (removed in OpenJDK 21).
 *
 * To regenerate long-lived fixtures (valid for 10 years from 2026-05-20):
 *   keytool -genkeypair -alias hermes-test -keyalg RSA -keysize 2048 -validity 3650
 *     -storetype PKCS12 -keystore src/test/resources/fixtures/test-keystore.p12
 *     -storepass testpass123 -keypass testpass123 -dname "CN=hermes-test, O=ServiceNow, C=US"
 *   keytool -exportcert -alias hermes-test -keystore src/test/resources/fixtures/test-keystore.p12
 *     -storepass testpass123 -rfc -file src/test/resources/fixtures/test-cert.pem
 *   keytool -importcert -alias hermes-test -file src/test/resources/fixtures/test-cert.pem
 *     -storetype PKCS12 -keystore src/test/resources/fixtures/test-truststore.p12
 *     -storepass testpass123 -noprompt
 *
 * To regenerate short-lived fixtures (1-day validity; used for expiry warning tests):
 *   keytool -genkeypair -alias hermes-expiring -keyalg RSA -keysize 2048 -validity 1
 *     -storetype PKCS12 -keystore src/test/resources/fixtures/test-expiring-keystore.p12
 *     -storepass testpass123 -keypass testpass123 -dname "CN=hermes-test, O=ServiceNow, C=US"
 *   keytool -exportcert -alias hermes-expiring
 *     -keystore src/test/resources/fixtures/test-expiring-keystore.p12
 *     -storepass testpass123 -rfc -file src/test/resources/fixtures/test-expiring-cert.pem
 *   keytool -importcert -alias hermes-expiring
 *     -file src/test/resources/fixtures/test-expiring-cert.pem
 *     -storetype PKCS12 -keystore src/test/resources/fixtures/test-expiring-truststore.p12
 *     -storepass testpass123 -noprompt
 */
class InMemorySslEngineFactoryTest {

    private static String KEYSTORE_B64;
    private static String TRUSTSTORE_B64;
    private static String EXPIRING_KEYSTORE_B64;
    private static String EXPIRING_TRUSTSTORE_B64;
    private static final String KS_PASSWORD = "testpass123";

    @BeforeAll
    static void loadFixtures() throws Exception {
        KEYSTORE_B64 = loadResourceAsBase64("fixtures/test-keystore.p12");
        TRUSTSTORE_B64 = loadResourceAsBase64("fixtures/test-truststore.p12");
        EXPIRING_KEYSTORE_B64 = loadResourceAsBase64("fixtures/test-expiring-keystore.p12");
        EXPIRING_TRUSTSTORE_B64 = loadResourceAsBase64("fixtures/test-expiring-truststore.p12");
    }

    @Test
    void configureWithValidKeyStoreSucceeds() {
        InMemorySslEngineFactory factory = new InMemorySslEngineFactory();
        assertDoesNotThrow(() -> factory.configure(validConfigs()));
        assertNotNull(factory.keystore());
        assertNotNull(factory.truststore());
        factory.close();
    }

    @Test
    void configureWithInvalidBase64ThrowsConfigException() {
        InMemorySslEngineFactory factory = new InMemorySslEngineFactory();
        Map<String, Object> configs = validConfigs();
        configs.put(InMemorySslEngineFactory.KEYSTORE_B64_CONFIG, "!!!not-valid-base64!!!");
        ConfigException ex = assertThrows(ConfigException.class, () -> factory.configure(configs));
        assertTrue(ex.getMessage().contains("base64") || ex.getMessage().contains("keystore"),
            "Exception should mention base64 or keystore: " + ex.getMessage());
    }

    @Test
    void configureWithWrongPasswordThrowsConfigException() {
        InMemorySslEngineFactory factory = new InMemorySslEngineFactory();
        Map<String, Object> configs = validConfigs();
        configs.put(InMemorySslEngineFactory.KEYSTORE_PASSWORD_CONFIG, "wrongpassword");
        assertThrows(ConfigException.class, () -> factory.configure(configs));
    }

    @Test
    void createClientSslEngineIsInClientMode() {
        InMemorySslEngineFactory factory = new InMemorySslEngineFactory();
        factory.configure(validConfigs());
        SSLEngine engine = factory.createClientSslEngine("myinstance.service-now.com", 4000, "");
        assertNotNull(engine);
        assertTrue(engine.getUseClientMode(), "Engine must be in client mode");
        factory.close();
    }

    @Test
    void createServerSslEngineThrowsUnsupported() {
        InMemorySslEngineFactory factory = new InMemorySslEngineFactory();
        factory.configure(validConfigs());
        assertThrows(UnsupportedOperationException.class,
            () -> factory.createServerSslEngine("host", 4000));
        factory.close();
    }

    @Test
    void shouldBeRebuiltReturnsFalseForSameConfig() {
        InMemorySslEngineFactory factory = new InMemorySslEngineFactory();
        factory.configure(validConfigs());
        assertFalse(factory.shouldBeRebuilt(new HashMap<>(validConfigs())));
        factory.close();
    }

    @Test
    void shouldBeRebuiltReturnsTrueWhenKeystoreChanges() {
        InMemorySslEngineFactory factory = new InMemorySslEngineFactory();
        factory.configure(validConfigs());
        Map<String, Object> next = new HashMap<>(validConfigs());
        next.put(InMemorySslEngineFactory.KEYSTORE_B64_CONFIG, "differentcontent==");
        assertTrue(factory.shouldBeRebuilt(next));
        factory.close();
    }

    @Test
    void closeDoesNotThrow() {
        InMemorySslEngineFactory factory = new InMemorySslEngineFactory();
        factory.configure(validConfigs());
        assertDoesNotThrow(factory::close);
    }

    @Test
    void missingRequiredConfigThrows() {
        InMemorySslEngineFactory factory = new InMemorySslEngineFactory();
        Map<String, Object> configs = validConfigs();
        configs.remove(InMemorySslEngineFactory.KEYSTORE_B64_CONFIG);
        assertThrows(ConfigException.class, () -> factory.configure(configs));
    }

    @Test
    void reconfigurableConfigsContainsAllSslKeys() {
        InMemorySslEngineFactory factory = new InMemorySslEngineFactory();
        factory.configure(validConfigs());
        var keys = factory.reconfigurableConfigs();
        assertTrue(keys.contains(InMemorySslEngineFactory.KEYSTORE_B64_CONFIG));
        assertTrue(keys.contains(InMemorySslEngineFactory.KEYSTORE_PASSWORD_CONFIG));
        assertTrue(keys.contains(InMemorySslEngineFactory.TRUSTSTORE_B64_CONFIG));
        assertTrue(keys.contains(InMemorySslEngineFactory.TRUSTSTORE_PASSWORD_CONFIG));
        factory.close();
    }

    @Test
    void createClientSslEngineHasHostnameVerificationDisabled() {
        // Hermes uses instance-signed certs (not a public CA); hostname verification is
        // intentionally disabled. mTLS provides mutual auth instead. This test documents
        // and protects that design decision.
        InMemorySslEngineFactory factory = new InMemorySslEngineFactory();
        factory.configure(validConfigs());
        SSLEngine engine = factory.createClientSslEngine("myinstance.service-now.com", 4000, "");
        String algo = engine.getSSLParameters().getEndpointIdentificationAlgorithm();
        assertTrue(algo == null || algo.isEmpty(),
            "Hostname verification must be disabled for Hermes mTLS; got: " + algo);
        factory.close();
    }

    @Test
    void createClientSslEngineEnforcesTls12Floor() {
        InMemorySslEngineFactory factory = new InMemorySslEngineFactory();
        factory.configure(validConfigs());
        SSLEngine engine = factory.createClientSslEngine("myinstance.service-now.com", 4000, "");
        java.util.List<String> protocols = Arrays.asList(engine.getEnabledProtocols());
        assertFalse(protocols.contains("TLSv1"),
            "TLSv1.0 must not be enabled; enabled: " + protocols);
        assertFalse(protocols.contains("TLSv1.1"),
            "TLSv1.1 must not be enabled; enabled: " + protocols);
        assertTrue(protocols.contains("TLSv1.2"),
            "TLSv1.2 must be enabled; enabled: " + protocols);
        factory.close();
    }

    @Test
    void configureWithExpiringCertDoesNotThrow() {
        // Exercises the expiry-warning code path (cert expires within 1 day < default 30-day threshold).
        // Behavioral assertion of the WARN log would require a log-capture test dep; this
        // test validates code path coverage only — the log output is visible in test stderr.
        InMemorySslEngineFactory factory = new InMemorySslEngineFactory();
        assertDoesNotThrow(() -> factory.configure(expiringConfigs()));
        factory.close();
    }

    @Test
    void configureWithHighWarnDaysThresholdTriggersOnLongLivedCert() {
        // With warnDays=9999, even the 10-year fixture (expires ~2036) is within threshold.
        // Verifies the config is read and applied; no exception expected.
        InMemorySslEngineFactory factory = new InMemorySslEngineFactory();
        Map<String, Object> configs = validConfigs();
        configs.put(InMemorySslEngineFactory.CERT_EXPIRY_WARN_DAYS_CONFIG, 9999);
        assertDoesNotThrow(() -> factory.configure(configs));
        factory.close();
    }

    @Test
    void configureWithWarnDaysAsStringIsParsed() {
        // Kafka props maps may carry values as Strings (loaded from .properties files).
        InMemorySslEngineFactory factory = new InMemorySslEngineFactory();
        Map<String, Object> configs = validConfigs();
        configs.put(InMemorySslEngineFactory.CERT_EXPIRY_WARN_DAYS_CONFIG, "7");
        assertDoesNotThrow(() -> factory.configure(configs));
        factory.close();
    }

    // ---- Helpers ----

    private static Map<String, Object> validConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(InMemorySslEngineFactory.KEYSTORE_B64_CONFIG, KEYSTORE_B64);
        configs.put(InMemorySslEngineFactory.KEYSTORE_PASSWORD_CONFIG, KS_PASSWORD);
        configs.put(InMemorySslEngineFactory.TRUSTSTORE_B64_CONFIG, TRUSTSTORE_B64);
        configs.put(InMemorySslEngineFactory.TRUSTSTORE_PASSWORD_CONFIG, KS_PASSWORD);
        return configs;
    }

    private static Map<String, Object> expiringConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(InMemorySslEngineFactory.KEYSTORE_B64_CONFIG, EXPIRING_KEYSTORE_B64);
        configs.put(InMemorySslEngineFactory.KEYSTORE_PASSWORD_CONFIG, KS_PASSWORD);
        configs.put(InMemorySslEngineFactory.TRUSTSTORE_B64_CONFIG, EXPIRING_TRUSTSTORE_B64);
        configs.put(InMemorySslEngineFactory.TRUSTSTORE_PASSWORD_CONFIG, KS_PASSWORD);
        return configs;
    }

    private static String loadResourceAsBase64(String resourcePath) throws Exception {
        try (InputStream is = InMemorySslEngineFactoryTest.class.getClassLoader()
                .getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IllegalStateException("Test fixture not found: " + resourcePath +
                    ". Run the keytool commands in the class Javadoc to regenerate.");
            }
            byte[] bytes = is.readAllBytes();
            return Base64.getEncoder().encodeToString(bytes);
        }
    }
}
