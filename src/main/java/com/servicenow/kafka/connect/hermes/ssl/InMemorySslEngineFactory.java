package com.servicenow.kafka.connect.hermes.ssl;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.SslEngineFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.ByteArrayInputStream;
import java.security.KeyStore;
import java.util.*;

/**
 * KIP-519 custom SslEngineFactory that loads keystore and truststore from base64-encoded
 * PKCS12 strings supplied as connector config properties.
 *
 * Required because Confluent Cloud cannot mount files into the Connect worker — keystores
 * must be injected as config strings (base64 PKCS12) rather than file paths.
 *
 * Set ssl.engine.factory.class=com.servicenow.kafka.connect.hermes.ssl.InMemorySslEngineFactory
 * in the producer/admin client properties passed to the Hermes producers.
 */
public class InMemorySslEngineFactory implements SslEngineFactory {

    private static final Logger log = LoggerFactory.getLogger(InMemorySslEngineFactory.class);

    public static final String KEYSTORE_B64_CONFIG = "hermes.ssl.keystore.b64";
    public static final String KEYSTORE_PASSWORD_CONFIG = "hermes.ssl.keystore.password";
    public static final String TRUSTSTORE_B64_CONFIG = "hermes.ssl.truststore.b64";
    public static final String TRUSTSTORE_PASSWORD_CONFIG = "hermes.ssl.truststore.password";
    public static final String CERT_EXPIRY_WARN_DAYS_CONFIG = "hermes.ssl.cert.expiry.warn.days";

    private static final Set<String> RECONFIGURABLE_CONFIGS = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList(
            KEYSTORE_B64_CONFIG,
            KEYSTORE_PASSWORD_CONFIG,
            TRUSTSTORE_B64_CONFIG,
            TRUSTSTORE_PASSWORD_CONFIG,
            CERT_EXPIRY_WARN_DAYS_CONFIG
        ))
    );

    private SSLContext sslContext;
    private KeyStore keystore;
    private KeyStore truststore;
    private Map<String, Object> currentConfigs;

    @Override
    public void configure(Map<String, ?> configs) {
        currentConfigs = new HashMap<>(configs);

        String keystoreB64 = getRequiredString(configs, KEYSTORE_B64_CONFIG);
        String keystorePassword = getRequiredString(configs, KEYSTORE_PASSWORD_CONFIG);
        String truststoreB64 = getRequiredString(configs, TRUSTSTORE_B64_CONFIG);
        String truststorePassword = getRequiredString(configs, TRUSTSTORE_PASSWORD_CONFIG);

        try {
            keystore = loadKeyStore(keystoreB64, keystorePassword, "keystore");
            truststore = loadKeyStore(truststoreB64, truststorePassword, "truststore");

            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(keystore, keystorePassword.toCharArray());

            // Check for upcoming cert expiration; threshold is configurable via CERT_EXPIRY_WARN_DAYS_CONFIG
            Object rawWarnDays = configs.get(CERT_EXPIRY_WARN_DAYS_CONFIG);
            int warnDays;
            if (rawWarnDays == null) {
                warnDays = 30;
            } else if (rawWarnDays instanceof Number) {
                warnDays = ((Number) rawWarnDays).intValue();
            } else {
                try {
                    warnDays = Integer.parseInt(rawWarnDays.toString());
                } catch (NumberFormatException e) {
                    throw new ConfigException(CERT_EXPIRY_WARN_DAYS_CONFIG, rawWarnDays.toString(),
                        "Value must be a positive integer.");
                }
            }
            Enumeration<String> aliases = keystore.aliases();
            while (aliases.hasMoreElements()) {
                java.security.cert.Certificate cert = keystore.getCertificate(aliases.nextElement());
                if (cert instanceof java.security.cert.X509Certificate) {
                    java.security.cert.X509Certificate x509 = (java.security.cert.X509Certificate) cert;
                    long daysLeft = java.time.temporal.ChronoUnit.DAYS.between(
                            java.time.Instant.now(), x509.getNotAfter().toInstant());
                    if (daysLeft <= warnDays) {
                        log.warn("InMemorySslEngineFactory: mTLS certificate expires in {} day(s) on {}. "
                                + "Rotate the certificate via the Hermes Instance PKI Certificate Generator.",
                                daysLeft, x509.getNotAfter());
                    }
                }
            }

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(truststore);

            sslContext = SSLContext.getInstance("TLS");
            sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

            log.info("InMemorySslEngineFactory: SSL context initialized successfully");
        } catch (ConfigException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigException("Failed to initialize SSL context from in-memory keystores: " + e.getMessage(), e);
        }
    }

    @Override
    public SSLEngine createClientSslEngine(String peerHost, int peerPort, String endpointIdentification) {
        SSLEngine engine = sslContext.createSSLEngine(peerHost, peerPort);
        engine.setUseClientMode(true);
        engine.setEnabledProtocols(new String[]{"TLSv1.3", "TLSv1.2"});

        SSLParameters params = engine.getSSLParameters();
        // Disable hostname verification: Hermes uses instance-signed certs (not a public CA),
        // so hostname verification against the SAN would fail. mTLS provides mutual auth instead.
        params.setEndpointIdentificationAlgorithm("");
        engine.setSSLParameters(params);

        return engine;
    }

    @Override
    public SSLEngine createServerSslEngine(String peerHost, int peerPort) {
        throw new UnsupportedOperationException(
            "InMemorySslEngineFactory is for client (producer/consumer) use only — server mode is not supported.");
    }

    @Override
    public boolean shouldBeRebuilt(Map<String, Object> nextConfigs) {
        for (String key : RECONFIGURABLE_CONFIGS) {
            Object current = currentConfigs.get(key);
            Object next = nextConfigs.get(key);
            if (!Objects.equals(current, next)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return RECONFIGURABLE_CONFIGS;
    }

    @Override
    public KeyStore keystore() {
        return keystore;
    }

    @Override
    public KeyStore truststore() {
        return truststore;
    }

    @Override
    public void close() {
        sslContext = null;
        keystore = null;
        truststore = null;
        if (currentConfigs != null) {
            currentConfigs.clear();
        }
    }

    // ---- Helpers ----

    private static KeyStore loadKeyStore(String b64Content, String password, String label) {
        byte[] bytes;
        try {
            bytes = Base64.getDecoder().decode(b64Content.trim());
        } catch (IllegalArgumentException e) {
            throw new ConfigException(
                "hermes.ssl." + label + ".b64",
                "[redacted]",
                "Value is not valid base64: " + e.getMessage());
        }

        try {
            KeyStore ks = KeyStore.getInstance("PKCS12");
            ks.load(new ByteArrayInputStream(bytes), password.toCharArray());
            return ks;
        } catch (Exception e) {
            throw new ConfigException(
                "hermes.ssl." + label + ".b64",
                "[redacted]",
                "Failed to load PKCS12 " + label + " — check content and password. Cause: " + e.getMessage());
        }
    }

    private static String getRequiredString(Map<String, ?> configs, String key) {
        Object val = configs.get(key);
        if (val == null) {
            throw new ConfigException(key, null, "Required config is missing.");
        }
        // Kafka may pass Password objects; unwrap if needed
        if (val instanceof org.apache.kafka.common.config.types.Password) {
            return ((org.apache.kafka.common.config.types.Password) val).value();
        }
        return val.toString();
    }
}
