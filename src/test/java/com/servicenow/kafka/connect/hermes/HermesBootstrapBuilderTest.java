package com.servicenow.kafka.connect.hermes;

import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class HermesBootstrapBuilderTest {

    private static String brokers(String instance, int start, int end) {
        return IntStream.rangeClosed(start, end)
            .mapToObj(p -> instance + ".service-now.com:" + p)
            .collect(Collectors.joining(","));
    }

    private static final String EXPECTED_SINK     = brokers("myinstance", 4000, 4050);
    private static final String EXPECTED_CLUSTER1 = brokers("myinstance", 4100, 4150);
    private static final String EXPECTED_CLUSTER2 = brokers("myinstance", 4200, 4250);

    @Test
    void bareInstanceNameProducesCorrectSinkBootstrap() {
        assertEquals(EXPECTED_SINK, HermesBootstrapBuilder.buildSinkBootstrap("myinstance"));
    }

    @Test
    void fullUrlProducesSameSinkBootstrap() {
        assertEquals(EXPECTED_SINK,
            HermesBootstrapBuilder.buildSinkBootstrap("https://myinstance.service-now.com"));
    }

    @Test
    void fullUrlWithTrailingSlash() {
        assertEquals(EXPECTED_SINK,
            HermesBootstrapBuilder.buildSinkBootstrap("https://myinstance.service-now.com/"));
    }

    @Test
    void hostWithDomainSuffix() {
        assertEquals(EXPECTED_SINK,
            HermesBootstrapBuilder.buildSinkBootstrap("myinstance.service-now.com"));
    }

    @Test
    void sourceCluster1BootstrapPorts() {
        assertEquals(EXPECTED_CLUSTER1,
            HermesBootstrapBuilder.buildSourceCluster1Bootstrap("myinstance"));
    }

    @Test
    void sourceCluster2BootstrapPorts() {
        assertEquals(EXPECTED_CLUSTER2,
            HermesBootstrapBuilder.buildSourceCluster2Bootstrap("myinstance"));
    }

    @Test
    void nullInputThrows() {
        assertThrows(IllegalArgumentException.class,
            () -> HermesBootstrapBuilder.buildSinkBootstrap(null));
    }

    @Test
    void blankInputThrows() {
        assertThrows(IllegalArgumentException.class,
            () -> HermesBootstrapBuilder.buildSinkBootstrap("   "));
    }

    @Test
    void instanceNameWithPathThrows() {
        assertThrows(IllegalArgumentException.class,
            () -> HermesBootstrapBuilder.extractInstanceName("myinstance/foo"));
    }

    @Test
    void instanceNameWithExplicitPortThrows() {
        assertThrows(IllegalArgumentException.class,
            () -> HermesBootstrapBuilder.extractInstanceName("myinstance:4000"));
    }

    @Test
    void extractInstanceNameStripsProtocol() {
        assertEquals("myinstance",
            HermesBootstrapBuilder.extractInstanceName("https://myinstance.service-now.com"));
    }

    @Test
    void extractInstanceNameStripesDomainSuffix() {
        assertEquals("myinstance",
            HermesBootstrapBuilder.extractInstanceName("myinstance.service-now.com"));
    }
}
