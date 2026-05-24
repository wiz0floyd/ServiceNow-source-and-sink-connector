package com.servicenow.kafka.connect.hermes;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HermesBootstrapBuilderTest {

    private static final String EXPECTED_SINK =
        "myinstance.service-now.com:4000,myinstance.service-now.com:4001," +
        "myinstance.service-now.com:4002,myinstance.service-now.com:4003," +
        "myinstance.service-now.com:4004,myinstance.service-now.com:4005," +
        "myinstance.service-now.com:4006,myinstance.service-now.com:4007";

    private static final String EXPECTED_CLUSTER1 =
        "myinstance.service-now.com:4100,myinstance.service-now.com:4101," +
        "myinstance.service-now.com:4102,myinstance.service-now.com:4103," +
        "myinstance.service-now.com:4104,myinstance.service-now.com:4105," +
        "myinstance.service-now.com:4106,myinstance.service-now.com:4107";

    private static final String EXPECTED_CLUSTER2 =
        "myinstance.service-now.com:4200,myinstance.service-now.com:4201," +
        "myinstance.service-now.com:4202,myinstance.service-now.com:4203," +
        "myinstance.service-now.com:4204,myinstance.service-now.com:4205," +
        "myinstance.service-now.com:4206,myinstance.service-now.com:4207";

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
