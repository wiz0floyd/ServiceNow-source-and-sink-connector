package com.servicenow.kafka.connect.hermes;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Derives Hermes bootstrap server strings from a ServiceNow instance name.
 *
 * Hermes cluster port conventions (docs/hermes/create-test-topic.md):
 *   Sink  (producer → Hermes LB): full range 4000–4050
 *   Source consumer cluster 1:    full range 4100–4150
 *   Source consumer cluster 2:    full range 4200–4250
 *
 * Bootstrap strings only list the first 8 ports per range. Kafka only needs to
 * reach one broker to fetch cluster metadata; subsequent data-plane connections
 * use whatever addresses the broker returns (which may be anywhere in the full
 * range). The egress allowlist in the deploy tool must cover the full range.
 * Most Hermes clusters have 4–8 brokers, all on the first 8 ports of each range.
 */
public final class HermesBootstrapBuilder {

    private static final String HERMES_DOMAIN = ".service-now.com";
    private static final int[] SINK_PORTS = {4000, 4001, 4002, 4003, 4004, 4005, 4006, 4007};
    private static final int[] SOURCE_CLUSTER_1_PORTS = {4100, 4101, 4102, 4103, 4104, 4105, 4106, 4107};
    private static final int[] SOURCE_CLUSTER_2_PORTS = {4200, 4201, 4202, 4203, 4204, 4205, 4206, 4207};

    private HermesBootstrapBuilder() {}

    /**
     * Returns bootstrap string for the sink (producer → Hermes LB) direction.
     *
     * @param instanceNameOrUrl bare instance name ("myinstance") or full URL
     *                          ("https://myinstance.service-now.com") — both accepted
     */
    public static String buildSinkBootstrap(String instanceNameOrUrl) {
        String name = extractInstanceName(instanceNameOrUrl);
        return buildBootstrap(name, SINK_PORTS);
    }

    /**
     * Returns bootstrap string for Hermes source cluster 1 (consumer bootstrap: 4100–4107).
     */
    public static String buildSourceCluster1Bootstrap(String instanceNameOrUrl) {
        String name = extractInstanceName(instanceNameOrUrl);
        return buildBootstrap(name, SOURCE_CLUSTER_1_PORTS);
    }

    /**
     * Returns bootstrap string for Hermes source cluster 2 (consumer bootstrap: 4200–4207).
     */
    public static String buildSourceCluster2Bootstrap(String instanceNameOrUrl) {
        String name = extractInstanceName(instanceNameOrUrl);
        return buildBootstrap(name, SOURCE_CLUSTER_2_PORTS);
    }

    static String extractInstanceName(String input) {
        if (input == null || input.trim().isEmpty()) {
            throw new IllegalArgumentException("Instance name must not be null or blank.");
        }
        String s = input.trim();

        // Strip trailing slash
        if (s.endsWith("/")) {
            s = s.substring(0, s.length() - 1);
        }

        // If it looks like a URL, parse it
        if (s.contains("://")) {
            try {
                URI uri = new URI(s);
                String host = uri.getHost();
                if (host == null || host.isEmpty()) {
                    throw new IllegalArgumentException(
                        "Cannot extract host from URL: " + input);
                }
                s = host;
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(
                    "Invalid instance URL: " + input, e);
            }
        }

        // Strip the .service-now.com suffix if present (idempotent)
        if (s.endsWith(HERMES_DOMAIN)) {
            s = s.substring(0, s.length() - HERMES_DOMAIN.length());
        }

        // After stripping, must be a bare hostname segment (no dots, slashes, colons)
        if (s.contains("/") || s.contains(":")) {
            throw new IllegalArgumentException(
                "Instance name contains unexpected path or port component: " + input);
        }
        if (s.isEmpty()) {
            throw new IllegalArgumentException(
                "Instance name is empty after parsing: " + input);
        }

        return s;
    }

    private static String buildBootstrap(String instanceName, int[] ports) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ports.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(instanceName).append(HERMES_DOMAIN).append(':').append(ports[i]);
        }
        return sb.toString();
    }
}
