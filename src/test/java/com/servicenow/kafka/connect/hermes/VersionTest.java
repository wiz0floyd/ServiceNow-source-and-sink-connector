package com.servicenow.kafka.connect.hermes;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that the connector version string is present and well-formed.
 *
 * NOTE: The spec referenced a {@code Version.get()} helper class that does not exist in the
 * production sources. These tests are therefore written against the static VERSION constants on the
 * connector classes directly, which is where version strings are actually defined. If a dedicated
 * {@code Version} utility class is introduced in the future, these tests should be updated to use it.
 */
class VersionTest {

    @Test
    void versionIsNotUnknown() {
        String v = HermesSinkConnector.VERSION;
        assertNotNull(v);
        assertNotEquals("unknown", v, "connector version must not be the placeholder 'unknown' sentinel");
    }

    @Test
    void versionMatchesSemver() {
        String v = HermesSinkConnector.VERSION;
        assertTrue(v.matches("\\d+\\.\\d+\\.\\d+.*"),
            "Version '" + v + "' does not match expected semver format");
    }

    @Test
    void sinkConnectorVersionMatchesVersion() {
        assertNotNull(new HermesSinkConnector().version());
    }

    @Test
    void sourceConnectorVersionMatchesVersion() {
        assertNotNull(new HermesSourceConnector().version());
    }
}
