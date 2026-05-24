package com.servicenow.kafka.connect.hermes;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

final class Version {
    private static final String VERSION;

    static {
        String v = "unknown";
        try (InputStream in = Version.class.getResourceAsStream("version.properties")) {
            if (in != null) {
                Properties props = new Properties();
                props.load(in);
                v = props.getProperty("version", "unknown");
            }
        } catch (IOException e) {
            // fall through
        }
        VERSION = v;
    }

    static String get() { return VERSION; }
    private Version() {}
}
