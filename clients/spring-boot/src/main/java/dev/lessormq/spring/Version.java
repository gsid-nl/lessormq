package dev.lessormq.spring;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/** The version of this library, from version.properties, which Maven fills in at build time. */
final class Version {
    static final String VALUE = read();

    private Version() {
    }

    private static String read() {
        try (InputStream in = Version.class.getResourceAsStream("version.properties")) {
            if (in == null) {
                return "unknown";
            }
            Properties properties = new Properties();
            properties.load(in);
            String version = properties.getProperty("version", "");
            return version.isEmpty() || version.startsWith("${") ? "unknown" : version;
        } catch (IOException e) {
            return "unknown";
        }
    }
}
