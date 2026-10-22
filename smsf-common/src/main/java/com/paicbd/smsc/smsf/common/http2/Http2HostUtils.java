package com.paicbd.smsc.smsf.common.http2;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

public final class Http2HostUtils {

    private Http2HostUtils() {
        throw new IllegalStateException("Utility class");
    }

    private static final int HTTPS_DEFAULT_PORT = 443;
    private static final int HTTP_DEFAULT_PORT = 80;

    public static HostPort parseHostPort(String raw, int defaultPort) {
        if (Objects.isNull(raw) || raw.isBlank()) {
            throw new IllegalArgumentException("host is required");
        }
        String value = raw.trim();
        if (value.contains("://")) {
            String authority = stripPathAndQuery(value);
            try {
                URI uri = new URI(authority);
                String host = uri.getHost();
                if (Objects.nonNull(host) && !host.isBlank()) {
                    int port = uri.getPort();
                    if (port <= 0) {
                        port = portFromScheme(uri.getScheme(), defaultPort);
                    }
                    return new HostPort(host, port);
                }
            } catch (URISyntaxException ignored) {
                // fall through to host:port parsing
            }
        }
        String host = value;
        int port = defaultPort;
        int sep = host.lastIndexOf(':');
        if (sep > 0 && sep < host.length() - 1) {
            try {
                port = Integer.parseInt(host.substring(sep + 1));
                host = host.substring(0, sep);
            } catch (NumberFormatException ignored) {
                // host has no explicit port — keep default
            }
        }
        return new HostPort(host, port);
    }

    /**
     * Removes the path/query components from an absolute URL so the
     * {@code scheme://authority} portion can be parsed by {@link URI} even when
     * the path contains templated placeholders such as {@code {supi}} that are
     * not valid URI characters.
     */
    private static String stripPathAndQuery(String value) {
        int schemeSep = value.indexOf("://");
        if (schemeSep < 0) {
            return value;
        }
        int authorityEnd = value.length();
        for (int i = schemeSep + 3; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '/' || c == '?' || c == '#') {
                authorityEnd = i;
                break;
            }
        }
        return value.substring(0, authorityEnd);
    }

    /**
     * Extracts the request path from a gateway host value. When the value is an
     * absolute URL (e.g. {@code http://127.0.0.1/nsmsf-sms/v2/ue-contexts/{supi}/sendsms})
     * the URL path is returned; otherwise the raw value is returned unchanged.
     *
     * @param raw the configured gateway host
     * @return the path component, or an empty string when none is present
     */
    public static String parsePath(String raw) {
        if (Objects.isNull(raw) || raw.isBlank()) {
            throw new IllegalArgumentException("host is required");
        }
        String value = raw.trim();
        int schemeSep = value.indexOf("://");
        if (schemeSep < 0) {
            return value;
        }
        // Extract the path manually instead of java.net.URI so templated
        // placeholders such as "{supi}" (illegal URI characters) are preserved.
        int pathStart = value.indexOf('/', schemeSep + 3);
        return pathStart < 0 ? "" : value.substring(pathStart);
    }

    private static int portFromScheme(String scheme, int defaultPort) {
        if ("https".equalsIgnoreCase(scheme)) return HTTPS_DEFAULT_PORT;
        if ("http".equalsIgnoreCase(scheme)) return HTTP_DEFAULT_PORT;
        return defaultPort;
    }

    public record HostPort(String host, int port) {
    }
}
