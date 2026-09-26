/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.otel;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.ServiceConfigurationError;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import com.microsoft.sqlserver.jdbc.SQLServerAccessTokenCallback;
import com.microsoft.sqlserver.jdbc.SqlAuthenticationToken;


/** Immutable routing configuration and an instance-local, exporter-worker-only credential cache. */
final class OtlpConfiguration implements Supplier<Map<String, String>>, AutoCloseable {
    private static final int MAX_HEADER_VALUE = 2048;
    private static final long FAILURE_BACKOFF_MILLIS = 1000;
    final String endpoint;
    final String serviceName;
    final String approvedUserAgent;
    private final Map<String, String> headers;
    private final String callbackClass;
    private final String scope;
    private final String authority;
    private final ClassLoader callbackLoader;
    private final LongSupplier clock;
    private volatile boolean closed;
    private volatile String staticBearer;
    private volatile String cachedBearer;
    // All remaining authentication state is confined to synchronized get(), never touched by close().
    private SQLServerAccessTokenCallback callback;
    private long expiresAt;
    private long refreshAt;
    private long retryAt;

    OtlpConfiguration(Properties configuration) {
        this(configuration, System::currentTimeMillis);
    }

    OtlpConfiguration(Properties configuration, LongSupplier clock) {
        Objects.requireNonNull(configuration, "configuration");
        this.clock = clock;
        endpoint = normalizeEndpoint(configuration);
        serviceName = boundedText(configuration.getProperty("otelServiceName", "mssql-jdbc-connection-demo"), 256);
        approvedUserAgent = configuration.getProperty("otelApprovedUserAgent");
        if (approvedUserAgent != null && !ConnectionAttributePolicy.validUserAgent(approvedUserAgent)) {
            throw new IllegalArgumentException("Invalid approved user agent");
        }
        headers = parseHeaders(configuration.getProperty("otelHeaders"));
        String arm = configuration.getProperty("otelArmResourceId");
        if (arm != null) {
            headers.put("x-ms-arm-resource-id", boundedText(arm, MAX_HEADER_VALUE));
        }
        callbackClass = configuration.getProperty("otelAccessTokenCallbackClass");
        scope = configuration.getProperty("otelTokenScope");
        authority = configuration.getProperty("otelTokenAuthority");
        if (callbackClass != null) {
            boundedText(callbackClass, 512);
            boundedText(scope, MAX_HEADER_VALUE);
            boundedText(authority, MAX_HEADER_VALUE);
        } else {
            String token = configuration.getProperty("otelBearerToken");
            staticBearer = token == null ? null : bearer(token);
        }
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        callbackLoader = loader == null ? OtlpConfiguration.class.getClassLoader() : loader;
    }

    static String normalizeEndpoint(Properties properties) {
        String raw = properties.getProperty("otelEndpoint");
        try {
            if (raw == null || raw.length() > 4096) {
                throw new IllegalArgumentException();
            }
            URI uri = new URI(raw).normalize();
            String host = uri.getHost();
            int port = uri.getPort();
            if (host == null || uri.getRawUserInfo() != null || uri.getRawQuery() != null
                    || uri.getRawFragment() != null || port == 0 || port > 65535) {
                throw new IllegalArgumentException();
            }
            boolean loopback = "localhost".equalsIgnoreCase(host) || "127.0.0.1".equals(host) || "[::1]".equals(host);
            // A single DNS label is intended only for an isolated local container network, not arbitrary HTTP hosts.
            boolean developmentHost = !loopback && host.matches("[a-zA-Z][a-zA-Z0-9-]{0,62}");
            boolean localHttp = flag(properties, "otelAllowInsecureLocalEndpoint") && loopback;
            boolean developmentHttp = flag(properties, "otelAllowInsecureDevelopmentEndpoint") && developmentHost;
            if (!"https".equalsIgnoreCase(uri.getScheme())
                    && !("http".equalsIgnoreCase(uri.getScheme()) && (localHttp || developmentHttp))) {
                throw new IllegalArgumentException();
            }
            String path = uri.getRawPath();
            while (path.endsWith("/")) {
                path = path.substring(0, path.length() - 1);
            }
            if (path.endsWith("/v1/metrics")) {
                path = path.substring(0, path.length() - "/v1/metrics".length());
            } else if (path.endsWith("/v1/traces")) {
                path = path.substring(0, path.length() - "/v1/traces".length());
            }
            // Raw components avoid double-encoding escaped path segments (the multi-argument URI ctor would do so).
            return new URI(
                    uri.getScheme().toLowerCase(Locale.ROOT) + "://" + uri.getRawAuthority() + path + "/v1/traces")
                    .toASCIIString();
        } catch (URISyntaxException | IllegalArgumentException e) {
            // URI/parser exceptions may include credentials. Deliberately discard the cause and input.
            throw new IllegalArgumentException("Invalid or insecure telemetry endpoint");
        }
    }

    private static boolean flag(Properties properties, String key) {
        String value = properties.getProperty(key, "false");
        if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
            throw new IllegalArgumentException("Invalid telemetry transport flag");
        }
        return Boolean.parseBoolean(value);
    }

    private static Map<String, String> parseHeaders(String raw) {
        Map<String, String> result = new LinkedHashMap<>();
        if (raw != null && !raw.isEmpty()) {
            if (raw.length() > 8192) {
                throw new IllegalArgumentException("Invalid telemetry headers");
            }
            for (String pair : raw.split(",", -1)) {
                int equals = pair.indexOf('=');
                if (equals <= 0 || result.size() >= 16) {
                    throw new IllegalArgumentException("Invalid telemetry headers");
                }
                String name = pair.substring(0, equals).trim().toLowerCase(Locale.ROOT);
                if (name.length() > 64 || !name.matches("[!#$%&'*+.^_`|~0-9a-z-]+") || reserved(name)
                        || result.containsKey(name)) {
                    throw new IllegalArgumentException("Invalid telemetry header name");
                }
                // Validate before trimming: trim must not hide a trailing CR/LF injection.
                result.put(name, boundedText(pair.substring(equals + 1), MAX_HEADER_VALUE).trim());
            }
        }
        return result;
    }

    private static boolean reserved(String name) {
        return name.equals("authorization") || name.equals("host") || name.startsWith("content-")
                || name.equals("forwarded") || name.startsWith("x-forwarded-") || name.startsWith("proxy-")
                || name.equals("connection") || name.equals("keep-alive") || name.equals("transfer-encoding")
                || name.equals("te") || name.equals("trailer") || name.equals("upgrade") || name.equals("user-agent")
                || name.equals("x-ms-arm-resource-id") || name.equals("cookie") || name.equals("set-cookie");
    }

    private static String boundedText(String value, int maximum) {
        if (value == null || value.trim().isEmpty() || value.length() > maximum) {
            throw new IllegalArgumentException("Invalid telemetry configuration value");
        }
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) < 32 || value.charAt(i) > 126) {
                throw new IllegalArgumentException("Invalid telemetry configuration value");
            }
        }
        return value;
    }

    private static String bearer(String value) {
        boundedText(value, 16384);
        String token = value.regionMatches(true, 0, "Bearer ", 0, 7) ? value.substring(7) : value;
        if (!token.matches("[a-zA-Z0-9._~+/-]+=*")) {
            throw new IllegalArgumentException("Invalid telemetry bearer token");
        }
        return "Bearer " + token;
    }

    @Override
    public synchronized Map<String, String> get() {
        if (closed) {
            throw unavailable();
        }
        String authorization = callbackClass == null ? staticBearer : callbackBearer();
        if (closed) {
            cachedBearer = null;
            throw unavailable();
        }
        Map<String, String> result = new LinkedHashMap<>(headers);
        if (authorization != null) {
            result.put("Authorization", authorization);
        }
        return Collections.unmodifiableMap(result);
    }

    private String callbackBearer() {
        long now = clock.getAsLong();
        if (now < retryAt) {
            throw unavailable();
        }
        if (cachedBearer != null && now < refreshAt && now < expiresAt) {
            return cachedBearer;
        }
        try {
            // Even provider construction is deferred to the BatchSpanProcessor worker, never create() or JDBC.
            if (callback == null) {
                callback = Class.forName(callbackClass, true, callbackLoader)
                        .asSubclass(SQLServerAccessTokenCallback.class).getConstructor().newInstance();
            }
            SqlAuthenticationToken token = callback.getAccessToken(scope, authority);
            now = clock.getAsLong();
            if (closed || token == null || token.getExpiresOn() == null || token.getExpiresOn().getTime() <= now) {
                throw unavailable();
            }
            String value = bearer(token.getAccessToken());
            expiresAt = token.getExpiresOn().getTime();
            long lifetime = Math.subtractExact(expiresAt, now);
            // Refresh 20% early, capped at one minute. A fixed multi-minute skew breaks short-lived tokens.
            refreshAt = expiresAt - Math.max(1, Math.min(60_000, lifetime / 5));
            cachedBearer = value;
            retryAt = 0;
            return value;
        } catch (ReflectiveOperationException | RuntimeException | LinkageError | ServiceConfigurationError e) {
            cachedBearer = null;
            retryAt = clock.getAsLong() + FAILURE_BACKOFF_MILLIS;
            // Never return empty headers, a static fallback, stale credentials, or provider diagnostics.
            throw unavailable();
        }
    }

    private static IllegalStateException unavailable() {
        return new IllegalStateException("Telemetry authentication unavailable");
    }

    @Override
    public void close() {
        // Must not wait for the get() monitor: an application token callback can block indefinitely.
        closed = true;
        cachedBearer = null;
        staticBearer = null;
    }
}
