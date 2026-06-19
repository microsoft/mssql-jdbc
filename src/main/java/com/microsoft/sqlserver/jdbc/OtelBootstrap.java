/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporterBuilder;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporterBuilder;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;

/**
 * POC for docs/otelproposal.md Solution 4: when the user sets {@code otelEndpoint}
 * on the connection string, this class registers callbacks on {@link PerformanceLog}
 * that ship metrics AND traces to the configured OTLP/HTTP endpoint.
 *
 * <p>Failure modes are non-fatal: a missing OpenTelemetry classpath, an
 * unreachable endpoint, or any other init error logs once and leaves JDBC
 * traffic untouched.
 */
final class OtelBootstrap {

    private static final Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.OtelBootstrap");

    private static final AtomicBoolean initialized = new AtomicBoolean(false);

    /**
     * Holds the most-recent SQL AAD access token pre-formatted as {@code "Bearer <token>"}.
     * Updated on every fedAuth token acquisition (initial connect and pool-triggered reconnect)
     * so that the OTLP exporter's per-request header supplier always sees a fresh value without
     * rebuilding the exporter pipeline.
     */
    private static final AtomicReference<String> sqlBearerToken = new AtomicReference<>("");

    /**
     * Records the latest SQL AAD access token so the OTLP Authorization header stays current
     * across token renewals. Must be called before {@link #ensureInitialized} on every
     * {@code onFedAuthInfo} invocation — both first connect and pool-triggered reconnect.
     */
    static void updateSqlToken(SqlAuthenticationToken token) {
        if (token == null) {
            return;
        }
        String raw = token.getAccessToken();
        if (raw != null && !raw.isEmpty()) {
            String bearer = raw.regionMatches(true, 0, "Bearer ", 0, "Bearer ".length())
                    ? raw : "Bearer " + raw;
            sqlBearerToken.set(bearer);
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("OTel SQL bearer token updated (expires=" + token.getExpiresOn() + ")");
            }
        }
    }

    private OtelBootstrap() {}

    /**
     * Initializes OpenTelemetry export on the first connection that opts in.
     * Subsequent invocations are no-ops.
     */
    static void ensureInitialized(Properties props) {
        ensureInitialized(props, null);
    }

    /**
     * Initializes OpenTelemetry export on the first connection that opts in.
     * If otelUseSqlAccessToken is enabled, the SQL AAD token is reused as the
     * Authorization bearer token when available.
     *
     * @param props connection properties
     * @param sqlAuthToken SQL AAD access token, if one was acquired for the connection
     */
    static void ensureInitialized(Properties props, SqlAuthenticationToken sqlAuthToken) {
        if (props == null || initialized.get()) {
            return;
        }
        String endpoint = trim(props.getProperty(SQLServerDriverStringProperty.OTEL_ENDPOINT.toString()));
        if (endpoint.isEmpty()) {
            return;
        }
        boolean useSqlAccessToken = boolProp(props,
                SQLServerDriverBooleanProperty.OTEL_USE_SQL_ACCESS_TOKEN.toString(),
                SQLServerDriverBooleanProperty.OTEL_USE_SQL_ACCESS_TOKEN.getDefaultValue());
        String explicitBearerToken = trim(props.getProperty(SQLServerDriverStringProperty.OTEL_BEARER_TOKEN.toString()));
        // Guard: if SQL-token mode is on but no token is available yet (updateSqlToken not called)
        // and no explicit bearer is set either, defer until a token exists.
        if (useSqlAccessToken && sqlBearerToken.get().isEmpty() && explicitBearerToken.isEmpty()) {
            return;
        }
        if (!initialized.compareAndSet(false, true)) {
            return;
        }
        try {
            OpenTelemetry otel = buildOrReuseGlobal(props, endpoint);
            PerformanceLog.registerCallback(new OpenTelemetryPerformanceCallback(otel));
            if (logger.isLoggable(Level.INFO)) {
                logger.info("OpenTelemetry performance export enabled - metrics + traces (endpoint=" + endpoint + ")");
            }
        } catch (LinkageError e) {
            initialized.set(false);
            logger.log(Level.WARNING,
                    "otelEndpoint is set but OpenTelemetry libraries are not on the classpath; "
                            + "add io.opentelemetry:opentelemetry-sdk and opentelemetry-exporter-otlp. Disabling export.",
                    e);
        } catch (IllegalStateException e) {
            // PerformanceLog.registerCallback throws this when a user callback is already registered.
            logger.log(Level.WARNING,
                    "PerformanceLogCallback already registered; OpenTelemetry export will not be wired up.", e);
        } catch (RuntimeException e) {
            initialized.set(false);
            logger.log(Level.WARNING, "Failed to initialize OpenTelemetry export; disabling.", e);
        }
    }

    private static OpenTelemetry buildOrReuseGlobal(Properties props, String endpoint) {
        OpenTelemetry existing = GlobalOpenTelemetry.get();
        if (existing != OpenTelemetry.noop()) {
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("Reusing application-registered GlobalOpenTelemetry (otelEndpoint ignored).");
            }
            return existing;
        }

        String serviceName = orDefault(
                props.getProperty(SQLServerDriverStringProperty.OTEL_SERVICE_NAME.toString()),
                SQLServerDriverStringProperty.OTEL_SERVICE_NAME.getDefaultValue());

        int intervalSeconds = intProp(props,
                SQLServerDriverIntProperty.OTEL_EXPORT_INTERVAL.toString(),
                SQLServerDriverIntProperty.OTEL_EXPORT_INTERVAL.getDefaultValue());

        boolean useSqlToken = boolProp(props,
                SQLServerDriverBooleanProperty.OTEL_USE_SQL_ACCESS_TOKEN.toString(),
                SQLServerDriverBooleanProperty.OTEL_USE_SQL_ACCESS_TOKEN.getDefaultValue());

        OtlpHttpMetricExporterBuilder exporterBuilder = OtlpHttpMetricExporter.builder()
                .setEndpoint(endpoint);
        if (useSqlToken) {
            // Static custom headers (otelHeaders connection property) — baked once at pipeline init.
            for (String[] kv : parseHeaders(props.getProperty(SQLServerDriverStringProperty.OTEL_HEADERS.toString()))) {
                exporterBuilder.addHeader(kv[0], kv[1]);
            }
            // Dynamic Authorization header — this Supplier is called on every OTLP HTTP request,
            // so token renewals (pool reconnects, mid-session refreshes) are automatically picked
            // up without rebuilding the exporter pipeline. Mirrors the C# AuthorizationHeaderHandler
            // pattern from authenticated-otel-logger.
            exporterBuilder.setHeaders(() -> {
                String bearer = sqlBearerToken.get();
                return bearer.isEmpty() ? Collections.emptyMap()
                        : Collections.singletonMap("Authorization", bearer);
            });
        } else {
            for (String[] kv : otlpHeaders(props, null)) {
                exporterBuilder.addHeader(kv[0], kv[1]);
            }
        }

        Resource resource = Resource.getDefault().merge(
                Resource.create(Attributes.of(AttributeKey.stringKey("service.name"), serviceName)));

        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
                .setResource(resource)
                .registerMetricReader(PeriodicMetricReader.builder(exporterBuilder.build())
                        .setInterval(Duration.ofSeconds(intervalSeconds))
                        .build())
                .build();

        // OTLP/HTTP span exporter targets the same base host as the metrics endpoint but with the
        // /v1/traces path. Users can override with otelEndpoint=http://.../v1/metrics in which case
        // we swap the suffix; if the value already ends in /v1/traces we keep it.
        String traceEndpoint = endpoint.endsWith("/v1/metrics")
                ? endpoint.substring(0, endpoint.length() - "/v1/metrics".length()) + "/v1/traces"
                : endpoint;
        OtlpHttpSpanExporterBuilder spanExporterBuilder = OtlpHttpSpanExporter.builder()
                .setEndpoint(traceEndpoint);
        if (useSqlToken) {
            for (String[] kv : parseHeaders(props.getProperty(SQLServerDriverStringProperty.OTEL_HEADERS.toString()))) {
                spanExporterBuilder.addHeader(kv[0], kv[1]);
            }
            spanExporterBuilder.setHeaders(() -> {
                String bearer = sqlBearerToken.get();
                return bearer.isEmpty() ? Collections.emptyMap()
                        : Collections.singletonMap("Authorization", bearer);
            });
        } else {
            for (String[] kv : otlpHeaders(props, null)) {
                spanExporterBuilder.addHeader(kv[0], kv[1]);
            }
        }

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .setResource(resource)
                .addSpanProcessor(BatchSpanProcessor.builder(spanExporterBuilder.build()).build())
                .build();

        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setMeterProvider(meterProvider)
                .setTracerProvider(tracerProvider)
                .build();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            tracerProvider.close();
            meterProvider.close();
        }, "mssql-jdbc-otel-shutdown"));
        return sdk;
    }

    static String[][] otlpHeaders(Properties props, SqlAuthenticationToken sqlAuthToken) {
        LinkedHashMap<String, String> headers = new LinkedHashMap<>();
        for (String[] kv : parseHeaders(props.getProperty(SQLServerDriverStringProperty.OTEL_HEADERS.toString()))) {
            headers.put(kv[0], kv[1]);
        }

        boolean useSqlAccessToken = boolProp(props,
                SQLServerDriverBooleanProperty.OTEL_USE_SQL_ACCESS_TOKEN.toString(),
                SQLServerDriverBooleanProperty.OTEL_USE_SQL_ACCESS_TOKEN.getDefaultValue());
        String sqlAccessToken = sqlAuthToken == null ? "" : trim(sqlAuthToken.getAccessToken());
        String bearerToken = useSqlAccessToken && !sqlAccessToken.isEmpty() ? sqlAccessToken
                : trim(props.getProperty(SQLServerDriverStringProperty.OTEL_BEARER_TOKEN.toString()));
        if (!bearerToken.isEmpty()) {
            headers.put("Authorization", bearerToken.regionMatches(true, 0, "Bearer ", 0, "Bearer ".length())
                    ? bearerToken
                    : "Bearer " + bearerToken);
        }

        String[][] out = new String[headers.size()][];
        int i = 0;
        for (java.util.Map.Entry<String, String> entry : headers.entrySet()) {
            out[i++] = new String[] {entry.getKey(), entry.getValue()};
        }
        return out;
    }

    private static String[][] parseHeaders(String raw) {
        if (raw == null || raw.isEmpty()) {
            return new String[0][];
        }
        String[] pairs = raw.split(",");
        String[][] out = new String[pairs.length][];
        int n = 0;
        for (String pair : pairs) {
            int eq = pair.indexOf('=');
            if (eq <= 0 || eq == pair.length() - 1) {
                continue;
            }
            out[n++] = new String[] {pair.substring(0, eq).trim(), pair.substring(eq + 1).trim()};
        }
        if (n == pairs.length) {
            return out;
        }
        String[][] trimmed = new String[n][];
        System.arraycopy(out, 0, trimmed, 0, n);
        return trimmed;
    }

    private static int intProp(Properties props, String key, int defaultValue) {
        String raw = props.getProperty(key);
        if (raw == null || raw.isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static String trim(String s) {
        return s == null ? "" : s.trim();
    }

    private static String orDefault(String s, String defaultValue) {
        if (s == null) {
            return defaultValue;
        }
        String t = s.trim();
        return t.isEmpty() ? defaultValue : t;
    }

    private static boolean boolProp(Properties props, String key, boolean defaultValue) {
        String raw = props.getProperty(key);
        if (raw == null || raw.isEmpty()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(raw.trim());
    }
}
