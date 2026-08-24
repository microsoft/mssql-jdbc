package com.microsoft.sqlserver.jdbc.otel;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import com.microsoft.sqlserver.jdbc.TelemetryBridge;
import com.microsoft.sqlserver.jdbc.TelemetryEvent;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporterBuilder;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporterBuilder;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;

public final class OpenTelemetryTelemetryBridge implements TelemetryBridge {
    private static final String INSTRUMENTATION_SCOPE = "com.microsoft.sqlserver.jdbc.otel";

    /** Attribute key the driver core uses to convey the deduced telemetry endpoint. */
    private static final String ENDPOINT_ATTRIBUTE = "mssql.jdbc.otel.endpoint";

    private static final AttributeKey<String> ACTIVITY_KEY = AttributeKey.stringKey("mssql.jdbc.activity");
    private static final AttributeKey<Long> CONNECTION_ID_KEY = AttributeKey.longKey("mssql.jdbc.connection.id");
    private static final AttributeKey<Long> STATEMENT_ID_KEY = AttributeKey.longKey("mssql.jdbc.statement.id");
    private static final AttributeKey<Double> MEASUREMENT_VALUE_KEY = AttributeKey.doubleKey("mssql.jdbc.measurement.value");
    private static final AttributeKey<String> MEASUREMENT_UNIT_KEY = AttributeKey.stringKey("mssql.jdbc.measurement.unit");
    private static final AttributeKey<String> STATEMENT_TYPE_KEY = AttributeKey.stringKey("mssql.jdbc.statement.type");
    private static final AttributeKey<String> SQL_KEY = AttributeKey.stringKey("mssql.jdbc.sql");
    private static final AttributeKey<String> AUTH_SCHEME_KEY = AttributeKey.stringKey("mssql.jdbc.auth.scheme");
    private static final AttributeKey<String> CORRELATION_ID_KEY = AttributeKey.stringKey("mssql.jdbc.correlation.id");
    private static final AttributeKey<String> TRACE_PARENT_KEY = AttributeKey.stringKey("mssql.jdbc.trace.parent");

    /** Default (global) export target, used when the event carries no deduced endpoint. */
    private final Target defaultTarget;

    /** Per-endpoint export targets, keyed by endpoint + auth-header fingerprint. */
    private final ConcurrentHashMap<String, Target> endpointTargets = new ConcurrentHashMap<>();

    public OpenTelemetryTelemetryBridge() {
        this(GlobalOpenTelemetry.get());
    }

    public OpenTelemetryTelemetryBridge(OpenTelemetry openTelemetry) {
        Objects.requireNonNull(openTelemetry, "openTelemetry");
        this.defaultTarget = new Target(openTelemetry);
    }

    /** Bundles the tracer and instruments for a single export destination. */
    private static final class Target {
        private final Tracer tracer;
        private final LongCounter eventCounter;
        private final DoubleHistogram durationHistogram;

        Target(OpenTelemetry openTelemetry) {
            Meter meter = openTelemetry.getMeter(INSTRUMENTATION_SCOPE);
            this.tracer = openTelemetry.getTracer(INSTRUMENTATION_SCOPE);
            this.eventCounter = meter.counterBuilder("mssql.jdbc.operation.count")
                    .setDescription("Number of JDBC performance events observed")
                    .setUnit("{event}")
                    .build();
            this.durationHistogram = meter.histogramBuilder("mssql.jdbc.operation.duration")
                    .setDescription("Duration of JDBC operations")
                    .setUnit("ms")
                    .build();
        }
    }

    @Override
    public void publish(TelemetryEvent event) {
        Objects.requireNonNull(event, "event");
        Target target = resolveTarget(event);

        String spanName = event.getSpanName() != null && !event.getSpanName().isEmpty() ? event.getSpanName()
                : event.getActivity().name();
        SpanBuilder spanBuilder = target.tracer.spanBuilder(spanName)
                .setAttribute(ACTIVITY_KEY, event.getActivity().name())
                .setAttribute(CONNECTION_ID_KEY, (long) event.getConnectionId())
                .setAttribute(STATEMENT_ID_KEY, (long) event.getStatementId())
                .setAttribute(MEASUREMENT_VALUE_KEY, event.getMeasurementValue())
                .setAttribute(MEASUREMENT_UNIT_KEY, event.getMeasurementUnit())
                .setAttribute(STATEMENT_TYPE_KEY, event.getStatementType().name());

        if (event.getUserSql() != null && !event.getUserSql().isEmpty()) {
            spanBuilder.setAttribute(SQL_KEY, sanitizeSql(event.getUserSql()));
        }
        if (event.getAuthScheme() != null) {
            spanBuilder.setAttribute(AUTH_SCHEME_KEY, event.getAuthScheme());
        }
        if (event.getCorrelationId() != null) {
            spanBuilder.setAttribute(CORRELATION_ID_KEY, event.getCorrelationId());
        }
        if (event.getTraceParent() != null) {
            spanBuilder.setAttribute(TRACE_PARENT_KEY, event.getTraceParent());
        }
        addGenericAttributes(spanBuilder, event.getAttributes());

        Span span = spanBuilder.startSpan();
        try (io.opentelemetry.context.Scope ignored = span.makeCurrent()) {
            if (event.getException() != null) {
                span.recordException(event.getException());
                span.setStatus(StatusCode.ERROR, event.getException().getMessage());
            } else {
                span.setStatus(StatusCode.OK);
            }

            // Metrics carry ONLY low-cardinality dimensions so the number of time series stays bounded.
            // High-cardinality fields (connection id, statement id, measurement value/duration, SQL text,
            // correlation id, trace parent) are intentionally excluded here - they live on the span above,
            // which is designed for per-event detail. Putting them on metrics would fork a new time series
            // per event (and, under cumulative temporality, re-export it on every collection cycle).
            io.opentelemetry.api.common.Attributes metricAttributes = buildMetricAttributes(event);

            target.eventCounter.add(1, metricAttributes);
            target.durationHistogram.record(event.getMeasurementValue(), metricAttributes);
        } finally {
            span.end();
        }
    }

    /**
     * Builds the bounded, low-cardinality attribute set used for metric datapoints. Only dimensions with a
     * small, stable value domain are included so metric time-series count does not grow with the number of
     * connections or statements.
     */
    private static io.opentelemetry.api.common.Attributes buildMetricAttributes(TelemetryEvent event) {
        AttributesBuilder metricBuilder = io.opentelemetry.api.common.Attributes.builder()
                .put(ACTIVITY_KEY, event.getActivity().name())
                .put(MEASUREMENT_UNIT_KEY, event.getMeasurementUnit())
                .put(STATEMENT_TYPE_KEY, event.getStatementType().name());
        if (event.getAuthScheme() != null) {
            metricBuilder.put(AUTH_SCHEME_KEY, event.getAuthScheme());
        }
        // Generic attributes are driver-supplied configuration values (e.g. otel mode/endpoint/auth) with a
        // bounded domain, so they are safe to use as metric dimensions.
        addGenericAttributes(metricBuilder, event.getAttributes());
        return metricBuilder.build();
    }

    /**
     * Selects the export target for an event. When the event carries a deduced endpoint attribute, a
     * per-endpoint OTLP target (configured with the event's auth headers) is created and cached; otherwise the
     * default global target is used.
     */
    private Target resolveTarget(TelemetryEvent event) {
        Object endpointValue = event.getAttributes().get(ENDPOINT_ATTRIBUTE);
        if (endpointValue == null) {
            return defaultTarget;
        }
        String endpoint = String.valueOf(endpointValue);
        if (endpoint.isEmpty()) {
            return defaultTarget;
        }
        String key = endpoint + "|" + fingerprintHeaders(event.getAuthHeaders());
        return endpointTargets.computeIfAbsent(key, ignored -> buildEndpointTarget(endpoint, event.getAuthHeaders()));
    }

    /** Builds a per-endpoint OTLP/HTTP export target with the given auth headers. */
    private static Target buildEndpointTarget(String endpoint, Map<String, String> authHeaders) {
        OtlpHttpSpanExporterBuilder spanExporterBuilder = OtlpHttpSpanExporter.builder()
                .setEndpoint(joinPath(endpoint, "/v1/traces"));
        OtlpHttpMetricExporterBuilder metricExporterBuilder = OtlpHttpMetricExporter.builder()
                .setEndpoint(joinPath(endpoint, "/v1/metrics"));
        if (authHeaders != null) {
            for (Map.Entry<String, String> header : authHeaders.entrySet()) {
                spanExporterBuilder.addHeader(header.getKey(), header.getValue());
                metricExporterBuilder.addHeader(header.getKey(), header.getValue());
            }
        }

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(BatchSpanProcessor.builder(spanExporterBuilder.build()).build())
                .build();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
                .registerMetricReader(PeriodicMetricReader.builder(metricExporterBuilder.build()).build())
                .build();

        OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setMeterProvider(meterProvider)
                .build();
        return new Target(openTelemetry);
    }

    private static String joinPath(String endpoint, String path) {
        if (endpoint.endsWith("/")) {
            return endpoint.substring(0, endpoint.length() - 1) + path;
        }
        return endpoint + path;
    }

    private static String fingerprintHeaders(Map<String, String> authHeaders) {
        if (authHeaders == null || authHeaders.isEmpty()) {
            return "";
        }
        // Order-independent, value-insensitive fingerprint: header names identify the target's auth shape.
        // Token values may rotate but should not spawn new export pipelines.
        StringBuilder sb = new StringBuilder();
        authHeaders.keySet().stream().sorted().forEach(k -> sb.append(k).append(';'));
        return sb.toString();
    }

    private static void addGenericAttributes(SpanBuilder builder, Map<String, Object> attributes) {
        if (attributes == null || attributes.isEmpty()) {
            return;
        }
        attributes.forEach((key, value) -> {
            if (value == null) {
                return;
            }
            if (value instanceof Boolean booleanValue) {
                builder.setAttribute(AttributeKey.booleanKey(key), booleanValue);
            } else if (value instanceof Long longValue) {
                builder.setAttribute(AttributeKey.longKey(key), longValue);
            } else if (value instanceof Integer intValue) {
                builder.setAttribute(AttributeKey.longKey(key), intValue.longValue());
            } else if (value instanceof Double doubleValue) {
                builder.setAttribute(AttributeKey.doubleKey(key), doubleValue);
            } else if (value instanceof Float floatValue) {
                builder.setAttribute(AttributeKey.doubleKey(key), floatValue.doubleValue());
            } else {
                builder.setAttribute(AttributeKey.stringKey(key), String.valueOf(value));
            }
        });
    }

    private static void addGenericAttributes(AttributesBuilder builder, Map<String, Object> attributes) {
        if (attributes == null || attributes.isEmpty()) {
            return;
        }
        attributes.forEach((key, value) -> {
            if (value == null) {
                return;
            }
            if (value instanceof Boolean booleanValue) {
                builder.put(AttributeKey.booleanKey(key), booleanValue);
            } else if (value instanceof Long longValue) {
                builder.put(AttributeKey.longKey(key), longValue);
            } else if (value instanceof Integer intValue) {
                builder.put(AttributeKey.longKey(key), intValue.longValue());
            } else if (value instanceof Double doubleValue) {
                builder.put(AttributeKey.doubleKey(key), doubleValue);
            } else if (value instanceof Float floatValue) {
                builder.put(AttributeKey.doubleKey(key), floatValue.doubleValue());
            } else {
                builder.put(AttributeKey.stringKey(key), String.valueOf(value));
            }
        });
    }

    private static String sanitizeSql(String sql) {
        if (sql == null || sql.isEmpty()) {
            return "";
        }
        String trimmed = sql.trim();
        if (trimmed.length() > 256) {
            return trimmed.substring(0, 256) + "...";
        }
        return trimmed.replaceAll("\\s+", " ");
    }
}
