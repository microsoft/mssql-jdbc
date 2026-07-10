package com.microsoft.sqlserver.jdbc.otel;

import java.util.Map;
import java.util.Objects;

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

public final class OpenTelemetryTelemetryBridge implements TelemetryBridge {
    private static final String INSTRUMENTATION_SCOPE = "com.microsoft.sqlserver.jdbc.otel";

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

    private final Tracer tracer;
    private final LongCounter eventCounter;
    private final DoubleHistogram durationHistogram;

    public OpenTelemetryTelemetryBridge() {
        this(GlobalOpenTelemetry.get());
    }

    public OpenTelemetryTelemetryBridge(OpenTelemetry openTelemetry) {
        Objects.requireNonNull(openTelemetry, "openTelemetry");
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

    @Override
    public void publish(TelemetryEvent event) {
        Objects.requireNonNull(event, "event");

        String spanName = event.getSpanName() != null && !event.getSpanName().isEmpty() ? event.getSpanName()
                : event.getActivity().name();
        SpanBuilder spanBuilder = tracer.spanBuilder(spanName)
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

            AttributesBuilder attributeBuilder = io.opentelemetry.api.common.Attributes.builder()
                    .put(ACTIVITY_KEY, event.getActivity().name())
                    .put(CONNECTION_ID_KEY, (long) event.getConnectionId())
                    .put(STATEMENT_ID_KEY, (long) event.getStatementId())
                    .put(MEASUREMENT_VALUE_KEY, event.getMeasurementValue())
                    .put(MEASUREMENT_UNIT_KEY, event.getMeasurementUnit())
                    .put(STATEMENT_TYPE_KEY, event.getStatementType().name());
            if (event.getUserSql() != null && !event.getUserSql().isEmpty()) {
                attributeBuilder.put(SQL_KEY, sanitizeSql(event.getUserSql()));
            }
            if (event.getAuthScheme() != null) {
                attributeBuilder.put(AUTH_SCHEME_KEY, event.getAuthScheme());
            }
            if (event.getCorrelationId() != null) {
                attributeBuilder.put(CORRELATION_ID_KEY, event.getCorrelationId());
            }
            if (event.getTraceParent() != null) {
                attributeBuilder.put(TRACE_PARENT_KEY, event.getTraceParent());
            }
            addGenericAttributes(attributeBuilder, event.getAttributes());

            eventCounter.add(1, attributeBuilder.build());
            durationHistogram.record(event.getMeasurementValue(), attributeBuilder.build());
        } finally {
            span.end();
        }
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
