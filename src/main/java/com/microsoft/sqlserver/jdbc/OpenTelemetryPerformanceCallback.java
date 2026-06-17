/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;

/**
 * POC PerformanceLogCallback that records driver performance events as
 * OpenTelemetry metrics AND spans. Metric naming follows the Solution 4
 * instrument catalog in docs/otelproposal.md - one LongHistogram per
 * PerformanceActivity (e.g. {@code db.client.statement.execute.duration})
 * plus a single {@code db.client.errors} LongCounter for failed operations.
 *
 * <p>For each event one OTel span is also emitted, anchored to the exact
 * {@code startTime}/{@code endTime} captured by {@link PerformanceLog.Scope}.
 * Spans carry {@code db.connection_id}, {@code db.statement_id} and
 * {@code db.statement} attributes so a trace backend (Jaeger / Tempo /
 * App Insights / Honeycomb) can filter by connection for per-connection
 * drill-down.
 */
final class OpenTelemetryPerformanceCallback implements PerformanceLogCallback {

    private static final Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.OpenTelemetryPerformanceCallback");
    private static final String INSTRUMENTATION_NAME = "com.microsoft.sqlserver.jdbc";
    private static final String DURATION_UNIT_MS = "ms";

    private static final AttributeKey<String> ATTR_SYSTEM = AttributeKey.stringKey("db.system");
    private static final AttributeKey<String> ATTR_OPERATION = AttributeKey.stringKey("db.operation");
    private static final AttributeKey<String> ATTR_ERROR_TYPE = AttributeKey.stringKey("error.type");
    private static final AttributeKey<Long> ATTR_CONNECTION_ID = AttributeKey.longKey("db.connection_id");
    private static final AttributeKey<Long> ATTR_STATEMENT_ID = AttributeKey.longKey("db.statement_id");
    private static final AttributeKey<String> ATTR_STATEMENT = AttributeKey.stringKey("db.statement");
    private static final AttributeKey<String> ATTR_STATEMENT_TYPE = AttributeKey.stringKey("db.statement.type");
    private static final String SYSTEM_VALUE = "mssql";
    private static final Attributes BASE_ATTRS = Attributes.of(ATTR_SYSTEM, SYSTEM_VALUE);

    private final Map<PerformanceActivity, LongHistogram> durations;
    private final LongCounter errors;
    private final Tracer tracer;

    OpenTelemetryPerformanceCallback(OpenTelemetry otel) {
        Meter meter = otel.getMeter(INSTRUMENTATION_NAME);
        this.durations = new EnumMap<>(PerformanceActivity.class);
        for (PerformanceActivity activity : PerformanceActivity.values()) {
            durations.put(activity, meter.histogramBuilder(metricNameFor(activity))
                    .setDescription(activity.activity())
                    .setUnit(DURATION_UNIT_MS)
                    .ofLongs()
                    .build());
        }
        this.errors = meter.counterBuilder("db.client.errors")
                .setDescription("Count of failed mssql-jdbc client operations")
                .build();
        this.tracer = otel.getTracer(INSTRUMENTATION_NAME);
    }

    @Override
    public void publish(PerformanceActivity activity, int connectionId, long durationMs, Exception exception) {
        // Fallback path: no explicit start/end available - back-date span end to "now".
        long endMs = System.currentTimeMillis();
        safeRecord(activity, connectionId, -1, endMs - Math.max(0L, durationMs), endMs, exception);
    }

    @Override
    public void publish(PerformanceActivity activity, int connectionId, int statementId, long durationMs,
            Exception exception) {
        long endMs = System.currentTimeMillis();
        safeRecord(activity, connectionId, statementId, endMs - Math.max(0L, durationMs), endMs, exception);
    }

    @Override
    public void publish(PerformanceActivity activity, int connectionId, long startTime, long endTime,
            Exception exception) {
        safeRecord(activity, connectionId, -1, startTime, endTime, exception);
    }

    @Override
    public void publish(PerformanceActivity activity, int connectionId, int statementId, long startTime, long endTime,
            Exception exception) {
        safeRecord(activity, connectionId, statementId, startTime, endTime, exception);
    }

    private void safeRecord(PerformanceActivity activity, int connectionId, int statementId,
            long startMs, long endMs, Exception exception) {
        try {
            record(activity, connectionId, statementId, startMs, endMs, exception);
        } catch (Throwable t) {
            if (logger.isLoggable(Level.WARNING)) {
                logger.log(Level.WARNING, "OpenTelemetry publish failed for activity " + activity + "; event dropped", t);
            }
        }
    }

    private void record(PerformanceActivity activity, int connectionId, int statementId, long startMs, long endMs,
            Exception exception) {
        // Intentionally do NOT attach connectionId/statementId as metric labels:
        // they would create a new time series per connection, breaking
        // rate()/quantile() aggregations and exploding cardinality. Those IDs
        // live on spans instead, where high cardinality is fine.
        long durationMs = endMs - startMs;
        durations.get(activity).record(durationMs, BASE_ATTRS);
        if (exception != null) {
            errors.add(1, Attributes.of(
                    ATTR_SYSTEM, SYSTEM_VALUE,
                    ATTR_OPERATION, activity.name(),
                    ATTR_ERROR_TYPE, exception.getClass().getName()));
        }
        emitSpan(activity, connectionId, statementId, startMs, endMs, exception);
    }

    private void emitSpan(PerformanceActivity activity, int connectionId, int statementId, long startMs, long endMs,
            Exception exception) {
        long startNs = startMs * 1_000_000L;
        long endNs = endMs * 1_000_000L;

        SpanBuilder b = tracer.spanBuilder(spanNameFor(activity))
                .setStartTimestamp(startNs, TimeUnit.NANOSECONDS)
                .setAttribute(ATTR_SYSTEM, SYSTEM_VALUE)
                .setAttribute(ATTR_CONNECTION_ID, (long) connectionId)
                .setAttribute(ATTR_OPERATION, activity.name());
        if (statementId >= 0) {
            b.setAttribute(ATTR_STATEMENT_ID, (long) statementId);
        }
        String sql = PerformanceLog.currentUserSql.get();
        if (sql != null && !sql.isEmpty()) {
            b.setAttribute(ATTR_STATEMENT, sql);
        }
        StatementType type = PerformanceLog.currentStatementType.get();
        if (type != null) {
            b.setAttribute(ATTR_STATEMENT_TYPE, type.name());
        }

        Span span = b.startSpan();
        try {
            if (exception != null) {
                span.recordException(exception);
                span.setStatus(StatusCode.ERROR, exception.getClass().getName());
            }
        } finally {
            span.end(endNs, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * Maps each activity to the metric name from docs/otelproposal.md
     * (Solution 4 instrument catalog, extended consistently for the activities
     * not enumerated there). Connection-level events go under
     * {@code db.client.connection.*}, statement-level under
     * {@code db.client.statement.*}.
     */
    private static String metricNameFor(PerformanceActivity activity) {
        switch (activity) {
            case CONNECTION:
                return "db.client.connection.duration";
            case PRELOGIN:
                return "db.client.connection.prelogin.duration";
            case LOGIN:
                return "db.client.connection.login.duration";
            case TOKEN_ACQUISITION:
                return "db.client.connection.token_acquisition.duration";
            case STATEMENT_REQUEST_BUILD:
                return "db.client.statement.request_build.duration";
            case STATEMENT_FIRST_SERVER_RESPONSE:
                return "db.client.statement.first_server_response.duration";
            case STATEMENT_PREPARE:
                return "db.client.statement.prepare.duration";
            case STATEMENT_PREPEXEC:
                return "db.client.statement.prepexec.duration";
            case STATEMENT_EXECUTE:
                return "db.client.statement.execute.duration";
            default:
                return "db.client." + activity.name().toLowerCase(Locale.ROOT) + ".duration";
        }
    }

    private static String spanNameFor(PerformanceActivity activity) {
        switch (activity) {
            case CONNECTION:
                return "db.client.connection";
            case PRELOGIN:
                return "db.client.connection.prelogin";
            case LOGIN:
                return "db.client.connection.login";
            case TOKEN_ACQUISITION:
                return "db.client.connection.token_acquisition";
            case STATEMENT_REQUEST_BUILD:
                return "db.client.statement.request_build";
            case STATEMENT_FIRST_SERVER_RESPONSE:
                return "db.client.statement.first_server_response";
            case STATEMENT_PREPARE:
                return "db.client.statement.prepare";
            case STATEMENT_PREPEXEC:
                return "db.client.statement.prepexec";
            case STATEMENT_EXECUTE:
                return "db.client.statement.execute";
            default:
                return "db.client." + activity.name().toLowerCase(Locale.ROOT);
        }
    }
}
