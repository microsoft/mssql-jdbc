/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable payload passed from the core JDBC driver to an optional telemetry bridge.
 *
 * <p>This keeps the driver decoupled from OpenTelemetry-specific types while still allowing the bridge to
 * receive the details it needs for metrics or spans.</p>
 */
public final class TelemetryEvent {
    private final PerformanceActivity activity;
    private final int connectionId;
    private final int statementId;
    private final long duration;
    private final Exception exception;
    private final String userSql;
    private final StatementType statementType;
    private final Map<String, String> authHeaders;
    private final String authScheme;
    private final String spanName;
    private final String traceParent;
    private final String correlationId;
    private final double measurementValue;
    private final String measurementUnit;
    private final Map<String, Object> attributes;

    public TelemetryEvent(PerformanceActivity activity, int connectionId, int statementId, long duration,
            Exception exception, String userSql, StatementType statementType, Map<String, String> authHeaders,
            String authScheme, String spanName, String traceParent, String correlationId) {
        this(activity, connectionId, statementId, duration, exception, userSql, statementType, authHeaders,
                authScheme, spanName, traceParent, correlationId, "ms", Collections.emptyMap());
    }

    public TelemetryEvent(PerformanceActivity activity, int connectionId, int statementId, long duration,
            Exception exception, String userSql, StatementType statementType, Map<String, String> authHeaders,
            String authScheme, String spanName, String traceParent, String correlationId, String measurementUnit,
            Map<String, Object> attributes) {
        this.activity = Objects.requireNonNull(activity, "activity");
        this.connectionId = connectionId;
        this.statementId = statementId;
        this.duration = duration;
        this.exception = exception;
        this.userSql = userSql;
        this.statementType = statementType;
        this.authHeaders = copyHeaders(authHeaders);
        this.authScheme = authScheme;
        this.spanName = spanName;
        this.traceParent = traceParent;
        this.correlationId = correlationId;
        this.measurementValue = duration;
        this.measurementUnit = measurementUnit == null || measurementUnit.isEmpty() ? "ms" : measurementUnit;
        this.attributes = copyAttributes(attributes);
    }

    public PerformanceActivity getActivity() {
        return activity;
    }

    public int getConnectionId() {
        return connectionId;
    }

    public int getStatementId() {
        return statementId;
    }

    public long getDuration() {
        return duration;
    }

    public Exception getException() {
        return exception;
    }

    public String getUserSql() {
        return userSql;
    }

    public StatementType getStatementType() {
        return statementType;
    }

    public Map<String, String> getAuthHeaders() {
        return authHeaders;
    }

    public String getAuthScheme() {
        return authScheme;
    }

    public String getSpanName() {
        return spanName;
    }

    public String getTraceParent() {
        return traceParent;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public double getMeasurementValue() {
        return measurementValue;
    }

    public String getMeasurementUnit() {
        return measurementUnit;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public boolean hasAuthContext() {
        return !authHeaders.isEmpty() || authScheme != null || correlationId != null;
    }

    public boolean hasAttributes() {
        return !attributes.isEmpty();
    }

    private static Map<String, String> copyHeaders(Map<String, String> authHeaders) {
        if (null == authHeaders || authHeaders.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> copy = new LinkedHashMap<>();
        authHeaders.forEach((key, value) -> copy.put(key, value));
        return Collections.unmodifiableMap(copy);
    }

    private static Map<String, Object> copyAttributes(Map<String, Object> attributes) {
        if (null == attributes || attributes.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, Object> copy = new LinkedHashMap<>();
        attributes.forEach((key, value) -> copy.put(key, value));
        return Collections.unmodifiableMap(copy);
    }
}
