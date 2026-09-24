/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;


/**
 * Immutable snapshot of a connection activity boundary. Scope IDs are process-local correlation identifiers, not
 * OpenTelemetry trace or span IDs. Attribute maps contain only driver-approved scalar metadata and never raw messages,
 * connection strings, host names or credentials. The exception reference is retained for trusted in-process diagnostics;
 * it is not sanitized, is not deep-copied, and must not be serialized or recorded as a raw telemetry exception.
 */
public final class PerformanceLogEvent {
    /** Lifecycle boundary reported by this snapshot. */
    public enum Type {
        /** The activity has started. */
        START,
        /** The activity has ended, successfully or with a failure. */
        END
    }

    private final Type type;
    private final long scopeId;
    private final long parentScopeId;
    private final long rootScopeId;
    private final int connectionId;
    private final PerformanceActivity activity;
    private final String phase;
    private final long startEpochNanos;
    private final long endEpochNanos;
    private final long durationNanos;
    private final Exception exception;
    private final String failurePhase;
    private final Map<String, Object> attributes;
    private final Map<String, Object> errorAttributes;
    private final List<Map<String, Object>> diagnosticEvents;

    PerformanceLogEvent(Type type, long scopeId, long parentScopeId, long rootScopeId, int connectionId,
            PerformanceActivity activity, long startEpochNanos, long endEpochNanos, long durationNanos,
            Exception exception, String failurePhase, Map<String, Object> attributes,
            Map<String, Object> errorAttributes) {
        this(type, scopeId, parentScopeId, rootScopeId, connectionId, activity, startEpochNanos, endEpochNanos,
                durationNanos, exception, failurePhase, attributes, errorAttributes, Collections.emptyList());
    }

    PerformanceLogEvent(Type type, long scopeId, long parentScopeId, long rootScopeId, int connectionId,
            PerformanceActivity activity, long startEpochNanos, long endEpochNanos, long durationNanos,
            Exception exception, String failurePhase, Map<String, Object> attributes,
            Map<String, Object> errorAttributes, List<Map<String, Object>> diagnosticEvents) {
        this.type = type;
        this.scopeId = scopeId;
        this.parentScopeId = parentScopeId;
        this.rootScopeId = rootScopeId;
        this.connectionId = connectionId;
        this.activity = activity;
        this.phase = activity.connectionPhase();
        this.startEpochNanos = startEpochNanos;
        this.endEpochNanos = endEpochNanos;
        this.durationNanos = durationNanos;
        this.exception = exception;
        this.failurePhase = failurePhase;
        this.attributes = Collections.unmodifiableMap(new LinkedHashMap<>(attributes));
        this.errorAttributes = Collections.unmodifiableMap(new LinkedHashMap<>(errorAttributes));
        // Entries and their nested attribute maps are already immutable driver-owned snapshots.
        this.diagnosticEvents = Collections.unmodifiableList(new ArrayList<>(diagnosticEvents));
    }

    /** @return explicit START or END boundary, independent of timestamps and failure state */
    public Type getType() {
        return type;
    }

    /** @return unique process-local scope ID */
    public long getScopeId() {
        return scopeId;
    }

    /** @return parent scope ID, or zero for a root */
    public long getParentScopeId() {
        return parentScopeId;
    }

    /** @return owning root scope ID */
    public long getRootScopeId() {
        return rootScopeId;
    }

    /** @return driver connection ID, or zero when unavailable */
    public int getConnectionId() {
        return connectionId;
    }

    /** @return the accurately bounded connection activity */
    public PerformanceActivity getActivity() {
        return activity;
    }

    /** @return stable phase name; {@code connection.open} for CONNECTION */
    public String getPhase() {
        return phase;
    }

    /** @return scope start in epoch nanoseconds */
    public long getStartEpochNanos() {
        return startEpochNanos;
    }

    /** @return monotonic-clock-derived end in epoch nanoseconds, or zero at start */
    public long getEndEpochNanos() {
        return endEpochNanos;
    }

    /** @return monotonic elapsed nanoseconds, or zero at start */
    public long getDurationNanos() {
        return durationNanos;
    }

    /** @return original captured failure, or null; never export this reference raw */
    public Exception getException() {
        return exception;
    }

    /** @return original failing phase, or null when this scope did not fail */
    public String getFailurePhase() {
        return failurePhase;
    }

    /** @return immutable approved span metadata and, on failure, category/type summary */
    public Map<String, Object> getAttributes() {
        return attributes;
    }

    /** @return immutable detailed error metadata on the originating scope only; otherwise empty */
    public Map<String, Object> getErrorAttributes() {
        return errorAttributes;
    }

    /**
     * Returns bounded, sanitized decision events on the CONNECTION END snapshot only. Each immutable map contains
     * {@code name} (String), {@code timestamp} (Long, epoch nanoseconds derived from the root's monotonic clock), and
     * {@code attributes} (immutable Map of String keys to approved scalar values). Names are
     * {@code mssql.driver.retry}, {@code mssql.driver.connection.retry_decision}, {@code mssql.driver.redirect},
     * {@code mssql.driver.timeout}, and {@code mssql.driver.authentication}. At most 128 events are retained; omitted
     * events are counted in {@code mssql.connection.diagnostic_events_dropped} on the root. Events contain no raw
     * exceptions, messages, endpoints, tokens, callback class names or credential values. A retry decision does not
     * imply that another attempt started, or authorize statement/transaction replay.
     *
     * @return immutable list of immutable diagnostic event maps; empty on other boundaries
     */
    public List<Map<String, Object>> getDiagnosticEvents() {
        return diagnosticEvents;
    }
}