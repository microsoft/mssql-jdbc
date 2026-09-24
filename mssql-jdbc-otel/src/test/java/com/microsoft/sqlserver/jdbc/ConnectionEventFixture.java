/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


/** Test-only access to real core events/classification; no production API is widened. */
public final class ConnectionEventFixture {
    private ConnectionEventFixture() {}

    public static String driverUserAgent() {
        return SQLServerConnection.userAgentStr;
    }

    public static String classifiedErrorType(Throwable failure, String phase, String resourceKey, boolean callback) {
        return ConnectionTelemetryError.classify(failure, phase, resourceKey, callback).errorType;
    }

    public static Map<String, Object> classifiedErrorAttributes(Throwable failure, String phase, String resourceKey,
            boolean callback) {
        return ConnectionTelemetryError.classify(failure, phase, resourceKey, callback).attributes;
    }

    public static PerformanceLogEvent event(PerformanceLogEvent.Type type, long id, long parent, long root,
            PerformanceActivity activity, long start, long duration, Exception failure, String phase, boolean origin,
            Map<String, Object> extra) {
        Map<String, Object> attributes = new LinkedHashMap<>(extra);
        attributes.put("db.system.name", "microsoft.sql_server");
        Map<String, Object> error = Collections.emptyMap();
        if (type == PerformanceLogEvent.Type.END) {
            if (failure != null) {
                ConnectionTelemetryError classified = ConnectionTelemetryError.classify(failure, phase);
                attributes.put("mssql.error.category", classified.category);
                attributes.put("error.type", classified.errorType);
                if (origin) {
                    error = classified.attributes;
                }
                if (id == root) {
                    attributes.put("mssql.connection.failure_phase", classified.phase);
                    String outcome = "failure";
                    if ("timeout".equals(classified.category) || "canceled".equals(classified.category)) {
                        outcome = classified.category;
                    }
                    attributes.put("mssql.connection.outcome", outcome);
                }
            } else if (id == root) {
                attributes.put("mssql.connection.outcome", "success");
            }
        }
        return new PerformanceLogEvent(type, id, parent, root, 7, activity, start,
                type == PerformanceLogEvent.Type.END ? start + duration : 0,
                type == PerformanceLogEvent.Type.END ? duration : 0,
                type == PerformanceLogEvent.Type.END ? failure : null, phase, attributes, error);
    }

    public static void failThroughCore(Exception original) throws Exception {
        Logger logger = Logger.getLogger("otel.test");
        logger.setLevel(Level.OFF);
        SQLServerConnection connection = new SQLServerConnection("test");
        try (PerformanceLog.Scope root = PerformanceLog.createScope(logger, connection,
                PerformanceActivity.CONNECTION)) {
            root.setException(original);
            throw original;
        }
    }

    public static PerformanceLogEvent withErrorAttributes(PerformanceLogEvent event, Map<String, Object> errors) {
        return new PerformanceLogEvent(event.getType(), event.getScopeId(), event.getParentScopeId(),
                event.getRootScopeId(), event.getConnectionId(), event.getActivity(), event.getStartEpochNanos(),
                event.getEndEpochNanos(), event.getDurationNanos(), event.getException(), event.getFailurePhase(),
                event.getAttributes(), errors);
    }

    public static PerformanceLogEvent withoutMetadata(PerformanceLogEvent event) {
        return new PerformanceLogEvent(event.getType(), event.getScopeId(), event.getParentScopeId(),
                event.getRootScopeId(), 0, event.getActivity(), event.getStartEpochNanos(), event.getEndEpochNanos(),
                event.getDurationNanos(), event.getException(), null, Collections.emptyMap(), Collections.emptyMap());
    }

    public static PerformanceLogEvent withException(PerformanceLogEvent event, Exception failure) {
        return new PerformanceLogEvent(event.getType(), event.getScopeId(), event.getParentScopeId(),
                event.getRootScopeId(), event.getConnectionId(), event.getActivity(), event.getStartEpochNanos(),
                event.getEndEpochNanos(), event.getDurationNanos(), failure, event.getFailurePhase(),
                event.getAttributes(), event.getErrorAttributes(), event.getDiagnosticEvents());
    }

    public static PerformanceLogEvent withDiagnostics(PerformanceLogEvent event,
            List<Map<String, Object>> diagnostics) {
        return new PerformanceLogEvent(event.getType(), event.getScopeId(), event.getParentScopeId(),
                event.getRootScopeId(), event.getConnectionId(), event.getActivity(), event.getStartEpochNanos(),
                event.getEndEpochNanos(), event.getDurationNanos(), event.getException(), event.getFailurePhase(),
                event.getAttributes(), event.getErrorAttributes(), diagnostics);
    }
}
