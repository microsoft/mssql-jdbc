/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.logging.Level;
import java.util.logging.Logger;

class PerformanceLog {

    static final java.util.logging.Logger perfLoggerConnection = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.PerformanceMetrics.Connection");

    static final java.util.logging.Logger perfLoggerStatement = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.PerformanceMetrics.Statement");

    private static final Logger telemetryLogger = Logger.getLogger("com.microsoft.sqlserver.jdbc.Telemetry");

    private static PerformanceLogCallback callback;
    private static TelemetryBridge telemetryBridge;
    private static boolean callbackInitialized = false;
    private static boolean cachedUseNanos = false;
    private static boolean telemetryBridgeDiscoveryAttempted = false;
    private static boolean telemetryBridgeMissingLogged = false;

    // ThreadLocal to hold current SQL text and statement type for the duration of a publish callback
    static final ThreadLocal<String> currentUserSql = new ThreadLocal<>();
    static final ThreadLocal<StatementType> currentStatementType = new ThreadLocal<>();

    /**
     * Register a callback for performance log events.
     * The value of {@link PerformanceLogCallback#useNanoseconds()} is captured at registration
     * time and remains fixed for the lifetime of this callback. To change the duration unit,
     * unregister and re-register with the new setting.
     *
     * @param cb The callback to register.
     */
    public static synchronized void registerCallback(PerformanceLogCallback cb) {
        if (callbackInitialized) {
            throw new IllegalStateException("Callback has already been set");
        }
        callback = cb;
        cachedUseNanos = cb.useNanoseconds();
        callbackInitialized = true;
    }

    /**
     * Unregister the callback for performance log events.
     */
    public static synchronized void unregisterCallback() {
        callback = null;
        cachedUseNanos = false;
        callbackInitialized = false;
    }

    /**
     * Register an optional telemetry bridge that receives performance events as a typed payload.
     *
     * @param bridge the bridge implementation to invoke
     */
    public static synchronized void registerTelemetryBridge(TelemetryBridge bridge) {
        telemetryBridge = bridge;
        telemetryBridgeDiscoveryAttempted = bridge != null;
        telemetryBridgeMissingLogged = false;
    }

    /**
     * Unregister any previously registered telemetry bridge.
     */
    public static synchronized void unregisterTelemetryBridge() {
        telemetryBridge = null;
        telemetryBridgeDiscoveryAttempted = false;
        telemetryBridgeMissingLogged = false;
    }

    private static synchronized TelemetryBridge resolveTelemetryBridge() {
        if (telemetryBridge != null) {
            return telemetryBridge;
        }
        if (telemetryBridgeDiscoveryAttempted) {
            if (!telemetryBridgeMissingLogged) {
                telemetryLogger.log(Level.SEVERE,
                        "No TelemetryBridge implementation was found on the classpath. Add the optional mssql-jdbc-otel jar to enable telemetry publishing.");
                telemetryBridgeMissingLogged = true;
            }
            return null;
        }

        telemetryBridgeDiscoveryAttempted = true;
        try {
            Iterator<TelemetryBridge> providers = ServiceLoader.load(TelemetryBridge.class).iterator();
            if (providers.hasNext()) {
                telemetryBridge = providers.next();
            } else {
                telemetryLogger.log(Level.SEVERE,
                        "No TelemetryBridge implementation was found on the classpath. Add the optional mssql-jdbc-otel jar to enable telemetry publishing.");
                telemetryBridgeMissingLogged = true;
            }
        } catch (ServiceConfigurationError e) {
            telemetryLogger.log(Level.SEVERE,
                    "Failed to discover a TelemetryBridge implementation on the classpath. Telemetry publishing will be skipped.", e);
            telemetryBridgeMissingLogged = true;
        }

        return telemetryBridge;
    }

    public static class Scope implements AutoCloseable {
        private Logger logger;
        private int connectionId;
        private int statementId;
        private PerformanceActivity activity;
        private long startTime;
        private final boolean enabled;
        private final boolean useNanos;

        private Exception exception;
        private SQLServerStatement stmtHandle;
        private String userSql;
        private final Map<String, Object> attributes;

        // Constructor for connection-level activities
        public Scope(Logger logger, int connectionId, PerformanceActivity activity, SQLServerConnection connection) {
            this(logger, connectionId, 0, null, null, activity, connection);
        }

        // Constructor for statement-level activities
        public Scope(Logger logger, int connectionId, int statementId,
                     SQLServerStatement stmt, String userSql, PerformanceActivity activity,
                     SQLServerConnection connection) {
            this.enabled = logger.isLoggable(Level.FINE) || (callback != null);
            this.useNanos = cachedUseNanos;
            this.attributes = buildAttributes(connection);

            if (enabled) {
                this.logger = logger;
                this.connectionId = connectionId;
                this.statementId = statementId;
                this.activity = activity;
                this.startTime = useNanos ? System.nanoTime() : System.currentTimeMillis();

                // If we have a callback and statement info, capture it for use during publish
                if (callback != null && stmt != null) {
                    this.stmtHandle = stmt;
                    this.userSql = userSql;
                }
            }
        }

        public void setException(Exception e) {
            this.exception = e;
        }

        private String getTraceId() {
            if (statementId != 0) {
                return "ConnectionID:" + connectionId + ", StatementID:" + statementId;
            }
            return "ConnectionID:" + connectionId;
        }

        @Override
        public void close() {

            if (!enabled) {
                return;
            }

            long duration = useNanos ? (System.nanoTime() - startTime) : (System.currentTimeMillis() - startTime);

            TelemetryBridge bridge = resolveTelemetryBridge();
            if (callback != null || bridge != null) {
                try {
                    if (stmtHandle != null) {
                        // Set the current SQL and statement type for the callback to access via ThreadLocal during publish
                        // Note: we set these before calling publish, and remove them afterward to avoid leaking data across calls
                        currentUserSql.set(userSql);
                        currentStatementType.set(deriveStatementType(stmtHandle));
                    }

                    if (callback != null) {
                        if (statementId == 0) {
                            callback.publish(activity, connectionId, duration, exception);
                        } else {
                            callback.publish(activity, connectionId, statementId, duration, exception);
                        }
                    }

                    if (bridge != null) {
                        TelemetryEvent event = new TelemetryEvent(activity, connectionId, statementId, duration,
                                exception, currentUserSql.get(), currentStatementType.get(), Collections.emptyMap(),
                                null, null, null, null, useNanos ? "ns" : "ms", attributes);
                        bridge.publish(event);
                    }
                } catch (Exception e) {
                    logger.fine(String.format("Failed to publish performance log: %s", e.getMessage()));
                } finally {
                    if (stmtHandle != null) {
                        currentUserSql.remove();
                        currentStatementType.remove();
                    }
                }
            }

            if (logger != null && logger.isLoggable(Level.FINE)) {
                String unit = useNanos ? "ns" : "ms";
                if (exception != null) {
                    logger.fine(String.format("%s %s, duration: %d%s, exception: %s", getTraceId(), activity, duration, unit, exception.getMessage()));
                } else {
                    logger.fine(String.format("%s %s, duration: %d%s", getTraceId(), activity, duration, unit));
                }
            }
        }
    }

    public static Scope createScope(Logger logger, int connectionId, PerformanceActivity activity,
                                    SQLServerConnection connection) {
        return new Scope(logger, connectionId, activity, connection);
    }

    public static Scope createScope(Logger logger, int connectionId, int statementId,
                                    SQLServerStatement stmt, String userSql, PerformanceActivity activity,
                                    SQLServerConnection connection) {
        return new Scope(logger, connectionId, statementId, stmt, userSql, activity, connection);
    }

    private static Map<String, Object> buildAttributes(SQLServerConnection connection) {
        Map<String, Object> attrs = new LinkedHashMap<>();
        attrs.put("mssql.jdbc.useragent", SQLServerConnection.userAgentStr);
        if (connection != null) {
            addIfNotEmpty(attrs, "mssql.jdbc.otel.profile", connection.getOtelProfile());
            addIfNotEmpty(attrs, "mssql.jdbc.otel.auth", connection.getOtelAuth());
            addIfNotEmpty(attrs, "mssql.jdbc.otel.endpoint", connection.getOtelEndpoint());
            addIfNotEmpty(attrs, "mssql.jdbc.otel.access_token_callback_class",
                    connection.getOtelAccessTokenCallbackClass());
        }
        return attrs;
    }

    private static void addIfNotEmpty(Map<String, Object> attrs, String key, String value) {
        if (value != null && !value.isEmpty()) {
            attrs.put(key, value);
        }
    }

    // Helper method to derive statement type based on the statement class
    private static StatementType deriveStatementType(SQLServerStatement stmt) {
        if (stmt instanceof SQLServerCallableStatement) {
            return StatementType.CALLABLE_STATEMENT;
        }
        if (stmt instanceof SQLServerPreparedStatement) {
            return StatementType.PREPARED_STATEMENT;
        }
        return StatementType.STATEMENT;
    }

}
