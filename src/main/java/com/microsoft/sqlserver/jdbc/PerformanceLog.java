/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.logging.Level;
import java.util.logging.Logger;

class PerformanceLog {

    static final java.util.logging.Logger perfLoggerConnection = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.PerformanceMetrics.Connection");

    static final java.util.logging.Logger perfLoggerStatement = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.PerformanceMetrics.Statement");

    private static PerformanceLogCallback callback;
    private static boolean callbackInitialized = false;
    private static boolean cachedUseNanos = false;

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

        // Constructor for connection-level activities
        public Scope(Logger logger, int connectionId, PerformanceActivity activity) {
            this(logger, connectionId, 0, null, null, activity);
        }

        // Constructor for statement-level activities
        public Scope(Logger logger, int connectionId, int statementId,
                     SQLServerStatement stmt, String userSql, PerformanceActivity activity) {
            this.enabled = logger.isLoggable(Level.FINE) || (callback != null);
            this.useNanos = cachedUseNanos;

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

            long endTime = useNanos ? System.nanoTime() : System.currentTimeMillis();
            long duration = endTime - startTime;

            if (callback != null) {
                try {
                    if (stmtHandle != null) {
                        // Set the current SQL and statement type for the callback to access via ThreadLocal during publish
                        // Note: we set these before calling publish, and remove them afterward to avoid leaking data across calls
                        currentUserSql.set(userSql);
                        currentStatementType.set(deriveStatementType(stmtHandle));
                    }

                    if (statementId == 0) {
                        callback.publish(activity, connectionId, startTime, endTime, exception);
                    } else {
                        callback.publish(activity, connectionId, statementId, startTime, endTime, exception);
                    }
                } catch (Throwable e) {
                    if (logger.isLoggable(Level.WARNING)) {
                        logger.log(Level.WARNING, "PerformanceLogCallback.publish threw unexpectedly; event dropped", e);
                    }
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

    public static Scope createScope(Logger logger, int connectionId, PerformanceActivity activity) {
        return new Scope(logger, connectionId, activity);
    }

    public static Scope createScope(Logger logger, int connectionId, int statementId,
                                    SQLServerStatement stmt, String userSql, PerformanceActivity activity) {
        return new Scope(logger, connectionId, statementId, stmt, userSql, activity);
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
