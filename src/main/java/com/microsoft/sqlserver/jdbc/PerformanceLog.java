/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.Properties;
import java.util.ServiceConfigurationError;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

class PerformanceLog {

    static final java.util.logging.Logger perfLoggerConnection = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.PerformanceMetrics.Connection");

    static final java.util.logging.Logger perfLoggerStatement = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.PerformanceMetrics.Statement");

    private static volatile Registration registration;

    private static final class Registration {
        final PerformanceLogCallback callback;
        final boolean useNanos;
        final boolean lifecycleEnabled;

        Registration(PerformanceLogCallback callback) {
            this.callback = callback;
            useNanos = callback.useNanoseconds();
            boolean enabled = false;
            try {
                enabled = callback.getClass().getMethod("publish", PerformanceLogEvent.class)
                        .getDeclaringClass() != PerformanceLogCallback.class;
            } catch (ReflectiveOperationException | SecurityException e) {
                // Legacy callbacks remain usable when lifecycle capability cannot be established.
            }
            lifecycleEnabled = enabled;
        }
    }

    // ThreadLocal to hold current SQL text and statement type for the duration of a publish callback
    static final ThreadLocal<String> currentUserSql = new ThreadLocal<>();
    static final ThreadLocal<StatementType> currentStatementType = new ThreadLocal<>();
    static final ThreadLocal<String> currentApplicationName = new ThreadLocal<>();

    /**
     * Register a callback for performance log events.
     * The value of {@link PerformanceLogCallback#useNanoseconds()} is captured at registration
     * time and remains fixed for the lifetime of this callback. To change the duration unit,
     * unregister and re-register with the new setting.
     *
     * @param cb The callback to register.
     */
    public static synchronized void registerCallback(PerformanceLogCallback cb) {
        if (registration != null) {
            throw new IllegalStateException("Callback has already been set");
        }
        registration = new Registration(cb);
    }

    /**
     * Unregister the callback for performance log events.
     */
    public static synchronized void unregisterCallback() {
        registration = null;
    }

    public static class Scope implements AutoCloseable {
        private static final Scope NOOP = new Scope();
        private Logger logger;
        private SQLServerConnection con;
        private int connectionId;
        private int statementId;
        private PerformanceActivity activity;
        private long startTime;
        private final boolean enabled;
        private final boolean useNanos;
        private final PerformanceLogCallback lifecycleCallback;
        private final ConnectionPerformanceState.Node lifecycle;
        private boolean closed;

        private Exception exception;
        private SQLServerStatement stmtHandle;
        private String userSql;

        private Scope() {
            enabled = false;
            useNanos = false;
            lifecycleCallback = null;
            lifecycle = null;
            closed = true;
        }

        // Constructor for connection-level activities
        public Scope(Logger logger, SQLServerConnection con, PerformanceActivity activity) {
            this(logger, con, 0, null, null, activity);
        }

        // Constructor for statement-level activities
        public Scope(Logger logger, SQLServerConnection con, int statementId,
                     SQLServerStatement stmt, String userSql, PerformanceActivity activity) {
            this(logger, con, statementId, stmt, userSql, activity, true);
        }

        private Scope(Logger logger, SQLServerConnection con, int statementId,
            SQLServerStatement stmt, String userSql, PerformanceActivity activity, boolean allowLifecycle) {
            Registration registered = registration;
            this.enabled = !activity.isLifecycleOnly() && (logger.isLoggable(Level.FINE) || registered != null);
            this.useNanos = registered != null && registered.useNanos;
            lifecycleCallback = registered == null || !registered.lifecycleEnabled ? null : registered.callback;

            if (enabled) {
                this.logger = logger;
                this.con = con;
                this.connectionId = (con != null) ? con.getConnectionID() : 0;
                this.statementId = statementId;
                this.activity = activity;
                this.startTime = useNanos ? System.nanoTime() : System.currentTimeMillis();

                // If we have a callback and statement info, capture it for use during publish
                if (registered != null && stmt != null) {
                    this.stmtHandle = stmt;
                    this.userSql = userSql;
                }
            }
                lifecycle = allowLifecycle && lifecycleCallback != null && statementId == 0 && stmt == null
                    && activity.connectionPhase() != null
                    && (con == null || !con.getSessionRecovery().isReconnectRunning())
                    && (activity == PerformanceActivity.CONNECTION || ConnectionPerformanceState.current(con) != null)
                        ? ConnectionPerformanceState.enter(con, activity) : null;
            if (lifecycle != null && lifecycleCallback != null) {
                lifecycle.scope = this;
                boolean started = false;
                try {
                    notifyLifecycle(false);
                    started = true;
                } finally {
                    // Fatal VM failures must not leave thread-owned roots behind either.
                    if (!started) {
                        ConnectionPerformanceState.exit(lifecycle);
                    }
                }
            }
        }

        public void setException(Exception e) {
            if (closed) {
                return;
            }
            this.exception = e;
            if (lifecycle != null && !closed) {
                lifecycle.fail(e, null);
            }
        }

        private void notifyLifecycle(boolean end) {
            if (lifecycleCallback != null) {
                try {
                    PerformanceLogEvent event = lifecycle.event(end);
                    lifecycleCallback.publish(event);
                } catch (Exception | LinkageError | ServiceConfigurationError e) {
                    // SDK/exporter failures are not SQL errors. Do not log their messages or throwable objects.
                    logCallbackFailure();
                }
            }
        }

        private String getTraceId() {
            if (statementId != 0) {
                return "ConnectionID:" + connectionId + ", StatementID:" + statementId;
            }
            return "ConnectionID:" + connectionId;
        }

        /**
         * Resolves the application name lazily, at publish time rather than at scope creation time.
         * The CONNECTION scope is opened before the connection properties have been parsed, so the
         * value is not available when the scope is constructed.
         */
        private String getApplicationName() {
            return (con != null) ? con.getApplicationName() : null;
        }

        @Override
        public void close() {
            if (closed || (lifecycle != null && !lifecycle.isOwner())) {
                return;
            }
            closed = true;
            // Capture legacy elapsed time before callbacks, preserving its close-time measurement.
            long duration = enabled
                    ? (useNanos ? System.nanoTime() - startTime : System.currentTimeMillis() - startTime) : 0;
            try {
                if (lifecycle != null) {
                    notifyLifecycle(true);
                }
            } finally {
                if (lifecycle != null) {
                    ConnectionPerformanceState.exit(lifecycle);
                }
                publishLegacy(duration);
            }
        }

        private void publishLegacy(long duration) {
            if (!enabled) {
                return;
            }

            Registration registered = registration;
            PerformanceLogCallback callback = registered == null ? null : registered.callback;
            if (callback != null) {
                try {
                    // Set the current context for the callback to access via ThreadLocal during publish
                    // Note: we set these before calling publish, and remove them afterward to avoid leaking data across calls
                    currentApplicationName.set(getApplicationName());

                    if (stmtHandle != null) {
                        currentUserSql.set(userSql);
                        currentStatementType.set(deriveStatementType(stmtHandle));
                    }

                    if (statementId == 0) {
                        callback.publish(activity, connectionId, duration, exception);
                    } else {
                        callback.publish(activity, connectionId, statementId, duration, exception);
                    }
                } catch (Exception | LinkageError | ServiceConfigurationError e) {
                    logCallbackFailure();
                } finally {
                    currentApplicationName.remove();
                    if (stmtHandle != null) {
                        currentUserSql.remove();
                        currentStatementType.remove();
                    }
                }
            }

            if (logger != null && logger.isLoggable(Level.FINE)) {
                String unit = useNanos ? "ns" : "ms";
                if (exception != null && statementId != 0) {
                    logger.fine(String.format("%s %s, duration: %d%s, exception: %s", getTraceId(), activity, duration, unit, exception.getMessage()));
                } else {
                    logger.fine(String.format("%s %s, duration: %d%s", getTraceId(), activity, duration, unit));
                }
            }
        }
    }

    private static void logCallbackFailure() {
        if (perfLoggerConnection.isLoggable(Level.FINE)) {
            perfLoggerConnection.fine("Performance callback failed; SQL operation is unaffected.");
        }
    }

    /** Lifecycle-only factory. New phase activities never enter the legacy publish stream. */
    static Scope createConnectionScope(SQLServerConnection con, PerformanceActivity activity) {
        if (activity.connectionPhase() == null) {
            throw new IllegalArgumentException("Not a connection lifecycle activity");
        }
        if (activity.isLifecycleOnly() && !isConnectionLifecycleActive(con)) {
            return Scope.NOOP;
        }
        return createScope(perfLoggerConnection, con, activity);
    }

    static boolean isConnectionLifecycleActive(SQLServerConnection con) {
        Registration registered = registration;
        return registered != null && registered.lifecycleEnabled
                && (con == null || !con.getSessionRecovery().isReconnectRunning())
                && ConnectionPerformanceState.current(con) != null;
    }

    static Scope createConnectionOpenScope(SQLServerConnection con, boolean newOpen) {
        if (registration == null && !perfLoggerConnection.isLoggable(Level.FINE)) {
            return Scope.NOOP;
        }
        return new Scope(perfLoggerConnection, con, 0, null, null, PerformanceActivity.CONNECTION, newOpen);
    }

    /** LOGINACK accepted this endpoint. Initialization remains in its original SQL execution location. */
    static void completeConnectionAttempt(SQLServerConnection con) {
        ConnectionPerformanceState.Node node = ConnectionPerformanceState.current(con);
        if (node != null && node.activity == PerformanceActivity.CONNECTION_ATTEMPT) {
            node.scope.close();
        }
    }

    /** Current phase for this connection on this thread, or null outside a connection scope. */
    static String getConnectionPhase(SQLServerConnection con) {
        ConnectionPerformanceState.Node node = ConnectionPerformanceState.current(con);
        return node == null ? null : node.activity.connectionPhase();
    }

    /** Capture at the failure site, before wrappers discard the original cause. */
    static void recordConnectionFailure(SQLServerConnection con, Exception exception) {
        recordConnectionFailure(con, exception, null);
    }

    /** The optional resource key must be the literal key used at the source, never inferred from text. */
    static void recordConnectionFailure(SQLServerConnection con, Exception exception, String resourceKey) {
        ConnectionPerformanceState.Node node = ConnectionPerformanceState.current(con);
        if (node != null) {
            node.fail(exception, resourceKey);
        }
    }

    static void recordTokenCallbackFailure(SQLServerConnection con, Exception exception) {
        ConnectionPerformanceState.Node node = ConnectionPerformanceState.current(con);
        if (node != null) {
            node.fail(exception, null, "token_acquisition", true);
        }
    }

    /** Call only after merging, default resolution and validation; no properties are retained. */
    static void setConnectionSettings(SQLServerConnection con, Properties validatedEffectiveSettings) {
        ConnectionPerformanceState.settings(con, validatedEffectiveSettings);
    }

    /** Capture the actual allocated TDS UUID, not a possibly stale UUID left on the connection by a previous attempt. */
    static void setAttemptClientConnectionId(SQLServerConnection con, UUID id) {
        for (ConnectionPerformanceState.Node node = ConnectionPerformanceState.current(con); node != null;
                node = node.parent) {
            if (node.activity == PerformanceActivity.CONNECTION_ATTEMPT) {
                node.clientConnectionId = id;
                break;
            }
        }
    }

    public static Scope createScope(Logger logger, SQLServerConnection con, PerformanceActivity activity) {
        if (registration == null && !logger.isLoggable(Level.FINE)) {
            return Scope.NOOP;
        }
        return new Scope(logger, con, activity);
    }

    public static Scope createScope(Logger logger, SQLServerConnection con, int statementId,
                                    SQLServerStatement stmt, String userSql, PerformanceActivity activity) {
        return new Scope(logger, con, statementId, stmt, userSql, activity);
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
