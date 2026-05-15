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

    /**
     * Register a callback for performance log events.
     *
     * @param cb The callback to register.
     */
    public static synchronized void registerCallback(PerformanceLogCallback cb) {
        if (callbackInitialized) {
            throw new IllegalStateException("Callback has already been set");
        }
        callback = cb;
        callbackInitialized = true;
    }

    /**
     * Unregister the callback for performance log events.
     */
    public static synchronized void unregisterCallback() {
        callback = null;
        callbackInitialized = false;
    }

    //TODO
    //More loggers to be added here e.g. com.microsoft.sqlserver.jdbc.PerformanceMetrics.Statement
    
    public static class Scope implements AutoCloseable {
        private Logger logger;
        private int connectionId;
        private int statementId;
        private PerformanceActivity activity;
        private long startTime;
        private final boolean enabled;
        private final boolean nanos;

        private Exception exception;

        // Constructor for connection-level activities
        public Scope(Logger logger, int connectionId, PerformanceActivity activity) {
            this(logger, connectionId, 0, activity);
        }

        // Constructor for statement-level activities
        public Scope(Logger logger, int connectionId, int statementId, PerformanceActivity activity) {
            this.enabled = logger.isLoggable(Level.FINE) || (callback != null);
            boolean useNanos = false;
            if (callback != null) {
                try {
                    useNanos = callback.useNanoseconds();
                } catch (Exception e) {
                    logger.fine(String.format("Failed to call useNanoseconds(), defaulting to milliseconds: %s", e.getMessage()));
                }
            }
            this.nanos = useNanos;

            if (enabled) {
                this.logger = logger;
                this.connectionId = connectionId;
                this.statementId = statementId;
                this.activity = activity;
                this.startTime = nanos ? System.nanoTime() : System.currentTimeMillis();
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

            long duration = nanos ? (System.nanoTime() - startTime) : (System.currentTimeMillis() - startTime);

            if (callback != null) {
                try {
                    if (statementId == 0) {
                        callback.publish(activity, connectionId, duration, exception);
                    } else {
                        callback.publish(activity, connectionId, statementId, duration, exception);
                    }
                } catch (Exception e) {
                    logger.fine(String.format("Failed to publish performance log: %s", e.getMessage()));
                }
            }

            if (logger != null && logger.isLoggable(Level.FINE)) {
                String unit = nanos ? "ns" : "ms";
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

    public static Scope createScope(Logger logger, int connectionId, Integer statementId, PerformanceActivity activity) {
        return new Scope(logger, connectionId, statementId, activity);
    }

}
