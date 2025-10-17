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
        private PerformanceActivity activity;
        private long startTime;
        private final boolean enabled;

        private Exception exception;

        public Scope(Logger logger, int connectionId, PerformanceActivity activity) {

            // Check if logging is enabled
            this.enabled = logger.isLoggable(Level.INFO) || (callback != null);

            if (enabled) {
                this.logger = logger;
                this.connectionId = connectionId;
                this.activity = activity;
                this.startTime = System.currentTimeMillis();
            }
        }

        public void setException(Exception e) {
            this.exception = e;
        }

        private String getTraceId() {
            return "ConnectionID:" + connectionId;
        }

        @Override
        public void close() {

            if (!enabled) {
                return;
            }

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;

            if (callback != null) {
                try {
                    callback.publish(activity, connectionId, duration, exception);
                } catch (Exception e) {
                    logger.info(String.format("Failed to publish performance log: %s", e.getMessage()));
                }
            }

            if (logger != null && logger.isLoggable(Level.INFO)) {
                if (exception != null) {
                    logger.info(String.format("%s %s, duration: %dms, exception: %s", getTraceId(), activity, duration, exception.getMessage()));
                } else {
                    logger.info(String.format("%s %s, duration: %dms", getTraceId(), activity, duration));
                }
            }

        }
    }

    public static Scope createScope(Logger logger, int connectionId, PerformanceActivity activity) {
        return new Scope(logger, connectionId, activity);
    }
}
