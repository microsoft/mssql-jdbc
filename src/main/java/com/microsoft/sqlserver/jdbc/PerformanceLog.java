/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.logging.Level;
import java.util.logging.Logger;

public class PerformanceLog {

    static final java.util.logging.Logger perfLoggerConnection = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.PerformanceMetrics.Connection");

    private static PerformanceLogCallback callback;

    public static void setCallback(PerformanceLogCallback cb) {
        callback = cb;
    }

    //TODO
    //More loggers to be added here e.g. com.microsoft.sqlserver.jdbc.PerformanceMetrics.Statement
    
    public static class Scope implements AutoCloseable {
        private Logger logger;
        private String logPrefix;
        private PerformanceActivity activity;
        private long startTime;
        private final boolean enabled;

        private Exception exception;

        public Scope(Logger logger, String logPrefix, PerformanceActivity activity) {

            this.enabled = logger.isLoggable(Level.INFO) || (callback != null);

            if (enabled) {
                this.logger = logger;
                this.logPrefix = logPrefix;
                this.activity = activity;
                this.startTime = System.currentTimeMillis();
            }
        }

        public void setException(Exception e) {
            this.exception = e;
        }

        @Override
        public void close() {

            if (!enabled) {
                return;
            }

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            String exceptionMsg = (exception != null) ? exception.getMessage() : null;

            if (callback != null) {
                try {
                    callback.publish(duration, activity, exceptionMsg);
                } catch (Exception e) {
                    logger.info(String.format("Failed to publish performance log: %s", e.getMessage()));
                }
            }

            if (logger != null && logger.isLoggable(Level.INFO)) {
                if (exception != null) {
                    logger.info(String.format("%d %s %s, duration: %dms, exception: %s", endTime, logPrefix, activity, duration, exceptionMsg));
                } else {
                    logger.info(String.format("%d %s %s, duration: %dms", endTime, logPrefix, activity, duration));
                }
            }

        }
    }

    public static Scope createScope(Logger logger, String logPrefix, PerformanceActivity activity) {
        return new Scope(logger, logPrefix, activity);
    }
}
