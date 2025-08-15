package com.microsoft.sqlserver.jdbc;

import java.util.logging.Level;
import java.util.logging.Logger;

public class PerformanceLog {

    static final java.util.logging.Logger perfLoggerConnection = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.PerformanceMetrics.Connection");

    //TODO
    //More loggers to be added here e.g. com.microsoft.sqlserver.jdbc.PerformanceMetrics.Statement
    
    public static class Scope implements AutoCloseable {
        private Logger logger;
        private String logPrefix;
        private PerformanceActivity activity;
        private long startTime;
        private final boolean enabled;

        public Scope(Logger logger, String logPrefix, PerformanceActivity activity) {
            this.enabled = logger.isLoggable(Level.INFO);
            if (enabled) {
                this.logger = logger;
                this.logPrefix = logPrefix;
                this.activity = activity;
                this.startTime = enabled ? System.currentTimeMillis() : 0;
                logger.info(String.format("%d %s %s start", startTime, logPrefix, activity));
            }
        }

        @Override
        public void close() {
            if (enabled) {
                long endTime = System.currentTimeMillis();
                long duration = endTime - startTime;
                logger.info(String.format("%d %s %s end, duration: %dms", endTime, logPrefix, activity, duration));
            }
        }
    }

    public static Scope createScope(Logger logger, String logPrefix, PerformanceActivity activity) {
        return new Scope(logger, logPrefix, activity);
    }
}
