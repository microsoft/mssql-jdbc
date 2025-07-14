package com.microsoft.sqlserver.jdbc;

import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PerformanceLog {

    static final java.util.logging.Logger perfLoggerConnection = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.PerformanceMetrics.Connection");

    public static class Scope implements AutoCloseable {
        private final Logger logger;
        private final UUID connId;
        private final PerformanceActivity activity;
        private final long startTime;
        private final boolean enabled;

        public Scope(Logger logger, UUID connId, PerformanceActivity activity) {
            this.logger = logger;
            this.connId = connId;
            this.activity = activity;
            this.enabled = logger.isLoggable(Level.INFO);
            this.startTime = enabled ? System.currentTimeMillis() : 0;

            if (enabled) {
                logger.info(String.format("%d %s %s start", startTime, connId, activity));
            }
        }

        @Override
        public void close() {
            if (enabled) {
                long endTime = System.currentTimeMillis();
                long duration = endTime - startTime;
                logger.info(String.format("%d %s %s end, duration: %dms", endTime, connId, activity, duration));
            }
        }
    }

    public static Scope createScope(Logger logger, UUID connId, PerformanceActivity activity) {
        return new Scope(logger, connId, activity);
    }
}
