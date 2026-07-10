package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

import org.junit.jupiter.api.Test;

class PerformanceLogTelemetryBridgeTest {

    @Test
    void missingBridgeLogsErrorAndSkipsPublishing() {
        Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.PerformanceLogTelemetryBridgeTest");
        logger.setUseParentHandlers(false);
        logger.setLevel(Level.ALL);

        Logger telemetryLogger = Logger.getLogger("com.microsoft.sqlserver.jdbc.Telemetry");
        telemetryLogger.setUseParentHandlers(false);
        telemetryLogger.setLevel(Level.ALL);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Handler handler = new StreamHandler(output, new SimpleFormatter());
        telemetryLogger.addHandler(handler);

        PerformanceLog.unregisterCallback();
        PerformanceLog.unregisterTelemetryBridge();

        PerformanceLog.Scope scope = PerformanceLog.createScope(logger, 1, PerformanceActivity.CONNECTION, null);
        scope.close();

        handler.flush();
        telemetryLogger.removeHandler(handler);

        String logOutput = output.toString(StandardCharsets.UTF_8);
        assertTrue(logOutput.contains("SEVERE"), "Expected an error log when no telemetry bridge is available");
        assertTrue(logOutput.contains("mssql-jdbc-otel"), "Expected the log to mention the companion jar");
    }
}
