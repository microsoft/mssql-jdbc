/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) 2016 Microsoft Corporation All rights reserved. This program is
 * made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.testframework.AbstractTest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import static org.junit.jupiter.api.Assertions.assertTrue;

class PerformanceLogCallbackTest extends AbstractTest {

    private static final Logger perfLogger = Logger.getLogger("com.microsoft.sqlserver.jdbc.PerformanceMetrics.Connection");
    private static final Path logPath = Paths.get("performance.log");
    private static FileHandler perfLogHandler;

    static {
        try {
            perfLogHandler = new FileHandler("performance.log", true);
            perfLogHandler.setFormatter(new SimpleFormatter());
            perfLogger.addHandler(perfLogHandler);
            perfLogger.setLevel(Level.FINE);
            perfLogger.setUseParentHandlers(false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @AfterEach
    void cleanUpAfter() throws IOException {
        // Remove and close handler before deleting file
        if (perfLogHandler != null) {
            perfLogger.removeHandler(perfLogHandler);
            perfLogHandler.close();
        }
        Files.deleteIfExists(logPath);
    }

    /**
     * Test to validate the PerformanceLogCallback is called and the log file is created.
     */
    @Test
    void testPublishIsCalledAndLogFileCreated() throws Exception {
        AtomicBoolean called = new AtomicBoolean(false);

        PerformanceLogCallback callbackInstance = new PerformanceLogCallback() {
            @Override
            public void publish(PerformanceActivity activity, int connectionId, long durationMs, Exception exception) {
                called.set(true);
                System.out.println("PerformanceLogCallback.publish called for connection-level activity");
            }

            @Override
            public void publish(PerformanceActivity activity, int connectionId, int statementId, long durationMs,
                    Exception exception) throws Exception {
                called.set(true);
                System.out.println("PerformanceLogCallback.publish called for statement-level activity");
            }
        };

        SQLServerDriver.registerPerformanceLogCallback(callbackInstance);

        try (Connection con = getConnection()) {
            DatabaseMetaData metaData = con.getMetaData();
            System.out.println("Database Product Name: " + metaData.getDatabaseProductName());
            System.out.println("Database Product Version: " + metaData.getDatabaseProductVersion());
        }

        assertTrue(called.get(), "PerformanceLogCallback.publish should have been called");
        assertTrue(callbackInstance instanceof PerformanceLogCallback, "Callback must implement PerformanceLogCallback");
        assertTrue(Files.exists(logPath), "performance.log file should exist");
        System.out.println("Log file absolute path: " + logPath.toAbsolutePath());

        SQLServerDriver.unregisterPerformanceLogCallback();

    }
}