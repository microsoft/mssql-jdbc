/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) 2016 Microsoft Corporation All rights reserved. This program is
 * made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.PrepUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class to validate PerformanceLogCallback functionality and performance overhead.
 */
class PerformanceLogCallbackTest extends AbstractTest {

    private static final Logger perfLoggerConnection = Logger.getLogger("com.microsoft.sqlserver.jdbc.PerformanceMetrics.Connection");
    private static final Logger perfLoggerStatement = Logger.getLogger("com.microsoft.sqlserver.jdbc.PerformanceMetrics.Statement");
    private static final Path logPath = Paths.get("performance.log");
    private static FileHandler perfLogHandler;

    private static final String tableName = RandomUtil.getIdentifier("BatchPerfTest");

    @BeforeEach
    void setUp() throws IOException {
        // Create file handler for each test
        if (perfLogHandler == null) {
            perfLogHandler = new FileHandler("performance.log", true);
            perfLogHandler.setFormatter(new SimpleFormatter());
            perfLoggerConnection.addHandler(perfLogHandler);
            perfLoggerConnection.setUseParentHandlers(false);
            perfLoggerStatement.addHandler(perfLogHandler);
            perfLoggerStatement.setUseParentHandlers(false);
        }
        perfLoggerConnection.setLevel(Level.FINE);
        perfLoggerStatement.setLevel(Level.FINE);
    }

    @AfterEach
    void cleanUpAfter() throws IOException {

        SQLServerDriver.unregisterPerformanceLogCallback();

        // Remove and close handler before deleting file
        if (perfLogHandler != null) {
            perfLoggerConnection.removeHandler(perfLogHandler);
            perfLoggerStatement.removeHandler(perfLogHandler);
            perfLogHandler.close();
            perfLogHandler = null;
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
            }

            @Override
            public void publish(PerformanceActivity activity, int connectionId, int statementId, long durationMs,
                    Exception exception) throws Exception {
                called.set(true);
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
        
    }

    /**
     * Test to validate statement-level performance metrics are logged when executing statements.
     */
    @Test
    void testStatementExecutionPerformanceLogging() throws Exception {
        AtomicBoolean called = new AtomicBoolean(false);

        PerformanceLogCallback callbackInstance = new PerformanceLogCallback() {
            @Override
            public void publish(PerformanceActivity activity, int connectionId, long durationMs, Exception exception) {
                called.set(true);
            }

            @Override
            public void publish(PerformanceActivity activity, int connectionId, int statementId, long durationMs,
                    Exception exception) throws Exception {
                called.set(true);
            }
        };

        SQLServerDriver.registerPerformanceLogCallback(callbackInstance);

        try (Connection con = getConnection()) {
            // Test regular Statement execution
            try (Statement stmt = con.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT 1 AS test_column")) {
            }

            // Test PreparedStatement execution
            try (PreparedStatement pstmt = con.prepareStatement("SELECT ? AS param_value")) {
                pstmt.setInt(1, 42);
                try (ResultSet rs = pstmt.executeQuery()) {
                }
            }
        }

        assertTrue(called.get(), "PerformanceLogCallback.publish should have been called");
        assertTrue(callbackInstance instanceof PerformanceLogCallback, "Callback must implement PerformanceLogCallback");
        assertTrue(Files.exists(logPath), "performance.log file should exist");
    }

    /**
     * Performance overhead test for connection and statement operations.
     * Compares execution time with callback/logging disabled vs enabled.
     * For testing purpose - iterations set to 1
     */
    @Test
    void testPerformanceOverhead() throws Exception {
        int[] iterations = {1};
        // Uncomment below line to run with higher iterations
        // int[] iterations = {100, 1000};

        for (int count : iterations) {

            // Test with callback/logging DISABLED
            SQLServerDriver.unregisterPerformanceLogCallback();
            perfLoggerConnection.setLevel(Level.OFF);
            perfLoggerStatement.setLevel(Level.OFF);

            long disabledDuration = executeStatementsWithNewConnectionsPerIteration(count);

            // Test with callback/logging ENABLED
            PerformanceLogCallback callbackInstance = new PerformanceLogCallback() {
                @Override
                public void publish(PerformanceActivity activity, int connectionId, long durationMs, 
                        Exception exception) {
                    // No-op callback to measure overhead
                }

                @Override
                public void publish(PerformanceActivity activity, int connectionId, int statementId, 
                        long durationMs, Exception exception) {
                    // No-op callback to measure overhead
                }
            };
            SQLServerDriver.registerPerformanceLogCallback(callbackInstance);
            perfLoggerConnection.setLevel(Level.FINE);
            perfLoggerStatement.setLevel(Level.FINE);

            long enabledDuration = executeStatementsWithNewConnectionsPerIteration(count);

            // Uncomment below line to print performance comparison for connection overhead test
            // printPerformanceComparison(disabledDuration, enabledDuration, count, "new connection per iteration");

            // Cleanup for next iteration
            SQLServerDriver.unregisterPerformanceLogCallback();
        }
    }

    /**
     * High-volume statement execution performance test.
     * Reuses a single connection to test 10000 statement executions efficiently.
     * Compares performance with logging disabled vs enabled.
     */
    @Test
    void testHighVolumeStatementPerformance() throws Exception {
        int iterations = 10; // Set to 10000 for actual high-volume test

        // Test with callback/logging DISABLED
        SQLServerDriver.unregisterPerformanceLogCallback();
        perfLoggerConnection.setLevel(Level.OFF);
        perfLoggerStatement.setLevel(Level.OFF);

        long disabledDuration;
        try (Connection con = getConnection()) {
            disabledDuration = executeStatementsWithTiming(con, iterations);
        }

        // Test with callback/logging ENABLED
        PerformanceLogCallback callbackInstance = new PerformanceLogCallback() {
            @Override
            public void publish(PerformanceActivity activity, int connectionId, long durationMs, 
                    Exception exception) {
                // No-op callback
            }

            @Override
            public void publish(PerformanceActivity activity, int connectionId, int statementId, 
                    long durationMs, Exception exception) {
                // No-op callback
            }
        };
        SQLServerDriver.registerPerformanceLogCallback(callbackInstance);
        perfLoggerConnection.setLevel(Level.FINE);
        perfLoggerStatement.setLevel(Level.FINE);

        long enabledDuration;
        try (Connection con = getConnection()) {
            enabledDuration = executeStatementsWithTiming(con, iterations);
        }

        // Uncomment below line to print performance comparison for high-volume test
        // printPerformanceComparison(disabledDuration, enabledDuration, iterations, "single connection");

        // Cleanup
        SQLServerDriver.unregisterPerformanceLogCallback();

    }

    /**
     * Test to validate performance logging for batch statement execution.
     * Tests PreparedStatement.executeBatch() with parameterized inserts.
     */
    @Test
    void testBatchStatementPerformanceLogging() throws Exception {
        AtomicBoolean called = new AtomicBoolean(false);
        PerformanceLogCallback callbackInstance = new PerformanceLogCallback() {
            @Override
            public void publish(PerformanceActivity activity, int connectionId, long durationMs, Exception exception) {
                called.set(true);
            }

            @Override
            public void publish(PerformanceActivity activity, int connectionId, int statementId, long durationMs,
                    Exception exception) throws Exception {
                called.set(true);
            }
        };

        SQLServerDriver.registerPerformanceLogCallback(callbackInstance);
        perfLoggerConnection.setLevel(Level.FINE);
        perfLoggerStatement.setLevel(Level.FINE);

        try (Connection con = getConnection()) {
            // Create a test table for batch operations
            try (Statement stmt = con.createStatement()) {
                stmt.execute("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName) + 
                                " (id INT, name NVARCHAR(100), value INT);");
            }

            try {
                // Test PreparedStatement.executeBatch() with 10 batches
                try (PreparedStatement pstmt = con.prepareStatement("INSERT INTO " + 
                                                AbstractSQLGenerator.escapeIdentifier(tableName) + " VALUES (?, ?, ?)")) {
                    for (int i = 0; i < 10; i++) {
                        pstmt.setInt(1, i);
                        pstmt.setString(2, "Name" + i);
                        pstmt.setInt(3, i * 10);
                        pstmt.addBatch();
                    }
                    int[] results1 = pstmt.executeBatch();

                    for (int i = 0; i < 10; i++) {
                        pstmt.setInt(1, i);
                        pstmt.setString(2, "Name" + i);
                        pstmt.setInt(3, i * 10);
                        pstmt.addBatch();
                    }
                    int[] results2 = pstmt.executeBatch();
                    assertTrue(results1.length == 10, "Batch should have 10 results");
                    assertTrue(results2.length == 10, "Batch should have 10 results");
                }
            } finally {
                // Cleanup - drop table if exists
                try (Statement stmt = con.createStatement()) {
                    TestUtils.dropTableIfExists(tableName, stmt);
                }
            }
        }

        assertTrue(called.get(), "PerformanceLogCallback.publish should have been called");
        assertTrue(Files.exists(logPath), "performance.log file should exist");

        SQLServerDriver.unregisterPerformanceLogCallback();
    }

    /**
     * Test to validate all statement-level performance activities are logged.
     * Executes a complex query using different prepareMethod settings:
     * 1. Regular Statement - direct SQL execution (STATEMENT_EXECUTE)
     * 2. prepareMethod=prepexec (default) - sp_prepexec (STATEMENT_PREPEXEC)
     * 3. prepareMethod=prepare - sp_prepare + sp_execute (STATEMENT_PREPARE + STATEMENT_EXECUTE)
     * 4. prepareMethod=none - direct SQL execution without prepared statements (STATEMENT_EXECUTE)
     * 5. prepareMethod=scopeTempTablesToConnection - direct SQL for temp table operations
     */
    @Test
    void testAllStatementActivities() throws Exception {
        AtomicBoolean called = new AtomicBoolean(false);
        int iterations = 1;
        PerformanceLogCallback callbackInstance = new PerformanceLogCallback() {
            @Override
            public void publish(PerformanceActivity activity, int connectionId, long durationMs, Exception exception) {
                called.set(true);
            }

            @Override
            public void publish(PerformanceActivity activity, int connectionId, int statementId, long durationMs,
                    Exception exception) throws Exception {
                called.set(true);
            }
        };

        SQLServerDriver.registerPerformanceLogCallback(callbackInstance);

        // Complex query with aggregation, string operations, and date functions
        // This query does more work to show timing differences between methods
        final String complexQueryWithParam = 
            "SELECT " +
            "  ? AS input_id, " +
            "  COUNT(*) OVER() AS total_count, " +
            "  NEWID() AS unique_id, " +
            "  GETDATE() AS curr_time, " +
            "  UPPER(REPLICATE('x', 100)) AS repeated_string, " +
            "  ABS(CHECKSUM(NEWID())) % 1000 AS random_value, " +
            "  POWER(2.0, 10) AS power_calc, " +
            "  SQRT(144.0) AS sqrt_calc";
        
        final String complexQueryWithValue = 
            "SELECT " +
            "  42 AS input_id, " +
            "  COUNT(*) OVER() AS total_count, " +
            "  NEWID() AS unique_id, " +
            "  GETDATE() AS curr_time, " +
            "  UPPER(REPLICATE('x', 100)) AS repeated_string, " +
            "  ABS(CHECKSUM(NEWID())) % 1000 AS random_value, " +
            "  POWER(2.0, 10) AS power_calc, " +
            "  SQRT(144.0) AS sqrt_calc";

        // Scenario 1: Regular Statement - direct SQL execution (STATEMENT_EXECUTE)
        long scenario1Start = System.nanoTime();
        try (Connection con = getConnection()) {
            for (int i = 0; i < iterations; i++) {
                try (Statement stmt = con.createStatement();
                     ResultSet rs = stmt.executeQuery(complexQueryWithValue)) {
                }
            }
        }
        long scenario1Duration = System.nanoTime() - scenario1Start;

        // Scenario 2: prepareMethod=prepexec (default) - sp_prepexec (STATEMENT_PREPEXEC)
        long scenario2Start = System.nanoTime();
        try (Connection con = PrepUtil.getConnection(connectionString + 
                                        ";prepareMethod=prepexec;enablePrepareOnFirstPreparedStatementCall=true")) {
            for (int i = 0; i < iterations; i++) {
                try (PreparedStatement pstmt = con.prepareStatement(complexQueryWithParam)) {
                    pstmt.setInt(1, 42 + i);
                    try (ResultSet rs = pstmt.executeQuery()) {
                    }
                }
            }
        }
        long scenario2Duration = System.nanoTime() - scenario2Start;
        
        // Scenario 3: prepareMethod=prepare - sp_prepare + sp_execute (STATEMENT_PREPARE + STATEMENT_EXECUTE)
        long scenario3Start = System.nanoTime();
        try (Connection con = PrepUtil.getConnection(connectionString + 
                                        ";prepareMethod=prepare;enablePrepareOnFirstPreparedStatementCall=true")) {
            for (int i = 0; i < iterations; i++) {
                try (PreparedStatement pstmt = con.prepareStatement(complexQueryWithParam)) {
                    pstmt.setInt(1, 42 + i);
                    try (ResultSet rs = pstmt.executeQuery()) {
                    }
                }
            }
        }
        long scenario3Duration = System.nanoTime() - scenario3Start;

        // Scenario 4: prepareMethod=none - direct SQL execution (STATEMENT_EXECUTE)
        long scenario4Start = System.nanoTime();
        try (Connection con = PrepUtil.getConnection(connectionString + ";prepareMethod=none")) {
            for (int i = 0; i < iterations; i++) {
                try (PreparedStatement pstmt = con.prepareStatement(complexQueryWithParam)) {
                    pstmt.setInt(1, 42 + i);
                    try (ResultSet rs = pstmt.executeQuery()) {
                    }
                }
            }
        }
        long scenario4Duration = System.nanoTime() - scenario4Start;

        // Scenario 5: prepareMethod=scopeTempTablesToConnection - direct SQL for temp tables
        long scenario5Start = System.nanoTime();
        try (Connection con = PrepUtil.getConnection(connectionString + 
                                        ";prepareMethod=scopeTempTablesToConnection")) {
            for (int i = 0; i < iterations; i++) {
                // For scopeTempTablesToConnection, use a query that creates and uses temp tables
                try (Statement stmt = con.createStatement()) {
                    stmt.execute("CREATE TABLE #TempPerfTest" + i + " (id INT, name NVARCHAR(100), created_at DATETIME)");
                    stmt.execute("INSERT INTO #TempPerfTest" + i + " VALUES (" + (42 + i) + ", 'TestName', GETDATE())");
                    try (ResultSet rs = stmt.executeQuery("SELECT * FROM #TempPerfTest" + i)) {
                    }
                    stmt.execute("DROP TABLE #TempPerfTest" + i);
                }
            }
        }
        long scenario5Duration = System.nanoTime() - scenario5Start;

        // Uncomment below lines to print the performance summary for all scenarios

        // System.out.println("=== Performance Summary ===");
        // System.out.println("  Scenario 1 (Statement):                    " + String.format("%.2f", scenario1Duration / 1_000_000.0) + " ms");
        // System.out.println("  Scenario 2 (prepexec):                     " + String.format("%.2f", scenario2Duration / 1_000_000.0) + " ms");
        // System.out.println("  Scenario 3 (prepare):                      " + String.format("%.2f", scenario3Duration / 1_000_000.0) + " ms");
        // System.out.println("  Scenario 4 (none):                         " + String.format("%.2f", scenario4Duration / 1_000_000.0) + " ms");
        // System.out.println("  Scenario 5 (scopeTempTablesToConnection):  " + String.format("%.2f", scenario5Duration / 1_000_000.0) + " ms");

        // System.out.println("\n=== All Statement Activities Test Complete ===\n");
        // System.out.println("Check performance.log for detailed activity logs");
        // System.out.println("Log file path: " + logPath.toAbsolutePath());

        assertTrue(called.get(), "PerformanceLogCallback.publish should have been called");
        assertTrue(Files.exists(logPath), "performance.log file should exist");
    }

    /**
     * Test to validate that the default prepareMethod=prepexec optimization tracks
     * the correct PerformanceActivity for each execution phase:
     * 1st call: STATEMENT_EXECUTE (sp_executesql - assumes single use, no prepare overhead)
     * 2nd call: STATEMENT_PREPEXEC (sp_prepexec - reuse detected, combined prepare+execute)
     * 3rd call: STATEMENT_EXECUTE (sp_execute - handle cached from previous call, execute only)
     */
    @Test
    void testPrepExecActivityTrackedOnlyOnSecondExecution() throws Exception {
        List<PerformanceActivity> activities = new ArrayList<>();

        PerformanceLogCallback callbackInstance = new PerformanceLogCallback() {
            @Override
            public void publish(PerformanceActivity activity, int connectionId, long durationMs,
                    Exception exception) {
                // connection-level - not tracked here
            }

            @Override
            public void publish(PerformanceActivity activity, int connectionId, int statementId,
                    long durationMs, Exception exception) {
                // Only capture EXECUTE / PREPEXEC activities (ignore REQUEST_BUILD, FIRST_SERVER_RESPONSE, etc.)
                if (activity == PerformanceActivity.STATEMENT_EXECUTE
                        || activity == PerformanceActivity.STATEMENT_PREPEXEC) {
                    activities.add(activity);
                }
            }
        };

        SQLServerDriver.registerPerformanceLogCallback(callbackInstance);

        try (Connection con = getConnection()) {
            // Default prepareMethod=prepexec, enablePrepareOnFirstPreparedStatementCall=false
            try (PreparedStatement ps = con.prepareStatement("SELECT ? AS val")) {

                // FIRST CALL - internally uses sp_executesql (no prepare, assumes single use)
                ps.setInt(1, 100);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "First execution should return a row");
                }

                // SECOND CALL - internally uses sp_prepexec (reuse detected, prepare+execute)
                ps.setInt(1, 200);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "Second execution should return a row");
                }

                // THIRD CALL - internally uses sp_execute (already prepared, execute only)
                ps.setInt(1, 300);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "Third execution should return a row");
                }
            }
        }

        // Verify exactly 3 execute/prepexec activities were tracked
        assertEquals(3, activities.size(), "Should have tracked 3 statement execution activities");

        // 1st call: sp_executesql → STATEMENT_EXECUTE
        assertEquals(PerformanceActivity.STATEMENT_EXECUTE, activities.get(0),
                "First call should be STATEMENT_EXECUTE (sp_executesql)");

        // 2nd call: sp_prepexec → STATEMENT_PREPEXEC (only this one should be PREPEXEC)
        assertEquals(PerformanceActivity.STATEMENT_PREPEXEC, activities.get(1),
                "Second call should be STATEMENT_PREPEXEC (sp_prepexec)");

        // 3rd call: sp_execute → STATEMENT_EXECUTE
        assertEquals(PerformanceActivity.STATEMENT_EXECUTE, activities.get(2),
                "Third call should be STATEMENT_EXECUTE (sp_execute)");

        SQLServerDriver.unregisterPerformanceLogCallback();
        
    }

    /**
     * Test to validate that enablePrepareOnFirstPreparedStatementCall=true causes
     * sp_prepexec on the very first execution (no sp_executesql trial run):
     * 1st call: STATEMENT_PREPEXEC (sp_prepexec - eager prepare+execute on first call)
     * 2nd call: STATEMENT_EXECUTE (sp_execute - handle cached from previous call)
     * 3rd call: STATEMENT_EXECUTE (sp_execute - handle cached, execute only)
     */
    @Test
    void testPrepExecActivityTrackedOnFirstExecutionWhenEagerPrepareEnabled() throws Exception {
        List<PerformanceActivity> activities = new ArrayList<>();

        PerformanceLogCallback callbackInstance = new PerformanceLogCallback() {
            @Override
            public void publish(PerformanceActivity activity, int connectionId, long durationMs,
                    Exception exception) {
                // connection-level - not tracked here
            }

            @Override
            public void publish(PerformanceActivity activity, int connectionId, int statementId,
                    long durationMs, Exception exception) {
                // Only capture EXECUTE / PREPEXEC activities (ignore REQUEST_BUILD, FIRST_SERVER_RESPONSE, etc.)
                if (activity == PerformanceActivity.STATEMENT_EXECUTE
                        || activity == PerformanceActivity.STATEMENT_PREPEXEC) {
                    activities.add(activity);
                }
            }
        };

        SQLServerDriver.registerPerformanceLogCallback(callbackInstance);

        try (Connection con = PrepUtil.getConnection(connectionString
                + ";enablePrepareOnFirstPreparedStatementCall=true")) {
            // prepareMethod=prepexec (default), enablePrepareOnFirstPreparedStatementCall=true
            try (PreparedStatement ps = con.prepareStatement("SELECT ? AS val")) {

                // FIRST CALL - internally uses sp_prepexec (eager prepare+execute, no trial run)
                ps.setInt(1, 100);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "First execution should return a row");
                }

                // SECOND CALL - internally uses sp_execute (handle cached from first call)
                ps.setInt(1, 200);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "Second execution should return a row");
                }

                // THIRD CALL - internally uses sp_execute (handle cached, execute only)
                ps.setInt(1, 300);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "Third execution should return a row");
                }
            }
        }

        // Verify exactly 3 execute/prepexec activities were tracked
        assertEquals(3, activities.size(), "Should have tracked 3 statement execution activities");

        // 1st call: sp_prepexec → STATEMENT_PREPEXEC (eager prepare on first call)
        assertEquals(PerformanceActivity.STATEMENT_PREPEXEC, activities.get(0),
                "First call should be STATEMENT_PREPEXEC (sp_prepexec, eager prepare)");

        // 2nd call: sp_execute → STATEMENT_EXECUTE
        assertEquals(PerformanceActivity.STATEMENT_EXECUTE, activities.get(1),
                "Second call should be STATEMENT_EXECUTE (sp_execute)");

        // 3rd call: sp_execute → STATEMENT_EXECUTE
        assertEquals(PerformanceActivity.STATEMENT_EXECUTE, activities.get(2),
                "Third call should be STATEMENT_EXECUTE (sp_execute)");

        SQLServerDriver.unregisterPerformanceLogCallback();
        
    }

    /**
     * Helper method to execute Statement and PreparedStatement queries in a loop.
     * Uses a provided connection. Returns the duration in nanoseconds.
     */
    private long executeStatementsWithTiming(Connection con, int iterations) throws Exception {
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            try (Statement stmt = con.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT 1")) {
                rs.next();
            }
            try (PreparedStatement pstmt = con.prepareStatement("SELECT ?")) {
                pstmt.setInt(1, i);
                try (ResultSet rs = pstmt.executeQuery()) {
                    rs.next();
                }
            }
        }
        return System.nanoTime() - startTime;
    }

    /**
     * Helper method to execute Statement and PreparedStatement queries with new connections per iteration.
     * Creates a new connection for each iteration to test connection overhead. Returns the duration in nanoseconds.
     */
    private long executeStatementsWithNewConnectionsPerIteration(int iterations) throws Exception {
        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            try (Connection con = getConnection()) {
                try (Statement stmt = con.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT 1")) {
                    rs.next();
                }
                try (PreparedStatement pstmt = con.prepareStatement("SELECT ?")) {
                    pstmt.setInt(1, i);
                    try (ResultSet rs = pstmt.executeQuery()) {
                        rs.next();
                    }
                }
            }
        }
        return System.nanoTime() - startTime;
    }

    /**
     * Helper method to print performance comparison between disabled and enabled logging.
     */
    private void printPerformanceComparison(long disabledDuration, long enabledDuration, int iterations, String description) {
        double disabledMs = disabledDuration / 1_000_000.0;
        double enabledMs = enabledDuration / 1_000_000.0;
        double overheadMs = enabledMs - disabledMs;
        double overheadPercent = (overheadMs / disabledMs) * 100;
        double avgOverheadPerIterationUs = (overheadMs * 1000) / iterations;

        System.out.printf("--- %s (%d iterations) ---%n", description, iterations);
        System.out.printf("  Disabled:  %.2f ms (%.3f ms/iteration)%n", disabledMs, disabledMs / iterations);
        System.out.printf("  Enabled:   %.2f ms (%.3f ms/iteration)%n", enabledMs, enabledMs / iterations);
        System.out.printf("  Overhead:  %.2f ms (%.2f%%)%n", overheadMs, overheadPercent);
        System.out.printf("  Avg overhead per iteration: %.2f µs%n%n", avgOverheadPerIterationUs);
    }

}