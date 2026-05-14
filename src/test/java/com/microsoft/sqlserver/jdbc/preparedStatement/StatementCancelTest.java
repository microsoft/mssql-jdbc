/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.preparedStatement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;

/**
 * Statement cancellation tests: cancel during execution, query timeout enforcement,
 * cancel from another thread, cancel interaction with cursors.
 * Ported from FX statement/TC_Cancel tests.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxStatement)
public class StatementCancelTest extends AbstractTest {

    private static final String tableName = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("StmtCancel_Tab"));

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            stmt.executeUpdate("CREATE TABLE " + tableName
                    + " (ID INT IDENTITY PRIMARY KEY, COL1 VARCHAR(200), COL2 INT)");
            for (int i = 1; i <= 50; i++) {
                stmt.executeUpdate(
                        "INSERT INTO " + tableName + " (COL1, COL2) VALUES ('cancel_row" + i + "', " + i + ")");
            }
        }
    }

    @AfterAll
    public static void cleanupTests() throws Exception {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
        }
    }

    @Test
    public void testCancelBeforeExecution() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            // Cancel before execution should be a no-op
            stmt.cancel();
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
                assertTrue(rs.next());
            }
        }
    }

    @Test
    public void testQueryTimeout() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.setQueryTimeout(1); // 1 second timeout
            try {
                // WAITFOR DELAY should trigger timeout
                stmt.execute("WAITFOR DELAY '00:00:05'");
                fail("Expected timeout exception");
            } catch (SQLException e) {
                // Expected: query timeout or cancellation
                String message = String.valueOf(e.getMessage());
                assertTrue(e instanceof SQLTimeoutException
                        || message.contains("cancel")
                        || message.contains("timeout")
                        || message.contains("timed out"));
            }
        }
    }

    @Test
    public void testQueryTimeoutZero() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.setQueryTimeout(0); // No timeout
            assertEquals(0, stmt.getQueryTimeout());
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
                assertTrue(rs.next());
            }
        }
    }

    @Test
    public void testCancelFromAnotherThread() throws Exception {
        try (Connection conn = getConnection()) {
            Statement stmt = conn.createStatement();
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                // Schedule cancel after a brief delay
                Future<?> cancelFuture = executor.submit(() -> {
                    try {
                        Thread.sleep(500); // 500ms delay
                        stmt.cancel();
                    } catch (Exception e) {
                        // Ignore
                    }
                });

                try {
                    stmt.execute("WAITFOR DELAY '00:00:10'");
                } catch (SQLException e) {
                    // Expected: cancellation
                    String message = String.valueOf(e.getMessage());
                    assertTrue(message.contains("cancel")
                            || message.contains("attention"));
                }
                cancelFuture.get(5, TimeUnit.SECONDS);
            } finally {
                executor.shutdown();
                stmt.close();
            }
        }
    }

    @Test
    public void testCancelOnPreparedStatement() throws Exception {
        try (Connection conn = getConnection()) {
            PreparedStatement ps = conn.prepareStatement("WAITFOR DELAY '00:00:10'");
            ps.setQueryTimeout(1);
            try {
                ps.execute();
                fail("Expected timeout");
            } catch (SQLException e) {
                String message = String.valueOf(e.getMessage());
                assertTrue(e instanceof SQLTimeoutException
                        || message.contains("cancel")
                        || message.contains("timeout")
                        || message.contains("timed out"));
            } finally {
                ps.close();
            }
        }
    }

    @Test
    public void testCancelClosedStatement() throws SQLException {
        try (Connection conn = getConnection()) {
            Statement stmt = conn.createStatement();
            stmt.close();
            // Cancel on closed statement should throw
            assertThrows(SQLException.class, () -> stmt.cancel());
        }
    }

    @Test
    public void testTimeoutWithForwardOnlyCursor() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            stmt.setQueryTimeout(2);
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
                assertTrue(rs.next());
            }
        }
    }

    @Test
    public void testTimeoutWithScrollInsensitiveCursor() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY)) {
            stmt.setQueryTimeout(2);
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
                assertTrue(rs.next());
            }
        }
    }

    @Test
    public void testMultipleSequentialCancels() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            // Multiple cancels should not cause issues
            stmt.cancel();
            stmt.cancel();
            stmt.cancel();
            try (ResultSet rs = stmt.executeQuery("SELECT 1")) {
                assertTrue(rs.next());
            }
        }
    }

    @Test
    public void testQueryTimeoutOnBatch() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.setQueryTimeout(5);
            stmt.addBatch("INSERT INTO " + tableName + " (COL1, COL2) VALUES ('timeout_batch', 901)");
            stmt.addBatch("INSERT INTO " + tableName + " (COL1, COL2) VALUES ('timeout_batch2', 902)");
            int[] results = stmt.executeBatch();
            assertEquals(2, results.length);
        }
    }
}
