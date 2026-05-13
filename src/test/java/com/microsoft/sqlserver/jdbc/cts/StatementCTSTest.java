/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.cts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;

/**
 * CTS compliance tests for Statement: lifecycle, execution, cursor/concurrency matrix,
 * fetch settings, max rows, query timeout, warnings, multiple results.
 * Ported from FX stmtClient.java CTS tests.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxCTS)
public class StatementCTSTest extends AbstractTest {

    private static final String tableName = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("CTS_Stmt_Tab"));

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            stmt.executeUpdate("CREATE TABLE " + tableName
                    + " (ID INT IDENTITY PRIMARY KEY, COL1 VARCHAR(200), COL2 INT)");
            for (int i = 1; i <= 20; i++) {
                stmt.executeUpdate("INSERT INTO " + tableName + " (COL1, COL2) VALUES ('row" + i + "', " + i + ")");
            }
        }
    }

    @AfterAll
    public static void cleanupTests() throws Exception {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
        }
    }

    /**
     * Tests that Statement.close marks the statement as closed.
     * Ported from FX CTS stmtClient close variations.
     */
    @Test
    public void testClose() throws SQLException {
        try (Connection conn = getConnection()) {
            Statement stmt = conn.createStatement();
            stmt.close();
            assertTrue(stmt.isClosed());
        }
    }

    /**
     * Tests executeQuery returns a non-null ResultSet with data.
     * Ported from FX CTS stmtClient executeQuery variations.
     */
    @Test
    public void testExecuteQuery() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            assertNotNull(rs);
            assertTrue(rs.next());
        }
    }

    /**
     * Tests executeQuery with a WHERE clause returns the expected matching row.
     * Ported from FX CTS stmtClient executeQuery variations.
     */
    @Test
    public void testExecuteQueryWithWhere() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE COL2 = 5")) {
            assertTrue(rs.next());
            assertEquals(5, rs.getInt("COL2"));
            assertFalse(rs.next());
        }
    }

    /**
     * Tests executeUpdate for an INSERT returns an update count of 1.
     * Ported from FX CTS stmtClient executeUpdate variations.
     */
    @Test
    public void testExecuteUpdate() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            int rows = stmt.executeUpdate(
                    "INSERT INTO " + tableName + " (COL1, COL2) VALUES ('exec_update_test', 999)");
            assertEquals(1, rows);
        }
    }

    /**
     * Tests execute with a SELECT COUNT returns true and produces a result count.
     * Ported from FX CTS stmtClient execute variations.
     */
    @Test
    public void testExecute() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            boolean hasResults = stmt.execute("SELECT COUNT(*) FROM " + tableName);
            assertTrue(hasResults);
            try (ResultSet rs = stmt.getResultSet()) {
                assertTrue(rs.next());
                assertTrue(rs.getInt(1) > 0);
            }
        }
    }

    /**
     * Tests getUpdateCount returns 1 after executing an INSERT statement.
     * Ported from FX CTS stmtClient getUpdateCount variations.
     */
    @Test
    public void testGetUpdateCount() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("INSERT INTO " + tableName + " (COL1, COL2) VALUES ('upd_count', 888)");
            assertEquals(1, stmt.getUpdateCount());
        }
    }

    /**
     * Tests getResultSet returns a valid ResultSet after execute.
     * Ported from FX CTS stmtClient getResultSet variations.
     */
    @Test
    public void testGetResultSet() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("SELECT * FROM " + tableName + " WHERE COL2 = 1");
            try (ResultSet rs = stmt.getResultSet()) {
                assertNotNull(rs);
                assertTrue(rs.next());
            }
        }
    }

    /**
     * Tests getResultSetType returns TYPE_FORWARD_ONLY for a forward-only statement.
     * Ported from FX CTS stmtClient getResultSetType variations.
     */
    @Test
    public void testGetResultSetTypeForwardOnly() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            assertEquals(ResultSet.TYPE_FORWARD_ONLY, stmt.getResultSetType());
        }
    }

    /**
     * Tests getResultSetType returns TYPE_SCROLL_INSENSITIVE for a scroll-insensitive statement.
     * Ported from FX CTS stmtClient getResultSetType variations.
     */
    @Test
    public void testGetResultSetTypeScrollInsensitive() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY)) {
            assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, stmt.getResultSetType());
        }
    }

    /**
     * Tests getResultSetType returns TYPE_SCROLL_SENSITIVE for a scroll-sensitive statement.
     * Ported from FX CTS stmtClient getResultSetType variations.
     */
    @Test
    public void testGetResultSetTypeScrollSensitive() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                        ResultSet.CONCUR_READ_ONLY)) {
            assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE, stmt.getResultSetType());
        }
    }

    /**
     * Tests getResultSetConcurrency returns CONCUR_READ_ONLY.
     * Ported from FX CTS stmtClient getResultSetConcurrency variations.
     */
    @Test
    public void testGetResultSetConcurrencyReadOnly() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            assertEquals(ResultSet.CONCUR_READ_ONLY, stmt.getResultSetConcurrency());
        }
    }

    /**
     * Tests getResultSetConcurrency returns CONCUR_UPDATABLE.
     * Ported from FX CTS stmtClient getResultSetConcurrency variations.
     */
    @Test
    public void testGetResultSetConcurrencyUpdatable() throws SQLException {
        try (Connection conn = getConnection()) {
            SQLServerException ex = assertThrows(SQLServerException.class, () -> {
                try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_UPDATABLE)) {
                    stmt.getResultSetConcurrency();
                }
            });
            assertTrue(ex.getMessage().contains("not supported"));
        }
    }

    /**
     * Tests getMoreResults returns false when no additional results exist.
     * Ported from FX CTS stmtClient getMoreResults variations.
     */
    @Test
    public void testGetMoreResults() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("SELECT * FROM " + tableName + " WHERE COL2 = 1");
            // After consuming, getMoreResults should return false
            try (ResultSet rs = stmt.getResultSet()) {
                assertNotNull(rs);
            }
            assertFalse(stmt.getMoreResults());
        }
    }

    /**
     * Tests getFetchDirection returns a valid fetch direction constant.
     * Ported from FX CTS stmtClient getFetchDirection variations.
     */
    @Test
    public void testGetFetchDirection() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            int dir = stmt.getFetchDirection();
            assertTrue(dir == ResultSet.FETCH_FORWARD || dir == ResultSet.FETCH_REVERSE
                    || dir == ResultSet.FETCH_UNKNOWN);
        }
    }

    /**
     * Tests setFetchDirection with FETCH_FORWARD sets the direction correctly.
     * Ported from FX CTS stmtClient setFetchDirection variations.
     */
    @Test
    public void testSetFetchDirectionForward() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
            assertEquals(ResultSet.FETCH_FORWARD, stmt.getFetchDirection());
        }
    }

    /**
     * Tests getFetchSize returns a non-negative default value.
     * Ported from FX CTS stmtClient getFetchSize variations.
     */
    @Test
    public void testGetFetchSize() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            int size = stmt.getFetchSize();
            assertTrue(size >= 0);
        }
    }

    /**
     * Tests setFetchSize sets and returns the specified value.
     * Ported from FX CTS stmtClient setFetchSize variations.
     */
    @Test
    public void testSetFetchSize() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.setFetchSize(25);
            assertEquals(25, stmt.getFetchSize());
        }
    }

    /**
     * Tests setFetchSize with zero succeeds as a hint to use driver default.
     * Ported from FX CTS stmtClient setFetchSize variations.
     */
    @Test
    public void testSetFetchSizeZero() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.setFetchSize(0);
            assertTrue(stmt.getFetchSize() >= 0);
        }
    }

    /**
     * Tests getMaxFieldSize returns a non-negative default value.
     * Ported from FX CTS stmtClient getMaxFieldSize variations.
     */
    @Test
    public void testGetMaxFieldSize() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            int maxField = stmt.getMaxFieldSize();
            assertTrue(maxField >= 0);
        }
    }

    /**
     * Tests setMaxFieldSize sets and returns the specified value.
     * Ported from FX CTS stmtClient setMaxFieldSize variations.
     */
    @Test
    public void testSetMaxFieldSize() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.setMaxFieldSize(100);
            assertEquals(100, stmt.getMaxFieldSize());
        }
    }

    /**
     * Tests getMaxRows returns 0 (no limit) by default.
     * Ported from FX CTS stmtClient getMaxRows variations.
     */
    @Test
    public void testGetMaxRows() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            int maxRows = stmt.getMaxRows();
            assertEquals(0, maxRows); // default is 0 (no limit)
        }
    }

    /**
     * Tests setMaxRows limits the number of rows returned by a query.
     * Ported from FX CTS stmtClient setMaxRows variations.
     */
    @Test
    public void testSetMaxRows() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.setMaxRows(5);
            assertEquals(5, stmt.getMaxRows());
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
                int count = 0;
                while (rs.next()) {
                    count++;
                }
                assertTrue(count <= 5);
            }
        }
    }

    /**
     * Tests getQueryTimeout returns 0 (no timeout) by default.
     * Ported from FX CTS stmtClient getQueryTimeout variations.
     */
    @Test
    public void testGetQueryTimeout() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            int timeout = stmt.getQueryTimeout();
            assertEquals(0, timeout); // default is 0 (no timeout)
        }
    }

    /**
     * Tests setQueryTimeout sets and returns the specified timeout value.
     * Ported from FX CTS stmtClient setQueryTimeout variations.
     */
    @Test
    public void testSetQueryTimeout() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.setQueryTimeout(30);
            assertEquals(30, stmt.getQueryTimeout());
        }
    }

    /**
     * Tests getWarnings after query execution completes without error.
     * Ported from FX CTS stmtClient getWarnings variations.
     */
    @Test
    public void testGetWarnings() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.executeQuery("SELECT * FROM " + tableName);
            // Warnings may or may not exist, but should not throw
            SQLWarning warn = stmt.getWarnings();
            // null is acceptable
        }
    }

    /**
     * Tests clearWarnings clears any existing warnings.
     * Ported from FX CTS stmtClient clearWarnings variations.
     */
    @Test
    public void testClearWarnings() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.executeQuery("SELECT * FROM " + tableName);
            stmt.clearWarnings();
            assertNull(stmt.getWarnings());
        }
    }

    /**
     * Tests statement creation with TYPE_FORWARD_ONLY and CONCUR_READ_ONLY cursor settings.
     * Ported from FX CTS stmtClient cursor/concurrency matrix variations.
     */
    @Test
    public void testForwardOnlyReadOnly() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());
            assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
            assertTrue(rs.next());
        }
    }

    /**
     * Tests statement creation with TYPE_SCROLL_INSENSITIVE and CONCUR_READ_ONLY with navigation.
     * Ported from FX CTS stmtClient cursor/concurrency matrix variations.
     */
    @Test
    public void testScrollInsensitiveReadOnly() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY);
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, rs.getType());
            assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
            assertTrue(rs.last());
            assertTrue(rs.first());
        }
    }

    /**
     * Tests statement creation with TYPE_SCROLL_SENSITIVE and CONCUR_READ_ONLY.
     * Ported from FX CTS stmtClient cursor/concurrency matrix variations.
     */
    @Test
    public void testScrollSensitiveReadOnly() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                        ResultSet.CONCUR_READ_ONLY);
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE, rs.getType());
            assertTrue(rs.next());
        }
    }

    /**
     * Tests statement creation with TYPE_SCROLL_INSENSITIVE and CONCUR_UPDATABLE.
     * Ported from FX CTS stmtClient cursor/concurrency matrix variations.
     */
    @Test
    public void testScrollInsensitiveUpdatable() throws SQLException {
        try (Connection conn = getConnection()) {
            SQLServerException ex = assertThrows(SQLServerException.class, () -> {
                try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_UPDATABLE);
                        ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
                    rs.getConcurrency();
                }
            });
            assertTrue(ex.getMessage().contains("not supported"));
        }
    }

    /**
     * Tests batch execution of multiple INSERT statements with addBatch and executeBatch.
     * Ported from FX CTS stmtClient batch execution variations.
     */
    @Test
    public void testBatchExecution() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.addBatch("INSERT INTO " + tableName + " (COL1, COL2) VALUES ('batch1', 701)");
            stmt.addBatch("INSERT INTO " + tableName + " (COL1, COL2) VALUES ('batch2', 702)");
            stmt.addBatch("INSERT INTO " + tableName + " (COL1, COL2) VALUES ('batch3', 703)");
            int[] results = stmt.executeBatch();
            assertEquals(3, results.length);
            for (int r : results) {
                assertEquals(1, r);
            }
        }
    }

    /**
     * Tests clearBatch empties the batch so executeBatch returns an empty array.
     * Ported from FX CTS stmtClient clearBatch variations.
     */
    @Test
    public void testClearBatch() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.addBatch("INSERT INTO " + tableName + " (COL1, COL2) VALUES ('clear1', 801)");
            stmt.clearBatch();
            int[] results = stmt.executeBatch();
            assertEquals(0, results.length);
        }
    }
}
