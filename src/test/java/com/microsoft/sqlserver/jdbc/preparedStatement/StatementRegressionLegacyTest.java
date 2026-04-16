/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.preparedStatement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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
 * Statement regression tests for legacy FX scenarios: parameter reuse,
 * statement re-execution, sp_executesql edge cases, generated keys retrieval,
 * statement pool interaction.
 * Ported from FX statement regression and edge case tests.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxStatement)
public class StatementRegressionLegacyTest extends AbstractTest {

    private static final String tableName = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("StmtRegr_Tab"));

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            stmt.executeUpdate("CREATE TABLE " + tableName
                    + " (ID INT IDENTITY PRIMARY KEY, COL1 VARCHAR(200), COL2 INT, "
                    + "DECIMAL_COL DECIMAL(18,6))");
        }
    }

    @AfterAll
    public static void cleanupTests() throws Exception {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
        }
    }

    @Test
    public void testPreparedStatementReuse() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (COL1, COL2) VALUES (?, ?)")) {
            // Execute with first set of params
            ps.setString(1, "first");
            ps.setInt(2, 1);
            ps.executeUpdate();

            // Reuse with different params
            ps.setString(1, "second");
            ps.setInt(2, 2);
            ps.executeUpdate();

            // Reuse again
            ps.setString(1, "third");
            ps.setInt(2, 3);
            ps.executeUpdate();
        }

        // Verify all three rows
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT COUNT(*) FROM " + tableName + " WHERE COL1 IN ('first','second','third')")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
    }

    @Test
    public void testStatementReExecution() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            // Execute same query multiple times
            for (int i = 0; i < 5; i++) {
                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
                    assertTrue(rs.next());
                    assertTrue(rs.getInt(1) >= 0);
                }
            }
        }
    }

    @Test
    public void testGeneratedKeysRetrieval() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(
                    "INSERT INTO " + tableName + " (COL1, COL2) VALUES ('genkey_test', 100)",
                    Statement.RETURN_GENERATED_KEYS);
            try (ResultSet keys = stmt.getGeneratedKeys()) {
                assertTrue(keys.next());
                int generatedId = keys.getInt(1);
                assertTrue(generatedId > 0);
            }
        }
    }

    @Test
    public void testGeneratedKeysWithPreparedStatement() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (COL1, COL2) VALUES (?, ?)",
                        Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, "ps_genkey");
            ps.setInt(2, 200);
            ps.executeUpdate();
            try (ResultSet keys = ps.getGeneratedKeys()) {
                assertTrue(keys.next());
                assertTrue(keys.getInt(1) > 0);
            }
        }
    }

    @Test
    public void testParameterClearAndReset() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (COL1, COL2) VALUES (?, ?)")) {
            ps.setString(1, "clear_test");
            ps.setInt(2, 300);
            ps.clearParameters();
            // Set new values
            ps.setString(1, "after_clear");
            ps.setInt(2, 301);
            ps.executeUpdate();
        }
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT COL1 FROM " + tableName + " WHERE COL2 = 301")) {
            assertTrue(rs.next());
            assertEquals("after_clear", rs.getString(1));
        }
    }

    @Test
    public void testBatchWithPreparedStatement() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (COL1, COL2) VALUES (?, ?)")) {
            for (int i = 0; i < 10; i++) {
                ps.setString(1, "batch_" + i);
                ps.setInt(2, 400 + i);
                ps.addBatch();
            }
            int[] results = ps.executeBatch();
            assertEquals(10, results.length);
            for (int r : results) {
                assertEquals(1, r);
            }
        }
    }

    @Test
    public void testDecimalPrecisionPreservation() throws SQLException {
        BigDecimal precise = new BigDecimal("123456.789012");
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (DECIMAL_COL) VALUES (?)")) {
            ps.setBigDecimal(1, precise);
            ps.executeUpdate();
        }
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT TOP 1 DECIMAL_COL FROM " + tableName
                                + " WHERE DECIMAL_COL IS NOT NULL ORDER BY ID DESC")) {
            assertTrue(rs.next());
            BigDecimal result = rs.getBigDecimal(1);
            assertEquals(precise.compareTo(result), 0);
        }
    }

    @Test
    public void testMaxConcurrentStatements() throws SQLException {
        try (Connection conn = getConnection()) {
            Statement[] stmts = new Statement[20];
            try {
                for (int i = 0; i < 20; i++) {
                    stmts[i] = conn.createStatement();
                    try (ResultSet rs = stmts[i].executeQuery("SELECT " + i)) {
                        assertTrue(rs.next());
                    }
                }
            } finally {
                for (Statement s : stmts) {
                    if (s != null) {
                        s.close();
                    }
                }
            }
        }
    }

    @Test
    public void testStatementAfterConnectionReset() throws SQLException {
        try (Connection conn = getConnection()) {
            // Change isolation level and reset
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            // Statement should work after reset
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT 1")) {
                assertTrue(rs.next());
            }
        }
    }

    @Test
    public void testExecuteReturnValues() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            // execute() returns true for SELECT
            assertTrue(stmt.execute("SELECT * FROM " + tableName));
            assertNotNull(stmt.getResultSet());

            // execute() returns false for INSERT
            boolean hasResults = stmt.execute(
                    "INSERT INTO " + tableName + " (COL1, COL2) VALUES ('exec_ret', 500)");
            assertTrue(!hasResults);
            assertEquals(1, stmt.getUpdateCount());
        }
    }

    @Test
    public void testEmptyResultSet() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT * FROM " + tableName + " WHERE COL2 = -999")) {
            assertTrue(!rs.next());
        }
    }

    @Test
    public void testLargeUpdateCount() throws SQLException {
        // Insert multiple rows to test update count
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            int count = stmt.executeUpdate(
                    "INSERT INTO " + tableName + " (COL1, COL2) "
                            + "SELECT 'bulk_' + CAST(number AS VARCHAR), number "
                            + "FROM (SELECT TOP 100 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS number "
                            + "FROM sys.objects) t");
            assertEquals(100, count);
        }
    }
}
