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
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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

    // Generated keys, update counts and stored rows under every prepare method. The prepared path (prepexec, and
    // scopeTempTablesToConnection for a statement without temp-table operations) appends a SCOPE_IDENTITY() query to
    // the statement, and so does the direct path (prepareMethod=none, and scopeTempTablesToConnection with temp-table
    // SQL), so the keys, update counts and stored rows are the same everywhere. After executeBatch the keys are not
    // available in any mode.

    private static String insertOutcome(String prepareMethod, String table, boolean batch) throws SQLException {
        StringBuilder outcome = new StringBuilder();
        try (Connection conn = DriverManager.getConnection(connectionString + ";prepareMethod=" + prepareMethod);
                Statement stmt = conn.createStatement()) {
            try {
                stmt.executeUpdate("CREATE TABLE " + table + " (id int IDENTITY(10,5) PRIMARY KEY, v int)");
                try (PreparedStatement ps = conn.prepareStatement("INSERT INTO " + table + " (v) VALUES (?)",
                        Statement.RETURN_GENERATED_KEYS)) {
                    for (int v = 1; v <= 3; v++) {
                        ps.setInt(1, v);
                        if (batch) {
                            ps.addBatch();
                        } else {
                            outcome.append("count=").append(ps.executeUpdate());
                            try (ResultSet keys = ps.getGeneratedKeys()) {
                                outcome.append(" key=").append(keys.next() ? keys.getLong(1) : -1).append(';');
                            } catch (SQLException e) {
                                outcome.append(" keys unavailable;");
                            }
                        }
                    }
                    if (batch) {
                        outcome.append("counts=").append(java.util.Arrays.toString(ps.executeBatch()));
                    }
                }
                outcome.append(" rows=");
                try (ResultSet rs = stmt.executeQuery("SELECT id, v FROM " + table + " ORDER BY id")) {
                    while (rs.next()) {
                        outcome.append(rs.getInt(1)).append(':').append(rs.getInt(2)).append(',');
                    }
                }
            } finally {
                TestUtils.dropTableIfExists(table, stmt);
            }
        }
        return outcome.toString();
    }

    @ParameterizedTest(name = "batch={0}")
    @ValueSource(booleans = {false, true})
    public void testGeneratedKeysAndRowsOnThePreparedPath(boolean batch) throws SQLException {
        String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("GenKeyTab"));
        String typed = insertOutcome("prepexec", table, batch);
        String expected = batch ? "counts=[1, 1, 1] rows=10:1,15:2,20:3,"
                                : "count=1 key=10;count=1 key=15;count=1 key=20; rows=10:1,15:2,20:3,";
        assertEquals(expected, typed);
        // scopeTempTablesToConnection without temp-table SQL is the same prepared path.
        assertEquals(typed, insertOutcome("scopeTempTablesToConnection", table, batch));
    }

    @ParameterizedTest(name = "batch={0}")
    @ValueSource(booleans = {false, true})
    public void testDirectInsertReturnsGeneratedKeysLikeTypedBinding(boolean batch) throws SQLException {
        String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("GenKeyTab"));
        String expected = batch ? "counts=[1, 1, 1] rows=10:1,15:2,20:3,"
                                : "count=1 key=10;count=1 key=15;count=1 key=20; rows=10:1,15:2,20:3,";
        assertEquals(expected, insertOutcome("none", table, batch));
    }

    // A direct insert whose SQL ends with a line comment still returns its key: the generated-key query goes on a
    // new line.
    @Test
    public void testGeneratedKeyWithTrailingLineCommentInDirectMode() throws SQLException {
        String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("GenKeyTab"));
        try (Connection conn = DriverManager.getConnection(connectionString + ";prepareMethod=none");
                Statement stmt = conn.createStatement()) {
            try {
                stmt.executeUpdate("CREATE TABLE " + table + " (id int IDENTITY(10,5) PRIMARY KEY, v int)");
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + table + " (v) VALUES (?) -- trailing comment",
                        Statement.RETURN_GENERATED_KEYS)) {
                    ps.setInt(1, 1);
                    assertEquals(1, ps.executeUpdate());
                    try (ResultSet keys = ps.getGeneratedKeys()) {
                        assertTrue(keys.next());
                        assertEquals(10, keys.getLong(1));
                    }
                }
            } finally {
                TestUtils.dropTableIfExists(table, stmt);
            }
        }
    }

    // Not covered: getGeneratedKeys() for a multi-statement direct batch (for example CREATE TABLE ...; INSERT ...).
    // The server returns the key, but the driver's direct path does not hand the result set to getGeneratedKeys(),
    // while the prepared path does. Single INSERT statements, the case that matters for identity columns, work.
}
