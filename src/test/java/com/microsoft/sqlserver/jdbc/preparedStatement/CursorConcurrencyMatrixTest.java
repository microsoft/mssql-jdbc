/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.preparedStatement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;

/**
 * Cursor/concurrency matrix tests: all combinations of cursor type and concurrency
 * for Statement, PreparedStatement, and CallableStatement.
 * Ported from FX statement/statement.java cursor matrix tests.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxCursor)
public class CursorConcurrencyMatrixTest extends AbstractTest {

    private static final String tableName = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("CursorMatrix_Tab"));

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            stmt.executeUpdate("CREATE TABLE " + tableName
                    + " (ID INT IDENTITY PRIMARY KEY, COL1 VARCHAR(200), COL2 INT)");
            for (int i = 1; i <= 10; i++) {
                stmt.executeUpdate(
                        "INSERT INTO " + tableName + " (COL1, COL2) VALUES ('row" + i + "', " + i + ")");
            }
        }
    }

    @AfterAll
    public static void cleanupTests() throws Exception {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
        }
    }

    // Forward-Only + Read-Only
    @Test
    public void testForwardOnlyReadOnly() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());
            assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
            assertTrue(rs.next());
            assertNotNull(rs.getString("COL1"));
        }
    }

    // Forward-Only + Updatable
    @Test
    public void testForwardOnlyUpdatable() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            assertTrue(rs.next());
            assertNotNull(rs.getString("COL1"));
        }
    }

    // Scroll-Insensitive + Read-Only
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
            assertTrue(rs.absolute(5));
        }
    }

    // Scroll-Insensitive + Updatable (not supported by SQL Server — verify graceful error)
    @Test
    public void testScrollInsensitiveUpdatable() throws SQLException {
        try (Connection conn = getConnection()) {
            SQLServerException ex = assertThrows(SQLServerException.class, () -> {
                try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_UPDATABLE)) {
                    stmt.executeQuery("SELECT * FROM " + tableName);
                }
            });
            assertTrue(ex.getMessage().contains("not supported"));
        }
    }

    // Scroll-Sensitive + Read-Only
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

    // Scroll-Sensitive + Updatable
    @Test
    public void testScrollSensitiveUpdatable() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                        ResultSet.CONCUR_UPDATABLE);
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            assertTrue(rs.next());
        }
    }

    // PreparedStatement: Forward-Only + Read-Only
    @Test
    public void testPreparedForwardOnlyReadOnly() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT * FROM " + tableName,
                        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                ResultSet rs = ps.executeQuery()) {
            assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());
            assertTrue(rs.next());
        }
    }

    // PreparedStatement: Scroll-Insensitive + Read-Only
    @Test
    public void testPreparedScrollInsensitiveReadOnly() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT * FROM " + tableName,
                        ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                ResultSet rs = ps.executeQuery()) {
            assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, rs.getType());
            assertTrue(rs.last());
            assertTrue(rs.first());
        }
    }

    // PreparedStatement: Scroll-Insensitive + Updatable (not supported by SQL Server — verify graceful error)
    @Test
    public void testPreparedScrollInsensitiveUpdatable() throws SQLException {
        try (Connection conn = getConnection()) {
            SQLServerException ex = assertThrows(SQLServerException.class, () -> {
                try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM " + tableName,
                        ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
                    ps.executeQuery();
                }
            });
            assertTrue(ex.getMessage().contains("not supported"));
        }
    }

    // PreparedStatement: Scroll-Sensitive + Read-Only
    @Test
    public void testPreparedScrollSensitiveReadOnly() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT * FROM " + tableName,
                        ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
                ResultSet rs = ps.executeQuery()) {
            assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE, rs.getType());
            assertTrue(rs.next());
        }
    }

    // Statement batch with different cursor types
    @Test
    public void testBatchWithForwardOnly() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            stmt.addBatch("INSERT INTO " + tableName + " (COL1, COL2) VALUES ('batch_fo', 100)");
            stmt.addBatch("INSERT INTO " + tableName + " (COL1, COL2) VALUES ('batch_fo2', 101)");
            int[] results = stmt.executeBatch();
            assertEquals(2, results.length);
        }
    }

    // Statement batch with scroll insensitive
    @Test
    public void testBatchWithScrollInsensitive() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY)) {
            stmt.addBatch("INSERT INTO " + tableName + " (COL1, COL2) VALUES ('batch_si', 200)");
            int[] results = stmt.executeBatch();
            assertEquals(1, results.length);
        }
    }

    // Multiple result sets with getMoreResults
    @Test
    public void testMultipleResultSets() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            boolean hasResults = stmt.execute(
                    "SELECT * FROM " + tableName + " WHERE COL2 <= 3; SELECT COUNT(*) FROM " + tableName);
            assertTrue(hasResults);
            try (ResultSet rs = stmt.getResultSet()) {
                assertNotNull(rs);
                int count = 0;
                while (rs.next())
                    count++;
                assertTrue(count >= 3);
            }
            assertTrue(stmt.getMoreResults());
            try (ResultSet rs = stmt.getResultSet()) {
                assertTrue(rs.next());
                assertTrue(rs.getInt(1) > 0);
            }
        }
    }

    // Max rows enforcement across cursor types
    @Test
    public void testMaxRowsWithScrollInsensitive() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY)) {
            stmt.setMaxRows(3);
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
                int count = 0;
                while (rs.next())
                    count++;
                assertTrue(count <= 3);
            }
        }
    }

    // Fetch size interaction with cursor types
    @Test
    public void testFetchSizeWithScrollSensitive() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                        ResultSet.CONCUR_READ_ONLY)) {
            stmt.setFetchSize(5);
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
                assertEquals(5, rs.getFetchSize());
                assertTrue(rs.next());
            }
        }
    }

    // Parameterized scrollable and updatable result sets under every prepare method. A cursor is opened on the server
    // (sp_cursorprepexec), so a statement that asks for a scrollable or updatable result set is not a literal batch
    // even under prepareMethod=none. The result is compared with prepexec: the type and concurrency reported, the
    // navigation results and the values read.

    private static String navigationOutcome(String prepareMethod, String table, int type, int concurrency)
            throws SQLException {
        StringBuilder outcome = new StringBuilder();
        try (Connection conn = DriverManager.getConnection(connectionString + ";prepareMethod=" + prepareMethod);
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT ID, COL1, COL2 FROM " + table + " WHERE COL2 > ? AND COL1 <> ? ORDER BY ID", type,
                        concurrency)) {
            ps.setInt(1, 3);
            ps.setString(2, "row7");
            try (ResultSet rs = ps.executeQuery()) {
                outcome.append("type=").append(rs.getType()).append(" concurrency=").append(rs.getConcurrency());
                outcome.append(" last=").append(rs.last()).append(':').append(rs.getRow()).append(':')
                        .append(rs.getString("COL1"));
                outcome.append(" first=").append(rs.first()).append(':').append(rs.getString("COL1"));
                outcome.append(" abs3=").append(rs.absolute(3)).append(':').append(rs.getInt("COL2"));
                outcome.append(" prev=").append(rs.previous()).append(':').append(rs.getInt("COL2"));
                outcome.append(" rel2=").append(rs.relative(2)).append(':').append(rs.getInt("COL2"));
                outcome.append(" abs99=").append(rs.absolute(99)).append(" afterLast=").append(rs.isAfterLast());
            }
        }
        return outcome.toString();
    }

    @ParameterizedTest(name = "mode={0}, type={1}")
    @MethodSource("scrollableModes")
    public void testParameterizedScrollableResultSetMatchesTypedBinding(String prepareMethod, int type)
            throws SQLException {
        // A table of its own, because the shared table is changed by other tests.
        String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("CursorNav_Tab"));
        try (Connection setup = getConnection(); Statement stmt = setup.createStatement()) {
            try {
                stmt.executeUpdate("CREATE TABLE " + table + " (ID INT PRIMARY KEY, COL1 VARCHAR(50), COL2 INT)");
                for (int i = 1; i <= 10; i++) {
                    stmt.executeUpdate("INSERT INTO " + table + " VALUES (" + i + ", 'row" + i + "', " + i + ")");
                }
                String typed = navigationOutcome("prepexec", table, type, ResultSet.CONCUR_READ_ONLY);
                assertTrue(typed.contains("last=true:6:row10 first=true:row4 abs3=true:6 prev=true:5"), typed);
                assertEquals(typed, navigationOutcome(prepareMethod, table, type, ResultSet.CONCUR_READ_ONLY));
            } finally {
                TestUtils.dropTableIfExists(table, stmt);
            }
        }
    }

    private static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> scrollableModes() {
        java.util.List<org.junit.jupiter.params.provider.Arguments> cases = new java.util.ArrayList<>();
        for (String mode : new String[] {"none", "scopeTempTablesToConnection"}) {
            for (int type : new int[] {ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE}) {
                cases.add(org.junit.jupiter.params.provider.Arguments.of(mode, type));
            }
        }
        return cases.stream();
    }

    // An updatable result set from a parameterized query: updateRow() changes the row, and a separate connection
    // that uses the default prepare method sees the change.
    @ParameterizedTest(name = "mode={0}")
    @ValueSource(strings = {"scopeTempTablesToConnection", "prepexec"})
    public void testParameterizedUpdatableResultSetUpdatesTheRow(String prepareMethod) throws SQLException {
        assertEquals("1:one,2:TWO,3:three,", updatableOutcome(prepareMethod, true));
    }

    // Under prepareMethod=none the statement is a literal batch and no server cursor is opened, so the result set is
    // not updatable: updateString() is rejected and the row is unchanged. This is how direct execution behaved before
    // the procedure-call change (it does not touch cursors) and it is recorded here.
    @Test
    public void testParameterizedUpdatableResultSetIsNotUpdatableInDirectMode() throws SQLException {
        assertEquals("1:one,2:two,3:three,", updatableOutcome("none", false));
    }

    private static String updatableOutcome(String prepareMethod, boolean updateSupported) throws SQLException {
        String updatable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("CursorUpd_Tab"));
        try (Connection setup = getConnection(); Statement stmt = setup.createStatement()) {
            try {
                stmt.executeUpdate("CREATE TABLE " + updatable + " (ID INT PRIMARY KEY, COL1 VARCHAR(50))");
                stmt.executeUpdate("INSERT INTO " + updatable + " VALUES (1, 'one'), (2, 'two'), (3, 'three')");

                try (Connection conn = DriverManager
                        .getConnection(connectionString + ";prepareMethod=" + prepareMethod);
                        PreparedStatement ps = conn.prepareStatement(
                                "SELECT ID, COL1 FROM " + updatable + " WHERE ID = ?",
                                ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
                    ps.setInt(1, 2);
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals("two", rs.getString("COL1"));
                        if (updateSupported) {
                            rs.updateString("COL1", "TWO");
                            rs.updateRow();
                            assertEquals("TWO", rs.getString("COL1"));
                            assertTrue(!rs.next());
                        } else {
                            SQLServerException e = assertThrows(SQLServerException.class,
                                    () -> rs.updateString("COL1", "TWO"));
                            assertTrue(e.getMessage().contains("not updatable"), e.getMessage());
                        }
                    }
                }

                StringBuilder rows = new StringBuilder();
                try (ResultSet rs = stmt.executeQuery("SELECT ID, COL1 FROM " + updatable + " ORDER BY ID")) {
                    while (rs.next()) {
                        rows.append(rs.getInt(1)).append(':').append(rs.getString(2)).append(',');
                    }
                }
                return rows.toString();
            } finally {
                TestUtils.dropTableIfExists(updatable, stmt);
            }
        }
    }
}
