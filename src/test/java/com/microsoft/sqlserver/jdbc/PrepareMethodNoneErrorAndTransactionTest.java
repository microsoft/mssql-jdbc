/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.PrepUtil;


/** Detailed error, warning and transaction contracts for direct and prepared execution. */
@RunWith(JUnitPlatform.class)
public class PrepareMethodNoneErrorAndTransactionTest extends AbstractTest {
    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @ParameterizedTest(name = "error={0}, prepareOnFirst={1}")
    @MethodSource("serverErrors")
    public void testServerErrorDetailsAndStatementReuse(int errorCode, boolean prepareOnFirst) throws SQLException {
        List<String> expected = serverErrorOutcome("prepexec", prepareOnFirst, errorCode);
        assertEquals(expected, serverErrorOutcome("none", prepareOnFirst, errorCode));
    }

    private static Stream<Arguments> serverErrors() {
        return Stream.of(245, 8134, 2627).flatMap(code -> Stream.of(false, true)
                .map(prepareOnFirst -> Arguments.of(code, prepareOnFirst)));
    }

    private static List<String> serverErrorOutcome(String mode, boolean prepareOnFirst, int errorCode)
            throws SQLException {
        try (Connection con = openConnection(mode, prepareOnFirst); Statement stmt = con.createStatement()) {
            stmt.execute("SET ANSI_WARNINGS ON; SET ARITHABORT ON; SET ARITHIGNORE OFF; "
                    + "CREATE TABLE #error_rows (id int PRIMARY KEY)");
            String sql = 245 == errorCode ? "SELECT CONVERT(int, ?)"
                                         : 8134 == errorCode ? "SELECT 12 / ?" : "INSERT #error_rows VALUES (?)";
            try (PreparedStatement ps = con.prepareStatement(sql)) {
                // Warm the same statement so prepareOnFirst=false also reaches sp_prepexec before the error.
                for (int value : new int[] {1, 2}) {
                    bindErrorValue(ps, errorCode, value, false);
                    if (2627 == errorCode) {
                        assertEquals(1, ps.executeUpdate());
                    } else {
                        try (ResultSet rs = ps.executeQuery()) {
                            assertTrue(rs.next());
                            assertEquals(245 == errorCode ? value : 12 / value, rs.getInt(1));
                            assertFalse(rs.next());
                        }
                    }
                }
                bindErrorValue(ps, errorCode, 1, true);
                SQLException error = assertThrows(SQLException.class, ps::execute);
                assertEquals(errorCode, error.getErrorCode());
                assertNotNull(error.getSQLState());
                List<String> details = exceptionDetails(error);
                bindErrorValue(ps, errorCode, 3, false);
                if (2627 == errorCode) {
                    assertEquals(1, ps.executeUpdate());
                    try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM #error_rows")) {
                        assertTrue(rs.next());
                        assertEquals(3, rs.getInt(1), "The failed insert must not add a row");
                    }
                } else {
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(245 == errorCode ? 3 : 4, rs.getInt(1));
                        assertFalse(rs.next());
                    }
                }
                return details;
            }
        }
    }

    private static void bindErrorValue(PreparedStatement ps, int errorCode, int value, boolean fail)
            throws SQLException {
        if (245 == errorCode) {
            ps.setString(1, fail ? "not-an-integer" : Integer.toString(value));
        } else {
            ps.setInt(1, fail && 8134 == errorCode ? 0 : value);
        }
    }

    @ParameterizedTest(name = "largeBatch={0}, prepareOnFirst={1}")
    @MethodSource("batchConfigurations")
    public void testProcedureBatchErrorChainMatchesPreparedExecution(boolean largeBatch, boolean prepareOnFirst)
            throws SQLException {
        assertEquals(batchErrorOutcome("prepexec", prepareOnFirst, largeBatch),
                batchErrorOutcome("none", prepareOnFirst, largeBatch));
    }

    private static Stream<Arguments> batchConfigurations() {
        return Stream.of(false, true).flatMap(large -> Stream.of(false, true)
                .map(prepareOnFirst -> Arguments.of(large, prepareOnFirst)));
    }

    private static List<String> batchErrorOutcome(String mode, boolean prepareOnFirst, boolean largeBatch)
            throws SQLException {
        try (Connection con = openConnection(mode, prepareOnFirst); Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE #batch_errors (id int)");
            stmt.execute("CREATE PROCEDURE #batch_error_p @id int AS BEGIN "
                    + "IF @id = 13 THROW 50000, 'expected batch failure', 1; INSERT #batch_errors VALUES (@id) END");
            try (PreparedStatement ps = con.prepareStatement("EXEC #batch_error_p ?")) {
                for (int id : new int[] {1, 2, 13, 4}) {
                    ps.setInt(1, id);
                    ps.addBatch();
                }
                BatchUpdateException error = assertThrows(BatchUpdateException.class, () -> {
                    if (largeBatch) {
                        ps.executeLargeBatch();
                    } else {
                        ps.executeBatch();
                    }
                });
                assertEquals(50000, error.getErrorCode());
                assertArrayEquals(new long[] {1, 1, Statement.EXECUTE_FAILED, Statement.EXECUTE_FAILED},
                        error.getLargeUpdateCounts());
                List<String> details = exceptionDetails(error);
                try (ResultSet rs = stmt.executeQuery("SELECT id FROM #batch_errors ORDER BY id")) {
                    for (int id : new int[] {1, 2}) {
                        assertTrue(rs.next());
                        assertEquals(id, rs.getInt(1));
                    }
                    assertFalse(rs.next());
                }
                ps.clearBatch();
                ps.setInt(1, 5);
                assertEquals(1, ps.executeUpdate(), "The statement must remain usable after the failed batch");
                return details;
            }
        }
    }

    private static List<String> exceptionDetails(SQLException error) {
        List<String> details = new ArrayList<>();
        for (SQLException current = error; null != current; current = current.getNextException()) {
            assertTrue(details.size() < 10, "The exception chain must terminate");
            details.add(current.getClass().getName() + ":" + current.getSQLState() + ":" + current.getErrorCode());
        }
        return details;
    }

    @ParameterizedTest(name = "warnings, prepareOnFirst={0}")
    @ValueSource(booleans = {false, true})
    public void testWarningDetailsAndClearWarnings(boolean prepareOnFirst) throws SQLException {
        assertEquals(warningOutcome("prepexec", prepareOnFirst), warningOutcome("none", prepareOnFirst));
    }

    private static List<String> warningOutcome(String mode, boolean prepareOnFirst) throws SQLException {
        List<String> details = new ArrayList<>();
        try (Connection con = openConnection(mode, prepareOnFirst);
                PreparedStatement ps = con.prepareStatement("PRINT 'prepare-none-warning'; SELECT CAST(? AS int)")) {
            for (int value = 1; value <= 3; value++) {
                ps.setInt(1, value);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(value, rs.getInt(1));
                    assertFalse(rs.next());
                }
                SQLWarning warning = ps.getWarnings();
                assertNotNull(warning);
                assertEquals("prepare-none-warning", warning.getMessage());
                details.addAll(exceptionDetails(warning));
                ps.clearWarnings();
                assertNull(ps.getWarnings());
            }
        }
        return details;
    }

    @ParameterizedTest(name = "savepoints across RPC transition, prepareOnFirst={0}")
    @ValueSource(booleans = {false, true})
    public void testSavepointsAcrossDirectRpcTransition(boolean prepareOnFirst) throws SQLException {
        for (String mode : new String[] {"prepexec", "none"}) {
            try (Connection con = openConnection(mode, prepareOnFirst); Statement stmt = con.createStatement()) {
                stmt.execute("CREATE TABLE #savepoint_rows (id int PRIMARY KEY, payload varbinary(8))");
                con.setAutoCommit(false);
                try (PreparedStatement ps = con.prepareStatement("INSERT #savepoint_rows VALUES (?, ?)")) {
                    insert(ps, 1, false);
                    Savepoint beforeRpc = con.setSavepoint("before_rpc");
                    insert(ps, 2, true);
                    con.rollback(beforeRpc);
                    insert(ps, 3, false);
                    Savepoint beforeBatch = con.setSavepoint("before_batch");
                    for (int id : new int[] {4, 5}) {
                        ps.setInt(1, id);
                        if (5 == id) {
                            ps.setBinaryStream(2, new ByteArrayInputStream(new byte[] {(byte) id}));
                        } else {
                            ps.setBytes(2, new byte[] {(byte) id});
                        }
                        ps.addBatch();
                    }
                    assertArrayEquals(new int[] {1, 1}, ps.executeBatch());
                    con.rollback(beforeBatch);
                    con.commit();
                    insert(ps, 6, true);
                    con.rollback();
                    try (ResultSet rs = stmt.executeQuery("SELECT id, payload FROM #savepoint_rows ORDER BY id")) {
                        for (int id : new int[] {1, 3}) {
                            assertTrue(rs.next(), mode);
                            assertEquals(id, rs.getInt(1), mode);
                            assertArrayEquals(new byte[] {(byte) id}, rs.getBytes(2), mode);
                        }
                        assertFalse(rs.next(), mode);
                    }
                } finally {
                    con.rollback();
                    con.setAutoCommit(true);
                }
            }
        }
    }

    private static void insert(PreparedStatement ps, int id, boolean stream) throws SQLException {
        ps.setInt(1, id);
        if (stream) {
            ps.setBinaryStream(2, new ByteArrayInputStream(new byte[] {(byte) id}));
        } else {
            ps.setBytes(2, new byte[] {(byte) id});
        }
        assertEquals(1, ps.executeUpdate());
    }

    private static Connection openConnection(String mode, boolean prepareOnFirst) throws SQLException {
        String url = TestUtils.addOrOverrideProperty(connectionString, "prepareMethod", mode);
        for (String[] property : Arrays.asList(new String[] {"disableStatementPooling", "true"},
                new String[] {"statementPoolingCacheSize", "0"}, new String[] {"columnEncryptionSetting", "Disabled"},
                new String[] {"enablePrepareOnFirstPreparedStatementCall", Boolean.toString(prepareOnFirst)},
                new String[] {"useBulkCopyForBatchInsert", "false"}, new String[] {"retryExec", ""})) {
            url = TestUtils.addOrOverrideProperty(url, property[0], property[1]);
        }
        return PrepUtil.getConnection(url);
    }
}