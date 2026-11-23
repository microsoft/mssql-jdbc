/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.StringReader;
import java.lang.reflect.Field;
import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * Counts are result positions, not general compound-SQL entry boundaries, in a combined language batch.
 * These tests distinguish one-result entries from late results/errors and verify persisted state independently.
 */
public class DirectBatchUpdateCountsTest extends AbstractTest {
    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    private static SQLServerConnection open(String method) throws SQLException {
        SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(connectionString);
        con.setPrepareMethod(method);
        return con;
    }

    private static long[] execute(PreparedStatement ps, boolean large) throws SQLException {
        if (large)
            return ps.executeLargeBatch();
        int[] counts = ps.executeBatch();
        long[] result = new long[counts.length];
        for (int i = 0; i < counts.length; i++)
            result[i] = counts[i];
        return result;
    }

    private static void assertDirect(PreparedStatement ps, boolean expected) throws Exception {
        Field field = SQLServerPreparedStatement.class.getDeclaredField("isDirectSqlExecution");
        field.setAccessible(true);
        assertEquals(expected, field.getBoolean(ps));
    }

    private static int scalar(Statement stmt, String sql) throws SQLException {
        try (ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    @ParameterizedTest
    @CsvSource({"none,false", "none,true", "prepexec,false", "prepexec,true", "scopeTempTablesToConnection,false",
            "scopeTempTablesToConnection,true"})
    public void actualCountsAndReuse(String method, boolean large) throws Exception {
        try (SQLServerConnection con = open(method); Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE #counts (id int PRIMARY KEY, value int NOT NULL);"
                    + "INSERT INTO #counts VALUES (1,0),(2,0),(3,0),(4,0),(5,0)");
            // Merely referencing #counts does not select scopeTempTablesToConnection's direct path.
            try (PreparedStatement ps = con.prepareStatement("UPDATE #counts SET value=value+1 WHERE id <= ?")) {
                assertDirect(ps, "none".equals(method));
                for (int limit : new int[] {0, 5, 1, 0}) {
                    ps.setInt(1, limit);
                    ps.addBatch();
                }
                assertArrayEquals(new long[] {0, 5, 1, 0}, execute(ps, large));
                assertEquals(6, scalar(stmt, "SELECT SUM(value) FROM #counts"));
                assertArrayEquals(new long[0], execute(ps, large));
                assertEquals(0, ps.executeUpdate());
                ps.setInt(1, 2);
                ps.addBatch();
                assertArrayEquals(new long[] {2}, execute(ps, large));
                assertEquals(8, scalar(stmt, "SELECT SUM(value) FROM #counts"));
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"false", "true"})
    public void insertDeleteAndNoCount(boolean large) throws Exception {
        try (SQLServerConnection con = open("none"); Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE #counts (id int);");
            try (PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO #counts SELECT v FROM (VALUES (1),(2),(3),(4),(5)) s(v) WHERE v<=?")) {
                ps.setInt(1, 5);
                ps.addBatch();
                ps.setInt(1, 2);
                ps.addBatch();
                assertArrayEquals(new long[] {5, 2}, execute(ps, large));
            }
            assertEquals(7, scalar(stmt, "SELECT COUNT(*) FROM #counts"));
            try (PreparedStatement ps = con.prepareStatement("DELETE FROM #counts WHERE id=?")) {
                ps.setInt(1, 1);
                ps.addBatch();
                ps.setInt(1, 8);
                ps.addBatch();
                assertArrayEquals(new long[] {2, 0}, execute(ps, large));
            }
            stmt.execute("SET NOCOUNT ON");
            try (PreparedStatement ps = con.prepareStatement("UPDATE #counts SET id=id+?")) {
                ps.setInt(1, 1);
                ps.addBatch();
                ps.addBatch();
                assertArrayEquals(new long[] {Statement.SUCCESS_NO_INFO, Statement.SUCCESS_NO_INFO},
                        execute(ps, large));
            }
            assertEquals(5, scalar(stmt, "SELECT COUNT(*) FROM #counts"));
            assertEquals(26, scalar(stmt, "SELECT SUM(id) FROM #counts"));
        }
    }

    @ParameterizedTest
    @CsvSource({"none,false", "none,true", "prepexec,false", "prepexec,true"})
    public void recoverableErrorPreservesCounts(String method, boolean large) throws Exception {
        try (SQLServerConnection con = open(method); Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE #counts (id int PRIMARY KEY)");
            try (PreparedStatement ps = con.prepareStatement("INSERT INTO #counts VALUES (?)")) {
                for (int id : new int[] {1, 1, 2}) {
                    ps.setInt(1, id);
                    ps.addBatch();
                }
                BatchUpdateException error = assertThrows(BatchUpdateException.class, () -> execute(ps, large));
                assertEquals(2627, error.getErrorCode());
                assertArrayEquals(new long[] {1, Statement.EXECUTE_FAILED, 1}, error.getLargeUpdateCounts());
                assertEquals(2, scalar(stmt, "SELECT COUNT(*) FROM #counts"));
                ps.setInt(1, 3);
                ps.addBatch();
                assertArrayEquals(new long[] {1}, execute(ps, large));
                assertEquals(3, scalar(stmt, "SELECT COUNT(*) FROM #counts"));
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"none,false", "none,true", "prepexec,false", "prepexec,true"})
    @Tag(Constants.xAzureSQLDW)
    public void serverErrorThrowsBatchUpdateException(String method, boolean large) throws Exception {
        String message = "direct batch exception regression";
        try (SQLServerConnection con = open(method); Statement stmt = con.createStatement();
                PreparedStatement ps = con.prepareStatement("RAISERROR(?,16,1)")) {
            SQLServerException serverError = assertThrows(SQLServerException.class,
                    () -> stmt.execute("RAISERROR('direct batch exception regression',16,1)"));
            ps.setString(1, message);
            ps.addBatch();

            BatchUpdateException batchError = assertThrows(BatchUpdateException.class, () -> execute(ps, large));
            assertEquals(BatchUpdateException.class, batchError.getClass(),
                    "The public API must throw BatchUpdateException, not wrap it in SQLServerException");
            assertEquals(50000, batchError.getErrorCode());
            assertEquals(serverError.getSQLState(), batchError.getSQLState());
            assertTrue(batchError.getMessage().contains(message));
            if (large)
                assertArrayEquals(new long[] {Statement.EXECUTE_FAILED}, batchError.getLargeUpdateCounts());
            else
                assertArrayEquals(new int[] {Statement.EXECUTE_FAILED}, batchError.getUpdateCounts());
            assertArrayEquals(new long[0], execute(ps, large), "The failed batch must be cleared");
            assertEquals(1, scalar(stmt, "SELECT 1"));
        }
    }

    @ParameterizedTest
    @CsvSource({"false", "true"})
    public void abortedEntriesRemainFailed(boolean large) throws Exception {
        try (SQLServerConnection con = open("none"); Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE #counts (id int)");
            try (PreparedStatement ps = con.prepareStatement("INSERT INTO #counts VALUES (CONVERT(int, ?))")) {
                for (String id : new String[] {"1", "invalid", "2"}) {
                    ps.setString(1, id);
                    ps.addBatch();
                }
                BatchUpdateException error = assertThrows(BatchUpdateException.class, () -> execute(ps, large));
                assertEquals(245, error.getErrorCode());
                assertArrayEquals(new long[] {1, Statement.EXECUTE_FAILED, Statement.EXECUTE_FAILED},
                        error.getLargeUpdateCounts());
                assertEquals(1, scalar(stmt, "SELECT COUNT(*) FROM #counts"));
                assertEquals(1, scalar(stmt, "SELECT SUM(id) FROM #counts"));
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"false", "true"})
    public void compoundResultsArePositionsNotEntryTotals(boolean large) throws Exception {
        try (SQLServerConnection con = open("none"); Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE #counts (id int); INSERT INTO #counts VALUES (1),(2),(3)");
            try (PreparedStatement ps = con
                    .prepareStatement("UPDATE #counts SET id=id+?; UPDATE #counts SET id=id+1 WHERE id=2")) {
                ps.setInt(1, 1);
                ps.addBatch();
                // One slot receives the first observed count (3), not the sum of this entry's two counts (4).
                assertArrayEquals(new long[] {3}, execute(ps, large));
                assertEquals(10, scalar(stmt, "SELECT SUM(id) FROM #counts"));
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"none,false", "none,true", "prepexec,false", "prepexec,true"})
    public void conditionalCountsAndLateDuplicateKey(String method, boolean large) throws Exception {
        try (SQLServerConnection con = open(method); Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE #counts (id int PRIMARY KEY, value int);"
                    + "INSERT INTO #counts VALUES (1,0),(2,0),(3,0),(4,0),(5,0);"
                    + "CREATE TABLE #pk (id int PRIMARY KEY)");
            try (PreparedStatement ps = con.prepareStatement("IF ?=1 UPDATE #counts SET value=value+1 WHERE id<=?")) {
                for (int[] entry : new int[][] {{1, 3}, {1, 5}, {0, 0}}) {
                    ps.setInt(1, entry[0]);
                    ps.setInt(2, entry[1]);
                    ps.addBatch();
                }
                long[] expected = "none".equals(method)
                                                        ? new long[] {Statement.SUCCESS_NO_INFO, 3,
                                                                Statement.SUCCESS_NO_INFO}
                                                        : new long[] {3, 5, Statement.SUCCESS_NO_INFO};
                assertArrayEquals(expected, execute(ps, large));
                assertEquals(8, scalar(stmt, "SELECT SUM(value) FROM #counts"));
                ps.setInt(1, 1);
                ps.setInt(2, 1);
                ps.addBatch();
                assertArrayEquals("none".equals(method) ? new long[] {Statement.SUCCESS_NO_INFO} : new long[] {1},
                        execute(ps, large));
                assertEquals(9, scalar(stmt, "SELECT SUM(value) FROM #counts"));
            }
            try (PreparedStatement ps = con.prepareStatement(
                    "IF ?=1 UPDATE #counts SET value=value+1 WHERE id<=? ELSE INSERT INTO #pk VALUES (1)")) {
                for (int[] entry : new int[][] {{1, 3}, {0, 0}, {0, 0}}) {
                    ps.setInt(1, entry[0]);
                    ps.setInt(2, entry[1]);
                    ps.addBatch();
                }
                BatchUpdateException error = assertThrows(BatchUpdateException.class, () -> execute(ps, large));
                assertEquals(2627, error.getErrorCode());
                assertEquals("23000", error.getSQLState());
                assertArrayEquals(
                        "none".equals(method) ? new long[] {Statement.SUCCESS_NO_INFO, 3, Statement.SUCCESS_NO_INFO}
                                              : new long[] {3, 1, Statement.EXECUTE_FAILED},
                        error.getLargeUpdateCounts());
                assertEquals(12, scalar(stmt, "SELECT SUM(value) FROM #counts"));
                assertEquals(1, scalar(stmt, "SELECT COUNT(*) FROM #pk"));
                assertArrayEquals(new long[0], execute(ps, large));
                ps.setInt(1, 1);
                ps.setInt(2, 1);
                ps.addBatch();
                execute(ps, large);
                assertEquals(13, scalar(stmt, "SELECT SUM(value) FROM #counts"));
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"none,false", "none,true", "scopeTempTablesToConnection,false", "scopeTempTablesToConnection,true"})
    public void errorAfterCountSlotsAreFull(String method, boolean large) throws Exception {
        try (SQLServerConnection con = open(method); Statement stmt = con.createStatement();
                PreparedStatement ps = con.prepareStatement(
                        "CREATE TABLE #late_error (id int PRIMARY KEY); INSERT INTO #late_error VALUES (?),(?)")) {
            assertDirect(ps, true);
            ps.setInt(1, 1);
            ps.setInt(2, 1);
            ps.addBatch();
            BatchUpdateException error = assertThrows(BatchUpdateException.class, () -> execute(ps, large));
            assertEquals(2627, error.getErrorCode());
            assertArrayEquals(new long[] {0}, error.getLargeUpdateCounts(),
                    "The late failure has no count slot; do not overwrite the observed CREATE count");
            assertEquals(0, scalar(stmt, "SELECT COUNT(*) FROM #late_error"));
            stmt.execute("DROP TABLE #late_error");
            ps.setInt(2, 2);
            ps.addBatch();
            assertArrayEquals(new long[] {0}, execute(ps, large));
            assertEquals(2, scalar(stmt, "SELECT COUNT(*) FROM #late_error"));
        }
    }

    @ParameterizedTest
    @CsvSource({"false,1003,false", "true,1003,false", "false,1004,false", "true,1004,false", "false,1005,false",
            "true,1005,false", "false,1003,true", "true,1003,true", "false,1004,true", "true,1004,true",
            "false,1005,true", "true,1005,true"})
    @Timeout(30)
    public void trailingResultSetIsDrainedWithoutHidingRowErrors(boolean large, int resultType,
            boolean rowError) throws Exception {
        try (SQLServerConnection con = open("none"); Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE #counts (id int); INSERT INTO #counts VALUES (0);"
                    + "CREATE TABLE #rows (id int PRIMARY KEY CLUSTERED, v varchar(20));"
                    + "INSERT INTO #rows SELECT TOP (512) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), '1'"
                    + " FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) a(n)"
                    + " CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) b(n)"
                    + " CROSS JOIN (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) c(n)");
            if (rowError)
                stmt.execute("UPDATE #rows SET v='invalid' WHERE id=512");
            try (PreparedStatement ps = con.prepareStatement(
                    "UPDATE #counts SET id=id+?; SELECT CONVERT(int,v), REPLICATE('x',2000) FROM #rows ORDER BY id;"
                            + "UPDATE #counts SET id=id+1",
                    resultType, ResultSet.CONCUR_READ_ONLY)) {
                ps.setMaxRows(1);
                ((SQLServerStatement) ps).setResponseBuffering(large ? "full" : "adaptive");
                ps.setInt(1, 1);
                ps.addBatch();
                BatchUpdateException error = assertThrows(BatchUpdateException.class, () -> execute(ps, large));
                if (rowError)
                    assertEquals(245, error.getErrorCode(), "The server row error must survive result-set cleanup");
                else
                    assertEquals(SQLServerException.getErrString("R_resultsetGeneratedForUpdate"), error.getMessage());
                assertArrayEquals(new long[] {1}, error.getLargeUpdateCounts());
                assertEquals(rowError ? 1 : 2, scalar(stmt, "SELECT SUM(id) FROM #counts"));
                assertArrayEquals(new long[0], execute(ps, large));
                // Reuse the same statement after failure, now with no rows to drain.
                stmt.execute("DELETE FROM #rows");
                ps.addBatch();
                BatchUpdateException reused = assertThrows(BatchUpdateException.class, () -> execute(ps, large));
                assertEquals(SQLServerException.getErrString("R_resultsetGeneratedForUpdate"), reused.getMessage());
                assertEquals(rowError ? 3 : 4, scalar(stmt, "SELECT SUM(id) FROM #counts"));
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"false", "true"})
    public void initialConditionalResultSetIsRejected(boolean large) throws Exception {
        try (SQLServerConnection con = open("none"); Statement stmt = con.createStatement();
                PreparedStatement ps = con.prepareStatement("IF ?=1 SELECT 42; INSERT INTO #counts VALUES (1)")) {
            stmt.execute("CREATE TABLE #counts (id int)");
            ps.setInt(1, 1);
            ps.addBatch();
            BatchUpdateException error = assertThrows(BatchUpdateException.class, () -> execute(ps, large));
            assertEquals(SQLServerException.getErrString("R_resultsetGeneratedForUpdate"), error.getMessage());
            assertEquals(1, scalar(stmt, "SELECT COUNT(*) FROM #counts"));
        }
    }

    @ParameterizedTest
    @CsvSource({"false,1003", "true,1003", "false,1004", "true,1004"})
    public void multipleUnexpectedResultSetsAreConsumed(boolean large, int resultType) throws Exception {
        try (SQLServerConnection con = open("none"); Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE #counts (id int); INSERT INTO #counts VALUES (0)");
            try (PreparedStatement ps = con.prepareStatement(
                    "UPDATE #counts SET id=id+?; SELECT v FROM (VALUES (1),(2),(3)) s(v);"
                            + "SELECT CAST(NULL AS int) AS v UNION ALL SELECT 4; UPDATE #counts SET id=id+10",
                    resultType, ResultSet.CONCUR_READ_ONLY)) {
                ps.setMaxRows(1);
                ps.setInt(1, 1);
                for (int execution = 1; execution <= 2; execution++) {
                    ps.addBatch();
                    BatchUpdateException error = assertThrows(BatchUpdateException.class, () -> execute(ps, large));
                    assertEquals(SQLServerException.getErrString("R_resultsetGeneratedForUpdate"), error.getMessage());
                    assertArrayEquals(new long[] {1}, error.getLargeUpdateCounts());
                    assertTrue(((SQLServerStatement) ps).resultsReader().getCommand().getProcessedResponse(),
                            "Response must already be consumed before another statement can perform cleanup");
                    assertEquals(11 * execution, scalar(stmt, "SELECT SUM(id) FROM #counts"));
                    assertArrayEquals(new long[0], execute(ps, large));
                }
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"false", "true"})
    public void rollbackAfterCountSlotsAreFull(boolean large) throws Exception {
        try (SQLServerConnection con = open("none"); Statement stmt = con.createStatement()) {
            stmt.execute(
                    "CREATE TABLE #counts (id int PRIMARY KEY); INSERT INTO #counts VALUES (1);" + "SET XACT_ABORT ON");
            con.setAutoCommit(false);
            try (PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO #counts VALUES (?); INSERT INTO #counts VALUES (1); INSERT INTO #counts VALUES (3)")) {
                ps.setInt(1, 2);
                ps.addBatch();
                SQLException error = assertThrows(SQLException.class, () -> execute(ps, large));
                assertFalse(error instanceof BatchUpdateException, "Late transaction rollback must propagate");
                assertEquals(2627, error.getErrorCode());
                assertEquals("23000", error.getSQLState());
                assertEquals(0, scalar(stmt, "SELECT XACT_STATE()"),
                        "Verify server rollback before an explicit client rollback can hide it");
                assertEquals(1, scalar(stmt, "SELECT COUNT(*) FROM #counts"));
                assertEquals(1, scalar(stmt, "SELECT SUM(id) FROM #counts"));
                con.rollback();
                con.setAutoCommit(true);
                assertArrayEquals(new long[0], execute(ps, large));
                stmt.execute("DELETE FROM #counts");
                ps.addBatch();
                assertArrayEquals(new long[] {1}, execute(ps, large));
                assertEquals(3, scalar(stmt, "SELECT COUNT(*) FROM #counts"));
                assertEquals(6, scalar(stmt, "SELECT SUM(id) FROM #counts"));
            } finally {
                if (!con.getAutoCommit()) {
                    con.rollback();
                    con.setAutoCommit(true);
                }
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"false", "true"})
    public void reuseAfterDirectBatchAndPreSendRpcFallback(boolean large) throws Exception {
        try (SQLServerConnection con = open("none"); Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE #counts (v nvarchar(20), executions int); INSERT INTO #counts VALUES ('',0)");
            try (PreparedStatement ps = con.prepareStatement("UPDATE #counts SET v=?, executions=executions+1",
                    ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
                ps.setString(1, "direct");
                ps.addBatch();
                assertArrayEquals(new long[] {1}, execute(ps, large));
                assertDirect(ps, true);
                ps.setCharacterStream(1, new StringReader("stream"), 6);
                ps.addBatch();
                assertArrayEquals(new long[] {1}, execute(ps, large));
                assertDirect(ps, false);
                assertEquals(2, scalar(stmt, "SELECT executions FROM #counts WHERE v='stream'"));
                ps.setString(1, "reused");
                ps.addBatch();
                assertArrayEquals(new long[] {1}, execute(ps, large));
                assertEquals(3, scalar(stmt, "SELECT executions FROM #counts WHERE v='reused'"));
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"1003", "1004", "1005"})
    public void ordinaryCloseRemainsBestEffort(int resultType) throws Exception {
        try (SQLServerConnection con = open("none"); Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE #rows (id int PRIMARY KEY CLUSTERED, v varchar(20));"
                    + "INSERT INTO #rows VALUES (1,'1'),(2,'2'),(3,'invalid')");
            try (PreparedStatement ps = con.prepareStatement("SELECT CONVERT(int,v) FROM #rows ORDER BY id", resultType,
                    ResultSet.CONCUR_READ_ONLY)) {
                try (ResultSet rs = ps.executeQuery()) {
                    assertDoesNotThrow(rs::close, "Ordinary cleanup must still suppress unread row errors");
                }
                stmt.execute("UPDATE #rows SET v='3' WHERE id=3");
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1));
                    if (resultType != ResultSet.TYPE_FORWARD_ONLY) {
                        assertTrue(rs.last());
                        assertEquals(3, rs.getInt(1));
                        assertTrue(rs.first());
                        assertEquals(1, rs.getInt(1));
                    }
                }
                assertEquals(1, scalar(stmt, "SELECT 1"));
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"false", "true"})
    @Timeout(30)
    @Tag(Constants.xAzureSQLDW)
    public void cancellationAfterCountSlotsAreFull(boolean large) throws Exception {
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try (SQLServerConnection con = open("none"); Statement stmt = con.createStatement();
                PreparedStatement ps = con.prepareStatement(
                        "UPDATE #counts SET id=id+?;" + "RAISERROR('batch ready to cancel',0,1) WITH NOWAIT;"
                                + "WAITFOR DELAY '00:00:15'; UPDATE #counts SET id=id+1")) {
            stmt.execute("CREATE TABLE #counts (id int); INSERT INTO #counts VALUES (0)");
            CountDownLatch ready = new CountDownLatch(1);
            con.setServerMessageHandler(message -> {
                if (message.getErrorMessage().contains("batch ready to cancel"))
                    ready.countDown();
                return message;
            });
            ps.setQueryTimeout(10);
            ps.setInt(1, 1);
            ps.addBatch();
            Future<SQLException> execution = worker
                    .submit(() -> assertThrows(SQLException.class, () -> execute(ps, large)));
            assertTrue(ready.await(5, TimeUnit.SECONDS), "Wait for server execution before cancelling");
            ps.cancel();
            SQLException error = execution.get(20, TimeUnit.SECONDS);
            assertFalse(error instanceof BatchUpdateException, "Interrupts must not become recoverable entry errors");
            assertTrue(error instanceof SQLTimeoutException || "HY008".equals(error.getSQLState()));
            assertEquals(1, scalar(stmt, "SELECT SUM(id) FROM #counts"));
            ps.setQueryTimeout(0);
            assertArrayEquals(new long[0], execute(ps, large));
        } finally {
            worker.shutdownNow();
            assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest
    @CsvSource({"false", "true"})
    public void optimisticLockZeroIsNotSuccessOne(boolean large) throws Exception {
        try (SQLServerConnection con = open("none"); Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE #counts (id int, version int); INSERT INTO #counts VALUES (1,0)");
            try (PreparedStatement ps = con
                    .prepareStatement("UPDATE #counts SET version=version+1 WHERE id=? AND version=?")) {
                ps.setInt(1, 1);
                ps.setInt(2, 0);
                ps.addBatch();
                ps.addBatch();
                assertArrayEquals(new long[] {1, 0}, execute(ps, large));
                assertEquals(1, scalar(stmt, "SELECT version FROM #counts WHERE id=1"));
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"false", "true"})
    public void scopeRoutingAndConnectionLifetime(boolean large) throws Exception {
        try (SQLServerConnection con = open("scopeTempTablesToConnection"); Statement stmt = con.createStatement()) {
            try (PreparedStatement ps = con.prepareStatement("CREATE TABLE #created_by_batch (id int)")) {
                assertDirect(ps, true);
                ps.addBatch();
                assertArrayEquals(new long[] {0}, execute(ps, large));
            }
            try (PreparedStatement ps = con.prepareStatement("INSERT INTO #created_by_batch VALUES (?)")) {
                assertDirect(ps, false);
                ps.setInt(1, 8);
                ps.addBatch();
                assertArrayEquals(new long[] {1}, execute(ps, large));
            }
            try (PreparedStatement ps = con.prepareStatement("/* count and scope */ SELECT TOP (?) id"
                    + " INTO #selected_by_batch FROM (VALUES (1),(2),(3),(4),(5)) s(id)")) {
                assertDirect(ps, true);
                for (int count : new int[] {0, 5, 1, 0}) {
                    ps.setInt(1, count);
                    ps.addBatch();
                    assertArrayEquals(new long[] {count}, execute(ps, large));
                    assertEquals(count, scalar(stmt, "SELECT COUNT(*) FROM #selected_by_batch"));
                    stmt.execute("DROP TABLE #selected_by_batch");
                }
            }
            assertEquals(8, scalar(stmt, "SELECT SUM(id) FROM #created_by_batch"));
        }
    }

    @ParameterizedTest
    @CsvSource({"false", "true"})
    public void rollbackErrorPropagates(boolean large) throws Exception {
        try (SQLServerConnection con = open("none"); Statement stmt = con.createStatement()) {
            stmt.execute(
                    "CREATE TABLE #counts (id int PRIMARY KEY); INSERT INTO #counts VALUES (1); SET XACT_ABORT ON");
            con.setAutoCommit(false);
            try (PreparedStatement ps = con.prepareStatement("INSERT INTO #counts VALUES (?)")) {
                for (int id : new int[] {2, 1, 3}) {
                    ps.setInt(1, id);
                    ps.addBatch();
                }
                SQLException error = assertThrows(SQLException.class, () -> execute(ps, large));
                assertFalse(error instanceof BatchUpdateException);
                assertEquals(2627, error.getErrorCode());
                con.rollback();
                con.setAutoCommit(true);
                assertEquals(1, scalar(stmt, "SELECT COUNT(*) FROM #counts"));
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"false", "true"})
    @Timeout(30)
    @Tag(Constants.xAzureSQLDW)
    public void timeoutDoesNotBecomeSuccess(boolean large) throws Exception {
        try (SQLServerConnection con = open("none"); Statement stmt = con.createStatement();
                PreparedStatement ps = con.prepareStatement("WAITFOR DELAY '00:00:10'; UPDATE #counts SET id=?")) {
            stmt.execute("CREATE TABLE #counts (id int); INSERT INTO #counts VALUES (1)");
            ps.setQueryTimeout(1);
            ps.setInt(1, 2);
            ps.addBatch();
            SQLException error = assertThrows(SQLException.class, () -> execute(ps, large));
            assertTrue(error instanceof SQLTimeoutException || "HY008".equals(error.getSQLState()));
            assertEquals(1, scalar(stmt, "SELECT SUM(id) FROM #counts"));
            ps.setQueryTimeout(0);
            assertArrayEquals(new long[0], execute(ps, large));
        }
    }
}
