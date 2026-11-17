/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.microsoft.sqlserver.testframework.AbstractTest;
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
    public void lateErrorsAfterFilledSlotsAreNotDiscarded(boolean large) throws Exception {
        try (SQLServerConnection con = open("none"); Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE #counts (id int)");
            try (PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO #counts VALUES (?); RAISERROR('late first',16,1); RAISERROR('late second',16,1)")) {
                ps.setInt(1, 1);
                ps.addBatch();
                BatchUpdateException error = assertThrows(BatchUpdateException.class, () -> execute(ps, large));
                // The count precedes the error. Do not invent a parameter-set identity for that late error.
                assertArrayEquals(new long[] {1}, error.getLargeUpdateCounts());
                assertEquals(50000, error.getErrorCode());
                assertNotNull(error.getCause());
                assertTrue(error.getCause() instanceof SQLException);
                assertNotNull(((SQLException) error.getCause()).getNextException());
                assertEquals(1, scalar(stmt, "SELECT COUNT(*) FROM #counts"));
                ps.setInt(1, 2);
                ps.addBatch();
                assertThrows(BatchUpdateException.class, () -> execute(ps, large));
                assertEquals(3, scalar(stmt, "SELECT SUM(id) FROM #counts"));
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"false", "true"})
    public void unexpectedLateResultSetFails(boolean large) throws Exception {
        try (SQLServerConnection con = open("none"); Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE #counts (id int)");
            try (PreparedStatement ps = con
                    .prepareStatement("INSERT INTO #counts VALUES (?); SELECT id FROM #counts")) {
                ps.setInt(1, 7);
                ps.addBatch();
                BatchUpdateException error = assertThrows(BatchUpdateException.class, () -> execute(ps, large));
                assertArrayEquals(new long[] {1}, error.getLargeUpdateCounts());
                assertTrue(
                        error.getMessage().contains(SQLServerException.getErrString("R_resultsetGeneratedForUpdate")));
                assertEquals(7, scalar(stmt, "SELECT SUM(id) FROM #counts"));
                assertFalse(ps.getMoreResults());
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"false", "true"})
    public void resultSetInFirstSlotIsNotSuccessful(boolean large) throws Exception {
        try (SQLServerConnection con = open("none"); Statement stmt = con.createStatement();
                PreparedStatement ps = con.prepareStatement("/* result set first */ SELECT ?")) {
            ps.setInt(1, 7);
            ps.addBatch();
            BatchUpdateException error = assertThrows(BatchUpdateException.class, () -> execute(ps, large));
            assertArrayEquals(new long[] {Statement.EXECUTE_FAILED}, error.getLargeUpdateCounts());
            assertTrue(error.getMessage().contains(SQLServerException.getErrString("R_resultsetGeneratedForUpdate")));
            assertEquals(7, scalar(stmt, "SELECT 7"));
        }
    }

    @ParameterizedTest
    @CsvSource({"false", "true"})
    public void resultSetRowErrorIsNotCleanupOnly(boolean large) throws Exception {
        try (SQLServerConnection con = open("none"); Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE #counts (id int)");
            try (PreparedStatement ps = con
                    .prepareStatement("INSERT INTO #counts VALUES (?); SELECT 10/(id-id) FROM #counts")) {
                ps.setInt(1, 7);
                ps.addBatch();
                BatchUpdateException error = assertThrows(BatchUpdateException.class, () -> execute(ps, large));
                assertArrayEquals(new long[] {1}, error.getLargeUpdateCounts());
                assertEquals(8134, error.getErrorCode());
                assertEquals(7, scalar(stmt, "SELECT SUM(id) FROM #counts"));
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
    public void timeoutDoesNotBecomeSuccess(boolean large) throws Exception {
        try (SQLServerConnection con = open("none"); Statement stmt = con.createStatement();
                PreparedStatement ps = con.prepareStatement("WAITFOR DELAY '00:00:10'; UPDATE #counts SET id=?")) {
            stmt.execute("CREATE TABLE #counts (id int); INSERT INTO #counts VALUES (1)");
            ps.setQueryTimeout(1);
            ps.setInt(1, 2);
            ps.addBatch();
            SQLException error = assertThrows(SQLException.class, () -> execute(ps, large));
            assertFalse(error instanceof BatchUpdateException);
            assertTrue(error instanceof SQLTimeoutException || "HY008".equals(error.getSQLState()));
            assertEquals(1, scalar(stmt, "SELECT SUM(id) FROM #counts"));
            ps.setQueryTimeout(0);
            assertArrayEquals(new long[0], execute(ps, large));
        }
    }

    @ParameterizedTest
    @CsvSource({"false", "true"})
    @Timeout(30)
    public void cancelAfterCountStillFails(boolean large) throws Exception {
        try (SQLServerConnection con = open("none"); Statement stmt = con.createStatement();
                PreparedStatement ps = con.prepareStatement("UPDATE #counts SET id=?; WAITFOR DELAY '00:00:10'")) {
            stmt.execute("CREATE TABLE #counts (id int); INSERT INTO #counts VALUES (1)");
            ps.setInt(1, 2);
            ps.addBatch();
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                Future<?> cancel = executor.submit(() -> {
                    TimeUnit.SECONDS.sleep(1);
                    ps.cancel();
                    return null;
                });
                SQLException error = assertThrows(SQLException.class, () -> execute(ps, large));
                assertFalse(error instanceof BatchUpdateException);
                assertEquals("HY008", error.getSQLState());
                cancel.get(10, TimeUnit.SECONDS);
                assertEquals(2, scalar(stmt, "SELECT SUM(id) FROM #counts"));
            } finally {
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            }
        }
    }
}
