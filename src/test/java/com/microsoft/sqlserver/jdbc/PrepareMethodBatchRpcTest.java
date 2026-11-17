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

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


public class PrepareMethodBatchRpcTest extends AbstractTest {
    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    static Stream<Arguments> modes() {
        return Stream.of("none", "scopeTempTablesToConnection")
                .flatMap(mode -> Stream.of(false, true).map(large -> Arguments.of(mode, large)));
    }

    private static Connection open(String mode, String... overrides) throws SQLException {
        String url = connectionString;
        String[][] properties = {{"prepareMethod", mode}, {"useBulkCopyForBatchInsert", "false"},
                {"disableStatementPooling", "false"}, {"statementPoolingCacheSize", "10"},
                {"enablePrepareOnFirstPreparedStatementCall", "true"}, {"sendStringParametersAsUnicode", "false"},
                {"columnEncryptionSetting", "Disabled"}, {"retryExec", ""}};
        for (String[] property : properties) {
            url = TestUtils.addOrOverrideProperty(url, property[0], property[1]);
        }
        for (String override : overrides) {
            String[] property = override.split("=", 2);
            url = TestUtils.addOrOverrideProperty(url, property[0], property[1]);
        }
        return PrepUtil.getConnection(url);
    }

    private static long[] execute(PreparedStatement ps, boolean large) throws SQLException {
        if (large) {
            return ps.executeLargeBatch();
        }
        int[] counts = ps.executeBatch();
        long[] result = new long[counts.length];
        for (int i = 0; i < counts.length; i++) {
            result[i] = counts[i];
        }
        return result;
    }

    // The creation branch selects the direct path in scopeTempTablesToConnection, without creating a table.
    private static String direct(String mode, String sql) {
        return "none".equals(mode) ? sql : "IF 1=0 CREATE TABLE #routing_marker (v int); " + sql;
    }

    private static int scalar(Statement stmt, String sql) throws SQLException {
        try (ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    @ParameterizedTest
    @MethodSource("modes")
    public void testExactCountsAndCompoundEntryBoundaries(String mode, boolean large) throws SQLException {
        try (Connection con = open(mode); Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE #counts (v int); INSERT INTO #counts VALUES (1),(2),(3),(4),(5)");
            try (PreparedStatement ps = con.prepareStatement(direct(mode, "DELETE TOP (?) FROM #counts"))) {
                for (int count : new int[] {2, 0, 3, 5}) {
                    ps.setInt(1, count);
                    ps.addBatch();
                }
                assertArrayEquals(new long[] {2, 0, 3, 0}, execute(ps, large));
                assertEquals(0, scalar(stmt, "SELECT COUNT(*) FROM #counts"));
                assertArrayEquals(new long[0], execute(ps, large));
            }
            try (PreparedStatement ps = con.prepareStatement(
                    direct(mode, "INSERT INTO #counts VALUES (?),(?); UPDATE #counts SET v=v+10 WHERE v=1"))) {
                for (int value : new int[] {1, 3}) {
                    ps.setInt(1, value);
                    ps.setInt(2, value + 1);
                    ps.addBatch();
                }
                assertArrayEquals(new long[] {1, 0}, execute(ps, large));
                assertEquals(20, scalar(stmt, "SELECT SUM(v) FROM #counts"));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("modes")
    public void testNoCountAndRpcLocalScope(String mode, boolean large) throws SQLException {
        try (Connection con = open(mode); Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE #parent (v int)");
            try (PreparedStatement ps = con
                    .prepareStatement("SET NOCOUNT ON; CREATE TABLE #child (v int); INSERT INTO #child VALUES (?); "
                            + "INSERT INTO #parent SELECT v FROM #child")) {
                for (int value : new int[] {1, 2}) {
                    ps.setInt(1, value);
                    ps.addBatch();
                }
                assertArrayEquals(new long[] {Statement.SUCCESS_NO_INFO, Statement.SUCCESS_NO_INFO},
                        execute(ps, large));
                assertEquals(3, scalar(stmt, "SELECT SUM(v) FROM #parent"));
                assertEquals(0, scalar(stmt, "SELECT COALESCE(OBJECT_ID('tempdb..#child'),0)"));
            } finally {
                stmt.execute("SET NOCOUNT OFF");
            }
        }
    }

    @ParameterizedTest
    @MethodSource("modes")
    public void testErrorsAfterIntermediateCountsAreNotDiscarded(String mode, boolean large) throws SQLException {
        try (Connection con = open(mode); Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE #errors (v int PRIMARY KEY)");
            try (PreparedStatement ps = con.prepareStatement(
                    direct(mode, "INSERT INTO #errors VALUES (?); RAISERROR('trailing batch failure',16,1)"))) {
                for (int value : new int[] {1, 2}) {
                    ps.setInt(1, value);
                    ps.addBatch();
                }
                BatchUpdateException error = assertThrows(BatchUpdateException.class, () -> execute(ps, large));
                assertArrayEquals(new long[] {Statement.EXECUTE_FAILED, Statement.EXECUTE_FAILED},
                        error.getLargeUpdateCounts());
                assertTrue(error.getMessage().contains("trailing batch failure"));
                assertTrue(null != error.getNextException());
                assertEquals(2, scalar(stmt, "SELECT COUNT(*) FROM #errors"));
                assertEquals(1, scalar(stmt, "SELECT 1"));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("modes")
    public void testConstraintErrorAndTrailingResultSet(String mode, boolean large) throws SQLException {
        try (Connection con = open(mode); Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE #errors (v int PRIMARY KEY)");
            try (PreparedStatement ps = con.prepareStatement(direct(mode, "INSERT INTO #errors VALUES (?)"))) {
                for (int value : new int[] {1, 1, 2}) {
                    ps.setInt(1, value);
                    ps.addBatch();
                }
                BatchUpdateException error = assertThrows(BatchUpdateException.class, () -> execute(ps, large));
                assertArrayEquals(new long[] {1, Statement.EXECUTE_FAILED, 1}, error.getLargeUpdateCounts());
                assertEquals(2, scalar(stmt, "SELECT COUNT(*) FROM #errors"));
            }
            try (PreparedStatement ps = con
                    .prepareStatement(direct(mode, "INSERT INTO #errors VALUES (?); SELECT 42"))) {
                ps.setInt(1, 3);
                ps.addBatch();
                BatchUpdateException error = assertThrows(BatchUpdateException.class, () -> execute(ps, large));
                assertArrayEquals(new long[] {Statement.EXECUTE_FAILED}, error.getLargeUpdateCounts());
                assertEquals(3, scalar(stmt, "SELECT COUNT(*) FROM #errors"));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("modes")
    public void testTypedBindingsAndSingleReuseWithoutRebinding(String mode, boolean large) throws SQLException {
        try (Connection con = open(mode); Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE #chars (v varchar(20), n nvarchar(20), t varchar(20), nt varchar(20))");
            String sql = direct(mode,
                    "INSERT INTO #chars VALUES (?, ?, " + "CONVERT(varchar(20),SQL_VARIANT_PROPERTY(?, 'BaseType')), "
                            + "CONVERT(varchar(20),SQL_VARIANT_PROPERTY(?, 'BaseType')))");
            try (PreparedStatement ps = con.prepareStatement(sql)) {
                ps.setString(1, "a\u20ac");
                ps.setNString(2, "\u4e2d");
                ps.setString(3, "a");
                ps.setNString(4, "\u4e2d");
                ps.addBatch();
                assertArrayEquals(new long[] {1}, execute(ps, large));
                assertEquals(1, ps.executeUpdate());
                try (ResultSet rs = stmt.executeQuery("SELECT v,n,t,nt FROM #chars")) {
                    for (int row = 0; row < 2; row++) {
                        assertTrue(rs.next());
                        assertEquals("a\u20ac", rs.getString(1));
                        assertEquals("\u4e2d", rs.getNString(2));
                        assertEquals("varchar", rs.getString(3));
                        assertEquals("nvarchar", rs.getString(4));
                    }
                    assertFalse(rs.next());
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("modes")
    public void testGeneratedKeyRequestRetainsCountOnlyBatchBehavior(String mode, boolean large) throws SQLException {
        for (String prepareMode : new String[] {"prepexec", mode}) {
            try (Connection con = open(prepareMode); Statement stmt = con.createStatement()) {
                stmt.execute("CREATE TABLE #identities (id int IDENTITY, v int)");
                try (PreparedStatement ps = con.prepareStatement(
                        direct(prepareMode, "INSERT INTO #identities VALUES (?)"), Statement.RETURN_GENERATED_KEYS)) {
                    for (int value : new int[] {1, 2}) {
                        ps.setInt(1, value);
                        ps.addBatch();
                    }
                    assertArrayEquals(new long[] {1, 1}, assertDoesNotThrow(() -> execute(ps, large), prepareMode));
                    // Batch keys are not exposed by the existing prepared RPC path.
                    assertThrows(SQLException.class, ps::getGeneratedKeys);
                    assertEquals(2, scalar(stmt, "SELECT COUNT(*) FROM #identities"));
                    assertEquals(1, ps.executeUpdate());
                    try (ResultSet keys = ps.getGeneratedKeys()) {
                        assertTrue(keys.next());
                        assertEquals(3, keys.getInt(1));
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("modes")
    public void testStreamAndParameterlessBatches(String mode, boolean large) throws SQLException {
        try (Connection con = open(mode); Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE #streams (v varbinary(20))");
            try (PreparedStatement ps = con.prepareStatement(direct(mode, "INSERT INTO #streams VALUES (?)"))) {
                for (String value : new String[] {"one", "two"}) {
                    byte[] bytes = value.getBytes(StandardCharsets.US_ASCII);
                    ps.setBinaryStream(1, new ByteArrayInputStream(bytes), bytes.length);
                    ps.addBatch();
                }
                assertArrayEquals(new long[] {1, 1}, execute(ps, large));
                assertEquals(6, scalar(stmt, "SELECT SUM(DATALENGTH(v)) FROM #streams"));
            }
            try (PreparedStatement ps = con.prepareStatement(direct(mode, "UPDATE #streams SET v=0x01"))) {
                ps.addBatch();
                ps.addBatch();
                assertArrayEquals(new long[] {2, 2}, execute(ps, large));
                assertEquals(2, scalar(stmt, "SELECT SUM(DATALENGTH(v)) FROM #streams"));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("modes")
    public void testTimeoutAfterDmlDoesNotBecomeSuccess(String mode, boolean large) throws SQLException {
        try (Connection con = open(mode); Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE #timeout (v int)");
            try (PreparedStatement ps = con
                    .prepareStatement(direct(mode, "INSERT INTO #timeout VALUES (?); WAITFOR DELAY '00:00:05'"))) {
                ps.setQueryTimeout(1);
                ps.setInt(1, 1);
                ps.addBatch();
                SQLException error = assertThrows(SQLException.class, () -> execute(ps, large));
                assertEquals(SQLState.STATEMENT_CANCELED.getSQLStateCode(), error.getSQLState());
                assertEquals(1, scalar(stmt, "SELECT COUNT(*) FROM #timeout"));
                ps.setQueryTimeout(0);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("modes")
    public void testCancellationDoesNotBecomeSuccess(String mode, boolean large) throws Exception {
        try (Connection con = open(mode); Statement stmt = con.createStatement(); PreparedStatement ps = con
                .prepareStatement(direct(mode, "WAITFOR DELAY '00:00:10'; INSERT INTO #cancelled VALUES (?)"))) {
            stmt.execute("CREATE TABLE #cancelled (v int)");
            ps.setInt(1, 1);
            ps.addBatch();
            Logger commands = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.TDS.Command");
            Level previous = commands.getLevel();
            CountDownLatch reading = new CountDownLatch(1);
            Handler handler = new Handler() {
                @Override
                public void publish(LogRecord record) {
                    if (record.getMessage().contains("executeBatch")
                            && record.getMessage().contains("Reading response")) {
                        reading.countDown();
                    }
                }

                @Override
                public void flush() {}

                @Override
                public void close() {}
            };
            ExecutorService worker = Executors.newSingleThreadExecutor();
            commands.setLevel(Level.FINEST);
            commands.addHandler(handler);
            try {
                Future<SQLException> failure = worker
                        .submit(() -> assertThrows(SQLException.class, () -> execute(ps, large)));
                assertTrue(reading.await(10, TimeUnit.SECONDS));
                ps.cancel();
                assertEquals(SQLState.STATEMENT_CANCELED.getSQLStateCode(),
                        failure.get(15, TimeUnit.SECONDS).getSQLState());
                assertEquals(0, scalar(stmt, "SELECT COUNT(*) FROM #cancelled"));
            } finally {
                commands.removeHandler(handler);
                commands.setLevel(previous);
                worker.shutdownNow();
                assertTrue(worker.awaitTermination(15, TimeUnit.SECONDS));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("modes")
    public void testTransactionAbortingFailureIsPropagated(String mode, boolean large) throws SQLException {
        try (Connection con = open(mode); Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE #rollback (v int PRIMARY KEY)");
            con.setAutoCommit(false);
            stmt.execute("SET XACT_ABORT ON");
            try (PreparedStatement ps = con.prepareStatement(direct(mode, "INSERT INTO #rollback VALUES (?)"))) {
                for (int value : new int[] {1, 1, 2}) {
                    ps.setInt(1, value);
                    ps.addBatch();
                }
                assertThrows(SQLException.class, () -> execute(ps, large));
                assertEquals(0, scalar(stmt, "SELECT COUNT(*) FROM #rollback"));
            } finally {
                con.rollback();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("modes")
    @Tag(Constants.xSQLv12)
    public void testEncryptionMetadataDoesNotLeakIntoRetainedBindings(String mode, boolean large) throws SQLException {
        try (Connection con = open(mode, "columnEncryptionSetting=Enabled"); Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE #metadata (v varchar(20), n nvarchar(20))");
            try (PreparedStatement ps = con.prepareStatement(direct(mode, "INSERT INTO #metadata VALUES (?,?)"))) {
                ps.setString(1, "abc");
                ps.setNString(2, "\u4e2d");
                ps.addBatch();
                assertArrayEquals(new long[] {1}, execute(ps, large));
                assertEquals(1, ps.executeUpdate());
                ps.addBatch();
                assertArrayEquals(new long[] {1}, execute(ps, large));
                assertEquals(3, scalar(stmt, "SELECT COUNT(*) FROM #metadata WHERE v='abc' AND n=N'\u4e2d'"));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("modes")
    @Tag(Constants.reqExternalSetup)
    public void testFatalFailureIsNotHiddenByAnEarlierEntryFailure(String mode, boolean large) throws SQLException {
        try (Connection con = open(mode); Statement stmt = con.createStatement()) {
            Assumptions.assumeTrue(1 == scalar(stmt, "SELECT IS_SRVROLEMEMBER('sysadmin')"),
                    "Raising a connection-closing error requires sysadmin");
            try (PreparedStatement ps = con
                    .prepareStatement(direct(mode, "DECLARE @v int=?; IF @v=1 RAISERROR('entry failure',16,1); "
                            + "ELSE RAISERROR('fatal entry failure',20,1) WITH LOG"))) {
                for (int value : new int[] {1, 2}) {
                    ps.setInt(1, value);
                    ps.addBatch();
                }
                SQLException error = assertThrows(SQLException.class, () -> execute(ps, large));
                assertFalse(error instanceof BatchUpdateException);
                assertTrue(con.isClosed());
            }
        }
    }

    @ParameterizedTest
    @MethodSource("modes")
    public void testCompletionWithFullBufferingAndLastUpdateCountDisabled(String mode,
            boolean large) throws SQLException {
        try (Connection con = open(mode, "responseBuffering=full", "lastUpdateCount=false");
                Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE #completion (v int)");
            try (PreparedStatement ps = con.prepareStatement(
                    direct(mode, "INSERT INTO #completion VALUES (?),(99); DELETE FROM #completion WHERE v=99"))) {
                for (int value : new int[] {1, 2}) {
                    ps.setInt(1, value);
                    ps.addBatch();
                }
                assertArrayEquals(new long[] {1, 1}, execute(ps, large));
                assertEquals(3, scalar(stmt, "SELECT SUM(v) FROM #completion"));
            }
            try (PreparedStatement ps = con.prepareStatement(direct(mode,
                    "INSERT INTO #completion VALUES (?); SELECT 42; RAISERROR('after result set',16,1)"))) {
                ps.setInt(1, 3);
                ps.addBatch();
                BatchUpdateException error = assertThrows(BatchUpdateException.class, () -> execute(ps, large));
                assertArrayEquals(new long[] {Statement.EXECUTE_FAILED}, error.getLargeUpdateCounts());
                SQLException next = error.getNextException();
                boolean foundTrailingError = false;
                while (null != next) {
                    foundTrailingError |= next.getMessage().contains("after result set");
                    next = next.getNextException();
                }
                assertTrue(foundTrailingError);
                assertEquals(6, scalar(stmt, "SELECT SUM(v) FROM #completion"));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("modes")
    public void testUnpreparedRpcGroupingAndNonBatchScope(String mode, boolean large) throws SQLException {
        Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerStatement");
        Level previous = logger.getLevel();
        Logger commands = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.TDS.Command");
        Level previousCommands = commands.getLevel();
        List<String> calls = new ArrayList<>();
        List<String> requests = new ArrayList<>();
        Handler handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record.getMessage().contains(": calling sp_")) {
                    calls.add(record.getMessage());
                }
                if (record.getMessage().contains("executeBatch")
                        && record.getMessage().contains(": starting request...")) {
                    requests.add(record.getMessage());
                }
            }

            @Override
            public void flush() {}

            @Override
            public void close() {}
        };
        logger.setLevel(Level.FINE);
        logger.addHandler(handler);
        commands.setLevel(Level.FINEST);
        commands.addHandler(handler);
        try (Connection con = open(mode); Statement stmt = con.createStatement();
                SQLServerPreparedStatement ps = (SQLServerPreparedStatement) con
                        .prepareStatement("CREATE TABLE #local (v int); INSERT INTO #local VALUES (?)")) {
            for (int i = 0; i < 3; i++) {
                ps.setInt(1, i);
                ps.addBatch();
            }
            assertArrayEquals(new long[] {1, 1, 1}, execute(ps, large));
            assertEquals(3, calls.size());
            assertEquals(1, requests.size(), requests.toString());
            for (String call : calls) {
                assertTrue(call.contains("calling sp_executesql"), call);
            }
            assertEquals(0, ps.getPreparedStatementHandle());
            assertEquals(0, scalar(stmt, "SELECT COALESCE(OBJECT_ID('tempdb..#local'),0)"));
            ps.executeUpdate();
            assertEquals(1, scalar(stmt, "SELECT COUNT(*) FROM #local"));
        } finally {
            logger.removeHandler(handler);
            logger.setLevel(previous);
            commands.removeHandler(handler);
            commands.setLevel(previousCommands);
        }
    }
}
