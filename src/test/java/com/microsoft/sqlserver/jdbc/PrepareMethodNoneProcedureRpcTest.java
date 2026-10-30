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
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.BatchUpdateException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.MessageFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * A statement that calls a stored procedure is never executed as a direct SQL batch. Under prepareMethod=none it is
 * sent as an unprepared RPC with typed parameters. A direct statement with parameters that cannot be inlined as literals
 * (OUT parameters, table-valued parameters, streams and SQLXML) catches the rejection and is then sent as an unprepared
 * RPC.
 */
@RunWith(JUnitPlatform.class)
public class PrepareMethodNoneProcedureRpcTest extends AbstractTest {
    private static final String STATEMENT_LOGGER = "com.microsoft.sqlserver.jdbc.internals.SQLServerStatement";

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    // Detecting a procedure call in the SQL text. The same words in quoted text, quoted identifiers, comments
    // (including nested comments) and longer words are not calls.

    private static Stream<Arguments> statements() {
        return Stream.of(
                // Procedure calls.
                Arguments.of("EXEC p ?", true), Arguments.of("exec p", true), Arguments.of("EXECUTE dbo.p ?, ?, ?", true),
                Arguments.of("exec [db]..[p] ?, -?", true), Arguments.of("SET NOCOUNT ON; EXEC p ?", true),
                Arguments.of("SET NOCOUNT ON\nEXEC p ?", true), Arguments.of("BEGIN EXEC p ? END", true),
                Arguments.of("IF ? = 1 EXEC p ? ELSE SELECT ?", true), Arguments.of("INSERT INTO t EXEC p ?", true),
                Arguments.of("EXEC @rc = p ?", true), Arguments.of("EXEC (?)", true),
                Arguments.of("SELECT 1\nEXEC p", true), Arguments.of("/* c */ EXEC p ?", true),
                Arguments.of("-- c\nEXEC p ?", true), Arguments.of("EXEC p /* , */ ? -- , \n, ?", true),
                Arguments.of("EXEC p /* outer /* inner */ remaining */ ?", true),
                Arguments.of("EXEC AS_proc ?", true), Arguments.of("EXEC", true),
                Arguments.of("SELECT 1; EXEC [dbo].[p] ?", true), Arguments.of("EXEC \"dbo\".p ?", true),

                // A procedure named as the first statement, without EXEC.
                Arguments.of("dbo.p ?", true), Arguments.of("p ?, ?", true), Arguments.of("[dbo].[p] ?", true),
                Arguments.of("\"dbo\".\"p\" ?", true), Arguments.of("#p ?", true),
                Arguments.of("/* c */ dbo.p ?", true), Arguments.of("-- c\n dbo.p ?", true),

                // A permission statement that names EXECUTE is reported as a call and is simply sent as an RPC through
                // sp_executesql, which runs it unchanged.
                Arguments.of("GRANT EXECUTE ON p TO u", true), Arguments.of("DENY EXECUTE ON p TO u", true),
                Arguments.of("REVOKE EXECUTE ON p FROM u", true),
                Arguments.of("GRANT SELECT, EXECUTE ON SCHEMA::dbo TO u", true),
                Arguments.of("GRANT EXECUTE ON p TO u; EXEC p ?", true),

                // Not procedure calls.
                Arguments.of("SELECT ?", false), Arguments.of("SELECT 'EXEC p', ?", false),
                Arguments.of("SELECT 'it''s EXEC p', ?", false), Arguments.of("SELECT [EXEC] FROM t WHERE c = ?", false),
                Arguments.of("SELECT \"EXEC\" FROM t WHERE c = ?", false),
                Arguments.of("SELECT exec_count FROM t WHERE c = ?", false),
                Arguments.of("SELECT ? AS EXEC\u0301 INTO #comb", false),
                Arguments.of("SELECT ? AS EXECUTE\u0301", false),
                Arguments.of("SELECT executed FROM t WHERE c = ?", false), Arguments.of("SELECT @exec, ?", false),
                Arguments.of("SELECT #exec FROM t WHERE c = ?", false), Arguments.of("/* EXEC p */ SELECT ?", false),
                Arguments.of("-- EXEC p\nSELECT ?", false),
                Arguments.of("/* outer /* EXEC p */ still a comment EXEC q */ SELECT ?", false),
                Arguments.of("EXECUTE AS USER = 'u'; SELECT ?", false), Arguments.of("EXEC AS USER = 'u'", false),
                Arguments.of("EXECUTE /* c */ AS USER = 'u'", false),
                Arguments.of("EXECUTE /* a /* nested */ b */ -- c\n AS USER = 'u'", false),
                Arguments.of("SET NOCOUNT ON; EXECUTE /* context switch */ AS USER = 'u'; SELECT ?", false),
                Arguments.of("SELECT 1 FROM t WHERE c = '''EXEC'", false),
                Arguments.of("SELECT 1 /* unterminated EXEC p", false),

                // Statements that start with a keyword, a parenthesis, a variable or a string are not procedure calls.
                Arguments.of("WITH c AS (SELECT 1 AS v) SELECT v FROM c WHERE v = ?", false),
                Arguments.of("(SELECT ?)", false), Arguments.of("INSERT INTO t VALUES (?)", false),
                Arguments.of("SET ROWCOUNT ?", false), Arguments.of("DECLARE @v int = ?", false),
                Arguments.of("UPDATE t SET c = ?", false), Arguments.of("'abc'", false),
                Arguments.of("@v = ?", false),

                // A module definition does not run its body in this batch. A label is not a call. Service Broker
                // statements are not procedure calls.
                Arguments.of("CREATE PROCEDURE dbo.wrapper AS EXEC dbo.inner_proc ?", false),
                Arguments.of("CREATE OR ALTER PROC dbo.wrapper AS EXEC dbo.inner_proc ?", false),
                Arguments.of("ALTER /* c */ PROCEDURE dbo.wrapper AS EXEC dbo.inner_proc ?", false),
                Arguments.of("CREATE TRIGGER trg ON t AFTER INSERT AS EXEC dbo.inner_proc ?", false),
                Arguments.of("CREATE FUNCTION f() RETURNS int AS BEGIN RETURN ? END", false),
                Arguments.of("retry: SELECT ? AS v INTO #t", false),
                Arguments.of("retry /* c */ : SELECT ? AS v INTO #t", false),
                Arguments.of("retry /*comment*/: SELECT ? INTO #labeled;", false),
                Arguments.of("retry -- c\n : SELECT ?", false),
                Arguments.of("retry /* a /* nested */ b */ : SELECT ?", false),
                Arguments.of("retry /* c */ : EXEC p ?", true),
                Arguments.of("/* c */ retry: SELECT ? AS v INTO #t", false),
                Arguments.of("retry: EXEC p ?", true),
                Arguments.of("RECEIVE TOP (1) message_body FROM dbo.q; SELECT ? AS v INTO #t", false),
                Arguments.of("SEND ON CONVERSATION ? MESSAGE TYPE m", false),
                Arguments.of("GET CONVERSATION GROUP ? FROM dbo.q", false),
                Arguments.of("MOVE CONVERSATION ? TO ?", false),

                // Statements that follow a definition keyword, a label or a keyword can still hold a call.
                Arguments.of("CREATE TABLE #t (i int); EXEC p ?", true), Arguments.of("retry: EXEC p ?", true),
                Arguments.of("retry: dbo.p ?", true), Arguments.of("ALTER TABLE t ADD c int; EXEC p ?", true));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("statements")
    public void testContainsProcedureCall(String sql, boolean expected) {
        assertEquals(expected, SQLServerPreparedStatement.containsProcedureCall(sql), sql);
    }

    // The execution path: an unprepared sp_executesql with no server handle.

    /** Collects the statement log messages that name the server procedure used for the execution. */
    private static final class ProcedureLog extends Handler {
        final List<String> messages = new ArrayList<>();

        @Override
        public void publish(LogRecord record) {
            String message = record.getMessage();
            if (null != message && message.contains(": calling sp_")) {
                messages.add(message.substring(message.indexOf("calling sp_")));
            }
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}
    }

    private static List<String> executeThreeTimes(String mode, String sql) throws SQLException {
        Logger logger = Logger.getLogger(STATEMENT_LOGGER);
        Level previousLevel = logger.getLevel();
        ProcedureLog log = new ProcedureLog();
        log.setLevel(Level.FINE);
        logger.setLevel(Level.FINE);
        logger.addHandler(log);
        try (Connection conn = openConnection(mode); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE PROCEDURE #rpc_echo @v int AS SELECT @v");
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (int i = 1; i <= 3; i++) {
                    ps.setInt(1, i);
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(i, rs.getInt(1));
                    }
                }
            }
        } finally {
            logger.removeHandler(log);
            logger.setLevel(previousLevel);
        }
        return log.messages;
    }

    /** Under none, each execution is a plain sp_executesql, and no server handle is created or reused. */
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"{call #rpc_echo(?)}", "EXEC #rpc_echo ?", "SET NOCOUNT ON; EXEC #rpc_echo ?"})
    public void testProcedureCallIsSentAsUnpreparedRpc(String sql) throws SQLException {
        List<String> messages = executeThreeTimes("none", sql);
        assertEquals(3, messages.size(), messages.toString());
        for (String message : messages) {
            assertTrue(message.startsWith("calling sp_executesql"), message);
        }
    }

    // Under scopeTempTablesToConnection a procedure call is not sent as an unprepared RPC: it takes the mode's regular
    // prepared path (sp_executesql on the first execution, then sp_prepexec, then sp_execute with the cached handle),
    // with or without temporary-table operations in the SQL text. A statement with temp-table operations and no call is
    // still a direct batch (no RPC at all).
    private static final class ExecutionTrace {
        final List<String> rpcs;
        final boolean tableSurvives;

        ExecutionTrace(List<String> rpcs, boolean tableSurvives) {
            this.rpcs = rpcs;
            this.tableSurvives = tableSurvives;
        }
    }

    private static ExecutionTrace traceThreeExecutions(String mode, String sql, String table) throws SQLException {
        Logger logger = Logger.getLogger(STATEMENT_LOGGER);
        Level previousLevel = logger.getLevel();
        ProcedureLog log = new ProcedureLog();
        log.setLevel(Level.FINE);
        logger.setLevel(Level.FINE);
        logger.addHandler(log);
        try (Connection conn = openConnection(mode); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE PROCEDURE #rpc_echo @v int AS SELECT @v");
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (int i = 1; i <= 3; i++) {
                    ps.setInt(1, i);
                    ps.execute();
                }
            }
            boolean survives;
            try (ResultSet rs = stmt.executeQuery("SELECT OBJECT_ID('tempdb.." + table + "')")) {
                rs.next();
                survives = null != rs.getObject(1);
            }
            return new ExecutionTrace(log.messages, survives);
        } finally {
            logger.removeHandler(log);
            logger.setLevel(previousLevel);
        }
    }

    private static void assertPreparedSequence(List<String> rpcs) {
        assertEquals(3, rpcs.size(), rpcs.toString());
        assertTrue(rpcs.get(0).startsWith("calling sp_executesql"), rpcs.toString());
        assertTrue(rpcs.get(1).startsWith("calling sp_prepexec"), rpcs.toString());
        assertTrue(rpcs.get(2).startsWith("calling sp_execute:"), rpcs.toString());
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"EXEC #rpc_echo ?", "{call #rpc_echo(?)}", "SET NOCOUNT ON; EXEC #rpc_echo ?"})
    public void testProcedureCallInScopeModeUsesPreparedPath(String sql) throws SQLException {
        assertPreparedSequence(traceThreeExecutions("scopeTempTablesToConnection", sql, "#none").rpcs);
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"CREATE TABLE #scope_t (i int); EXEC #rpc_echo ?",
            "EXEC #rpc_echo ?; SELECT 1 AS a INTO #scope_t"})
    public void testProcedureCallWithTempTableInScopeModeUsesPreparedPath(String sql) throws SQLException {
        ExecutionTrace scope = traceThreeExecutions("scopeTempTablesToConnection", sql, "#scope_t");
        assertPreparedSequence(scope.rpcs);
        assertFalse(scope.tableSurvives, "a temp table created in the same statement as a call ends with the RPC");
    }

    @Test
    public void testProcedureCallWithTempTableInNoneModeIsAlwaysUnprepared() throws SQLException {
        ExecutionTrace none = traceThreeExecutions("none", "CREATE TABLE #scope_t (i int); EXEC #rpc_echo ?",
                "#scope_t");
        assertEquals(3, none.rpcs.size(), none.rpcs.toString());
        for (String rpc : none.rpcs) {
            assertTrue(rpc.startsWith("calling sp_executesql"), rpc);
        }
        assertFalse(none.tableSurvives);
    }

    @Test
    public void testTempTableStatementWithoutCallStaysDirectInScopeMode() throws SQLException {
        ExecutionTrace scope = traceThreeExecutions("scopeTempTablesToConnection",
                "IF OBJECT_ID('tempdb..#scope_d') IS NOT NULL DROP TABLE #scope_d; SELECT ? AS v INTO #scope_d",
                "#scope_d");
        assertTrue(scope.rpcs.isEmpty(), "a direct batch makes no RPC: " + scope.rpcs);
        assertTrue(scope.tableSurvives);
    }

    // A batch of procedure calls in scope mode takes the regular prepared batch path and gives the same rows as prepexec.
    @Test
    public void testProcedureCallBatchInScopeModeMatchesPrepexec() throws SQLException {
        String typed = null;
        for (String mode : new String[] {"prepexec", "scopeTempTablesToConnection"}) {
            try (Connection conn = openConnection(mode); Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE #scope_log (v int)");
                stmt.execute("CREATE PROCEDURE #scope_p @v int AS INSERT #scope_log VALUES (@v)");
                try (PreparedStatement ps = conn.prepareStatement("EXEC #scope_p ?")) {
                    for (int v = 1; v <= 3; v++) {
                        ps.setInt(1, v);
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
                String rows = tableRows(stmt, "#scope_log");
                if (null == typed) {
                    typed = rows;
                } else {
                    assertEquals(typed, rows);
                }
            }
        }
        assertEquals("1,2,3,", typed);
    }

    /** Control: the same statement under prepexec reuses a prepared handle, so the log check is meaningful. */
    @Test
    public void testPrepexecControlUsesPreparedHandle() throws SQLException {
        List<String> messages = executeThreeTimes("prepexec", "{call #rpc_echo(?)}");
        assertTrue(messages.stream().anyMatch(m -> m.startsWith("calling sp_prepexec")), messages.toString());
    }

    // Batches of procedure calls give the same update counts and rows as typed binding.

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"{call #batch_p(?)}", "EXEC #batch_p ?"})
    public void testProcedureCallBatchMatchesTypedBinding(String sql) throws SQLException {
        BatchResult typed = runBatch("prepexec", sql, new int[] {1, 2, 3, 4});
        BatchResult none = runBatch("none", sql, new int[] {1, 2, 3, 4});
        assertArrayEquals(typed.counts, none.counts);
        assertEquals(typed.rows, none.rows);
        assertEquals("1,2,3,4", none.rows);
    }

    // Entries of an unprepared RPC batch are combined into one request, like the later entries of a prepexec batch.
    // The server stops the request at the failing entry, so the entries before it succeed and the rest are failed.
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"{call #batch_p(?)}", "EXEC #batch_p ?"})
    public void testProcedureCallBatchWithFailingEntryReportsBatchUpdateException(String sql) throws SQLException {
        // The procedure rejects the value 13.
        BatchResult none = runBatch("none", sql, new int[] {1, 2, 13, 4});
        assertTrue(none.failed, "direct execution must report the failing entry as a BatchUpdateException");
        assertArrayEquals(new int[] {1, 1, Statement.EXECUTE_FAILED, Statement.EXECUTE_FAILED}, none.counts);
        assertEquals("1,2", none.rows);
    }

    // An unprepared RPC never reuses a prepared handle, so errors 586 and 8179 raised inside the procedure are not
    // cached-handle failures and the entry must not be executed again, with or without statement pooling, for a single
    // execution, executeBatch and executeLargeBatch. This holds for prepareMethod=none and for the first, sp_executesql
    // execution under scopeTempTablesToConnection. The procedure inserts a row and then raises the error, so every
    // extra execution is visible as an extra row. Error 8179: sp_execute with an unknown handle. Error 586: a handle
    // prepared in one database and executed after USE of another.
    private static final String RETRY_ERROR_8179 = "EXEC sp_execute 999999";
    private static final String RETRY_ERROR_586 = "DECLARE @h int, @sql nvarchar(200); "
            + "EXEC sp_prepare @h OUTPUT, N'', N'SELECT 1 AS x'; "
            + "SET @sql = N'USE tempdb; EXEC sp_execute ' + CAST(@h AS nvarchar(20)); EXEC (@sql)";

    private static Stream<Arguments> retryCases() {
        List<Arguments> cases = new ArrayList<>();
        // Under scopeTempTablesToConnection the first execution is also an sp_executesql, with or without a temp table
        // created by the same statement.
        String[][] modeAndSql = {{"none", "{call #retry_p(?)}"}, {"none", "EXEC #retry_p ?"},
                {"scopeTempTablesToConnection", "{call #retry_p(?)}"},
                {"scopeTempTablesToConnection", "CREATE TABLE #retry_scope (i int); EXEC #retry_p ?"}};
        for (String[] ms : modeAndSql) {
            for (boolean pooling : new boolean[] {true, false}) {
                for (String kind : new String[] {"single", "batch", "largeBatch"}) {
                    for (int error : new int[] {8179, 586}) {
                        cases.add(Arguments.of(ms[0], ms[1], pooling, kind, error));
                    }
                }
            }
        }
        return cases.stream();
    }

    @ParameterizedTest(name = "{0}, {1}, pooling={2}, {3}, error {4}")
    @MethodSource("retryCases")
    public void testErrorsFromInsideProcedureAreNotRetried(String mode, String sql, boolean pooling, String kind,
            int error) throws SQLException {
        String[] props = pooling ? new String[] {"disableStatementPooling=false", "statementPoolingCacheSize=10"}
                                 : new String[0];
        // A session-local temp table is lost when the connection is closed, which the prepared path does after a failed
        // batch entry, so the entries are logged in a regular table that a second connection reads.
        String logTable = "mssqljdbc_retry_" + UUID.randomUUID().toString().replace("-", "");
        // A unique procedure name makes the SQL text new to the connection, so the first execution is an sp_executesql
        // even when statement pooling has cached the parsed SQL of an earlier case.
        String proc = "#retry_p" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        String statement = sql.replace("#retry_p", proc);
        try (Connection conn = openConnection(mode, props); Statement stmt = conn.createStatement()) {
            // Error 586 needs USE to switch databases, which Azure SQL Database does not allow.
            Assumptions.assumeFalse(586 == error && TestUtils.isAzure(conn) && !TestUtils.isAzureMI(conn),
                    "USE cannot switch databases on Azure SQL Database");
            stmt.execute("CREATE TABLE " + logTable + " (v int)");
            stmt.execute("CREATE PROCEDURE " + proc + " @v int AS BEGIN INSERT " + logTable + " VALUES (@v); "
                    + (586 == error ? RETRY_ERROR_586 : RETRY_ERROR_8179) + " END");
            int[] values = "single".equals(kind) ? new int[] {1} : new int[] {1, 2, 3};
            try (PreparedStatement ps = conn.prepareStatement(statement)) {
                for (int v : values) {
                    ps.setInt(1, v);
                    if ("single".equals(kind)) {
                        try {
                            ps.execute();
                        } catch (SQLException e) {
                            // The error is raised inside the procedure; only the number of executions matters.
                        }
                    } else {
                        ps.addBatch();
                    }
                }
                try {
                    if ("batch".equals(kind)) {
                        ps.executeBatch();
                    } else if ("largeBatch".equals(kind)) {
                        ps.executeLargeBatch();
                    }
                } catch (SQLException e) {
                    // Depending on the server, an error raised inside the procedure can end the batch with a
                    // BatchUpdateException or a plain SQLServerException. Only the rows inserted are checked.
                }
            }
            try (Connection reader = openConnection("prepexec"); Statement readStmt = reader.createStatement()) {
                String rows = tableRows(readStmt, logTable);
                if ("none".equals(mode)) {
                    StringBuilder expected = new StringBuilder();
                    for (int v : values) {
                        expected.append(v).append(',');
                    }
                    assertEquals(expected.toString(), rows,
                            "an entry that failed inside the procedure must not be run twice");
                } else {
                    // The first execution of a statement is an sp_executesql, which never reuses a handle, so the
                    // first entry must have run exactly once. Later entries may use a prepared handle, where the
                    // driver cannot tell an error raised inside the procedure from a stale handle.
                    assertEquals(1, Arrays.stream(rows.split(",")).filter("1"::equals).count(),
                            "the first entry must run exactly once: " + rows);
                }
            } finally {
                try (Connection cleanup = openConnection("prepexec"); Statement cleanupStmt = cleanup.createStatement()) {
                    TestUtils.dropTableIfExists(logTable, cleanupStmt);
                }
            }
        }
    }

    /** Returns the first column of the table, in order, as "1,2,3,". */
    private static String tableRows(Statement stmt, String table) throws SQLException {
        StringBuilder rows = new StringBuilder();
        try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + table + " ORDER BY 1")) {
            while (rs.next()) {
                rows.append(rs.getInt(1)).append(',');
            }
        }
        return rows.toString();
    }

    // Complete migration workflow on one physical connection: a PreparedStatement creates and fills a temp table, a
    // CallableStatement updates it, and a later query reads it. The table survives every statement being closed, and is
    // dropped at the end. With prepexec the table does not survive its creating statement (control).
    @ParameterizedTest(name = "mode={0}")
    @ValueSource(strings = {"none", "scopeTempTablesToConnection"})
    public void testMigrationWorkflowKeepsTempTableAcrossStatements(String mode) throws SQLException {
        try (Connection conn = openConnection(mode); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE PROCEDURE #mig_p @id int, @bump int AS BEGIN "
                    + "UPDATE #mig_work SET v = v + @bump WHERE id = @id; SELECT COUNT(*) FROM #mig_work END");
            try (PreparedStatement ps = conn.prepareStatement("CREATE TABLE #mig_work (id int, v int)")) {
                ps.execute();
            }
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO #mig_work VALUES (?, ?)")) {
                for (int i = 1; i <= 3; i++) {
                    ps.setInt(1, i);
                    ps.setInt(2, i * 10);
                    assertEquals(1, ps.executeUpdate());
                }
            }
            try (CallableStatement cs = conn.prepareCall("{call #mig_p(?, ?)}")) {
                cs.setInt(1, 2);
                cs.setInt(2, 5);
                try (ResultSet rs = cs.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(3, rs.getInt(1), "the procedure must see the table and its three rows");
                }
            }
            StringBuilder rows = new StringBuilder();
            try (ResultSet rs = stmt.executeQuery("SELECT id, v FROM #mig_work ORDER BY id")) {
                while (rs.next()) {
                    rows.append(rs.getInt(1)).append(':').append(rs.getInt(2)).append(',');
                }
            }
            assertEquals("1:10,2:25,3:30,", rows.toString(), "table must survive closed statements and be updated");
            try (PreparedStatement ps = conn.prepareStatement("DROP TABLE #mig_work")) {
                ps.execute();
            }
            try (ResultSet rs = stmt.executeQuery("SELECT OBJECT_ID('tempdb..#mig_work')")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1), "cleanup must drop the table");
            }
        }
    }

    @Test
    public void testMigrationWorkflowFailsWithPrepexecControl() throws SQLException {
        try (Connection conn = openConnection("prepexec")) {
            try (PreparedStatement ps = conn.prepareStatement("CREATE TABLE #mig_ctrl (id int)")) {
                ps.execute();
            }
            assertThrows(SQLException.class, () -> {
                try (PreparedStatement ps = conn.prepareStatement("INSERT INTO #mig_ctrl VALUES (?)")) {
                    ps.setInt(1, 1);
                    ps.executeUpdate();
                }
            }, "with prepexec the table ends with the CREATE call");
        }
    }

    // Transactions on direct statements: a rollback removes the rows inserted by literal batches, and a commit keeps
    // them. Autocommit off, same physical connection.
    @ParameterizedTest(name = "mode={0}")
    @ValueSource(strings = {"none", "scopeTempTablesToConnection"})
    public void testDirectStatementRollbackAndCommit(String mode) throws SQLException {
        try (Connection conn = openConnection(mode); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE #tx_t (v int)");
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO #tx_t VALUES (?)")) {
                ps.setInt(1, 1);
                ps.executeUpdate();
                conn.rollback();
                assertEquals("", tableRows(stmt, "#tx_t"), "rollback must remove the row");
                ps.setInt(1, 2);
                ps.executeUpdate();
                conn.commit();
                conn.rollback();
                assertEquals("2,", tableRows(stmt, "#tx_t"), "a committed row must stay");
            }
            conn.setAutoCommit(true);
        }
    }

    // A batch of procedure calls in a transaction. The entries before the failing one are applied, the failing one and
    // the entries after it are not, and a rollback removes the applied rows; a commit keeps them. With autocommit on
    // the applied rows stay, so the batch is not atomic.
    @ParameterizedTest(name = "autoCommit={0}")
    @ValueSource(booleans = {true, false})
    public void testProcedureBatchFailureAndTransaction(boolean autoCommit) throws SQLException {
        try (Connection conn = openConnection("none"); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE #tx_b (v int)");
            stmt.execute("CREATE PROCEDURE #tx_p @v int AS BEGIN IF @v = 13 THROW 50000, 'unlucky', 1; "
                    + "INSERT #tx_b VALUES (@v) END");
            conn.setAutoCommit(autoCommit);
            for (int round = 1; round <= 2; round++) {
                try (PreparedStatement ps = conn.prepareStatement("EXEC #tx_p ?")) {
                    for (int v : new int[] {round * 10, round * 10 + 1, 13, round * 10 + 2}) {
                        ps.setInt(1, v);
                        ps.addBatch();
                    }
                    BatchUpdateException e = assertThrows(BatchUpdateException.class, ps::executeBatch);
                    int[] counts = e.getUpdateCounts();
                    assertEquals(4, counts.length);
                    assertEquals(1, counts[0]);
                    assertEquals(1, counts[1]);
                    assertEquals(Statement.EXECUTE_FAILED, counts[2]);
                }
                if (autoCommit) {
                    assertEquals(round == 1 ? "10,11," : "10,11,20,21,", tableRows(stmt, "#tx_b"),
                            "with autocommit the applied entries stay");
                } else if (round == 1) {
                    assertEquals("10,11,", tableRows(stmt, "#tx_b"));
                    conn.rollback();
                    assertEquals("", tableRows(stmt, "#tx_b"), "rollback removes the applied entries");
                } else {
                    assertEquals("20,21,", tableRows(stmt, "#tx_b"));
                    conn.commit();
                    conn.rollback();
                    assertEquals("20,21,", tableRows(stmt, "#tx_b"), "a commit keeps the applied entries");
                }
            }
            conn.setAutoCommit(true);
        }
    }

    // Result traversal of a procedure that returns two result sets, update counts, a return status and an OUT value. The
    // complete getMoreResults / getUpdateCount sequence, the return status and the OUT value must be the same as with
    // prepexec, on the first execution and on a repeated one, and when a result set is closed after its first row.
    private static String traverse(CallableStatement cs, boolean closeEarly) throws SQLException {
        StringBuilder seq = new StringBuilder();
        boolean isResultSet = cs.execute();
        for (int guard = 0; guard < 20; guard++) {
            if (isResultSet) {
                try (ResultSet rs = cs.getResultSet()) {
                    seq.append("RS(").append(rs.getMetaData().getColumnCount()).append(':');
                    while (rs.next()) {
                        seq.append(rs.getObject(1)).append(';');
                        if (closeEarly) {
                            break;
                        }
                    }
                    seq.append(')');
                }
            } else {
                int updateCount = cs.getUpdateCount();
                if (-1 == updateCount) {
                    break;
                }
                seq.append("UC=").append(updateCount).append(';');
            }
            isResultSet = cs.getMoreResults();
        }
        return seq.append(" ret=").append(cs.getInt(1)).append(" out=").append(cs.getInt(3)).toString();
    }

    private static String traverseTwice(String mode, boolean closeEarly) throws SQLException {
        try (Connection conn = openConnection(mode); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE #multi_t (v int)");
            stmt.execute("CREATE PROCEDURE #multi @in int, @out int OUTPUT AS BEGIN "
                    + "SELECT v FROM (VALUES (1), (2), (3)) AS t(v); INSERT #multi_t VALUES (@in); SELECT 'x', @in; "
                    + "UPDATE #multi_t SET v = v; SET @out = @in * 2; RETURN 7 END");
            StringBuilder all = new StringBuilder();
            try (CallableStatement cs = conn.prepareCall("{? = call #multi(?, ?)}")) {
                cs.registerOutParameter(1, Types.INTEGER);
                cs.registerOutParameter(3, Types.INTEGER);
                for (int run = 1; run <= 2; run++) {
                    cs.setInt(2, run);
                    all.append(traverse(cs, closeEarly)).append(" | ");
                }
            }
            return all.toString();
        }
    }

    @ParameterizedTest(name = "closeEarly={0}")
    @ValueSource(booleans = {false, true})
    public void testResultTraversalMatchesPrepexec(boolean closeEarly) throws SQLException {
        String typed = traverseTwice("prepexec", closeEarly);
        assertTrue(typed.contains("UC=") && typed.contains("ret=7") && typed.contains("out=2"),
                "the reference must contain update counts, the return status and the OUT value: " + typed);
        assertEquals(typed, traverseTwice("none", closeEarly));
    }

    // Dynamic EXEC, explicit sp_executesql and INSERT ... EXEC run against the server. The procedure scope sees a table
    // created in the parent scope and changes it, and a table created inside the nested scope disappears. Same in all
    // modes.
    @ParameterizedTest(name = "mode={0}")
    @ValueSource(strings = {"none", "scopeTempTablesToConnection", "prepexec"})
    public void testNestedScopesOfDynamicSqlAndInsertExec(String mode) throws SQLException {
        try (Connection conn = openConnection(mode); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE #parent (v int)");
            stmt.execute("CREATE PROCEDURE #p6 @v int AS SELECT @v");

            try (PreparedStatement ps = conn.prepareStatement(
                    "EXEC('INSERT INTO #parent VALUES (1)'); EXEC('CREATE TABLE #inner_dyn (i int)'); SELECT ?")) {
                ps.setInt(1, 0);
                ps.execute();
            }
            assertEquals("1,", tableRows(stmt, "#parent"), "dynamic EXEC must change the parent table");
            assertNull(objectId(stmt, "#inner_dyn"), "a table created by dynamic EXEC must disappear");

            try (PreparedStatement ps = conn.prepareStatement(
                    "EXEC sp_executesql N'INSERT INTO #parent VALUES (@v)', N'@v int', ?; "
                            + "EXEC sp_executesql N'CREATE TABLE #inner_sp (i int)'")) {
                ps.setInt(1, 2);
                ps.execute();
            }
            assertEquals("1,2,", tableRows(stmt, "#parent"), "sp_executesql must change the parent table");
            assertNull(objectId(stmt, "#inner_sp"), "a table created by sp_executesql must disappear");

            try (PreparedStatement ps = conn.prepareStatement("INSERT #parent EXEC #p6 ?")) {
                ps.setInt(1, 3);
                ps.execute();
            }
            assertEquals("1,2,3,", tableRows(stmt, "#parent"), "INSERT ... EXEC must fill the parent table");
        }
    }

    private static Object objectId(Statement stmt, String table) throws SQLException {
        try (ResultSet rs = stmt.executeQuery("SELECT OBJECT_ID('tempdb.." + table + "')")) {
            assertTrue(rs.next());
            return rs.getObject(1);
        }
    }

    // Qualified temp-table names. The driver's temp-table detector decides whether scopeTempTablesToConnection runs a
    // statement directly, so a table must survive exactly when the detector recognizes the statement. Under none the
    // statement is always direct and the table survives; under prepexec it never survives. This characterizes which
    // naming forms the detector recognizes, without assuming that it recognizes all of them.
    private static Stream<Arguments> qualifiedTempTables() {
        List<Arguments> cases = new ArrayList<>();
        String[][] statements = {{"SELECT ? AS a INTO #q_plain", "#q_plain"},
                {"SELECT ? AS a INTO tempdb..#q_tempdb", "#q_tempdb"},
                {"SELECT ? AS a INTO tempdb.dbo.#q_dbo", "#q_dbo"},
                {"CREATE TABLE tempdb..#q_create (i int)", "#q_create"},
                {"CREATE TABLE tempdb.dbo.#q_create2 (i int)", "#q_create2"},
                {"SELECT ? AS a INTO [#q_bracket]", "#q_bracket"}};
        for (String mode : new String[] {"none", "scopeTempTablesToConnection", "prepexec"}) {
            for (String[] statement : statements) {
                cases.add(Arguments.of(mode, statement[0], statement[1]));
            }
        }
        return cases.stream();
    }

    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("qualifiedTempTables")
    public void testQualifiedTempTableNames(String mode, String sql, String table) throws SQLException {
        try (Connection conn = openConnection(mode); Statement stmt = conn.createStatement()) {
            boolean detected;
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                detected = ((SQLServerPreparedStatement) ps).containsTemporaryTableOperations(sql);
                if (sql.indexOf('?') >= 0) {
                    ps.setInt(1, 1);
                }
                ps.execute();
            }
            boolean survives = null != objectId(stmt, table);
            if ("none".equals(mode)) {
                assertTrue(survives, "under none the statement is direct, so the table survives");
            } else if ("prepexec".equals(mode)) {
                assertFalse(survives, "under prepexec the table ends with the RPC");
            } else {
                assertEquals(detected, survives,
                        "under scopeTempTablesToConnection the table survives exactly when the detector recognizes it");
            }
            if (survives) {
                stmt.execute("DROP TABLE " + table);
            }
        }
    }

    // Valid SQL forms that run end to end and give the same result as prepexec: an implicit call with a quoted procedure
    // name, a call after EXECUTE AS ... REVERT in the same text, and a permission statement.
    private static Stream<Arguments> validSqlForms() {
        List<Arguments> cases = new ArrayList<>();
        String[][] forms = {{"[#p7] ?", "7"}, {"\"#p7\" ?", "7"}, {"EXECUTE AS USER = 'dbo'; EXEC #p7 ?; REVERT;", "7"},
                {"EXECUTE /* c */ AS USER = 'dbo'; SELECT ?; REVERT; EXEC #p7 ?", "7"},
                {"GRANT EXECUTE ON dbo.mssqljdbc_perm7 TO public; SELECT ?", "7"}};
        for (String mode : new String[] {"none", "scopeTempTablesToConnection", "prepexec"}) {
            for (String[] form : forms) {
                cases.add(Arguments.of(mode, form[0], form[1]));
            }
        }
        return cases.stream();
    }

    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("validSqlForms")
    public void testValidSqlFormsRunEndToEnd(String mode, String sql, String expected) throws SQLException {
        try (Connection conn = openConnection(mode); Statement stmt = conn.createStatement()) {
            stmt.execute("IF OBJECT_ID('dbo.mssqljdbc_perm7') IS NOT NULL DROP PROCEDURE dbo.mssqljdbc_perm7");
            stmt.execute("CREATE PROCEDURE dbo.mssqljdbc_perm7 AS SELECT 1");
            try {
                stmt.execute("CREATE PROCEDURE #p7 @v int AS SELECT @v");
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    for (int i = 1; i <= sql.replaceAll("[^?]", "").length(); i++) {
                        ps.setInt(i, 7);
                    }
                    boolean hasResultSet = ps.execute();
                    String last = null;
                    for (int guard = 0; guard < 10; guard++) {
                        if (hasResultSet) {
                            try (ResultSet rs = ps.getResultSet()) {
                                while (rs.next()) {
                                    last = rs.getString(1);
                                }
                            }
                        } else if (-1 == ps.getUpdateCount()) {
                            break;
                        }
                        hasResultSet = ps.getMoreResults();
                    }
                    assertEquals(expected, last, sql);
                }
            } finally {
                stmt.execute("IF OBJECT_ID('dbo.mssqljdbc_perm7') IS NOT NULL DROP PROCEDURE dbo.mssqljdbc_perm7");
            }
        }
    }

    // Parameterized row limits: TOP (?), TOP ?, and OFFSET ? ROWS FETCH NEXT ? ROWS ONLY, with int and long inputs, and
    // limits that the server rejects. TOP ? without parentheses is accepted by none (a literal batch) but not by the
    // typed prepexec path, which scopeTempTablesToConnection also uses for a statement without temp tables.
    private static final String FIVE_ROWS = "(VALUES (1), (2), (3), (4), (5)) AS t(v)";

    private static Stream<Arguments> paginationCases() {
        List<Arguments> cases = new ArrayList<>();
        for (String mode : new String[] {"none", "scopeTempTablesToConnection", "prepexec"}) {
            for (boolean asLong : new boolean[] {false, true}) {
                cases.add(Arguments.of(mode, "SELECT TOP (?) v FROM " + FIVE_ROWS + " ORDER BY v", asLong,
                        new long[] {2}, "1,2,"));
                cases.add(Arguments.of(mode,
                        "SELECT v FROM " + FIVE_ROWS + " ORDER BY v OFFSET ? ROWS FETCH NEXT ? ROWS ONLY", asLong,
                        new long[] {1, 2}, "2,3,"));
                cases.add(Arguments.of(mode, "SELECT v FROM " + FIVE_ROWS + " ORDER BY v OFFSET ? ROWS", asLong,
                        new long[] {3}, "4,5,"));
                if ("none".equals(mode)) {
                    cases.add(Arguments.of(mode, "SELECT TOP ? v FROM " + FIVE_ROWS + " ORDER BY v", asLong,
                            new long[] {2}, "1,2,"));
                }
            }
        }
        return cases.stream();
    }

    @ParameterizedTest(name = "{0}: {1} long={2}")
    @MethodSource("paginationCases")
    public void testParameterizedRowLimits(String mode, String sql, boolean asLong, long[] limits,
            String expected) throws SQLException {
        try (Connection conn = openConnection(mode); PreparedStatement ps = conn.prepareStatement(sql)) {
            bindLimits(ps, asLong, limits);
            StringBuilder rows = new StringBuilder();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    rows.append(rs.getInt(1)).append(',');
                }
            }
            assertEquals(expected, rows.toString());
        }
    }

    @ParameterizedTest(name = "{0}: {1} long={2}")
    @MethodSource("invalidLimitCases")
    public void testServerInvalidRowLimitsAreRejected(String mode, String sql, boolean asLong) throws SQLException {
        try (Connection conn = openConnection(mode); PreparedStatement ps = conn.prepareStatement(sql)) {
            bindLimits(ps, asLong, new long[] {-1});
            assertThrows(SQLException.class, () -> {
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                }
            }, "the server must reject a negative limit");
        }
    }

    private static Stream<Arguments> invalidLimitCases() {
        List<Arguments> cases = new ArrayList<>();
        for (String mode : new String[] {"none", "scopeTempTablesToConnection", "prepexec"}) {
            for (boolean asLong : new boolean[] {false, true}) {
                cases.add(Arguments.of(mode, "SELECT TOP (?) v FROM " + FIVE_ROWS + " ORDER BY v", asLong));
                cases.add(Arguments.of(mode,
                        "SELECT v FROM " + FIVE_ROWS + " ORDER BY v OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY", asLong));
            }
        }
        return cases.stream();
    }

    private static void bindLimits(PreparedStatement ps, boolean asLong, long[] limits) throws SQLException {
        for (int i = 0; i < limits.length; i++) {
            if (asLong) {
                ps.setLong(i + 1, limits[i]);
            } else {
                ps.setInt(i + 1, (int) limits[i]);
            }
        }
    }

    // Timestamp and LocalDateTime boundaries, compared with typed binding: day, month and year rollover from nine-digit
    // fractions, the ends of the datetime2 range (a value that rounds past the maximum is an error in both), and a
    // Calendar around daylight-saving transitions (a local time that does not exist, and one that occurs twice). Also
    // the direct SELECT ... INTO #temp form that scopeTempTablesToConnection runs as a literal batch.
    private static final String DT2 = "CONVERT(varchar(40), CAST(? AS datetime2(7)), 121)";

    private static String temporalOutcome(String mode, String sql, Binder binder) {
        try (Connection conn = openConnection(mode); PreparedStatement ps = conn.prepareStatement(sql)) {
            binder.bind(ps);
            boolean hasResultSet = ps.execute();
            while (!hasResultSet) {
                if (-1 == ps.getUpdateCount()) {
                    return "NO RESULT";
                }
                hasResultSet = ps.getMoreResults();
            }
            try (ResultSet rs = ps.getResultSet()) {
                return rs.next() ? rs.getString(1) : "NO ROW";
            }
        } catch (SQLException e) {
            return "ERROR";
        }
    }

    @FunctionalInterface
    private interface Binder {
        void bind(PreparedStatement ps) throws SQLException;
    }

    private static Stream<Arguments> timestampBoundaries() {
        List<Arguments> cases = new ArrayList<>();
        String[] values = {"2024-12-31 23:59:59.999999999", "2023-02-28 23:59:59.999999999",
                "2024-02-28 23:59:59.999999999", "2024-02-29 23:59:59.999999999", "2024-06-30 23:59:59.999999950",
                "2024-06-30 23:59:59.999999949", "0001-01-01 00:00:00.000000000", "9999-12-31 23:59:59.999999900",
                "9999-12-31 23:59:59.999999999"};
        for (String value : values) {
            cases.add(Arguments.of(value));
        }
        return cases.stream();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("timestampBoundaries")
    public void testTimestampBoundariesMatchTypedBinding(String text) {
        Binder binder = ps -> ps.setTimestamp(1, Timestamp.valueOf(text));
        String typed = temporalOutcome("prepexec", "SELECT " + DT2, binder);
        for (String mode : new String[] {"none", "scopeTempTablesToConnection"}) {
            assertEquals(typed, temporalOutcome(mode, "SELECT " + DT2, binder), text + " in " + mode);
        }
        // The same value through the temp-table form, which scopeTempTablesToConnection runs directly.
        String direct = temporalOutcome("scopeTempTablesToConnection",
                "SELECT CAST(? AS datetime2(7)) AS a INTO #bt; SELECT CONVERT(varchar(40), a, 121) FROM #bt",
                binder);
        assertEquals(typed, direct, text + " through SELECT INTO #temp");
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"2024-12-31T23:59:59.999999999", "2024-02-29T23:59:59.999999999",
            "2024-06-30T23:59:59.999999950", "0001-01-01T00:00:00", "9999-12-31T23:59:59.999999999",
            "2024-01-01T13:14:00", "2024-01-01T00:00:00", "2024-01-01T13:14:15.5"})
    public void testLocalDateTimeBoundariesMatchTypedBinding(String text) {
        Binder binder = ps -> ps.setObject(1, LocalDateTime.parse(text));
        for (String type : new String[] {"datetime2(7)", "datetime2(0)", "datetime", "smalldatetime", "date", "time(7)",
                "datetimeoffset(7)"}) {
            String sql = "SELECT CONVERT(varchar(40), CAST(? AS " + type + "), 121)";
            String typed = temporalOutcome("prepexec", sql, binder);
            for (String mode : new String[] {"none", "scopeTempTablesToConnection"}) {
                assertEquals(typed, temporalOutcome(mode, sql, binder), text + " as " + type + " in " + mode);
            }
        }
    }

    // A Timestamp or LocalDateTime bound to a DATE target is compared and converted as a date, so a value at noon
    // matches the same day and the day end does not carry into the next day.
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"2024-01-01T12:00:00", "2024-01-01T00:00:00", "2024-01-01T23:59:59.999999999",
            "2024-12-31T23:59:59.999999999", "0001-01-01T00:00:00", "9999-12-31T23:59:59.999999999"})
    public void testDateTargetUsesOnlyTheDate(String text) {
        LocalDateTime value = LocalDateTime.parse(text);
        Binder[] binders = {ps -> ps.setObject(1, value, Types.DATE),
                ps -> ps.setObject(1, Timestamp.valueOf(value), Types.DATE)};
        String[] queries = {"SELECT CONVERT(varchar(40), CAST(? AS varchar(40)), 121)",
                "SELECT CASE WHEN CAST('" + text.substring(0, 10) + "' AS date) = ? THEN 'match' ELSE 'differs' END"};
        for (Binder binder : binders) {
            for (String sql : queries) {
                String typed = temporalOutcome("prepexec", sql, binder);
                for (String mode : new String[] {"none", "scopeTempTablesToConnection"}) {
                    assertEquals(typed, temporalOutcome(mode, sql, binder), text + " in " + mode + ": " + sql);
                }
            }
        }
    }

    @ParameterizedTest(name = "{0} in {1}")
    @MethodSource("daylightSavingInstants")
    public void testCalendarAroundDaylightSavingMatchesTypedBinding(String instant, String zone) {
        Binder binder = ps -> ps.setTimestamp(1, Timestamp.from(Instant.parse(instant)),
                Calendar.getInstance(TimeZone.getTimeZone(zone)));
        String typed = temporalOutcome("prepexec", "SELECT " + DT2, binder);
        for (String mode : new String[] {"none", "scopeTempTablesToConnection"}) {
            assertEquals(typed, temporalOutcome(mode, "SELECT " + DT2, binder), instant + " in " + zone);
        }
    }

    private static Stream<Arguments> daylightSavingInstants() {
        List<Arguments> cases = new ArrayList<>();
        // Spring forward 2024-03-10 02:00 -> 03:00 and fall back 2024-11-03 02:00 -> 01:00 in New York; the same
        // for Sydney (spring forward 2024-10-06, fall back 2024-04-07).
        String[][] pairs = {{"2024-03-10T06:59:59.500Z", "America/New_York"}, {"2024-03-10T07:00:00Z", "America/New_York"},
                {"2024-11-03T05:30:00Z", "America/New_York"}, {"2024-11-03T06:30:00Z", "America/New_York"},
                {"2024-10-05T15:59:59.500Z", "Australia/Sydney"}, {"2024-04-06T15:30:00Z", "Australia/Sydney"},
                {"2024-04-06T16:30:00Z", "Australia/Sydney"}};
        for (String[] pair : pairs) {
            cases.add(Arguments.of(pair[0], pair[1]));
        }
        return cases.stream();
    }

    // Large procedure arguments keep their full content: a 1,000,000 character varchar, a 600,000 byte varbinary and a
    // 300,000 character nvarchar, sent as values, as streams with a length, and as streams without a length. The
    // procedure returns the lengths and SHA-256 hashes, which are compared with hashes computed here.
    private static String largeAscii() {
        StringBuilder text = new StringBuilder(1000000);
        for (int i = 0; i < 1000000; i++) {
            text.append((char) ('a' + (i * 7 + i / 26) % 26));
        }
        return text.toString();
    }

    private static String largeUnicode() {
        StringBuilder text = new StringBuilder(300000);
        for (int i = 0; i < 300000; i++) {
            text.append((char) (0x4E00 + (i * 31) % 500));
        }
        return text.toString();
    }

    private static byte[] largeBinary() {
        byte[] data = new byte[600000];
        new java.util.Random(42).nextBytes(data);
        return data;
    }

    @ParameterizedTest(name = "mode={0}, {1}")
    @MethodSource("largeArgumentCases")
    @Tag(Constants.xSQLv12)
    public void testLargeProcedureArgumentsKeepFullContent(String mode, String kind) throws Exception {
        String ascii = largeAscii();
        String unicode = largeUnicode();
        byte[] binary = largeBinary();
        try (Connection conn = openConnection(mode); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE PROCEDURE #big @s varchar(max), @b varbinary(max), @n nvarchar(max) AS SELECT "
                    + "LEN(@s), DATALENGTH(@b), LEN(@n), HASHBYTES('SHA2_256', @s), HASHBYTES('SHA2_256', @b), "
                    + "HASHBYTES('SHA2_256', @n)");
            try (CallableStatement cs = conn.prepareCall("{call #big(?, ?, ?)}")) {
                switch (kind) {
                    case "values":
                        cs.setString(1, ascii);
                        cs.setBytes(2, binary);
                        cs.setNString(3, unicode);
                        break;
                    case "streams with length":
                        cs.setAsciiStream(1, new ByteArrayInputStream(ascii.getBytes(StandardCharsets.US_ASCII)),
                                ascii.length());
                        cs.setBinaryStream(2, new ByteArrayInputStream(binary), binary.length);
                        cs.setNCharacterStream(3, new StringReader(unicode), unicode.length());
                        break;
                    default:
                        cs.setCharacterStream(1, new StringReader(ascii));
                        cs.setBinaryStream(2, new ByteArrayInputStream(binary));
                        cs.setNCharacterStream(3, new StringReader(unicode));
                        break;
                }
                try (ResultSet rs = cs.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(ascii.length(), rs.getInt(1));
                    assertEquals(binary.length, rs.getInt(2));
                    assertEquals(unicode.length(), rs.getInt(3));
                    MessageDigest sha = MessageDigest.getInstance("SHA-256");
                    assertArrayEquals(sha.digest(ascii.getBytes(StandardCharsets.US_ASCII)), rs.getBytes(4));
                    assertArrayEquals(sha.digest(binary), rs.getBytes(5));
                    assertArrayEquals(sha.digest(unicode.getBytes(StandardCharsets.UTF_16LE)), rs.getBytes(6));
                }
            }
        }
    }

    private static Stream<Arguments> largeArgumentCases() {
        List<Arguments> cases = new ArrayList<>();
        for (String mode : new String[] {"none", "prepexec"}) {
            for (String kind : new String[] {"values", "streams with length", "streams without length"}) {
                cases.add(Arguments.of(mode, kind));
            }
        }
        return cases.stream();
    }

    // Empty inputs are empty, not NULL: an empty string, an empty byte array, and zero-length streams.
    @ParameterizedTest(name = "mode={0}, {1}")
    @MethodSource("emptyArgumentCases")
    public void testEmptyProcedureArgumentsAreNotNull(String mode, String kind) throws SQLException {
        try (Connection conn = openConnection(mode); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE PROCEDURE #empty @s varchar(max), @b varbinary(max) AS SELECT LEN(@s), "
                    + "DATALENGTH(@b), CASE WHEN @s IS NULL THEN 1 ELSE 0 END, CASE WHEN @b IS NULL THEN 1 ELSE 0 END");
            try (CallableStatement cs = conn.prepareCall("{call #empty(?, ?)}")) {
                if ("values".equals(kind)) {
                    cs.setString(1, "");
                    cs.setBytes(2, new byte[0]);
                } else {
                    cs.setCharacterStream(1, new StringReader(""), 0);
                    cs.setBinaryStream(2, new ByteArrayInputStream(new byte[0]), 0);
                }
                try (ResultSet rs = cs.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getInt(1), "length of the empty string");
                    assertEquals(0, rs.getInt(2), "length of the empty binary");
                    assertEquals(0, rs.getInt(3), "the empty string must not be NULL");
                    assertEquals(0, rs.getInt(4), "the empty binary must not be NULL");
                }
            }
        }
    }

    private static Stream<Arguments> emptyArgumentCases() {
        List<Arguments> cases = new ArrayList<>();
        for (String mode : new String[] {"none", "prepexec"}) {
            for (String kind : new String[] {"values", "zero-length streams"}) {
                cases.add(Arguments.of(mode, kind));
            }
        }
        return cases.stream();
    }

    // Table-valued parameter variants: an empty TVP, a multi-column TVP with NULL cells, and a TVP with many rows. The
    // outcome must be the same as with prepexec, and the NULL cells must stay NULL.
    private static String tvpOutcome(String mode, int variant) throws SQLException {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String typeName = "PrepareMethodTvpVariant_" + suffix;
        String procedureName = "dbo.PrepareMethodTvpVariantProc_" + suffix;
        try (Connection conn = openConnection(mode); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TYPE " + typeName + " AS TABLE (a int, b nvarchar(20), c datetime2(7))");
            try {
                stmt.execute("CREATE PROCEDURE " + procedureName + " @t " + typeName + " READONLY AS SELECT COUNT(*), "
                        + "ISNULL(SUM(a), 0), SUM(CASE WHEN b IS NULL THEN 1 ELSE 0 END), "
                        + "SUM(CASE WHEN c IS NULL THEN 1 ELSE 0 END) FROM @t");
                SQLServerDataTable table = new SQLServerDataTable();
                table.addColumnMetadata("a", Types.INTEGER);
                table.addColumnMetadata("b", Types.NVARCHAR);
                table.addColumnMetadata("c", Types.TIMESTAMP);
                if (1 == variant) {
                    table.addRow(1, "x", Timestamp.valueOf("2024-01-01 00:00:00"));
                    table.addRow(2, null, Timestamp.valueOf("2024-01-02 00:00:00"));
                    table.addRow(null, "z", null);
                } else if (2 == variant) {
                    for (int i = 1; i <= 1000; i++) {
                        table.addRow(i, "row" + i, Timestamp.valueOf("2024-01-01 00:00:00"));
                    }
                }
                try (SQLServerCallableStatement cs = (SQLServerCallableStatement) conn
                        .prepareCall("{call " + procedureName + "(?)}")) {
                    cs.setStructured(1, typeName, table);
                    try (ResultSet rs = cs.executeQuery()) {
                        rs.next();
                        return rs.getInt(1) + "/" + rs.getInt(2) + "/" + rs.getInt(3) + "/" + rs.getInt(4);
                    }
                }
            } finally {
                dropIfExists(stmt, "PROCEDURE", procedureName);
                dropIfExists(stmt, "TYPE", typeName);
            }
        }
    }

    @ParameterizedTest(name = "variant {0}")
    @ValueSource(ints = {0, 1, 2})
    @Tag(Constants.xSQLv12)
    public void testTableValuedParameterVariantsMatchPrepexec(int variant) throws SQLException {
        String typed = tvpOutcome("prepexec", variant);
        assertEquals(typed, tvpOutcome("none", variant));
        if (1 == variant) {
            assertEquals("3/3/1/1", typed, "NULL cells must stay NULL");
        } else if (2 == variant) {
            assertEquals("1000/500500/0/0", typed);
        }
    }

    // A statement-aborting error inside the procedure (division by zero) does not stop the procedure or the batch, so
    // the entry still inserts its row. Each entry runs once, the failing entry is reported as failed, and the entries
    // before it succeed.
    @Test
    public void testStatementAbortingErrorInProcedureBatch() throws SQLException {
        try (Connection conn = openConnection("none"); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE #tx_s (v int)");
            stmt.execute("CREATE PROCEDURE #tx_sp @v int AS BEGIN IF @v = 13 SELECT 1 / 0; "
                    + "INSERT #tx_s VALUES (@v) END");
            int[] counts;
            try (PreparedStatement ps = conn.prepareStatement("EXEC #tx_sp ?")) {
                for (int v : new int[] {1, 2, 13, 4}) {
                    ps.setInt(1, v);
                    ps.addBatch();
                }
                BatchUpdateException e = assertThrows(BatchUpdateException.class, ps::executeBatch);
                counts = e.getUpdateCounts();
            }
            assertEquals(4, counts.length);
            assertEquals(1, counts[0]);
            assertEquals(1, counts[1]);
            assertEquals(Statement.EXECUTE_FAILED, counts[2]);
            String rows = tableRows(stmt, "#tx_s");
            assertTrue(rows.startsWith("1,2,"), "entries before the failure must be applied: " + rows);
            assertEquals(rows.length(), rows.replaceAll("(\\d+,)\\1", "$1").length(),
                    "no entry may be applied twice: " + rows);
        }
    }

    private static final class BatchResult {
        int[] counts;
        String rows;
        boolean failed;
    }

    private static BatchResult runBatch(String mode, String sql, int[] values) throws SQLException {
        BatchResult result = new BatchResult();
        try (Connection conn = openConnection(mode); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE #batch_t (i int)");
            stmt.execute("CREATE PROCEDURE #batch_p @v int AS BEGIN "
                    + "IF @v = 13 THROW 50000, 'unlucky', 1; INSERT INTO #batch_t VALUES (@v) END");
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (int v : values) {
                    ps.setInt(1, v);
                    ps.addBatch();
                }
                try {
                    result.counts = ps.executeBatch();
                } catch (BatchUpdateException e) {
                    result.failed = true;
                    result.counts = e.getUpdateCounts();
                }
            }
            StringBuilder rows = new StringBuilder();
            try (ResultSet rs = stmt.executeQuery("SELECT i FROM #batch_t ORDER BY i")) {
                while (rs.next()) {
                    rows.append(rows.length() > 0 ? "," : "").append(rs.getInt(1));
                }
            }
            result.rows = rows.toString();
        }
        return result;
    }

    // Parameters that cannot be inlined as literals: a direct statement catches the rejection and is sent as an
    // unprepared RPC, as is a procedure call.

    @Test
    public void testOutParameterOnNonProcedureStatementIsRetriedAsRpc() throws SQLException {
        try (Connection conn = openConnection("none"); CallableStatement cs = conn.prepareCall("SELECT ?")) {
            cs.registerOutParameter(1, Types.INTEGER);
            assertTrue(cs.execute());
            try (ResultSet rs = cs.getResultSet()) {
                assertTrue(rs.next());
            }
        }
    }

    @Test
    public void testStreamInDirectStatementIsRetriedAsRpc() throws SQLException {
        try (Connection conn = openConnection("none");
                PreparedStatement ps = conn.prepareStatement("SELECT DATALENGTH(?)")) {
            ps.setBinaryStream(1, new ByteArrayInputStream(new byte[] {1, 2, 3}));
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
            }
        }
    }

    /** After the retry the statement keeps being sent as an unprepared RPC, also for values that could be inlined. */
    @Test
    public void testStatementStaysOnRpcAfterRetry() throws SQLException {
        Logger logger = Logger.getLogger(STATEMENT_LOGGER);
        Level previousLevel = logger.getLevel();
        ProcedureLog log = new ProcedureLog();
        log.setLevel(Level.FINE);
        logger.setLevel(Level.FINE);
        logger.addHandler(log);
        try (Connection conn = openConnection("none");
                PreparedStatement ps = conn.prepareStatement("SELECT DATALENGTH(?)")) {
            ps.setBinaryStream(1, new ByteArrayInputStream(new byte[] {1, 2, 3}));
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
            }
            for (int i = 0; i < 2; i++) {
                ps.setBytes(1, new byte[] {1, 2, 3, 4});
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(4, rs.getInt(1));
                }
            }
        } finally {
            logger.removeHandler(log);
            logger.setLevel(previousLevel);
        }
        assertEquals(3, log.messages.size(), log.messages.toString());
        for (String message : log.messages) {
            assertTrue(message.startsWith("calling sp_executesql"), message);
        }
    }

    @Test
    public void testStreamInDirectBatchIsRetriedAsRpc() throws SQLException {
        try (Connection conn = openConnection("none"); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE ##stream_batch_t (v varbinary(max))");
            try {
                try (PreparedStatement ps = conn.prepareStatement("INSERT INTO ##stream_batch_t VALUES (?)")) {
                    ps.setBytes(1, new byte[] {1});
                    ps.addBatch();
                    ps.setBinaryStream(1, new ByteArrayInputStream(new byte[] {1, 2, 3}));
                    ps.addBatch();
                    assertArrayEquals(new int[] {1, 1}, ps.executeBatch());
                }
                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*), SUM(DATALENGTH(v)) FROM ##stream_batch_t")) {
                    assertTrue(rs.next());
                    assertEquals(2, rs.getInt(1));
                    assertEquals(4, rs.getInt(2));
                }
            } finally {
                stmt.execute("DROP TABLE ##stream_batch_t");
            }
        }
    }

    @Test
    public void testTenBinaryStreamsInDirectBatchAreRetriedAsRpc() throws SQLException {
        byte[][] values = new byte[10][];
        int[] expectedCounts = new int[values.length];
        Arrays.fill(expectedCounts, 1);
        for (int i = 0; i < values.length; i++) {
            values[i] = new byte[i + 1];
            Arrays.fill(values[i], (byte) (i + 1));
        }

        try (Connection conn = openConnection("none"); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE #stream_batch_rows (id int PRIMARY KEY, v varbinary(max))");
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO #stream_batch_rows VALUES (?, ?)")) {
                for (int i = 0; i < values.length; i++) {
                    ps.setInt(1, i);
                    ps.setBinaryStream(2, new ByteArrayInputStream(values[i]));
                    ps.addBatch();
                }
                assertArrayEquals(expectedCounts, ps.executeBatch());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT id, v FROM #stream_batch_rows ORDER BY id")) {
                for (int i = 0; i < values.length; i++) {
                    assertTrue(rs.next(), "Missing batch row " + i);
                    assertEquals(i, rs.getInt(1));
                    assertArrayEquals(values[i], rs.getBytes(2), "Unexpected bytes in batch row " + i);
                }
                assertFalse(rs.next(), "The batch must insert exactly ten rows");
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testRpcRetryPreservesCharacterParameterTypes(boolean unicode) throws SQLException {
        try (Connection conn = openConnection("none", "sendStringParametersAsUnicode=" + unicode);
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT DATALENGTH(?), DATALENGTH(?), DATALENGTH(?), DATALENGTH(?), DATALENGTH(?)")) {
            for (int attempt = 0; attempt < 2; attempt++) {
                ps.setString(1, "abc");
                ps.setCharacterStream(2, new StringReader("abc"), 3);
                ps.setNString(3, "abc");
                ps.setNull(4, Types.VARCHAR);
                ps.setBinaryStream(5, new ByteArrayInputStream(new byte[] {1, 2, 3}));
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(unicode ? 6 : 3, rs.getInt(1));
                    assertEquals(unicode ? 6 : 3, rs.getInt(2));
                    assertEquals(6, rs.getInt(3));
                    assertNull(rs.getObject(4));
                    assertEquals(3, rs.getInt(5));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void testBatchRpcRetryPreservesCharacterParameterTypes() throws SQLException {
        try (Connection conn = openConnection("none", "sendStringParametersAsUnicode=false");
                Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE #retry_lengths (textLength int, binaryLength int)");
            try (PreparedStatement ps = conn
                    .prepareStatement("INSERT INTO #retry_lengths VALUES (DATALENGTH(?), DATALENGTH(?))")) {
                ps.setString(1, "abc");
                ps.setBytes(2, new byte[] {1});
                ps.addBatch();
                ps.setBinaryStream(2, new ByteArrayInputStream(new byte[] {1, 2}));
                ps.addBatch();
                assertArrayEquals(new int[] {1, 1}, ps.executeBatch());

                // The current bindings must also be restored, even when only the stream is rebound after the batch.
                ps.setBinaryStream(2, new ByteArrayInputStream(new byte[] {1, 2, 3}));
                assertEquals(1, ps.executeUpdate());
            }
            try (ResultSet rs = stmt
                    .executeQuery("SELECT textLength, binaryLength FROM #retry_lengths ORDER BY binaryLength")) {
                for (int i = 1; i <= 3; i++) {
                    assertTrue(rs.next());
                    assertEquals(3, rs.getInt(1));
                    assertEquals(i, rs.getInt(2));
                }
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testSingleRpcRetryRestoresQueuedBatchParameters() throws SQLException {
        try (Connection conn = openConnection("none", "sendStringParametersAsUnicode=false");
                Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE #queued_retry_lengths (textLength int, binaryLength int)");
            try (PreparedStatement ps = conn
                    .prepareStatement("INSERT INTO #queued_retry_lengths VALUES (DATALENGTH(?), DATALENGTH(?))")) {
                ps.setString(1, "abc");
                ps.setBytes(2, new byte[] {1});
                ps.addBatch();
                ps.setBinaryStream(2, new ByteArrayInputStream(new byte[] {1, 2}));
                assertEquals(1, ps.executeUpdate());
                assertArrayEquals(new int[] {1}, ps.executeBatch());
            }
            try (ResultSet rs = stmt
                    .executeQuery("SELECT textLength, binaryLength FROM #queued_retry_lengths ORDER BY binaryLength")) {
                for (int i = 1; i <= 2; i++) {
                    assertTrue(rs.next());
                    assertEquals(3, rs.getInt(1));
                    assertEquals(i, rs.getInt(2));
                }
                assertFalse(rs.next());
            }
        }
    }

    /** Retrying local-temp-table creation uses RPC scope and executes earlier SQL only once. */
    @ParameterizedTest
    @ValueSource(strings = {"CREATE TABLE #stream_reject (v varbinary(max)); INSERT INTO #stream_reject VALUES (?)",
            "CREATE /* comment */ TABLE #stream_reject (v varbinary(max)); INSERT INTO #stream_reject VALUES (?)",
            "CREATE TABLE /* outer /* nested */ comment */ #stream_reject (v varbinary(max)); INSERT INTO #stream_reject VALUES (?)",
            "CREATE TABLE tempdb..#stream_reject (v varbinary(max)); INSERT INTO #stream_reject VALUES (?)",
            "CREATE TABLE [tempdb] . /* schema */ . [#stream_reject] (v varbinary(max)); INSERT INTO #stream_reject VALUES (?)",
            "CREATE -- comment\n TABLE #stream_reject (v varbinary(max)); INSERT INTO #stream_reject VALUES (?)",
            "SELECT ? AS v INTO /* comment */ tempdb..#stream_reject", "SELECT ? AS v INTO [tempdb]..[#stream_reject]"})
    public void testStreamInTempTableStatementIsRetriedAsRpc(String sql) throws SQLException {
        for (boolean batch : new boolean[] {false, true}) {
            try (Connection conn = openConnection("none"); Statement stmt = conn.createStatement();
                    PreparedStatement ps = conn.prepareStatement("INSERT INTO #retry_side_effect VALUES (1); " + sql)) {
                stmt.execute("CREATE TABLE #retry_side_effect (v int)");
                ps.setBinaryStream(1, new ByteArrayInputStream(new byte[] {1, 2, 3}));
                if (batch) {
                    ps.addBatch();
                    assertArrayEquals(new int[] {1}, ps.executeBatch());
                } else {
                    assertFalse(ps.execute());
                }
                assertNull(objectId(stmt, "#stream_reject"), "The new local temp table has RPC scope");
                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM #retry_side_effect")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1), "Earlier SQL must execute exactly once");
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testRpcRetryWithScalarSubqueryAndOutputIntoExistingTempTable(boolean batch) throws SQLException {
        try (Connection conn = openConnection("none"); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE #output_data (v varbinary(max), n int); "
                    + "INSERT INTO #output_data VALUES (0x00, 0); CREATE TABLE #out (n int)");
            try (PreparedStatement ps = conn
                    .prepareStatement("UPDATE #output_data SET v = ?, n = (SELECT 1) OUTPUT inserted.n INTO #out")) {
                byte[] value = {1, 2, 3};
                ps.setBinaryStream(1, new ByteArrayInputStream(value));
                if (batch) {
                    ps.addBatch();
                    assertArrayEquals(new int[] {1}, ps.executeBatch());
                } else {
                    assertEquals(1, ps.executeUpdate());
                }
                try (ResultSet rs = stmt.executeQuery("SELECT v, n FROM #output_data")) {
                    assertTrue(rs.next());
                    assertArrayEquals(value, rs.getBytes(1));
                    assertEquals(1, rs.getInt(2));
                    assertFalse(rs.next());
                }
                try (ResultSet rs = stmt.executeQuery("SELECT n FROM #out")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"SELECT DATALENGTH(?) /* CREATE TABLE #ignored */",
            "SELECT DATALENGTH(?) -- CREATE TABLE #ignored", "SELECT DATALENGTH(?), 'INTO #ignored'",
            "SELECT DATALENGTH(?) AS [INTO #ignored]"})
    public void testTempTableTextDoesNotPreventRpcRetry(String sql) throws SQLException {
        try (Connection conn = openConnection("none"); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setBinaryStream(1, new ByteArrayInputStream(new byte[] {1, 2, 3}));
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testNullStreamIsStillInlinedInDirectStatement() throws SQLException {
        try (Connection conn = openConnection("none");
                PreparedStatement ps = conn.prepareStatement("SELECT DATALENGTH(?)")) {
            ps.setBinaryStream(1, null);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
            }
        }
    }

    @ParameterizedTest(name = "mode={0}")
    @ValueSource(strings = {"prepexec", "none"})
    public void testStreamInProcedureCall(String mode) throws SQLException {
        try (Connection conn = openConnection(mode); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE PROCEDURE #stream_len @v varbinary(max) AS SELECT DATALENGTH(@v)");
            try (PreparedStatement ps = conn.prepareStatement("{call #stream_len(?)}")) {
                ps.setBinaryStream(1, new ByteArrayInputStream(new byte[] {1, 2, 3}));
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(3, rs.getInt(1));
                }
            }
        }
    }

    @Test
    @Tag(Constants.xSQLv12)
    public void testTableValuedParameterInDirectStatementIsRetriedAsRpc() throws SQLException {
        String typeName = "PrepareMethodRpcTvp_" + UUID.randomUUID().toString().replace("-", "");
        try (Connection conn = openConnection("none"); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TYPE " + typeName + " AS TABLE (value int)");
            try {
                try (SQLServerPreparedStatement ps = (SQLServerPreparedStatement) conn
                        .prepareStatement("SELECT SUM(value) FROM ?")) {
                    ps.setStructured(1, typeName, tvpWithRows());
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        assertTrue(rs.getInt(1) > 0);
                    }
                }
            } finally {
                dropIfExists(stmt, "TYPE", typeName);
            }
        }
    }

    @ParameterizedTest(name = "mode={0}")
    @ValueSource(strings = {"prepexec", "none"})
    @Tag(Constants.xSQLv12)
    public void testTableValuedParameterInProcedureCall(String mode) throws SQLException {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String typeName = "PrepareMethodRpcTvp_" + suffix;
        String procedureName = "PrepareMethodRpcTvpProc_" + suffix;
        try (Connection conn = openConnection(mode); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TYPE " + typeName + " AS TABLE (value int)");
            try {
                stmt.execute("CREATE PROCEDURE " + procedureName + " @t " + typeName
                        + " READONLY AS SELECT SUM(value) FROM @t");
                try (SQLServerPreparedStatement ps = (SQLServerPreparedStatement) conn
                        .prepareStatement("{call " + procedureName + "(?)}")) {
                    ps.setStructured(1, typeName, tvpWithRows());
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(6, rs.getInt(1));
                    }
                }
            } finally {
                dropIfExists(stmt, "PROCEDURE", procedureName);
                dropIfExists(stmt, "TYPE", typeName);
            }
        }
    }

    private static SQLServerDataTable tvpWithRows() throws SQLException {
        SQLServerDataTable table = new SQLServerDataTable();
        table.addColumnMetadata("value", Types.INTEGER);
        table.addRow(1);
        table.addRow(2);
        table.addRow(3);
        return table;
    }

    // Temp tables: an EXEC is an RPC, so a table created in the same statement ends with the call. A statement that
    // is not a procedure call is still executed as a literal batch and keeps its temp tables.

    @Test
    public void testTempTableCreatedWithExecIsScopedToTheCall() throws SQLException {
        try (Connection conn = openConnection("none"); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE PROCEDURE #scope_p @v int AS SELECT @v");
            try (PreparedStatement ps = conn
                    .prepareStatement("EXEC #scope_p ?; SELECT ? AS a INTO #created_with_exec")) {
                ps.setInt(1, 1);
                ps.setInt(2, 2);
                ps.execute();
            }
            try (ResultSet rs = stmt.executeQuery("SELECT OBJECT_ID('tempdb..#created_with_exec')")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1), "a table created in the same statement as an EXEC ends with the call");
            }
        }
    }

    @Test
    public void testNonProcedureStatementStillKeepsTempTable() throws SQLException {
        try (Connection conn = openConnection("none")) {
            try (PreparedStatement ps = conn.prepareStatement("SELECT ? AS a INTO #kept_table")) {
                ps.setInt(1, 1);
                ps.execute();
            }
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT OBJECT_ID('tempdb..#kept_table')")) {
                assertTrue(rs.next());
                assertFalse(null == rs.getObject(1), "temp table created by a direct statement must stay");
            }
        }
    }

    // EXECUTE AS switches the security context. It is not a procedure call, so the statement stays a literal batch
    // and a temp table it creates stays on the connection, with or without comments before AS.
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"EXECUTE AS USER = 'dbo'; SELECT ? AS v INTO #kept_after_exec_as; REVERT;",
            "SET NOCOUNT ON; EXECUTE /* context switch */ AS USER = 'dbo'; SELECT ? AS v INTO #kept_after_exec_as; REVERT;",
            "EXECUTE /* a /* nested */ b */ -- c\n AS USER = 'dbo'; SELECT ? AS v INTO #kept_after_exec_as; REVERT;"})
    public void testExecuteAsKeepsTempTable(String sql) throws SQLException {
        try (Connection conn = openConnection("none")) {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setInt(1, 1);
                ps.execute();
            }
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT OBJECT_ID('tempdb..#kept_after_exec_as')")) {
                assertTrue(rs.next());
                assertFalse(null == rs.getObject(1), "EXECUTE AS must not turn the statement into an RPC");
            }
        }
    }

    // A procedure named as the first statement without EXEC is also a call. Its argument must stay a plain value, and
    // the statement is sent as an RPC.
    @ParameterizedTest(name = "mode={0}")
    @ValueSource(strings = {"none", "prepexec", "scopeTempTablesToConnection"})
    public void testImplicitProcedureCallWithLong(String mode) throws SQLException {
        try (Connection conn = openConnection(mode); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE PROCEDURE #implicit_p @v bigint AS SELECT @v");
            for (String sql : new String[] {"#implicit_p ?", "/* c */ #implicit_p ?"}) {
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setLong(1, Long.MAX_VALUE);
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next(), sql);
                        assertEquals(Long.MAX_VALUE, rs.getLong(1), sql);
                        assertFalse(rs.next(), sql);
                    }
                }
            }
        }
    }

    // SET ROWCOUNT and TOP accept a number or a variable, not a CAST expression, so a Long stays a plain literal there,
    // also when comments or a lot of white space come before the marker.
    @ParameterizedTest(name = "mode={0}")
    @ValueSource(strings = {"none", "prepexec", "scopeTempTablesToConnection"})
    public void testLongInConstantOnlyPosition(String mode) throws SQLException {
        String rows = "SELECT v FROM (VALUES (1), (2), (3)) AS t(v)";
        String spaces = new String(new char[100]).replace('\0', ' ');
        try (Connection conn = openConnection(mode)) {
            for (String sql : new String[] {"SET ROWCOUNT ?; " + rows, "SET ROWCOUNT /* limit */ ?; " + rows,
                    "SET ROWCOUNT -- limit\n ?; " + rows, "SET ROWCOUNT" + spaces + "?; " + rows,
                    "SET ROWCOUNT /* outer /* inner */ tail */ ?; " + rows}) {
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setLong(1, 2L);
                    assertEquals(2, countRows(ps), sql);
                }
            }
            if ("none".equals(mode)) {
                for (String sql : new String[] {"SET ROWCOUNT 0; SELECT TOP ? v FROM (VALUES (1), (2), (3)) AS t(v)",
                        "SET ROWCOUNT 0; SELECT TOP /* limit */ ? v FROM (VALUES (1), (2), (3)) AS t(v)",
                        "SET ROWCOUNT 0; SELECT TOP" + spaces + "? v FROM (VALUES (1), (2), (3)) AS t(v)",
                        // A '--' in quoted text, or a nested comment, before the marker must not hide TOP.
                        "SET ROWCOUNT 0; DECLARE @s varchar(9) = '--'; SELECT TOP ? v FROM (VALUES (1), (2)) AS t(v)",
                        "SET ROWCOUNT 0; SELECT TOP /* a /* b */ c */ ? v FROM (VALUES (1), (2), (3)) AS t(v)"}) {
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setLong(1, 2L);
                        assertEquals(2, countRows(ps), sql);
                    }
                }
            }
        }
    }

    // FETCH ABSOLUTE/RELATIVE and RAISERROR arguments accept a constant or a variable, not a CAST expression, so a Long
    // stays a plain literal there. Only the arguments of RAISERROR are affected, not the rest of the batch.
    @Test
    public void testLongInFetchAndRaiserror() throws SQLException {
        try (Connection conn = openConnection("none")) {
            for (String fetch : new String[] {"FETCH ABSOLUTE ? FROM c", "FETCH FIRST FROM c; FETCH RELATIVE ? FROM c",
                    "FETCH ABSOLUTE /* row */ ? FROM c"}) {
                String sql = "SET NOCOUNT ON; DECLARE c CURSOR LOCAL SCROLL FOR SELECT v FROM (VALUES (10), (20), (30))"
                        + " AS t(v); OPEN c; " + fetch + "; CLOSE c; DEALLOCATE c";
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setLong(1, fetch.contains("RELATIVE") ? 1L : 2L);
                    assertEquals("20", lastResultSetFirstValue(ps), sql);
                }
            }
            for (String sql : new String[] {"RAISERROR('m', 10, ?)",
                    "SET NOCOUNT ON; DECLARE @v bigint = ?; RAISERROR('m', 10, 1)"}) {
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setLong(1, 1L);
                    ps.execute();
                }
            }
            // A variable or temp table named RAISERROR is not the statement, so Longs keep their bigint CAST.
            for (String sql : new String[] {"SELECT ? + ? AS total INTO #RAISERROR; SELECT total FROM #RAISERROR",
                    "DECLARE @RAISERROR bigint = ? + ?; SELECT @RAISERROR"}) {
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setLong(1, Integer.MAX_VALUE);
                    ps.setLong(2, 1L);
                    assertEquals(String.valueOf(Integer.MAX_VALUE + 1L), lastResultSetFirstValue(ps), sql);
                }
            }
        }
    }

    // A batch that has RAISERROR keeps the normal formatting for every parameter outside the RAISERROR arguments: a Long
    // is a bigint (so Long.MAX_VALUE + 1 overflows as in typed binding), and a JDBC target type given to setObject is
    // honored (DECIMAL keeps the fraction of a division, VARCHAR gives the length of the text).
    @Test
    public void testParametersOutsideRaiserrorKeepNormalFormatting() {
        ValueRun targets = conn -> {
            try (PreparedStatement ps = conn
                    .prepareStatement("RAISERROR(N'n', 0, ?) WITH NOWAIT; SELECT ? / 4, DATALENGTH(?)")) {
                ps.setLong(1, 7L);
                ps.setObject(2, 7L, Types.DECIMAL, 2);
                ps.setObject(3, 12345L, Types.VARCHAR);
                return lastResultSetColumns(ps, 2);
            }
        };
        ValueRun overflow = conn -> {
            try (PreparedStatement ps = conn
                    .prepareStatement("RAISERROR(N'n', 0, ?) WITH NOWAIT; SELECT ? + 1")) {
                ps.setLong(1, 1L);
                ps.setLong(2, Long.MAX_VALUE);
                return lastResultSetColumns(ps, 1);
            }
        };
        assertValueMatchesTypedBinding(targets, null);
        assertValueMatchesTypedBinding(overflow, "ERROR");
    }

    private static String lastResultSetColumns(PreparedStatement ps, int columns) throws SQLException {
        String value = null;
        boolean hasResultSet = ps.execute();
        while (hasResultSet || -1 != ps.getUpdateCount()) {
            if (hasResultSet) {
                try (ResultSet rs = ps.getResultSet()) {
                    assertTrue(rs.next());
                    StringBuilder row = new StringBuilder();
                    for (int c = 1; c <= columns; c++) {
                        row.append(rs.getString(c)).append('|');
                    }
                    value = row.toString();
                }
            }
            hasResultSet = ps.getMoreResults();
        }
        return value;
    }

    // A combining mark after EXEC continues the identifier, so EXEC followed by U+0301 is a column name and not the
    // EXEC keyword. The statement stays a direct batch and its temp table stays on the connection.
    @ParameterizedTest(name = "mode={0}")
    @ValueSource(strings = {"none", "scopeTempTablesToConnection"})
    public void testCombiningMarkAfterExecIsNotAProcedureCall(String mode) throws SQLException {
        try (Connection conn = openConnection(mode)) {
            try (PreparedStatement ps = conn.prepareStatement("SELECT ? AS EXEC\u0301 INTO #comb")) {
                ps.setInt(1, 7);
                ps.execute();
            }
            try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("SELECT * FROM #comb")) {
                assertTrue(rs.next(), "the temp table must stay on the connection");
                assertEquals(7, rs.getInt(1));
            }
        }
    }

    // A LocalDateTime bound to a TIME target is sent as its time of day, as typed binding sends it.
    @ParameterizedTest(name = "sendTimeAsDatetime={0}")
    @ValueSource(booleans = {true, false})
    public void testLocalDateTimeWithTimeTarget(boolean sendTimeAsDatetime) throws SQLException {
        LocalDateTime value = LocalDateTime.of(2024, 5, 6, 7, 8, 9, 123000000);
        String property = "sendTimeAsDatetime=" + sendTimeAsDatetime;
        String expected;
        try (Connection conn = openConnection("prepexec", property);
                PreparedStatement ps = conn.prepareStatement("SELECT CONVERT(varchar(40), ?, 121)")) {
            ps.setObject(1, value, Types.TIME);
            expected = lastResultSetFirstValue(ps);
        }
        assertFalse(expected.startsWith("2024"), expected);
        try (Connection conn = openConnection("none", property);
                PreparedStatement ps = conn.prepareStatement("SELECT CONVERT(varchar(40), ?, 121)")) {
            ps.setObject(1, value, Types.TIME);
            assertEquals(expected, lastResultSetFirstValue(ps));
        }
    }

    // An integral value bound to a character target is sent as varchar or nvarchar according to the target type,
    // whatever sendStringParametersAsUnicode is, as typed binding sends it.
    @ParameterizedTest(name = "sendStringParametersAsUnicode={0}")
    @ValueSource(booleans = {true, false})
    public void testIntegralValueWithCharacterTarget(boolean unicode) throws SQLException {
        try (Connection conn = openConnection("none", "sendStringParametersAsUnicode=" + unicode);
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT CAST(SQL_VARIANT_PROPERTY(?, 'BaseType') AS varchar(20)) + ':' + CAST(DATALENGTH(?) AS"
                                + " varchar(10))")) {
            ps.setObject(1, 123, Types.VARCHAR);
            ps.setObject(2, 123L, Types.VARCHAR);
            assertEquals("varchar:3", lastResultSetFirstValue(ps));
            ps.setObject(1, 123, Types.NVARCHAR);
            ps.setObject(2, 123L, Types.NVARCHAR);
            assertEquals("nvarchar:6", lastResultSetFirstValue(ps));
        }
    }

    private static String lastResultSetFirstValue(PreparedStatement ps) throws SQLException {
        String value = null;
        boolean hasResultSet = ps.execute();
        while (hasResultSet || -1 != ps.getUpdateCount()) {
            if (hasResultSet) {
                try (ResultSet rs = ps.getResultSet()) {
                    assertTrue(rs.next());
                    value = rs.getString(1);
                }
            }
            hasResultSet = ps.getMoreResults();
        }
        return value;
    }

    // A nested comment between CREATE and PROC must not hide the module definition: the body still gets a plain
    // literal, because a CAST expression is not valid as an EXEC argument.
    @Test
    public void testModuleDefinitionWithNestedComment() throws SQLException {
        try (Connection conn = openConnection("none"); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE PROCEDURE #echo_nested @v bigint AS SELECT @v");
            try (PreparedStatement ps = conn
                    .prepareStatement("CREATE /* a /* b */ c */ PROCEDURE #probe_nested AS EXEC #echo_nested ?")) {
                ps.setLong(1, 7L);
                ps.execute();
            }
            try (ResultSet rs = stmt.executeQuery("EXEC #probe_nested")) {
                assertTrue(rs.next());
                assertEquals(7L, rs.getLong(1));
            }
        }
    }

    // A subnormal double such as 4.9E-324 is written as a literal that SQL Server rounds to 0, so the direct path
    // catches the rejection and sends the statement as an unprepared RPC, which sends the exact bits and keeps the
    // value, as the typed modes do.
    @ParameterizedTest(name = "mode={0}")
    @ValueSource(strings = {"none", "prepexec"})
    public void testSubnormalDouble(String mode) throws SQLException {
        double[] values = {Double.MIN_VALUE, -Double.MIN_VALUE, Math.nextDown(Double.MIN_NORMAL)};
        try (Connection conn = openConnection(mode)) {
            for (double value : values) {
                try (PreparedStatement ps = conn.prepareStatement("SELECT CAST(? AS float)")) {
                    ps.setDouble(1, value);
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(value, rs.getDouble(1));
                    }
                }
            }
        }
    }

    private static int countRows(PreparedStatement ps) throws SQLException {
        boolean hasResultSet = ps.execute();
        while (!hasResultSet) {
            assertTrue(-1 != ps.getUpdateCount());
            hasResultSet = ps.getMoreResults();
        }
        int count = 0;
        try (ResultSet rs = ps.getResultSet()) {
            while (rs.next()) {
                count++;
            }
        }
        return count;
    }

    // The body of a procedure definition runs later, not in this batch, so the definition stays a literal batch and
    // the bound value is embedded in the body.
    @ParameterizedTest(name = "mode={0}")
    @ValueSource(strings = {"none"})
    public void testProcedureDefinitionEmbedsTheBoundValue(String mode) throws SQLException {
        try (Connection conn = openConnection(mode); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE PROCEDURE #inner_def @v bigint AS SELECT @v");
            try (PreparedStatement ps = conn.prepareStatement("CREATE PROCEDURE #wrapper_def AS EXEC #inner_def ?")) {
                ps.setInt(1, 5);
                ps.execute();
            }
            try (ResultSet rs = stmt.executeQuery("EXEC #wrapper_def")) {
                assertTrue(rs.next());
                assertEquals(5, rs.getInt(1));
                assertFalse(rs.next());
            }
            stmt.execute("DROP PROCEDURE #wrapper_def");
            // A Long must stay a plain value in the body, because EXEC does not accept a CAST expression.
            try (PreparedStatement ps = conn.prepareStatement("CREATE PROCEDURE #wrapper_def AS EXEC #inner_def ?")) {
                ps.setLong(1, 5L);
                ps.execute();
            }
            try (ResultSet rs = stmt.executeQuery("EXEC #wrapper_def")) {
                assertTrue(rs.next());
                assertEquals(5L, rs.getLong(1));
                assertFalse(rs.next());
            }
        }
    }

    // Several Long markers after a large literal are all processed, and the large literal is not rescanned for them.
    @Test
    public void testManyLongMarkersAfterLargeLiteral() throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT LEN(?)");
        int markers = 200;
        for (int i = 0; i < markers; i++) {
            sql.append(", ? + 1");
        }
        String large = new String(new char[200000]).replace('\0', 'x');
        try (Connection conn = openConnection("none"); PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            ps.setString(1, large);
            for (int i = 0; i < markers; i++) {
                ps.setLong(i + 2, Integer.MAX_VALUE);
            }
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(large.length(), rs.getInt(1));
                assertEquals(Integer.MAX_VALUE + 1L, rs.getLong(markers + 1));
            }
        }
    }

    // A leading label is not a call, so the statement stays a literal batch and keeps its temp table, also when
    // comments sit between the label name and the colon.
    @ParameterizedTest(name = "mode={0}, {1}")
    @MethodSource("labelStatements")
    public void testLabelKeepsTempTable(String mode, String label) throws SQLException {
        try (Connection conn = openConnection(mode)) {
            try (PreparedStatement ps = conn.prepareStatement(label + " SELECT ? AS v INTO #kept_after_label")) {
                ps.setInt(1, 1);
                ps.execute();
            }
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT OBJECT_ID('tempdb..#kept_after_label')")) {
                assertTrue(rs.next());
                assertFalse(null == rs.getObject(1), "a label must not turn the statement into an RPC");
            }
        }
    }

    private static Stream<Arguments> labelStatements() {
        return Stream.of("none", "scopeTempTablesToConnection")
                .flatMap(mode -> Stream.of("retry:", "/* c */ retry:", "-- c\n retry:").map(label -> Arguments.of(mode, label)));
    }

    // A LocalTime is sent as typed binding sends it, for both targets and with sendTimeAsDatetime on and off. With a
    // TIME target and sendTimeAsDatetime it is a datetime, which rounds to a millisecond and carries past midnight. With a
    // TIMESTAMP target it is a datetime2 on the base date. The result is compared after a cast to several types, so the
    // date part, the rounding and the end of the day are all checked.
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"13:14:15.123456789", "23:59:59.999999999", "13:14:15.001000000", "00:00:00.000000000",
            "13:14:15.001500000", "13:14:15.001700000", "13:14:15.004900000", "13:14:15.998500000",
            "23:59:59.999499999", "23:59:59.999500000", "23:59:59.999000000", "13:14:15.1234567"})
    public void testLocalTimeMatchesTypedBinding(String text) throws SQLException {
        java.time.LocalTime time = java.time.LocalTime.parse(text);
        for (boolean sendTimeAsDatetime : new boolean[] {true, false}) {
            for (int target : new int[] {Types.TIME, Types.TIMESTAMP}) {
                for (String type : new String[] {"time(7)", "time(3)", "datetime", "datetime2(7)"}) {
                    String typed = convertedTime("prepexec", type, time, target, sendTimeAsDatetime);
                    assertEquals(typed, convertedTime("none", type, time, target, sendTimeAsDatetime),
                            text + " target=" + target + " sendTimeAsDatetime=" + sendTimeAsDatetime + " as " + type);
                }
            }
        }
    }

    private static String convertedTime(String mode, String type, java.time.LocalTime time, int targetType,
            boolean sendTimeAsDatetime) throws SQLException {
        String url = TestUtils.addOrOverrideProperty(TestUtils.removeProperty(connectionString, "prepareMethod"),
                "prepareMethod", mode);
        url = TestUtils.addOrOverrideProperty(url, "sendTimeAsDatetime", String.valueOf(sendTimeAsDatetime));
        try (Connection conn = PrepUtil.getConnection(url);
                PreparedStatement ps = conn
                        .prepareStatement("SELECT CONVERT(varchar(40), CAST(? AS " + type + "), 121)")) {
            ps.setObject(1, time, targetType);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getString(1);
            }
        }
    }

    // Value scenarios of the conformance matrix, each run under prepexec (typed binding), none and
    // scopeTempTablesToConnection, with sendStringParametersAsUnicode on and off. The result of none and scope must
    // equal the prepexec result, and the prepexec result must be the expected value, so a result that is the same
    // wrong value everywhere is also caught.

    @FunctionalInterface
    private interface ValueRun {
        String run(Connection conn) throws SQLException;
    }

    private static String valueOutcome(String mode, boolean unicode, ValueRun run) {
        try (Connection conn = openConnection(mode, "sendStringParametersAsUnicode=" + unicode)) {
            return run.run(conn);
        } catch (SQLException e) {
            return "ERROR";
        }
    }

    private static void assertValueMatchesTypedBinding(ValueRun run, String expected) {
        for (boolean unicode : new boolean[] {true, false}) {
            String typed = valueOutcome("prepexec", unicode, run);
            if (null != expected) {
                assertEquals(expected, typed, "prepexec, unicode=" + unicode);
            }
            for (String mode : new String[] {"none", "scopeTempTablesToConnection"}) {
                assertEquals(typed, valueOutcome(mode, unicode, run), mode + ", unicode=" + unicode);
            }
        }
    }

    // IN (?, ?, ?) with matching and non-matching values for int, varchar and nvarchar columns: every marker is
    // replaced and bound to the right position, so two of the four rows match and none match the other list.
    @ParameterizedTest(name = "column {0}")
    @ValueSource(strings = {"i", "v", "n"})
    public void testInListMatchesTypedBinding(String column) {
        String[][] matching = {{"1", "3", "99"}, {"a", "c", "zz"}, {"x", "z", "zz"}};
        String[][] nonMatching = {{"98", "99", "100"}, {"q", "r", "s"}, {"q", "r", "s"}};
        int index = "i".equals(column) ? 0 : "v".equals(column) ? 1 : 2;
        ValueRun run = conn -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE #in_t (i int, v varchar(10), n nvarchar(10))");
                stmt.execute("INSERT #in_t VALUES (1,'a',N'x'),(2,'b',N'y'),(3,'c',N'z'),(4,'d',N'w')");
            }
            StringBuilder result = new StringBuilder();
            String[][] lists = {matching[index], nonMatching[index]};
            String[] labels = {"matching", "nonMatching"};
            for (int l = 0; l < lists.length; l++) {
                try (PreparedStatement ps = conn
                        .prepareStatement("SELECT COUNT(*) FROM #in_t WHERE " + column + " IN (?, ?, ?)")) {
                    for (int p = 0; p < 3; p++) {
                        if (0 == index) {
                            ps.setInt(p + 1, Integer.parseInt(lists[l][p]));
                        } else {
                            ps.setString(p + 1, lists[l][p]);
                        }
                    }
                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        result.append(labels[l]).append('=').append(rs.getInt(1)).append(' ');
                    }
                }
            }
            return result.toString();
        };
        assertValueMatchesTypedBinding(run, "matching=2 nonMatching=0 ");
    }

    // NaN and infinity cannot be stored in a float column through a typed parameter (the server rejects the RPC), and
    // a direct statement cannot write them as a number either. Every mode rejects them, and nothing is stored.
    @ParameterizedTest(name = "value {0}")
    @ValueSource(doubles = {Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NaN})
    public void testInfinityAndNaNAreRejectedInEveryMode(double value) {
        ValueRun run = conn -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE #f (col float)");
            }
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO #f (col) VALUES (?)")) {
                ps.setDouble(1, value);
                ps.executeUpdate();
            } catch (SQLException e) {
                return "rejected";
            }
            return "stored";
        };
        assertValueMatchesTypedBinding(run, "rejected");
    }

    // setObject(java.util.Date) into datetime2(3): a plain java.util.Date, not a java.sql type, keeps its date, time
    // and milliseconds.
    @Test
    public void testUtilDateSetObjectRoundTripsToDatetime2() {
        java.util.Date value = new java.util.Date(Timestamp.valueOf("2026-10-15 12:23:45.123").getTime());
        ValueRun run = conn -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE #d (col datetime2(3))");
            }
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO #d (col) VALUES (?)")) {
                ps.setObject(1, value);
                ps.executeUpdate();
            }
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT CONVERT(varchar(40), col, 121) FROM #d")) {
                rs.next();
                return rs.getString(1);
            }
        };
        assertValueMatchesTypedBinding(run, "2026-10-15 12:23:45.123");
    }

    // A UUID object and the text of a GUID give the same uniqueidentifier.
    @ParameterizedTest(name = "asObject={0}")
    @ValueSource(booleans = {true, false})
    public void testUuidObjectAndGuidStringRoundTrip(boolean asObject) {
        UUID value = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
        ValueRun run = conn -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE #g (id uniqueidentifier)");
            }
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO #g (id) VALUES (?)")) {
                if (asObject) {
                    ps.setObject(1, value);
                } else {
                    ps.setString(1, value.toString());
                }
                ps.executeUpdate();
            }
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT CONVERT(varchar(40), id) FROM #g")) {
                rs.next();
                return rs.getString(1);
            }
        };
        assertValueMatchesTypedBinding(run, "123E4567-E89B-12D3-A456-426614174000");
    }

    // A batch INSERT of a string, a date and an integer stores every row, in insertion order.
    @ParameterizedTest(name = "largeBatch={0}")
    @ValueSource(booleans = {false, true})
    public void testBatchInsertOfStringDateAndIntKeepsInsertionOrder(boolean largeBatch) {
        ValueRun run = conn -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE #b (seq int IDENTITY(1,1), s varchar(20), d date, i int)");
            }
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO #b (s, d, i) VALUES (?, ?, ?)")) {
                String[] strings = {"alpha", "beta", "gamma"};
                String[] dates = {"2020-01-02", "2021-03-04", "2022-05-06"};
                int[] ints = {11, 22, 33};
                for (int row = 0; row < strings.length; row++) {
                    ps.setString(1, strings[row]);
                    ps.setDate(2, java.sql.Date.valueOf(dates[row]));
                    ps.setInt(3, ints[row]);
                    ps.addBatch();
                }
                if (largeBatch) {
                    ps.executeLargeBatch();
                } else {
                    ps.executeBatch();
                }
            }
            StringBuilder rows = new StringBuilder();
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT s, CONVERT(varchar(10), d, 120), i FROM #b ORDER BY seq")) {
                while (rs.next()) {
                    rows.append(rs.getString(1)).append('|').append(rs.getString(2)).append('|').append(rs.getInt(3))
                            .append(';');
                }
            }
            return rows.toString();
        };
        assertValueMatchesTypedBinding(run, "alpha|2020-01-02|11;beta|2021-03-04|22;gamma|2022-05-06|33;");
    }

    // Design scenario P1: OUT before IN, multiple OUT parameters, a return value followed by multiple IN parameters, and
    // mixed IN/INOUT/OUT. Every output and return value is read, so a wrong marker-to-parameter association shows up.
    @Test
    public void testOutParameterLayoutsMatchTypedBinding() {
        ValueRun run = conn -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE PROCEDURE #out_first @o int OUTPUT, @i int AS SET @o = @i * 2");
                stmt.execute("CREATE PROCEDURE #multi_out @i int, @o1 int OUTPUT, @o2 int OUTPUT AS "
                        + "BEGIN SET @o1 = @i + 1; SET @o2 = @i + 2 END");
                stmt.execute("CREATE PROCEDURE #add_values @a int, @b int AS RETURN @a + @b");
                stmt.execute("CREATE PROCEDURE #mixed @i int, @io int OUTPUT, @o int OUTPUT AS "
                        + "BEGIN SET @o = @i + @io; SET @io = @io * 2 END");
            }
            StringBuilder result = new StringBuilder();
            try (CallableStatement cs = conn.prepareCall("{call #out_first(?,?)}")) {
                cs.registerOutParameter(1, Types.INTEGER);
                cs.setInt(2, 21);
                cs.execute();
                result.append(cs.getInt(1)).append('|');
            }
            try (CallableStatement cs = conn.prepareCall("{call #multi_out(?,?,?)}")) {
                cs.setInt(1, 7);
                cs.registerOutParameter(2, Types.INTEGER);
                cs.registerOutParameter(3, Types.INTEGER);
                cs.execute();
                result.append(cs.getInt(2)).append(',').append(cs.getInt(3)).append('|');
            }
            try (CallableStatement cs = conn.prepareCall("{? = call #add_values(?,?)}")) {
                cs.registerOutParameter(1, Types.INTEGER);
                cs.setInt(2, 20);
                cs.setInt(3, 30);
                cs.execute();
                result.append(cs.getInt(1)).append('|');
            }
            try (CallableStatement cs = conn.prepareCall("{call #mixed(?,?,?)}")) {
                cs.setInt(1, 10);
                cs.setInt(2, 5);
                cs.registerOutParameter(2, Types.INTEGER);
                cs.registerOutParameter(3, Types.INTEGER);
                cs.execute();
                result.append(cs.getInt(2)).append(',').append(cs.getInt(3));
            }
            return result.toString();
        };
        for (String mode : new String[] {"none", "scopeTempTablesToConnection", "prepexec"}) {
            assertEquals("42|8,9|50|10,15", valueOutcome(mode, true, run), mode);
        }
    }

    // Design scenario P2: several temporal arguments of a procedure call, mixed with a string and an integer.
    @Test
    public void testMultipleTemporalProcedureArgumentsMatchTypedBinding() {
        ValueRun run = conn -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE PROCEDURE #two_dates @a date, @b date, @s varchar(10), @n int AS "
                        + "SELECT CONVERT(varchar(10), @a, 120) + '|' + CONVERT(varchar(10), @b, 120) + '|' + @s + '|' "
                        + "+ CAST(@n AS varchar(10))");
                stmt.execute("CREATE PROCEDURE #three_datetimes @a datetime, @b datetime, @c datetime, @s varchar(8) "
                        + "AS SELECT CONVERT(varchar(23), @a, 121) + '|' + CONVERT(varchar(23), @b, 121) + '|' "
                        + "+ CONVERT(varchar(23), @c, 121) + '|' + @s");
            }
            StringBuilder result = new StringBuilder();
            try (CallableStatement cs = conn.prepareCall("{call #two_dates(?,?,?,?)}")) {
                cs.setDate(1, java.sql.Date.valueOf("2026-07-29"));
                cs.setDate(2, java.sql.Date.valueOf("2026-07-19"));
                cs.setString(3, "ACCT-ALL");
                cs.setInt(4, 0);
                try (ResultSet rs = cs.executeQuery()) {
                    rs.next();
                    result.append(rs.getString(1)).append(';');
                }
            }
            try (CallableStatement cs = conn.prepareCall("{call #three_datetimes(?,?,?,?)}")) {
                cs.setTimestamp(1, Timestamp.valueOf("2026-07-29 00:00:00.0"));
                cs.setTimestamp(2, Timestamp.valueOf("2026-07-29 00:00:00.0"));
                cs.setTimestamp(3, Timestamp.valueOf("2025-07-29 00:00:00.0"));
                cs.setString(4, "NAV");
                try (ResultSet rs = cs.executeQuery()) {
                    rs.next();
                    result.append(rs.getString(1));
                }
            }
            return result.toString();
        };
        assertValueMatchesTypedBinding(run, "2026-07-29|2026-07-19|ACCT-ALL|0;"
                + "2026-07-29 00:00:00.000|2026-07-29 00:00:00.000|2025-07-29 00:00:00.000|NAV");
    }

    // Design scenario L7: a marker directly followed by a keyword (user_name = ?and portfolio_code = 'X'). The statement
    // must run and return the intended row, also when strings are not sent as Unicode.
    @Test
    public void testMarkerAdjacentToKeywordReturnsTheIntendedRows() {
        ValueRun run = conn -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE #permission_test (perm_type varchar(20), user_name varchar(30), "
                        + "portfolio_code varchar(10))");
                stmt.execute("INSERT #permission_test VALUES ('READ','svc_user','X'), ('READ','svc_user','Y'), "
                        + "('WRITE','svc_user','X'), ('READ','other','X')");
            }
            StringBuilder rows = new StringBuilder();
            try (PreparedStatement ps = conn.prepareStatement("SELECT portfolio_code FROM #permission_test "
                    + "WHERE perm_type = ? AND user_name = ?and portfolio_code = 'X'")) {
                ps.setString(1, "READ");
                ps.setString(2, "svc_user");
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        rows.append(rs.getString(1)).append(',');
                    }
                }
            }
            return rows.toString();
        };
        assertValueMatchesTypedBinding(run, "X,");
    }

    // Design scenarios L4 and L6: the explicit JDBC target type wins over the Java type. An Integer bound as VARCHAR
    // is character data, and a String bound as INTEGER is a number; the stored values are read back and used in
    // character and numeric expressions, so an implicit conversion cannot hide a wrong literal type.
    @Test
    public void testExplicitJdbcTargetTypesKeepCharacterAndNumericSemantics() {
        ValueRun run = conn -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE #target_type_test (text_value varchar(20), int_value int)");
            }
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO #target_type_test (text_value) VALUES (?)")) {
                ps.setObject(1, Integer.valueOf(7777777), Types.VARCHAR);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO #target_type_test (int_value) VALUES (?)")) {
                ps.setObject(1, "7777777", Types.INTEGER);
                ps.executeUpdate();
            }
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT MAX(text_value + 'x'), MAX(int_value + 1) "
                            + "FROM #target_type_test")) {
                rs.next();
                return rs.getString(1) + "|" + rs.getString(2);
            }
        };
        assertValueMatchesTypedBinding(run, "7777777x|7777778");
    }

    // Design scenario T1: setObject(java.sql.Date) into a date column and a String bound to a datetimeoffset column.
    @Test
    public void testSetObjectSqlDateAndDatetimeoffsetStringRoundTrip() {
        ValueRun run = conn -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE #temporal_test (d date, dto datetimeoffset)");
            }
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO #temporal_test (d) VALUES (?)")) {
                ps.setObject(1, java.sql.Date.valueOf("2026-10-15"));
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO #temporal_test (dto) VALUES (?)")) {
                ps.setString(1, "2026-10-15 12:23:45.1234567 +05:30");
                ps.executeUpdate();
            }
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT MAX(CONVERT(varchar(10), d, 120)), "
                            + "MAX(CAST(dto AS varchar(40))) FROM #temporal_test")) {
                rs.next();
                return rs.getString(1) + "|" + rs.getString(2);
            }
        };
        assertValueMatchesTypedBinding(run, "2026-10-15|2026-10-15 12:23:45.1234567 +05:30");
    }

    // Design scenario L1: inferred setObject(Integer) and setObject(BigDecimal) stored in typed columns and read back.
    @Test
    public void testInferredSetObjectIntegerAndBigDecimalRoundTrip() {
        ValueRun run = conn -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE #inferred_test (i int, d decimal(10,4))");
            }
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO #inferred_test (i, d) VALUES (?, ?)")) {
                ps.setObject(1, Integer.valueOf(42));
                ps.setObject(2, new java.math.BigDecimal("123456.7890"));
                ps.executeUpdate();
            }
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT i, d FROM #inferred_test")) {
                rs.next();
                return rs.getInt(1) + "|" + rs.getBigDecimal(2).toPlainString();
            }
        };
        assertValueMatchesTypedBinding(run, "42|123456.7890");
    }

    // Design scenario R13: generated key of a single INSERT into a session-local identity table with a String value.
    @Test
    public void testGeneratedKeyOfSessionLocalIdentityTableMatchesTypedBinding() {
        ValueRun run = conn -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE #generated_key_test (id INT IDENTITY(1,1) PRIMARY KEY, value VARCHAR(32))");
            }
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO #generated_key_test(value) VALUES (?)",
                    Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, "GK");
                int count = ps.executeUpdate();
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    return "count=" + count + " key=" + (keys.next() ? keys.getInt(1) : -1);
                }
            }
        };
        assertValueMatchesTypedBinding(run, "count=1 key=1");
    }

    // Design scenario L12: IN (?, ?, ?) does not prove marker order, so the order of three markers is checked with a
    // shape that is sensitive to it.
    @Test
    public void testMarkerOrderIsPreserved() {
        ValueRun run = conn -> {
            try (PreparedStatement ps = conn.prepareStatement("SELECT CAST(? AS varchar(5)) + '|' + "
                    + "CAST(? AS varchar(5)) + '|' + CAST(? AS varchar(5))")) {
                ps.setString(1, "a");
                ps.setString(2, "b");
                ps.setString(3, "c");
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    return rs.getString(1);
                }
            }
        };
        assertValueMatchesTypedBinding(run, "a|b|c");
    }

    // Statement reuse and metadata parity across direct and RPC execution.

    private static SQLServerConnection openMetadataConnection(String mode) throws SQLException {
        return (SQLServerConnection) openConnection(mode, "disableStatementPooling=true", "statementPoolingCacheSize=0",
                "columnEncryptionSetting=Disabled", "sendStringParametersAsUnicode=true", "useFmtOnly=false");
    }

    @ParameterizedTest(name = "mixed bindings, prepareOnFirst={0}")
    @ValueSource(booleans = {true, false})
    public void testRepeatedBindingsAndPreparedHandles(boolean prepareOnFirst) throws SQLException {
        String sql = "SELECT CAST(? AS nvarchar(64)), CAST(? AS bigint), CAST(? AS decimal(19,4)), "
                + "CAST(? AS int), CAST(? AS date), CAST(? AS datetime2(7))";
        String[] texts = {"ordinary", "O'Brien", "caf\u00e9 \u540d\u524d"};
        long[] integers = {4294967296L, -4294967296L, 9223372036854775806L};
        String[] decimals = {"12345.6700", "-23456.7800", "34567.8900"};
        try (SQLServerConnection actual = openMetadataConnection("none");
                SQLServerConnection reference = openMetadataConnection("prepexec")) {
            actual.setEnablePrepareOnFirstPreparedStatementCall(prepareOnFirst);
            reference.setEnablePrepareOnFirstPreparedStatementCall(prepareOnFirst);
            assertFalse(actual.isStatementPoolingEnabled());
            assertFalse(reference.isStatementPoolingEnabled());
            try (SQLServerPreparedStatement direct = (SQLServerPreparedStatement) actual.prepareStatement(sql);
                    SQLServerPreparedStatement rpc = (SQLServerPreparedStatement) reference.prepareStatement(sql)) {
                int preparedHandle = 0;
                assertEquals(0, direct.getPreparedStatementHandle());
                assertEquals(0, rpc.getPreparedStatementHandle());
                for (int execution = 0; execution < 3; execution++) {
                    BigDecimal decimal = new BigDecimal(decimals[execution]);
                    Date date = Date.valueOf(LocalDate.of(2024, 2, 13).plusDays(execution));
                    Timestamp timestamp = Timestamp.valueOf("2024-02-13 12:23:45.1234567");
                    timestamp.setNanos(123456700 + execution * 100);
                    for (SQLServerPreparedStatement ps : new SQLServerPreparedStatement[] {direct, rpc}) {
                        ps.setString(1, texts[execution]);
                        ps.setLong(2, integers[execution]);
                        ps.setBigDecimal(3, decimal);
                        ps.setNull(4, Types.INTEGER);
                        ps.setDate(5, date);
                        ps.setTimestamp(6, timestamp);
                    }
                    try (ResultSet expected = rpc.executeQuery(); ResultSet result = direct.executeQuery()) {
                        assertTrue(expected.next());
                        assertTrue(result.next());
                        Object[] values = {texts[execution], integers[execution], decimal, null, date, timestamp};
                        for (int column = 1; column <= values.length; column++) {
                            assertEquals(values[column - 1], expected.getObject(column), "Reference column " + column);
                            assertEquals(expected.getObject(column), result.getObject(column),
                                    "Execution " + execution + ", column " + column);
                        }
                        assertFalse(expected.next());
                        assertFalse(result.next());
                    }
                    // The prepared handle is returned after the result set, not in the row metadata.
                    assertFalse(rpc.getMoreResults());
                    assertEquals(-1, rpc.getUpdateCount());
                    assertFalse(direct.getMoreResults());
                    assertEquals(-1, direct.getUpdateCount());
                    assertEquals(0, direct.getPreparedStatementHandle(), "none must never prepare");
                    int handle = rpc.getPreparedStatementHandle();
                    if (!prepareOnFirst && 0 == execution) {
                        assertEquals(0, handle, "The first prepexec execution must remain unprepared");
                    } else {
                        assertTrue(handle > 0, "prepexec must acquire a server handle");
                        if (0 == preparedHandle) {
                            preparedHandle = handle;
                        }
                        assertEquals(preparedHandle, handle, "Rebinding must reuse the prepared handle");
                    }
                }
            }
        }
    }

    @ParameterizedTest(name = "result metadata across stream fallback, prepareOnFirst={0}")
    @ValueSource(booleans = {true, false})
    public void testResultMetadataAcrossDirectRpcTransition(boolean prepareOnFirst) throws SQLException, IOException {
        String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("none_result_metadata"));
        byte[][] values = {{1, 2}, {3, 4}, {5, 6}};
        String sql = "SELECT payload AS binary_label, amount AS decimal_label, label AS text_label FROM " + table
                + " WHERE payload = ?";
        try (SQLServerConnection actual = openMetadataConnection("none");
                SQLServerConnection reference = openMetadataConnection("prepexec");
                Statement setup = actual.createStatement()) {
            actual.setEnablePrepareOnFirstPreparedStatementCall(prepareOnFirst);
            reference.setEnablePrepareOnFirstPreparedStatementCall(prepareOnFirst);
            setup.executeUpdate("CREATE TABLE " + table
                    + " (payload varbinary(32) NOT NULL, amount decimal(19,4) NULL, label nvarchar(40) NOT NULL)");
            try {
                setup.executeUpdate("INSERT INTO " + table
                        + " VALUES (0x0102,1.2500,N'first'),(0x0304,2.5000,N'second'),(0x0506,3.7500,N'third')");
                try (SQLServerPreparedStatement direct = (SQLServerPreparedStatement) actual.prepareStatement(sql);
                        SQLServerPreparedStatement rpc = (SQLServerPreparedStatement) reference.prepareStatement(sql);
                        Statement schema = reference.createStatement()) {
                    List<List<Object>> before;
                    try (ResultSet rs = schema.executeQuery("SELECT payload AS binary_label, amount AS decimal_label, "
                            + "label AS text_label FROM " + table + " WHERE 1 = 0")) {
                        before = resultMetadata(rs.getMetaData());
                        assertFalse(rs.next());
                    }
                    for (int execution = 0; execution < 3; execution++) {
                        // A non-null stream forces an unprepared RPC for none; the final bytes keep that RPC route.
                        try (ByteArrayInputStream directStream = new ByteArrayInputStream(values[execution]);
                                ByteArrayInputStream rpcStream = new ByteArrayInputStream(values[execution])) {
                            if (1 == execution) {
                                direct.setBinaryStream(1, directStream, values[execution].length);
                                rpc.setBinaryStream(1, rpcStream, values[execution].length);
                            } else {
                                direct.setBytes(1, values[execution]);
                                rpc.setBytes(1, values[execution]);
                            }
                            try (ResultSet expected = rpc.executeQuery(); ResultSet result = direct.executeQuery()) {
                                assertEquals(before, resultMetadata(expected.getMetaData()));
                                assertEquals(before, resultMetadata(result.getMetaData()), "Execution " + execution);
                                assertEquals(before, resultMetadata(direct.getMetaData()));
                                assertTrue(expected.next());
                                assertTrue(result.next());
                                assertArrayEquals(values[execution], expected.getBytes(1));
                                assertArrayEquals(expected.getBytes(1), result.getBytes(1));
                                assertEquals(expected.getBigDecimal(2), result.getBigDecimal(2));
                                assertEquals(expected.getString(3), result.getString(3));
                                assertFalse(expected.next());
                                assertFalse(result.next());
                            }
                        }
                        assertEquals(0, direct.getPreparedStatementHandle(), "RPC fallback must not prepare none");
                    }
                }
            } finally {
                TestUtils.dropTableIfExists(table, setup);
            }
        }
    }

    @ParameterizedTest(name = "parameter metadata, prepareOnFirst={0}")
    @ValueSource(booleans = {true, false})
    public void testParameterMetadataBeforeAndAfterExecution(boolean prepareOnFirst) throws SQLException {
        String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("none_parameter_metadata"));
        long[] values = {4294967296L, -4294967296L, 9223372036854775806L};
        try (SQLServerConnection actual = openMetadataConnection("none");
                SQLServerConnection reference = openMetadataConnection("prepexec");
                Statement setup = actual.createStatement()) {
            actual.setEnablePrepareOnFirstPreparedStatementCall(prepareOnFirst);
            reference.setEnablePrepareOnFirstPreparedStatementCall(prepareOnFirst);
            // Use a real local table: sp_describe_undeclared_parameters cannot reliably describe temp tables.
            setup.executeUpdate("CREATE TABLE " + table + " (id bigint NOT NULL)");
            try {
                setup.executeUpdate(
                        "INSERT INTO " + table + " VALUES (4294967296),(-4294967296),(9223372036854775806)");
                String sql = "SELECT id FROM " + table + " WHERE id = ?";
                try (SQLServerPreparedStatement direct = (SQLServerPreparedStatement) actual.prepareStatement(sql);
                        SQLServerPreparedStatement rpc = (SQLServerPreparedStatement) reference.prepareStatement(sql)) {
                    ParameterMetaData before = rpc.getParameterMetaData();
                    assertEquals(1, before.getParameterCount());
                    assertEquals(Types.BIGINT, before.getParameterType(1));
                    assertEquals("bigint", before.getParameterTypeName(1));
                    assertEquals(Long.class.getName(), before.getParameterClassName(1));
                    assertEquals(19, before.getPrecision(1));
                    assertEquals(0, before.getScale(1));
                    assertTrue(before.isSigned(1));
                    assertEquals(ParameterMetaData.parameterModeIn, before.getParameterMode(1));
                    for (int execution = -1; execution < 3; execution++) {
                        for (SQLServerPreparedStatement ps : new SQLServerPreparedStatement[] {direct, rpc}) {
                            if (execution >= 0) {
                                ps.setLong(1, values[execution]);
                                try (ResultSet rs = ps.executeQuery()) {
                                    assertTrue(rs.next());
                                    assertEquals(values[execution], rs.getLong(1));
                                    assertFalse(rs.next());
                                }
                            }
                            // Force refresh so this verifies server metadata, not just the connection's cached object.
                            ParameterMetaData metadata = ps.getParameterMetaData(true);
                            assertEquals(before.getParameterCount(), metadata.getParameterCount());
                            assertEquals(before.getParameterType(1), metadata.getParameterType(1));
                            assertEquals(before.getParameterTypeName(1), metadata.getParameterTypeName(1));
                            assertEquals(before.getParameterClassName(1), metadata.getParameterClassName(1));
                            assertEquals(before.getPrecision(1), metadata.getPrecision(1));
                            assertEquals(before.getScale(1), metadata.getScale(1));
                            assertEquals(before.isNullable(1), metadata.isNullable(1));
                            assertEquals(before.isSigned(1), metadata.isSigned(1));
                            assertEquals(before.getParameterMode(1), metadata.getParameterMode(1));
                        }
                        assertEquals(0, direct.getPreparedStatementHandle());
                    }
                }
            } finally {
                TestUtils.dropTableIfExists(table, setup);
            }
        }
    }

    private static List<List<Object>> resultMetadata(ResultSetMetaData metadata) throws SQLException {
        assertNotNull(metadata);
        List<List<Object>> columns = new ArrayList<>();
        for (int column = 1; column <= metadata.getColumnCount(); column++) {
            columns.add(Arrays.asList(metadata.getColumnType(column), metadata.getColumnTypeName(column),
                    metadata.getColumnClassName(column), metadata.getPrecision(column), metadata.getScale(column),
                    metadata.getColumnDisplaySize(column), metadata.isNullable(column),
                    metadata.getColumnLabel(column)));
        }
        return columns;
    }

    // Error chains, warnings and savepoints across direct and RPC execution.

    @ParameterizedTest(name = "error={0}, prepareOnFirst={1}")
    @MethodSource("serverErrors")
    public void testServerErrorDetailsAndStatementReuse(int errorCode, boolean prepareOnFirst) throws SQLException {
        List<String> expected = serverErrorOutcome("prepexec", prepareOnFirst, errorCode);
        assertEquals(expected, serverErrorOutcome("none", prepareOnFirst, errorCode));
    }

    private static Stream<Arguments> serverErrors() {
        return Stream.of(245, 8134, 2627)
                .flatMap(code -> Stream.of(false, true).map(prepareOnFirst -> Arguments.of(code, prepareOnFirst)));
    }

    private static List<String> serverErrorOutcome(String mode, boolean prepareOnFirst,
            int errorCode) throws SQLException {
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
                SQLException error = assertThrows(SQLException.class, () -> {
                    if (2627 == errorCode) {
                        ps.executeUpdate();
                    } else {
                        // Conversion and arithmetic errors may arrive only when the first row is consumed.
                        try (ResultSet rs = ps.executeQuery()) {
                            while (rs.next()) {
                                rs.getInt(1);
                            }
                        }
                    }
                });
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

    private static void bindErrorValue(PreparedStatement ps, int errorCode, int value,
            boolean fail) throws SQLException {
        if (245 == errorCode) {
            ps.setString(1, fail ? "not-an-integer" : Integer.toString(value));
        } else {
            ps.setInt(1, fail && 8134 == errorCode ? 0 : value);
        }
    }

    @ParameterizedTest(name = "largeBatch={0}, prepareOnFirst={1}")
    @MethodSource("batchConfigurations")
    public void testProcedureBatchErrorChainMatchesPreparedExecution(boolean largeBatch,
            boolean prepareOnFirst) throws SQLException {
        assertEquals(batchErrorOutcome("prepexec", prepareOnFirst, largeBatch),
                batchErrorOutcome("none", prepareOnFirst, largeBatch));
    }

    private static Stream<Arguments> batchConfigurations() {
        return Stream.of(false, true)
                .flatMap(large -> Stream.of(false, true).map(prepareOnFirst -> Arguments.of(large, prepareOnFirst)));
    }

    private static List<String> batchErrorOutcome(String mode, boolean prepareOnFirst,
            boolean largeBatch) throws SQLException {
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
    @Tag(Constants.xAzureSQLDW)

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
        return openConnection(mode, "disableStatementPooling=true", "statementPoolingCacheSize=0",
                "columnEncryptionSetting=Disabled", "enablePrepareOnFirstPreparedStatementCall=" + prepareOnFirst,
                "useBulkCopyForBatchInsert=false", "retryExec=");
    }

    // Stream-send failures and timeout recovery on the same statement and connection.

    private static final String IO_FAILURE = "prepare-none-stream-failure";
    private static final String TEXT = "a\u540d\u00e9Z!";
    private static final byte[] BYTES = {0, 1, (byte) 0xFF, 127, 42};

    @ParameterizedTest(name = "mode={0}, reader={1}, batch={2}")
    @MethodSource("executionCases")
    @Timeout(90)
    public void testIOExceptionRecovery(String mode, boolean reader, boolean batch) throws SQLException {
        try (Connection con = openStreamRecoveryConnection(mode); Statement stmt = con.createStatement()) {
            createTable(stmt, reader);
            try (PreparedStatement ps = con.prepareStatement("INSERT INTO #stream_recovery VALUES (?, ?)")) {
                seed(ps, reader);
                ps.setInt(1, 1);
                if (reader) {
                    ps.setCharacterStream(2, new FailingReader(), 4);
                } else {
                    ps.setBinaryStream(2, new FailingInputStream(), 4);
                }
                SQLException error = assertThrows(SQLException.class, () -> execute(ps, batch));
                assertEquals("22000", error.getSQLState());
                assertEquals(0, error.getErrorCode());
                assertEquals(MessageFormat.format(SQLServerException.getErrString("R_errorReadingStream"),
                        new IOException(IO_FAILURE).toString()), error.getMessage());
                recoverAndCheck(con, ps, reader, batch);
            }
        }
    }

    @ParameterizedTest(name = "mode={0}, reader={1}, batch={2}, actualLength={3}")
    @MethodSource("lengthCases")
    @Timeout(90)
    public void testDeclaredLengthMismatchRecovery(String mode, boolean reader, boolean batch,
            int actualLength) throws SQLException {
        try (Connection con = openStreamRecoveryConnection(mode); Statement stmt = con.createStatement()) {
            createTable(stmt, reader);
            try (PreparedStatement ps = con.prepareStatement("INSERT INTO #stream_recovery VALUES (?, ?)")) {
                seed(ps, reader);
                ps.setInt(1, 1);
                bindStream(ps, 2, reader, actualLength, 4);
                SQLException error = assertThrows(SQLException.class, () -> execute(ps, batch));
                assertEquals("22026", error.getSQLState());
                assertEquals(0, error.getErrorCode());
                assertEquals(MessageFormat.format(SQLServerException.getErrString("R_mismatchedStreamLength"), 4,
                        actualLength), error.getMessage());
                recoverAndCheck(con, ps, reader, batch);
            }
        }
    }

    @ParameterizedTest(name = "mode={0}, reader={1}")
    @MethodSource("streamCases")
    @Tag(Constants.xAzureSQLDW)
    @Timeout(90)
    public void testQueryTimeoutAfterRpcTransition(String mode, boolean reader) throws SQLException {
        try (Connection con = openStreamRecoveryConnection(mode);
                PreparedStatement ps = con.prepareStatement("WAITFOR DELAY ?; SELECT DATALENGTH(?)")) {
            ps.setString(1, "00:00:00");
            bindLiteral(ps, reader);
            assertLength(ps, reader);
            // Same statement: literal execution, successful RPC, timeout with a fresh stream, successful RPC.
            bindStream(ps, 2, reader, 5, 5);
            assertLength(ps, reader);
            ps.setString(1, "00:00:10");
            bindStream(ps, 2, reader, 5, 5);
            ps.setQueryTimeout(1);
            SQLTimeoutException error = assertThrows(SQLTimeoutException.class, () -> {
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                }
            });
            assertEquals(SQLServerException.getErrString("R_queryTimedOut"), error.getMessage());
            assertEquals("HY008", error.getSQLState());
            assertEquals(0, error.getErrorCode());
            assertFalse(con.isClosed());
            ps.setQueryTimeout(10);
            ps.setString(1, "00:00:00");
            bindStream(ps, 2, reader, 5, 5);
            assertLength(ps, reader);
            try (Statement stmt = con.createStatement(); ResultSet rs = stmt.executeQuery("SELECT 42")) {
                assertTrue(rs.next());
                assertEquals(42, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    private static Stream<Arguments> streamCases() {
        return Stream.of("prepexec", "none")
                .flatMap(mode -> Stream.of(false, true).map(reader -> Arguments.of(mode, reader)));
    }

    private static Stream<Arguments> executionCases() {
        return streamCases().flatMap(
                args -> Stream.of(false, true).map(batch -> Arguments.of(args.get()[0], args.get()[1], batch)));
    }

    private static Stream<Arguments> lengthCases() {
        return executionCases().flatMap(args -> Stream.of(3, 5)
                .map(length -> Arguments.of(args.get()[0], args.get()[1], args.get()[2], length)));
    }

    private static Connection openStreamRecoveryConnection(String mode) throws SQLException {
        return openConnection(mode, "columnEncryptionSetting=Disabled", "disableStatementPooling=true",
                "statementPoolingCacheSize=0", "enablePrepareOnFirstPreparedStatementCall=true",
                "useBulkCopyForBatchInsert=false", "sendStringParametersAsUnicode=true", "packetSize=4096",
                "queryTimeout=10", "cancelQueryTimeout=5", "socketTimeout=20000", "retryExec=");
    }

    private static void createTable(Statement stmt, boolean reader) throws SQLException {
        stmt.execute(
                "CREATE TABLE #stream_recovery (id int, value " + (reader ? "nvarchar(max)" : "varbinary(max)") + ")");
    }

    private static void bindLiteral(PreparedStatement ps, boolean reader) throws SQLException {
        if (reader) {
            ps.setString(2, TEXT);
        } else {
            ps.setBytes(2, BYTES);
        }
    }

    private static void seed(PreparedStatement ps, boolean reader) throws SQLException {
        ps.setInt(1, 0);
        bindLiteral(ps, reader);
        assertEquals(1, ps.executeUpdate());
    }

    private static void bindStream(PreparedStatement ps, int index, boolean reader, int actualLength,
            int declaredLength) throws SQLException {
        if (reader) {
            ps.setCharacterStream(index, new StringReader(TEXT.substring(0, actualLength)), declaredLength);
        } else {
            ps.setBinaryStream(index, new ByteArrayInputStream(BYTES, 0, actualLength), declaredLength);
        }
    }

    private static void execute(PreparedStatement ps, boolean batch) throws SQLException {
        if (batch) {
            ps.addBatch();
            assertArrayEquals(new int[] {1}, ps.executeBatch());
        } else {
            assertEquals(1, ps.executeUpdate());
        }
    }

    private static void recoverAndCheck(Connection con, PreparedStatement ps, boolean reader,
            boolean batch) throws SQLException {
        // These tiny, known-length values fail before any packet is sent. TDSWriter.error interrupts the
        // incomplete request; TDSCommand.onRequestComplete discards it without expecting an attention ack.
        // This is the recoverable prepexec contract, not a claim about arbitrary partially sent LOBs.
        assertFalse(con.isClosed());
        assertFalse(ps.isClosed());
        ps.clearBatch();
        assertRows(con, reader, 0);
        for (int id = 2; id <= 3; id++) {
            ps.setInt(1, id);
            bindStream(ps, 2, reader, 5, 5);
            execute(ps, batch);
        }
        assertRows(con, reader, 0, 2, 3);
    }

    private static void assertRows(Connection con, boolean reader, int... ids) throws SQLException {
        try (Statement stmt = con.createStatement(); ResultSet rs = stmt
                .executeQuery("SELECT id, CONVERT(varbinary(max), value) FROM #stream_recovery ORDER BY id")) {
            for (int id : ids) {
                assertTrue(rs.next());
                assertEquals(id, rs.getInt(1));
                assertArrayEquals(reader ? TEXT.getBytes(StandardCharsets.UTF_16LE) : BYTES, rs.getBytes(2));
            }
            assertFalse(rs.next(), "No failed, duplicate, or ghost rows may survive");
        }
    }

    private static void assertLength(PreparedStatement ps, boolean reader) throws SQLException {
        try (ResultSet rs = ps.executeQuery()) {
            assertTrue(rs.next());
            assertEquals(reader ? 10 : 5, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    private static final class FailingInputStream extends InputStream {
        private int position;

        @Override
        public int read() throws IOException {
            if (position == 3) {
                throw new IOException(IO_FAILURE);
            }
            return BYTES[position++] & 0xFF;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            if (length == 0) {
                return 0;
            }
            if (position == 3) {
                throw new IOException(IO_FAILURE);
            }
            int count = Math.min(length, 3 - position);
            System.arraycopy(BYTES, position, buffer, offset, count);
            position += count;
            return count;
        }
    }

    private static final class FailingReader extends Reader {
        private int position;

        @Override
        public int read(char[] buffer, int offset, int length) throws IOException {
            if (length == 0) {
                return 0;
            }
            if (position == 3) {
                throw new IOException(IO_FAILURE);
            }
            int count = Math.min(length, 3 - position);
            TEXT.getChars(position, position + count, buffer, offset);
            position += count;
            return count;
        }

        @Override
        public void close() {
            // No resources; stream ownership and reuse after failure are deliberately not asserted.
        }
    }

    // DROP ... IF EXISTS needs SQL Server 2016; OBJECT_ID and TYPE_ID work on every supported version, also for names
    // with a schema.
    private static void dropIfExists(Statement stmt, String kind, String name) throws SQLException {
        String exists = "TYPE".equals(kind) ? "TYPE_ID" : "OBJECT_ID";
        stmt.execute("IF " + exists + "(N'" + name.replace("'", "''") + "') IS NOT NULL DROP " + kind + " " + name);
    }

    private static Connection openConnection(String mode, String... properties) throws SQLException {
        String url = TestUtils.addOrOverrideProperty(TestUtils.removeProperty(connectionString, "prepareMethod"),
                "prepareMethod", mode);
        for (String property : properties) {
            String[] keyValue = property.split("=", 2);
            url = TestUtils.addOrOverrideProperty(url, keyValue[0], keyValue[1]);
        }
        return PrepUtil.getConnection(url);
    }
}
