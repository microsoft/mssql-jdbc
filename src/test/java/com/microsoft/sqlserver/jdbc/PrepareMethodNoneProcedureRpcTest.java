/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.BatchUpdateException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * A statement that calls a stored procedure is never executed as a direct SQL batch. Under prepareMethod=none it is
 * sent as an unprepared RPC with typed parameters, and parameters that cannot be inlined as literals in a direct
 * statement (OUT parameters, table-valued parameters, streams and SQLXML) are rejected instead of falling back to RPC.
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

                // A GRANT EXECUTE permission statement is reported as a call and is simply sent as an RPC.
                Arguments.of("GRANT EXECUTE ON p TO u", true),

                // Not procedure calls.
                Arguments.of("SELECT ?", false), Arguments.of("SELECT 'EXEC p', ?", false),
                Arguments.of("SELECT 'it''s EXEC p', ?", false), Arguments.of("SELECT [EXEC] FROM t WHERE c = ?", false),
                Arguments.of("SELECT \"EXEC\" FROM t WHERE c = ?", false),
                Arguments.of("SELECT exec_count FROM t WHERE c = ?", false),
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
    // execution, executeBatch and executeLargeBatch. The procedure inserts a row and then raises the error, so every
    // extra execution is visible as an extra row. Error 8179: sp_execute with an unknown handle. Error 586: a handle
    // prepared in one database and executed after USE of another.
    private static final String RETRY_ERROR_8179 = "EXEC sp_execute 999999";
    private static final String RETRY_ERROR_586 = "DECLARE @h int, @sql nvarchar(200); "
            + "EXEC sp_prepare @h OUTPUT, N'', N'SELECT 1 AS x'; "
            + "SET @sql = N'USE tempdb; EXEC sp_execute ' + CAST(@h AS nvarchar(20)); EXEC (@sql)";

    private static Stream<Arguments> retryCases() {
        List<Arguments> cases = new ArrayList<>();
        for (String sql : new String[] {"{call #retry_p(?)}", "EXEC #retry_p ?"}) {
            for (boolean pooling : new boolean[] {true, false}) {
                for (String kind : new String[] {"single", "batch", "largeBatch"}) {
                    for (int error : new int[] {8179, 586}) {
                        cases.add(Arguments.of(sql, pooling, kind, error));
                    }
                }
            }
        }
        return cases.stream();
    }

    @ParameterizedTest(name = "{0}, pooling={1}, {2}, error {3}")
    @MethodSource("retryCases")
    public void testErrorsFromInsideProcedureAreNotRetried(String sql, boolean pooling, String kind,
            int error) throws SQLException {
        String[] props = pooling ? new String[] {"disableStatementPooling=false", "statementPoolingCacheSize=10"}
                                 : new String[0];
        try (Connection conn = openConnection("none", props); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE #retry_log (v int)");
            stmt.execute("CREATE PROCEDURE #retry_p @v int AS BEGIN INSERT #retry_log VALUES (@v); "
                    + (586 == error ? RETRY_ERROR_586 : RETRY_ERROR_8179) + " END");
            int[] values = "single".equals(kind) ? new int[] {1} : new int[] {1, 2, 3};
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
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
                } catch (BatchUpdateException e) {
                    // Only the rows inserted are checked.
                }
            }
            StringBuilder expected = new StringBuilder();
            for (int v : values) {
                expected.append(v).append(',');
            }
            assertEquals(expected.toString(), tableRows(stmt, "#retry_log"),
                    "an entry that failed inside the procedure must not be run twice");
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
                stmt.execute("DROP PROCEDURE IF EXISTS " + procedureName);
                stmt.execute("DROP TYPE IF EXISTS " + typeName);
            }
        }
    }

    @ParameterizedTest(name = "variant {0}")
    @ValueSource(ints = {0, 1, 2})
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

    // Parameters that cannot be inlined as literals: rejected in a direct statement, accepted in a procedure call.

    @Test
    public void testOutParameterOnNonProcedureStatementIsRejected() throws SQLException {
        try (Connection conn = openConnection("none"); CallableStatement cs = conn.prepareCall("SELECT ?")) {
            cs.registerOutParameter(1, Types.INTEGER);
            SQLException e = assertThrows(SQLException.class, cs::execute);
            assertTrue(e.getMessage().contains("not supported"), e.getMessage());
        }
    }

    @Test
    public void testStreamIsRejectedInDirectStatement() throws SQLException {
        try (Connection conn = openConnection("none");
                PreparedStatement ps = conn.prepareStatement("SELECT DATALENGTH(?)")) {
            ps.setBinaryStream(1, new ByteArrayInputStream(new byte[] {1, 2, 3}));
            SQLException e = assertThrows(SQLException.class, ps::executeQuery);
            assertTrue(e.getMessage().contains("not supported"), e.getMessage());
        }
    }

    @Test
    public void testStreamInDirectBatchIsRejected() throws SQLException {
        try (Connection conn = openConnection("none"); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE #stream_t (v varbinary(max))");
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO #stream_t VALUES (?)")) {
                ps.setBinaryStream(1, new ByteArrayInputStream(new byte[] {1, 2, 3}));
                ps.addBatch();
                SQLException e = assertThrows(SQLException.class, ps::executeBatch);
                assertTrue(e.getMessage().contains("not supported"), e.getMessage());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM #stream_t")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1), "nothing must be sent when a parameter is rejected");
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
    public void testTableValuedParameterIsRejectedInDirectStatement() throws SQLException {
        String typeName = "PrepareMethodRpcTvp_" + UUID.randomUUID().toString().replace("-", "");
        try (Connection conn = openConnection("none"); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TYPE " + typeName + " AS TABLE (value int)");
            try {
                try (SQLServerPreparedStatement ps = (SQLServerPreparedStatement) conn
                        .prepareStatement("SELECT SUM(value) FROM ?")) {
                    ps.setStructured(1, typeName, tvpWithRows());
                    SQLException e = assertThrows(SQLException.class, ps::executeQuery);
                    assertTrue(e.getMessage().contains("not supported"), e.getMessage());
                }
            } finally {
                stmt.execute("DROP TYPE IF EXISTS " + typeName);
            }
        }
    }

    @ParameterizedTest(name = "mode={0}")
    @ValueSource(strings = {"prepexec", "none"})
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
                stmt.execute("DROP PROCEDURE IF EXISTS " + procedureName);
                stmt.execute("DROP TYPE IF EXISTS " + typeName);
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
                    "SET ROWCOUNT -- limit\n ?; " + rows, "SET ROWCOUNT" + spaces + "?; " + rows}) {
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setLong(1, 2L);
                    assertEquals(2, countRows(ps), sql);
                }
            }
            if ("none".equals(mode)) {
                for (String sql : new String[] {"SET ROWCOUNT 0; SELECT TOP ? v FROM (VALUES (1), (2), (3)) AS t(v)",
                        "SET ROWCOUNT 0; SELECT TOP /* limit */ ? v FROM (VALUES (1), (2), (3)) AS t(v)",
                        "SET ROWCOUNT 0; SELECT TOP" + spaces + "? v FROM (VALUES (1), (2), (3)) AS t(v)"}) {
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setLong(1, 2L);
                        assertEquals(2, countRows(ps), sql);
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
