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
import java.sql.BatchUpdateException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
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
                Arguments.of("SELECT 1 /* unterminated EXEC p", false));
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

    private static Connection openConnection(String mode) throws SQLException {
        String url = TestUtils.addOrOverrideProperty(TestUtils.removeProperty(connectionString, "prepareMethod"),
                "prepareMethod", mode);
        return PrepUtil.getConnection(url);
    }
}
