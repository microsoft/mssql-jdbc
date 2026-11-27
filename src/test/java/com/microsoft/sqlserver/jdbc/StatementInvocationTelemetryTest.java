/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.sql.BatchUpdateException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Exercises public JDBC boundaries without opening a socket or executing SQL. */
class StatementInvocationTelemetryTest {
    private final PerformanceLogLifecycleTest.Collector collector = new PerformanceLogLifecycleTest.Collector();
    private StubConnection connection;

    @BeforeEach
    void setup() throws Exception {
        connection = new StubConnection();
        Field state = SQLServerConnection.class.getDeclaredField("state");
        state.setAccessible(true);
        for (Object value : state.getType().getEnumConstants()) {
            if ("OPENED".equals(value.toString())) {
                state.set(connection, value);
            }
        }
        set(SQLServerConnection.class, connection, "responseBuffering", "adaptive");
        PerformanceLog.registerCallback(collector);
    }

    @AfterEach
    void cleanup() {
        PerformanceLog.unregisterCallback();
    }

    @Test
    void preparedBatchFailureIsThePublicInvocationOutcome() throws Exception {
        for (boolean large : new boolean[] {false, true}) {
            collector.ends.clear();
            SQLServerPreparedStatement statement = prepared();
            statement.batchParamValues = new ArrayList<>();
            statement.batchParamValues.add(new Parameter[0]);
            if (large) {
                assertThrows(BatchUpdateException.class, statement::executeLargeBatch);
            } else {
                assertThrows(BatchUpdateException.class, statement::executeBatch);
            }
            List<PerformanceLogEvent> roots = ends(PerformanceActivity.STATEMENT_INVOCATION);
            assertEquals(1, roots.size());
            PerformanceLogEvent root = roots.get(0);
            assertEquals("failure", root.getAttributes().get("mssql.statement.outcome"));
            assertEquals(large ? "execute_large_batch" : "execute_batch", root.getAttributes().get("mssql.statement.api"));
            assertEquals(1L, root.getAttributes().get("mssql.statement.attempt_count"));
            assertTrue(root.getException() instanceof BatchUpdateException);
            assertNull(StatementPerformanceState.current(statement));
        }
    }

    @Test
    void emptyBulkBatchStillHasOneInvocationAndNoFabricatedAttempt() throws Exception {
        for (boolean large : new boolean[] {false, true}) {
            collector.ends.clear();
            SQLServerPreparedStatement statement = prepared();
            set(SQLServerPreparedStatement.class, statement, "useBulkCopyForBatchInsert", true);
            if (large) {
                assertArrayEquals(new long[0], statement.executeLargeBatch());
            } else {
                assertArrayEquals(new int[0], statement.executeBatch());
            }
            List<PerformanceLogEvent> roots = ends(PerformanceActivity.STATEMENT_INVOCATION);
            assertEquals(1, roots.size());
            assertEquals(0L, roots.get(0).getAttributes().get("mssql.statement.attempt_count"));
            assertEquals(0L, roots.get(0).getAttributes().get("mssql.statement.batch_size"));
            assertEquals("success", roots.get(0).getAttributes().get("mssql.statement.outcome"));
        }
    }

    @Test
    void nestedHelperCannotCreateAnotherInvocationRoot() throws Exception {
        SQLServerPreparedStatement statement = prepared();
        try (PerformanceLog.Scope root = PerformanceLog.createStatementInvocationScope(statement, statement.userSQL)) {
            try (PerformanceLog.Scope helper = PerformanceLog.createStatementInvocationScope(statement, statement.userSQL)) {}
            assertNotNull(StatementPerformanceState.current(statement));
        }
        assertEquals(1, ends(PerformanceActivity.STATEMENT_INVOCATION).size());
        assertNull(StatementPerformanceState.current(statement));
    }

    @Test
    void internalRetriesReplaceRatherThanNestOrDoubleCountAttempts() throws Exception {
        SQLServerPreparedStatement statement = prepared();
        for (String reason : new String[] {"invalid_handle_retry", "enclave_retry"}) {
            collector.ends.clear();
            try (PerformanceLog.Scope root = PerformanceLog.createStatementInvocationScope(statement, statement.userSQL)) {
                PerformanceLog.createStatementAttemptScope(statement, statement.userSQL);
                SQLException failure = new SQLException("SECRET");
                try (PerformanceLog.Scope call = call(statement)) {
                    call.setException(failure);
                }
                PerformanceLog.retryStatementAttempt(statement, failure, reason, statement.userSQL);
                // The outer execution wrapper must not count the internal retry a second time.
                try (PerformanceLog.Scope ignored = PerformanceLog.createStatementAttemptScope(statement, statement.userSQL);
                        PerformanceLog.Scope call = call(statement)) {}
                PerformanceLog.completeStatementAttempt(statement, null);
            }
            List<PerformanceLogEvent> attempts = ends(PerformanceActivity.STATEMENT_ATTEMPT);
            assertEquals(2, attempts.size());
            assertEquals("failure", attempts.get(0).getAttributes().get("mssql.statement.attempt_outcome"));
            assertEquals("initial", attempts.get(0).getAttributes().get("mssql.statement.attempt_reason"));
            assertEquals("success", attempts.get(1).getAttributes().get("mssql.statement.attempt_outcome"));
            assertEquals(reason, attempts.get(1).getAttributes().get("mssql.statement.attempt_reason"));
            PerformanceLogEvent root = ends(PerformanceActivity.STATEMENT_INVOCATION).get(0);
            assertEquals(2L, root.getAttributes().get("mssql.statement.attempt_count"));
            assertEquals(1L, root.getAttributes().get("mssql.statement.retry_count"));
            assertEquals("success", root.getAttributes().get("mssql.statement.outcome"));
            assertNull(root.getException());
            assertEquals(attempts.get(0).getParentScopeId(), attempts.get(1).getParentScopeId());
            List<PerformanceLogEvent> calls = ends(PerformanceActivity.STATEMENT_SERVER_CALL);
            for (int i = 0; i < 2; i++) {
                assertEquals(attempts.get(i).getScopeId(), calls.get(i).getParentScopeId());
                assertEquals((long) i + 1, calls.get(i).getAttributes().get("mssql.statement.attempt"));
            }
            assertNull(StatementPerformanceState.current(statement));
        }
    }

    @Test
    void terminalInternalRetryFailsOnlyItsOwnAttemptAndTheOwningRoot() throws Exception {
        SQLServerPreparedStatement statement = prepared();
        SQLException terminal = new SQLException("SECRET terminal");
        try (PerformanceLog.Scope root = PerformanceLog.createStatementInvocationScope(statement, statement.userSQL)) {
            PerformanceLog.createStatementAttemptScope(statement, statement.userSQL);
            PerformanceLog.retryStatementAttempt(statement, new SQLException("SECRET first"), "enclave_retry", statement.userSQL);
            try (PerformanceLog.Scope call = call(statement)) {
                call.setException(terminal);
            }
            PerformanceLog.completeStatementAttempt(statement, terminal);
            root.setException(terminal);
        }
        PerformanceLogEvent root = ends(PerformanceActivity.STATEMENT_INVOCATION).get(0);
        assertEquals("failure", root.getAttributes().get("mssql.statement.outcome"));
        assertSame(terminal, root.getException());
        assertEquals(2L, root.getAttributes().get("mssql.statement.attempt_count"));
        assertTrue(collector.ends.stream().allMatch(event -> !event.getAttributes().toString().contains("SECRET")));
        assertNull(StatementPerformanceState.current(statement));
    }

    @Test
    void plainEmptyBatchHasOneRootForEachPublicApi() throws Exception {
        SQLServerStatement statement = new SQLServerStatement(connection, ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY, SQLServerStatementColumnEncryptionSetting.USE_CONNECTION_SETTING);
        assertArrayEquals(new int[0], statement.executeBatch());
        assertArrayEquals(new long[0], statement.executeLargeBatch());
        List<PerformanceLogEvent> roots = ends(PerformanceActivity.STATEMENT_INVOCATION);
        assertEquals(2, roots.size());
        assertEquals("execute_batch", roots.get(0).getAttributes().get("mssql.statement.api"));
        assertEquals("execute_large_batch", roots.get(1).getAttributes().get("mssql.statement.api"));
        assertNull(StatementPerformanceState.current(statement));
    }

    @Test
    void bulkCopyPublicApisOwnSuccessAndFailureAndReportActualProtocol() throws Exception {
        connection.metadataStatement = mock(SQLServerStatement.class);
        SQLServerResultSet metadata = mock(SQLServerResultSet.class);
        when(connection.metadataStatement.executeQueryInternal(anyString())).thenReturn(metadata);
        for (boolean large : new boolean[] {false, true}) {
            for (boolean fail : new boolean[] {false, true}) {
                collector.ends.clear();
                SQLServerPreparedStatement statement = prepared();
                statement.batchParamValues = new ArrayList<>();
                statement.batchParamValues.add(new Parameter[0]);
                set(SQLServerPreparedStatement.class, statement, "useBulkCopyForBatchInsert", true);
                set(SQLServerPreparedStatement.class, statement, "bcOperationTableName", "test");
                set(SQLServerPreparedStatement.class, statement, "bcOperationColumnList", new ArrayList<>(java.util.Arrays.asList("x")));
                set(SQLServerPreparedStatement.class, statement, "bcOperationValueList", new ArrayList<>(java.util.Arrays.asList("1")));
                SQLServerBulkCopy bulkCopy = mock(SQLServerBulkCopy.class);
                set(SQLServerPreparedStatement.class, statement, "bcOperation", bulkCopy);
                if (fail) {
                    doThrow(new SQLServerException("SECRET", null)).when(bulkCopy).writeToServer(any(ISQLServerBulkData.class));
                    if (large) {
                        assertThrows(BatchUpdateException.class, statement::executeLargeBatch);
                    } else {
                        assertThrows(BatchUpdateException.class, statement::executeBatch);
                    }
                } else if (large) {
                    assertArrayEquals(new long[] {1}, statement.executeLargeBatch());
                } else {
                    assertArrayEquals(new int[] {1}, statement.executeBatch());
                }
                verify(bulkCopy).writeToServer(any(ISQLServerBulkData.class));
                List<PerformanceLogEvent> roots = ends(PerformanceActivity.STATEMENT_INVOCATION);
                assertEquals(1, roots.size());
                PerformanceLogEvent root = roots.get(0);
                assertEquals(fail ? "failure" : "success", root.getAttributes().get("mssql.statement.outcome"));
                assertEquals(1L, root.getAttributes().get("mssql.statement.attempt_count"));
                assertEquals(1L, root.getAttributes().get("mssql.statement.batch_size"));
                PerformanceLogEvent call = ends(PerformanceActivity.STATEMENT_SERVER_CALL).get(0);
                assertEquals("bulk_copy", call.getAttributes().get("mssql.statement.protocol"));
                assertEquals("bulk_copy", call.getAttributes().get("mssql.statement.protocol_operation"));
                assertEquals(ends(PerformanceActivity.STATEMENT_ATTEMPT).get(0).getScopeId(), call.getParentScopeId());
                assertNull(StatementPerformanceState.current(statement));
            }
        }
    }

    private PerformanceLog.Scope call(SQLServerStatement statement) {
        return PerformanceLog.createScope(PerformanceLog.perfLoggerStatement, connection, statement.getStatementID(),
                statement, "select 1", PerformanceActivity.STATEMENT_SERVER_CALL);
    }

    private SQLServerPreparedStatement prepared() throws Exception {
        return new SQLServerPreparedStatement(connection, "INSERT INTO test VALUES (1)", ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY, SQLServerStatementColumnEncryptionSetting.USE_CONNECTION_SETTING);
    }

    private List<PerformanceLogEvent> ends(PerformanceActivity activity) {
        List<PerformanceLogEvent> result = new ArrayList<>();
        for (PerformanceLogEvent event : collector.ends) {
            if (event.getActivity() == activity) {
                result.add(event);
            }
        }
        return result;
    }

    private static void set(Class<?> type, Object target, String name, Object value) throws Exception {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static class StubConnection extends SQLServerConnection {
        SQLServerStatement metadataStatement;

        StubConnection() throws SQLServerException {
            super("test");
        }

        @Override
        public Statement createStatement(int type, int concurrency, int holdability,
                SQLServerStatementColumnEncryptionSetting encryption) throws SQLServerException {
            if (metadataStatement != null) {
                return metadataStatement;
            }
            throw new AssertionError("Unexpected metadata query");
        }

        @Override
        boolean executeCommand(TDSCommand command) throws SQLServerException {
            try {
                set(command.getClass(), command, "updateCounts", new long[] {1, -3});
                set(command.getClass(), command, "batchException", new SQLServerException("SECRET", null));
                return false;
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }
    }
}