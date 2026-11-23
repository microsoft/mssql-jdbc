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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;


class DirectBatchResponseTest {
    private static final class Response {
        final SQLServerConnection con = mock(SQLServerConnection.class);
        final SQLServerPreparedStatement stmt = mock(SQLServerPreparedStatement.class, CALLS_REAL_METHODS);
        final TDSCommand command;
        final Method execute;
        final AtomicBoolean finished = new AtomicBoolean();
        final AtomicInteger next = new AtomicInteger();

        Response(int entries, Object... results) throws Exception {
            Class<?> type = Arrays.stream(SQLServerPreparedStatement.class.getDeclaredClasses())
                    .filter(c -> "PrepStmtBatchExecCmd".equals(c.getSimpleName())).findFirst().get();
            command = (TDSCommand) mock(type);
            execute = SQLServerPreparedStatement.class.getDeclaredMethod("doExecuteExecMethodBatchCombined", type);
            execute.setAccessible(true);
            set(SQLServerStatement.class, stmt, "connection", con);
            set(SQLServerPreparedStatement.class, stmt, "userSQL", "UPDATE t SET v=1");
            set(SQLServerPreparedStatement.class, stmt, "userSQLParamPositions", new int[0]);
            stmt.batchParamValues = new ArrayList<>();
            for (int i = 0; i < entries; i++)
                stmt.batchParamValues.add(new Parameter[0]);
            set(type, command, "updateCounts", new long[entries]);
            stmt.executedSqlDirectly = false;
            stmt.expectCursorOutParams = true;
            set(SQLServerPreparedStatement.class, stmt, "expectPrepStmtHandle", true);
            set(SQLServerPreparedStatement.class, stmt, "outParamIndexAdjustment", 3);
            when(command.startRequest(TDS.PKT_QUERY)).thenAnswer(invocation -> {
                assertTrue(stmt.executedSqlDirectly);
                assertFalse(stmt.expectCursorOutParams);
                assertEquals(false, field(stmt, "expectPrepStmtHandle"));
                assertEquals(0, field(stmt, "outParamIndexAdjustment"));
                return mock(TDSWriter.class);
            });
            when(command.getProcessedResponse()).thenAnswer(invocation -> finished.get());
            doReturn(null).when(command).startResponse(anyBoolean());
            doAnswer(invocation -> {
                if (!stmt.moreResults)
                    return false;
                int index = next.getAndIncrement();
                if (index >= results.length) {
                    finished.set(true);
                    return false;
                }
                Object result = results[index];
                if (result instanceof SQLServerException)
                    throw (SQLServerException) result;
                if (Boolean.FALSE.equals(result)) {
                    stmt.moreResults = false;
                    stmt.updateCount = -1;
                    return true;
                }
                if (result instanceof SQLServerResultSet) {
                    stmt.resultSet = (SQLServerResultSet) result;
                    stmt.updateCount = -1;
                } else {
                    stmt.updateCount = (Long) result;
                }
                return true;
            }).when(stmt).getNextResult(true);
        }

        long[] run() throws Throwable {
            try {
                execute.invoke(stmt, command);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
            verify(command).startRequest(TDS.PKT_QUERY);
            return (long[]) field(command, "updateCounts");
        }

        SQLServerException error() throws Exception {
            return (SQLServerException) field(command, "batchException");
        }
    }

    private static void set(Class<?> type, Object target, String name, Object value) throws Exception {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object field(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    private static SQLServerException databaseError() {
        SQLServerException error = new SQLServerException("late error", "S0001", 50000, null);
        error.setDriverErrorCode(SQLServerException.DRIVER_ERROR_FROM_DATABASE);
        return error;
    }

    @Test
    void preservesLongCountsAndUnknownSuccess() throws Throwable {
        Response response = new Response(3, 0L, 1L + Integer.MAX_VALUE, -1L);
        assertArrayEquals(new long[] {0, 1L + Integer.MAX_VALUE, Statement.SUCCESS_NO_INFO}, response.run());
        verify(response.stmt, never()).getUpdateCount();
    }

    @Test
    void recordsFailedEntryAndContinues() throws Throwable {
        SQLServerException error = databaseError();
        Response response = new Response(3, 1L, error, 2L);
        assertArrayEquals(new long[] {1, Statement.EXECUTE_FAILED, 2}, response.run());
        assertSame(error, response.error());
    }

    @Test
    void leavesUnexecutedSlotsFailed() throws Throwable {
        Response response = new Response(3, 1L, databaseError());
        assertArrayEquals(new long[] {1, Statement.EXECUTE_FAILED, Statement.EXECUTE_FAILED}, response.run());
    }

    @Test
    void resumesAfterLogicalCompletionUntilResponseEOF() throws Throwable {
        Response response = new Response(3, false, 3L, false, 5L, false);
        assertArrayEquals(new long[] {Statement.SUCCESS_NO_INFO, 3, Statement.SUCCESS_NO_INFO}, response.run());
        assertTrue(response.finished.get(), "A logical batch completion must not leave the response unread");
    }

    @Test
    void retainsFirstErrorBeyondCountSlots() throws Throwable {
        SQLServerException error = databaseError();
        Response response = new Response(1, 3L, 2L, error, databaseError(), 5L);
        assertArrayEquals(new long[] {3}, response.run());
        assertSame(error, response.error());
        assertTrue(response.finished.get());
    }

    @Test
    void propagatesCancellationAndUnexpectedDriverFailure() throws Exception {
        for (String state : new String[] {SQLState.STATEMENT_CANCELED.getSQLStateCode(), "HY000"}) {
            SQLServerException error = new SQLServerException("stop processing", state, 0, null);
            Response response = new Response(2, error, 1L);
            assertSame(error, assertThrows(SQLServerException.class, response::run).getCause());
            assertEquals(1, response.next.get(), "Do not resume parsing after a non-database failure");
        }
    }

    @Test
    void scrollableDiscardReleasesMarksAndPreservesRowErrors() throws Throwable {
        for (boolean rowError : new boolean[] {false, true}) {
            SQLServerResultSet rs = mock(SQLServerResultSet.class, CALLS_REAL_METHODS);
            SQLServerStatement stmt = mock(SQLServerStatement.class);
            TDSReader reader = mock(TDSReader.class);
            ScrollWindow window = mock(ScrollWindow.class);
            when(stmt.getSQLResultSetType()).thenReturn(SQLServerResultSet.TYPE_SS_SCROLL_STATIC);
            when(reader.mark()).thenReturn(mock(TDSReaderMark.class));
            when(reader.readUnsignedByte()).thenReturn(TDS.TDS_ROW);
            set(SQLServerResultSet.class, rs, "stmt", stmt);
            set(SQLServerResultSet.class, rs, "tdsReader", reader);
            set(SQLServerResultSet.class, rs, "scrollWindow", window);
            Class<?> type = Arrays.stream(SQLServerResultSet.class.getDeclaredClasses())
                    .filter(c -> "FetchBuffer".equals(c.getSimpleName())).findFirst().get();
            Constructor<?> constructor = type.getDeclaredConstructor(SQLServerResultSet.class);
            constructor.setAccessible(true);
            Object buffer = constructor.newInstance(rs);
            set(SQLServerResultSet.class, rs, "fetchBuffer", buffer);
            assertNotNull(field(buffer, "startMark"));
            TDSTokenHandler handler = (TDSTokenHandler) field(buffer, "fetchBufferTokenHandler");
            SQLServerException error = databaseError();
            AtomicInteger rows = new AtomicInteger();
            doAnswer(invocation -> {
                assertNull(field(buffer, "startMark"), "Discard must release retained marks");
                if (rows.getAndIncrement() == 100) {
                    if (rowError)
                        throw error;
                    return false;
                }
                handler.onRow(reader);
                assertNull(field(buffer, "startMark"), "Discard must not recreate marks");
                return true;
            }).when(rs).fetchBufferNext();
            doNothing().when(rs).close();
            Response response = new Response(2, rs, 1L);
            assertArrayEquals(new long[] {Statement.EXECUTE_FAILED, 1}, response.run());
            assertEquals(101, rows.get());
            assertNull(field(buffer, "startMark"));
            assertEquals(false, field(buffer, "discarding"), "Restore normal cursor behavior even after row errors");
            verify(window).clear();
            verify(rs).close();
            verify(rs, never()).next();
            if (rowError)
                assertSame(error, response.error());
            else
                assertEquals(SQLServerException.getErrString("R_resultsetGeneratedForUpdate"),
                        response.error().getMessage());
        }
    }

    @Test
    void rowConsumptionDoesNotSwallowDriverFailures() throws Exception {
        SQLServerResultSet rs = mock(SQLServerResultSet.class);
        SQLServerException error = new SQLServerException("row read failure", "HY000", 0, null);
        doThrow(error).when(rs).discardFetchBufferWithErrors();
        Response response = new Response(2, rs, 1L);
        assertSame(error, assertThrows(SQLServerException.class, response::run).getCause());
        verify(rs).close();
        assertEquals(1, response.next.get());
    }

    @Test
    void propagatesSessionAndRollbackFailures() throws Exception {
        SQLServerException error = databaseError();
        Response session = new Response(1, error);
        when(session.con.isSessionUnAvailable()).thenReturn(true);
        assertSame(error, assertThrows(SQLServerException.class, session::run).getCause());
        Response rollback = new Response(1, error);
        when(rollback.con.rolledBackTransaction()).thenReturn(true);
        assertSame(error, assertThrows(SQLServerException.class, rollback::run).getCause());
    }
}
