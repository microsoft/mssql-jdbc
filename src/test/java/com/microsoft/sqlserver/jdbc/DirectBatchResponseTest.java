/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
            when(command.startRequest(TDS.PKT_QUERY)).thenReturn(mock(TDSWriter.class));
            when(command.getProcessedResponse()).thenAnswer(invocation -> finished.get());
            doReturn(null).when(command).startResponse(anyBoolean());
            doAnswer(invocation -> {
                int index = next.getAndIncrement();
                if (index >= results.length) {
                    finished.set(true);
                    return false;
                }
                Object result = results[index];
                if (result instanceof SQLServerException)
                    throw (SQLServerException) result;
                stmt.updateCount = (Long) result;
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
