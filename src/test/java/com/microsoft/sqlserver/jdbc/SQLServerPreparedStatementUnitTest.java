/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;

import com.microsoft.sqlserver.jdbc.SQLServerConnection.CityHash128Key;
import com.microsoft.sqlserver.jdbc.SQLServerConnection.PreparedStatementHandle;


public class SQLServerPreparedStatementUnitTest {
    private SQLServerConnection connection;
    private SQLServerStatement first;
    private SQLServerStatement second;
    private ResultSetMetaData metadata;

    @BeforeEach
    public void setup() throws SQLException {
        connection = mock(SQLServerConnection.class);
        first = mock(SQLServerStatement.class);
        second = mock(SQLServerStatement.class);
        SQLServerResultSet resultSet = mock(SQLServerResultSet.class);
        metadata = mock(ResultSetMetaData.class);
        when(connection.getResponseBuffering()).thenReturn("adaptive");
        when(connection.getPrepareMethod()).thenReturn("prepexec");
        when(connection.createStatement()).thenReturn(first, second);
        when(first.executeQueryInternal(anyString())).thenReturn(resultSet);
        when(second.executeQueryInternal(anyString())).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(metadata);
    }

    private SQLServerPreparedStatement prepare(String sql) throws SQLServerException {
        return new SQLServerPreparedStatement(connection, sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                SQLServerStatementColumnEncryptionSetting.USE_CONNECTION_SETTING);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testHandleLookupOnlyConstructsKeyWhenPoolingEnabled(boolean pooling) throws Exception {
        when(connection.isStatementPoolingEnabled()).thenReturn(pooling);
        PreparedStatementHandle handle = mock(PreparedStatementHandle.class);
        when(handle.getHandle()).thenReturn(42);
        when(handle.tryAddReference()).thenReturn(true);
        when(connection.getCachedPreparedStatementHandle(any(CityHash128Key.class)))
                .thenReturn(pooling ? handle : null);
        try (SQLServerPreparedStatement statement = prepare("SELECT ?")) {
            setField(statement, "preparedSQL", "SELECT @P0");
            setField(statement, "preparedTypeDefinitions", "@P0 int");
            // Start after statement construction: the parsed-SQL cache key is independent of handle pooling.
            try (MockedConstruction<CityHash128Key> keys = mockConstruction(CityHash128Key.class)) {
                assertEquals(pooling, reuseCachedHandle(statement, false, false));
                assertEquals(pooling ? 1 : 0, keys.constructed().size());
                assertEquals(pooling ? 42 : 0, statement.getPreparedStatementHandle());
                if (pooling) {
                    verify(connection).getCachedPreparedStatementHandle(keys.constructed().get(0));
                    verify(handle).tryAddReference();
                } else {
                    verify(connection, never()).getCachedPreparedStatementHandle(any(CityHash128Key.class));
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testHandleRegistrationOnlyConstructsKeyWhenPoolingEnabled(boolean pooling) throws Exception {
        when(connection.isStatementPoolingEnabled()).thenReturn(pooling);
        TDSReader reader = mock(TDSReader.class);
        when(reader.peekTokenType()).thenReturn(TDS.TDS_RETURN_VALUE, -1);
        when(reader.getCommand()).thenReturn(mock(TDSCommand.class));
        try (SQLServerPreparedStatement statement = prepare("SELECT ?")) {
            setField(statement, "preparedSQL", "SELECT @P0");
            setField(statement, "preparedTypeDefinitions", "@P0 int");
            setField(statement, "expectPrepStmtHandle", true);
            try (MockedConstruction<Parameter> parameters = mockConstruction(Parameter.class,
                    (parameter, context) -> when(parameter.getInt(reader, statement)).thenReturn(42));
                    MockedConstruction<CityHash128Key> keys = mockConstruction(CityHash128Key.class)) {
                assertTrue(statement.consumeExecOutParam(reader));
                assertEquals(42, statement.getPreparedStatementHandle());
                assertEquals(pooling ? 1 : 0, keys.constructed().size());
                if (pooling) {
                    verify(connection).registerCachedPreparedStatementHandle(keys.constructed().get(0), 42, false);
                } else {
                    verify(connection, never()).registerCachedPreparedStatementHandle(any(CityHash128Key.class),
                            anyInt(), anyBoolean());
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testDisabledPoolingStillReleasesOldHandle(boolean discard) throws Exception {
        PreparedStatementHandle handle = mock(PreparedStatementHandle.class);
        when(handle.getHandle()).thenReturn(42);
        try (SQLServerPreparedStatement statement = prepare("SELECT ?")) {
            setField(statement, "cachedPreparedStatementHandle", handle);
            setField(statement, "prepStmtHandle", 42);
            try (MockedConstruction<CityHash128Key> keys = mockConstruction(CityHash128Key.class)) {
                assertFalse(reuseCachedHandle(statement, !discard, discard));
                verify(handle).removeReference();
                if (discard) {
                    verify(handle).setIsExplicitlyDiscarded();
                }
                assertEquals(0, statement.getPreparedStatementHandle());
                assertEquals(0, keys.constructed().size());
            }
        }
    }

    private static void setField(SQLServerPreparedStatement statement, String name, Object value) throws Exception {
        Field field = SQLServerPreparedStatement.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(statement, value);
    }

    private static boolean reuseCachedHandle(SQLServerPreparedStatement statement, boolean newTypes,
            boolean discard) throws Exception {
        Method method = SQLServerPreparedStatement.class.getDeclaredMethod("reuseCachedHandle", boolean.class,
                boolean.class);
        method.setAccessible(true);
        return (boolean) method.invoke(statement, newTypes, discard);
    }

    @Test
    public void testRepeatedMetadataClosesReplacedStatement() throws SQLException {
        try (SQLServerPreparedStatement statement = prepare("SELECT 1 AS value")) {
            assertSame(metadata, statement.getMetaData());
            verify(first, never()).close();
            assertSame(metadata, statement.getMetaData());
            InOrder order = inOrder(connection, first, second);
            order.verify(connection).createStatement();
            order.verify(first).executeQueryInternal("set fmtonly on SELECT 1 AS value\nset fmtonly off");
            order.verify(first).close();
            order.verify(connection).createStatement();
            order.verify(second).executeQueryInternal("set fmtonly on SELECT 1 AS value\nset fmtonly off");
            verify(second, never()).close();
        }
        verify(second).close();
    }

    @Test
    public void testMetadataRetryClosesFailedStatement() throws SQLException {
        when(first.executeQueryInternal(anyString()))
                .thenThrow(new SQLServerException("Parameter marker failed", null));
        try (SQLServerPreparedStatement statement = prepare("SELECT ? AS value")) {
            assertSame(metadata, statement.getMetaData());
            InOrder order = inOrder(connection, first, second);
            order.verify(connection).createStatement();
            order.verify(first).executeQueryInternal("set fmtonly on SELECT ? AS value\nset fmtonly off");
            order.verify(first).close();
            order.verify(connection).createStatement();
            order.verify(second).executeQueryInternal("set fmtonly on SELECT null AS value\nset fmtonly off");
            verify(second, never()).close();
        }
        verify(second).close();
    }

    @Test
    public void testMetadataRetryAfterStatementCreationFailure() throws SQLException {
        when(connection.createStatement()).thenThrow(new SQLServerException("Statement creation failed", null))
                .thenReturn(second);
        try (SQLServerPreparedStatement statement = prepare("SELECT ? AS value")) {
            assertSame(metadata, statement.getMetaData());
            verify(connection, times(2)).createStatement();
            verify(second).executeQueryInternal("set fmtonly on SELECT null AS value\nset fmtonly off");
            verify(second, never()).close();
        }
        verify(second).close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testMetadataTimeoutPropagationAndParentOwnership(boolean retry) throws SQLException {
        SQLTimeoutException timeout = new SQLTimeoutException("Metadata query timed out");
        SQLServerStatement current = retry ? second : first;
        if (retry) {
            when(first.executeQueryInternal(anyString()))
                    .thenThrow(new SQLServerException("Parameter marker failed", null));
        }
        when(current.executeQueryInternal(anyString())).thenThrow(timeout);
        try (SQLServerPreparedStatement statement = prepare("SELECT ? AS value")) {
            assertSame(timeout, assertThrows(SQLTimeoutException.class, statement::getMetaData));
            verify(connection, times(retry ? 2 : 1)).createStatement();
            if (retry) {
                verify(first).close();
            }
            verify(current, never()).close();
        }
        verify(current).close();
    }

    @Test
    public void testMetadataFailedRetryRemainsParentOwned() throws SQLException {
        SQLServerException failure = new SQLServerException("Metadata retry failed", null);
        when(first.executeQueryInternal(anyString()))
                .thenThrow(new SQLServerException("Parameter marker failed", null));
        when(second.executeQueryInternal(anyString())).thenThrow(failure);
        try (SQLServerPreparedStatement statement = prepare("SELECT ? AS value")) {
            assertSame(failure, assertThrows(SQLServerException.class, statement::getMetaData));
            verify(first).close();
            verify(second, never()).close();
        }
        verify(second).close();
    }

    @Test
    public void testMetadataNoResultRemainsParentOwned() throws SQLException {
        when(first.executeQueryInternal(anyString()))
                .thenThrow(new SQLServerException(SQLServerException.getErrString("R_noResultset"), null));
        try (SQLServerPreparedStatement statement = prepare("DECLARE @value INT")) {
            assertNull(statement.getMetaData());
            verify(connection).createStatement();
            verify(first, never()).close();
        }
        verify(first).close();
    }
}
