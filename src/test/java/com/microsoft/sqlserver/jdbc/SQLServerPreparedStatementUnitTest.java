/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;


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
