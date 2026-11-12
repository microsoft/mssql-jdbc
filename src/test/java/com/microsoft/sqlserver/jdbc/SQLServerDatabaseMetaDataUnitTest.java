/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InOrder;


public class SQLServerDatabaseMetaDataUnitTest {

    @Test
    public void testIndexInfoStatementCloseOnCompletion() throws SQLException {
        SQLServerConnection connection = mock(SQLServerConnection.class);
        SQLServerPreparedStatement statement = mock(SQLServerPreparedStatement.class);
        SQLServerResultSet resultSet = mock(SQLServerResultSet.class);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);

        assertSame(resultSet, new SQLServerDatabaseMetaData(connection).getIndexInfo(null, "dbo", "t1", false, false));

        InOrder order = inOrder(statement);
        order.verify(statement).closeOnCompletion();
        order.verify(statement).setString(1, "t1");
        order.verify(statement).executeQuery();
        verify(statement, never()).close();
        verify(resultSet, never()).close();
        verify(connection, never()).prepareCall(anyString());
    }

    static Stream<Arguments> indexInfoFailureCases() {
        return Stream.of("prepare", "completion", "bind", "execute")
                .flatMap(stage -> Stream.of(Arguments.of(stage, false), Arguments.of(stage, true)));
    }

    @ParameterizedTest
    @MethodSource("indexInfoFailureCases")
    public void testIndexInfoClosesFailedStatement(String stage, boolean fallbackFails) throws SQLException {
        SQLServerConnection connection = mock(SQLServerConnection.class);
        SQLServerPreparedStatement statement = mock(SQLServerPreparedStatement.class);
        SQLServerCallableStatement fallback = mock(SQLServerCallableStatement.class);
        SQLServerResultSet resultSet = mock(SQLServerResultSet.class);
        SQLServerException queryFailure = new SQLServerException("Combined query failed", null);
        SQLServerException fallbackFailure = new SQLServerException("Fallback failed", null);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(connection.prepareCall(anyString())).thenReturn(fallback);
        if ("prepare".equals(stage)) {
            when(connection.prepareStatement(anyString())).thenThrow(queryFailure);
        } else if ("completion".equals(stage)) {
            doThrow(queryFailure).when(statement).closeOnCompletion();
        } else if ("bind".equals(stage)) {
            doThrow(queryFailure).when(statement).setString(1, "t1");
        } else {
            when(statement.executeQuery()).thenThrow(queryFailure);
        }
        if (fallbackFails) {
            when(fallback.executeQueryInternal()).thenThrow(fallbackFailure);
        } else {
            when(fallback.executeQueryInternal()).thenReturn(resultSet);
        }

        SQLServerDatabaseMetaData metadata = new SQLServerDatabaseMetaData(connection);
        if (fallbackFails) {
            assertSame(fallbackFailure, assertThrows(SQLServerException.class,
                    () -> metadata.getIndexInfo(null, "dbo", "t1", false, false)));
        } else {
            assertSame(resultSet, metadata.getIndexInfo(null, "dbo", "t1", false, false));
        }

        InOrder order = inOrder(statement, fallback);
        if ("prepare".equals(stage)) {
            verify(statement, never()).close();
        } else {
            order.verify(statement).close();
        }
        order.verify(fallback).executeQueryInternal();
        verify(fallback, never()).close();
        verify(resultSet, never()).close();
    }

}
