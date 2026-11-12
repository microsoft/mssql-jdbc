/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
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

    @ParameterizedTest
    @ValueSource(strings = {"completion", "bindTable", "bindSchema", "execute", "timeout", "success"})
    public void testIndexInfoAzureDWFallbackCleanup(String stage) throws SQLException {
        SQLServerConnection connection = mock(SQLServerConnection.class);
        SQLServerPreparedStatement storedProcedure = mock(SQLServerPreparedStatement.class);
        SQLServerPreparedStatement fallback = mock(SQLServerPreparedStatement.class);
        SQLServerResultSet resultSet = mock(SQLServerResultSet.class);
        SQLException failure = "timeout".equals(stage) ? new SQLTimeoutException("Fallback timed out")
                                                       : new SQLServerException("Fallback failed", null);
        when(connection.isAzureDW()).thenReturn(true);
        when(connection.prepareStatement(anyString())).thenReturn(storedProcedure, fallback);
        when(storedProcedure.executeQuery()).thenThrow(new SQLServerException("Stored procedure failed", null));
        when(fallback.executeQuery()).thenReturn(resultSet);
        if ("completion".equals(stage)) {
            doThrow(failure).when(fallback).closeOnCompletion();
        } else if ("bindTable".equals(stage)) {
            doThrow(failure).when(fallback).setString(1, "t1");
        } else if ("bindSchema".equals(stage)) {
            doThrow(failure).when(fallback).setString(2, "dbo");
        } else if (!"success".equals(stage)) {
            when(fallback.executeQuery()).thenThrow(failure);
        }

        SQLServerDatabaseMetaData metadata = new SQLServerDatabaseMetaData(connection);
        if ("success".equals(stage)) {
            assertSame(resultSet, metadata.getIndexInfo(null, "dbo", "t1", false, false));
            verify(fallback).closeOnCompletion();
            verify(fallback).setString(1, "t1");
            verify(fallback).setString(2, "dbo");
            verify(fallback).executeQuery();
            verify(fallback, never()).close();
            verify(resultSet, never()).close();
        } else {
            assertSame(failure,
                    assertThrows(SQLException.class, () -> metadata.getIndexInfo(null, "dbo", "t1", false, false)));
            verify(fallback).close();
        }
        verify(connection).prepareStatement("EXEC sp_statistics ?,?,?,?,?,?;");
        verify(storedProcedure).close();
    }

    static Stream<Arguments> internalQueryCleanupCases() {
        return Stream.of("create", "completion", "execute", "timeout", "success")
                .flatMap(stage -> Stream.of(Arguments.of(stage, false), Arguments.of(stage, true)));
    }

    @ParameterizedTest
    @MethodSource("internalQueryCleanupCases")
    public void testInternalQueryCleanupAndCatalogRestoration(String stage, boolean restoreFails) throws SQLException {
        SQLServerConnection connection = mock(SQLServerConnection.class);
        SQLServerStatement statement = mock(SQLServerStatement.class);
        SQLServerResultSet resultSet = mock(SQLServerResultSet.class);
        SQLException queryFailure = "timeout".equals(stage) ? new SQLTimeoutException(
                "Query timed out") : new SQLServerException("Query setup or execution failed", null);
        SQLServerException restoreFailure = new SQLServerException("Catalog restoration failed", null);
        when(connection.getCatalog()).thenReturn("original");
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQueryInternal(anyString())).thenReturn(resultSet);
        if ("create".equals(stage)) {
            when(connection.createStatement()).thenThrow(queryFailure);
        } else if ("completion".equals(stage)) {
            doThrow(queryFailure).when(statement).closeOnCompletion();
        } else if (!"success".equals(stage)) {
            when(statement.executeQueryInternal(anyString())).thenThrow(queryFailure);
        }
        if (restoreFails) {
            doThrow(restoreFailure).when(connection).setCatalog("original");
        }

        SQLServerDatabaseMetaData metadata = new SQLServerDatabaseMetaData(connection);
        boolean success = "success".equals(stage) && !restoreFails;
        if (success) {
            assertSame(resultSet, metadata.getPseudoColumns("other", null, null, null));
            verify(resultSet, never()).close();
        } else {
            SQLException expected = restoreFails ? restoreFailure : queryFailure;
            assertSame(expected,
                    assertThrows(SQLException.class, () -> metadata.getPseudoColumns("other", null, null, null)));
        }

        InOrder order = inOrder(connection, statement);
        order.verify(connection).setCatalog("other");
        order.verify(connection).createStatement();
        if (!"create".equals(stage)) {
            order.verify(statement).closeOnCompletion();
            if (!"completion".equals(stage)) {
                order.verify(statement).executeQueryInternal(anyString());
            }
        }
        order.verify(connection).setCatalog("original");
        if (success || "create".equals(stage)) {
            verify(statement, never()).close();
        } else {
            order.verify(statement).close();
        }
    }

    static Stream<Arguments> schemaQueryCases() {
        return Stream.of(null, "", "catalog", "catalog'name", "catalog']name")
                .flatMap(catalog -> Stream.of(Arguments.of(catalog, null), Arguments.of(catalog, "schema%")));
    }

    @ParameterizedTest
    @MethodSource("schemaQueryCases")
    public void testSchemaQueryParameters(String catalog, String schemaPattern) throws SQLException {
        SQLServerConnection connection = mock(SQLServerConnection.class);
        SQLServerPreparedStatement preparedStatement = mock(SQLServerPreparedStatement.class);
        SQLServerStatement statement = mock(SQLServerStatement.class);
        SQLServerResultSet resultSet = mock(SQLServerResultSet.class);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(connection.createStatement()).thenReturn(statement);
        when(preparedStatement.executeQueryInternal()).thenReturn(resultSet);
        when(statement.executeQueryInternal(anyString())).thenReturn(resultSet);

        assertSame(resultSet, new SQLServerDatabaseMetaData(connection).getSchemas(catalog, schemaPattern));

        boolean hasCatalog = null != catalog && !catalog.isEmpty();
        ArgumentCaptor<String> query = ArgumentCaptor.forClass(String.class);
        if (hasCatalog || null != schemaPattern) {
            verify(connection).prepareStatement(query.capture());
            verify(connection, never()).createStatement();
            verify(preparedStatement).closeOnCompletion();
            int parameterIndex = 1;
            if (hasCatalog) {
                verify(preparedStatement).setString(parameterIndex++, catalog);
            }
            if (null != schemaPattern) {
                verify(preparedStatement).setString(parameterIndex++, schemaPattern);
            }
            assertEquals(parameterIndex - 1, query.getValue().chars().filter(c -> c == '?').count());
            verify(preparedStatement).executeQueryInternal();
        } else {
            verify(statement).executeQueryInternal(query.capture());
            verify(statement).closeOnCompletion();
            verify(connection, never()).prepareStatement(anyString());
        }

        String schema = hasCatalog ? Util.escapeSQLId(catalog) + ".sys.schemas" : "sys.schemas";
        String projection = hasCatalog ? "? 'TABLE_CATALOG'"
                                       : (null == catalog ? " DB_NAME() 'TABLE_CATALOG'" : "null 'TABLE_CATALOG'");
        assertTrue(query.getValue().startsWith("select " + schema + ".name 'TABLE_SCHEM'," + projection));
        assertTrue(query.getValue().contains("   from " + schema));
        if (null != schemaPattern) {
            assertTrue(query.getValue().contains(" where " + schema + ".name like ?"));
        }
        if (null != catalog && catalog.isEmpty()) {
            assertTrue(query.getValue().contains(schema + ".name in "));
        } else if (hasCatalog && null == schemaPattern) {
            assertTrue(query.getValue().contains(schema + ".name not in "));
        }
        assertTrue(query.getValue().endsWith(" order by 2, 1"));
        verify(resultSet, never()).close();
        verify(statement, never()).close();
        verify(preparedStatement, never()).close();
        verify(connection, never()).setCatalog(anyString());
    }
}
