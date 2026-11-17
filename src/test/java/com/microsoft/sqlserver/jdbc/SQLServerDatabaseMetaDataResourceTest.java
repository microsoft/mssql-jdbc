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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


public class SQLServerDatabaseMetaDataResourceTest extends AbstractTest {
    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testIndexInfoAzureDWFallbackCatalogRestorationFailure() throws SQLException {
        try (SQLServerConnection con = getConnection()) {
            SQLServerConnection metadataConnection = mock(SQLServerConnection.class);
            PreparedStatement storedProcedure = mock(PreparedStatement.class);
            SQLServerException restoreFailure = new SQLServerException("Catalog restoration failed", null);
            String catalog = con.getCatalog();
            String originalCatalog = catalog + "_previous";
            List<PreparedStatement> statements = new ArrayList<>();
            when(metadataConnection.getCatalog()).thenReturn(originalCatalog, catalog);
            when(metadataConnection.isAzureDW()).thenReturn(true);
            when(storedProcedure.executeQuery()).thenThrow(new SQLServerException("Stored procedure failed", null));
            when(metadataConnection.prepareStatement(anyString())).thenAnswer(invocation -> {
                String sql = invocation.getArgument(0);
                if ("EXEC sp_statistics ?,?,?,?,?,?;".equals(sql)) {
                    return storedProcedure;
                }
                PreparedStatement statement = con.prepareStatement(sql);
                statements.add(statement);
                return statement;
            });
            doThrow(restoreFailure).when(metadataConnection).setCatalog(originalCatalog);
            try {
                SQLServerDatabaseMetaData metadata = new SQLServerDatabaseMetaData(metadataConnection);
                assertSame(restoreFailure, assertThrows(SQLServerException.class,
                        () -> metadata.getIndexInfo(catalog, "dbo", "unused_index_metadata_table", false, false)));
                assertEquals(1, statements.size());
                assertTrue(statements.get(0).isClosed(),
                        "Catalog restoration failure must close the real fallback statement");
            } finally {
                for (PreparedStatement statement : statements) {
                    statement.close();
                }
            }
        }
    }
}
