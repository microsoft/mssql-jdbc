/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.databasemetadata;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;

@RunWith(JUnitPlatform.class)
public class DatabaseMetaDataTest extends AbstractTest {
    
    /**
     * Verify DatabaseMetaData#isWrapperFor and DatabaseMetaData#unwrap.
     * 
     * @throws SQLException
     */
    @Test
    public void testDatabaseMetaDataWrapper() throws SQLException {
        try (Connection con = DriverManager.getConnection(connectionString)) {
            DatabaseMetaData databaseMetaData = con.getMetaData();
            assertTrue(databaseMetaData.isWrapperFor(DatabaseMetaData.class));
            assertSame(databaseMetaData, databaseMetaData.unwrap(DatabaseMetaData.class));
        }
    }

}
