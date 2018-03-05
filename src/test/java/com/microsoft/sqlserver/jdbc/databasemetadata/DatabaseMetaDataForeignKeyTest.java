/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.databasemetadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerDatabaseMetaData;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Utils;

/**
 * Test class for testing DatabaseMetaData with foreign keys.
 */
@RunWith(JUnitPlatform.class)
public class DatabaseMetaDataForeignKeyTest extends AbstractTest {
    private static SQLServerConnection conn = null;
    private static SQLServerStatement stmt = null;

    private static String table1 = "DatabaseMetaDataForeignKeyTest_table_1";
    private static String table2 = "DatabaseMetaDataForeignKeyTest_table_2";
    private static String table3 = "DatabaseMetaDataForeignKeyTest_table_3";
    private static String table4 = "DatabaseMetaDataForeignKeyTest_table_4";
    private static String table5 = "DatabaseMetaDataForeignKeyTest_table_5";
    
    private static String schema = null;
    private static String catalog = null;
    
    private static final String EXPECTED_ERROR_MESSAGE = "An object or column name is missing or empty.";
    private static final String EXPECTED_ERROR_MESSAGE2 = "The database name component of the object qualifier must be the name of the current database.";

    
    @BeforeAll
    private static void setupVariation() throws SQLException {
        conn = (SQLServerConnection) DriverManager.getConnection(connectionString);
        SQLServerStatement stmt = (SQLServerStatement) conn.createStatement();

        catalog = conn.getCatalog();
        schema = conn.getSchema();

        connection.createStatement().executeUpdate("if object_id('" + table1 + "','U') is not null drop table " + table1);

        connection.createStatement().executeUpdate("if object_id('" + table2 + "','U') is not null drop table " + table2);
        stmt.execute("Create table " + table2 + " (c21 int NOT NULL PRIMARY KEY)");

        connection.createStatement().executeUpdate("if object_id('" + table3 + "','U') is not null drop table " + table3);
        stmt.execute("Create table " + table3 + " (c31 int NOT NULL PRIMARY KEY)");

        connection.createStatement().executeUpdate("if object_id('" + table4 + "','U') is not null drop table " + table4);
        stmt.execute("Create table " + table4 + " (c41 int NOT NULL PRIMARY KEY)");

        connection.createStatement().executeUpdate("if object_id('" + table5 + "','U') is not null drop table " + table5);
        stmt.execute("Create table " + table5 + " (c51 int NOT NULL PRIMARY KEY)");

        connection.createStatement().executeUpdate("if object_id('" + table1 + "','U') is not null drop table " + table1);
        stmt.execute("Create table " + table1 + " (c11 int primary key," 
                + " c12 int FOREIGN KEY REFERENCES " + table2 + "(c21) ON DELETE no action ON UPDATE set default," 
                + " c13 int FOREIGN KEY REFERENCES " + table3 + "(c31) ON DELETE cascade ON UPDATE set null," 
                + " c14 int FOREIGN KEY REFERENCES " + table4 + "(c41) ON DELETE set null ON UPDATE cascade," 
                + " c15 int FOREIGN KEY REFERENCES " + table5 + "(c51) ON DELETE set default ON UPDATE no action," 
                + ")");
    }

    @AfterAll
    private static void terminateVariation() throws SQLException {
        conn = (SQLServerConnection) DriverManager.getConnection(connectionString);
        stmt = (SQLServerStatement) conn.createStatement();

        Utils.dropTableIfExists(table1, stmt);
        Utils.dropTableIfExists(table2, stmt);
        Utils.dropTableIfExists(table3, stmt);
        Utils.dropTableIfExists(table4, stmt);
        Utils.dropTableIfExists(table5, stmt);
    }

    /**
     * test getImportedKeys() methods
     * 
     * @throws SQLException
     * @throws SQLTimeoutException 
     */
    @Test
    public void testGetImportedKeys() throws SQLException {
        SQLServerDatabaseMetaData dmd = (SQLServerDatabaseMetaData) connection.getMetaData();

        SQLServerResultSet rs1 = (SQLServerResultSet) dmd.getImportedKeys(null, null, table1);
        validateGetImportedKeysResults(rs1);

        SQLServerResultSet rs2 = (SQLServerResultSet) dmd.getImportedKeys(catalog, schema, table1);
        validateGetImportedKeysResults(rs2);

        SQLServerResultSet rs3 = (SQLServerResultSet) dmd.getImportedKeys(catalog, "", table1);
        validateGetImportedKeysResults(rs3);

        try {
            dmd.getImportedKeys("", schema, table1);
            fail("Exception is not thrown.");
        }
        catch (SQLException e) {
            assertTrue(e.getMessage().startsWith(EXPECTED_ERROR_MESSAGE));
        }
    }

    private void validateGetImportedKeysResults(SQLServerResultSet rs) throws SQLException {
        int expectedRowCount = 4;
        int rowCount = 0;
        
        rs.next();
        assertEquals(4, rs.getInt("UPDATE_RULE"));
        assertEquals(3, rs.getInt("DELETE_RULE"));
        rowCount++;
        
        rs.next();
        assertEquals(2, rs.getInt("UPDATE_RULE"));
        assertEquals(0, rs.getInt("DELETE_RULE"));
        rowCount++;
        
        rs.next();
        assertEquals(0, rs.getInt("UPDATE_RULE"));
        assertEquals(2, rs.getInt("DELETE_RULE"));
        rowCount++;
        
        rs.next();
        assertEquals(3, rs.getInt("UPDATE_RULE"));
        assertEquals(4, rs.getInt("DELETE_RULE"));
        rowCount++;

        if(expectedRowCount != rowCount) {
            assertEquals(expectedRowCount, rowCount, "number of foreign key entries is incorrect.");
        }
    }
    
    /**
     * test getExportedKeys() methods
     * 
     * @throws SQLException
     * @throws SQLTimeoutException 
     */
    @Test
    public void testGetExportedKeys() throws SQLException {
        String[] tableNames = {table2, table3, table4, table5};
        int[][] values = {
                // expected UPDATE_RULE, expected DELETE_RULE
                {4, 3}, 
                {2, 0}, 
                {0, 2}, 
                {3, 4}
                };

        SQLServerDatabaseMetaData dmd = (SQLServerDatabaseMetaData) connection.getMetaData();

        for (int i = 0; i < tableNames.length; i++) {
            String pkTable = tableNames[i];
            SQLServerResultSet rs1 = (SQLServerResultSet) dmd.getExportedKeys(null, null, pkTable);
            rs1.next();
            assertEquals(values[i][0], rs1.getInt("UPDATE_RULE"));
            assertEquals(values[i][1], rs1.getInt("DELETE_RULE"));

            SQLServerResultSet rs2 = (SQLServerResultSet) dmd.getExportedKeys(catalog, schema, pkTable);
            rs2.next();
            assertEquals(values[i][0], rs2.getInt("UPDATE_RULE"));
            assertEquals(values[i][1], rs2.getInt("DELETE_RULE"));

            SQLServerResultSet rs3 = (SQLServerResultSet) dmd.getExportedKeys(catalog, "", pkTable);
            rs3.next();
            assertEquals(values[i][0], rs3.getInt("UPDATE_RULE"));
            assertEquals(values[i][1], rs3.getInt("DELETE_RULE"));

            try {
                dmd.getExportedKeys("", schema, pkTable);
                fail("Exception is not thrown.");
            }
            catch (SQLException e) {
                assertTrue(e.getMessage().startsWith(EXPECTED_ERROR_MESSAGE));
            }
        }
    }
    
    /**
     * test getCrossReference() methods
     * 
     * @throws SQLException
     * @throws SQLTimeoutException 
     */
    @Test
    public void testGetCrossReference() throws SQLException {
        String fkTable = table1;
        String[] tableNames = {table2, table3, table4, table5};
        int[][] values = {
                // expected UPDATE_RULE, expected DELETE_RULE
                {4, 3}, 
                {2, 0}, 
                {0, 2}, 
                {3, 4}
                };

        SQLServerDatabaseMetaData dmd = (SQLServerDatabaseMetaData) connection.getMetaData();

        for (int i = 0; i < tableNames.length; i++) {
            String pkTable = tableNames[i];
            SQLServerResultSet rs1 = (SQLServerResultSet) dmd.getCrossReference(null, null, pkTable, null, null, fkTable);
            rs1.next();
            assertEquals(values[i][0], rs1.getInt("UPDATE_RULE"));
            assertEquals(values[i][1], rs1.getInt("DELETE_RULE"));

            SQLServerResultSet rs2 = (SQLServerResultSet) dmd.getCrossReference(catalog, schema, pkTable, catalog, schema, fkTable);
            rs2.next();
            assertEquals(values[i][0], rs2.getInt("UPDATE_RULE"));
            assertEquals(values[i][1], rs2.getInt("DELETE_RULE"));

            SQLServerResultSet rs3 = (SQLServerResultSet) dmd.getCrossReference(catalog, "", pkTable, catalog, "", fkTable);
            rs3.next();
            assertEquals(values[i][0], rs3.getInt("UPDATE_RULE"));
            assertEquals(values[i][1], rs3.getInt("DELETE_RULE"));

            try {
                dmd.getCrossReference("", schema, pkTable, "", schema, fkTable);
                fail("Exception is not thrown.");
            }
            catch (SQLException e) {
                assertEquals(EXPECTED_ERROR_MESSAGE2, e.getMessage());
            }
        }
    }
}
