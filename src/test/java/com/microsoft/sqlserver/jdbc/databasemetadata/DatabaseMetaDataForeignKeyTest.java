/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.databasemetadata;

import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerDatabaseMetaData;
import com.microsoft.sqlserver.jdbc.SQLServerException;
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

    @Test
    public void testGetImportedKeys() throws SQLServerException {
        SQLServerDatabaseMetaData dmd = (SQLServerDatabaseMetaData) connection.getMetaData();

        SQLServerResultSet rs1 = (SQLServerResultSet) dmd.getImportedKeys(null, null, table1);
        validateResults(rs1);

//        SQLServerResultSet rs2 = (SQLServerResultSet) dmd.getImportedKeys("", "", table1);
//        validateResults(rs2);

        SQLServerResultSet rs3 = (SQLServerResultSet) dmd.getImportedKeys("test", "dbo", table1);
        validateResults(rs3);

    }
    
    private void validateResults(SQLServerResultSet rs) throws SQLServerException {
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

}
