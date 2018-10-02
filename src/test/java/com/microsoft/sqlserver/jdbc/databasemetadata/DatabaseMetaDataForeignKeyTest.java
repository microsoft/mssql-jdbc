/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
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

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerDatabaseMetaData;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;


/**
 * Test class for testing DatabaseMetaData with foreign keys.
 */
@RunWith(JUnitPlatform.class)
public class DatabaseMetaDataForeignKeyTest extends AbstractTest {

    private static String table1 = RandomUtil.getIdentifier("DatabaseMetaDataForeignKeyTest_table_1");
    private static String table2 = RandomUtil.getIdentifier("DatabaseMetaDataForeignKeyTest_table_2");
    private static String table3 = RandomUtil.getIdentifier("DatabaseMetaDataForeignKeyTest_table_3");
    private static String table4 = RandomUtil.getIdentifier("DatabaseMetaDataForeignKeyTest_table_4");
    private static String table5 = RandomUtil.getIdentifier("DatabaseMetaDataForeignKeyTest_table_5");

    private static String schema = null;
    private static String catalog = null;

    @BeforeAll
    public static void setupVariation() throws SQLException {
        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString);
                SQLServerStatement stmt = (SQLServerStatement) conn.createStatement()) {
            catalog = conn.getCatalog();
            schema = conn.getSchema();

            stmt.executeUpdate("if object_id('" + TestUtils.escapeSingleQuotes(table1)
                    + "','U') is not null drop table " + AbstractSQLGenerator.escapeIdentifier(table1));
            stmt.executeUpdate("if object_id('" + TestUtils.escapeSingleQuotes(table2)
                    + "','U') is not null drop table " + AbstractSQLGenerator.escapeIdentifier(table2));
            stmt.execute("Create table " + AbstractSQLGenerator.escapeIdentifier(table2)
                    + " (c21 int NOT NULL PRIMARY KEY)");

            stmt.executeUpdate("if object_id('" + TestUtils.escapeSingleQuotes(table3)
                    + "','U') is not null drop table " + AbstractSQLGenerator.escapeIdentifier(table3));
            stmt.execute("Create table " + AbstractSQLGenerator.escapeIdentifier(table3)
                    + " (c31 int NOT NULL PRIMARY KEY)");

            stmt.executeUpdate("if object_id('" + TestUtils.escapeSingleQuotes(table4)
                    + "','U') is not null drop table " + AbstractSQLGenerator.escapeIdentifier(table4));
            stmt.execute("Create table " + AbstractSQLGenerator.escapeIdentifier(table4)
                    + " (c41 int NOT NULL PRIMARY KEY)");

            stmt.executeUpdate("if object_id('" + TestUtils.escapeSingleQuotes(table5)
                    + "','U') is not null drop table " + AbstractSQLGenerator.escapeIdentifier(table5));
            stmt.execute("Create table " + AbstractSQLGenerator.escapeIdentifier(table5)
                    + " (c51 int NOT NULL PRIMARY KEY)");

            stmt.executeUpdate("if object_id('" + TestUtils.escapeSingleQuotes(table1)
                    + "','U') is not null drop table " + AbstractSQLGenerator.escapeIdentifier(table1));
            stmt.execute("Create table " + AbstractSQLGenerator.escapeIdentifier(table1) + " (c11 int primary key,"
                    + " c12 int FOREIGN KEY REFERENCES " + AbstractSQLGenerator.escapeIdentifier(table2)
                    + "(c21) ON DELETE no action ON UPDATE set default," + " c13 int FOREIGN KEY REFERENCES "
                    + AbstractSQLGenerator.escapeIdentifier(table3) + "(c31) ON DELETE cascade ON UPDATE set null,"
                    + " c14 int FOREIGN KEY REFERENCES " + AbstractSQLGenerator.escapeIdentifier(table4)
                    + "(c41) ON DELETE set null ON UPDATE cascade," + " c15 int FOREIGN KEY REFERENCES "
                    + AbstractSQLGenerator.escapeIdentifier(table5) + "(c51) ON DELETE set default ON UPDATE no action,"
                    + ")");
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.toString());
        }
    }

    @AfterAll
    public static void terminateVariation() throws SQLException {
        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString);
                SQLServerStatement stmt = (SQLServerStatement) conn.createStatement()) {

            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(table1), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(table2), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(table3), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(table4), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(table5), stmt);
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.toString());
        }
    }

    /**
     * test getImportedKeys() methods
     * 
     * @throws SQLException
     * @throws SQLTimeoutException
     */
    @Test
    public void testGetImportedKeys() throws SQLException {
        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString)) {
            SQLServerDatabaseMetaData dmd = (SQLServerDatabaseMetaData) conn.getMetaData();

            try (SQLServerResultSet rs1 = (SQLServerResultSet) dmd.getImportedKeys(null, null, table1);
                    SQLServerResultSet rs2 = (SQLServerResultSet) dmd.getImportedKeys(catalog, schema, table1);
                    SQLServerResultSet rs3 = (SQLServerResultSet) dmd.getImportedKeys(catalog, "", table1)) {

                validateGetImportedKeysResults(rs1);
                validateGetImportedKeysResults(rs2);
                validateGetImportedKeysResults(rs3);

                try {
                    dmd.getImportedKeys("", schema, table1);
                    fail(TestResource.getResource("R_expectedExceptionNotThrown"));
                } catch (SQLException e) {
                    assertTrue(e.getMessage().startsWith(TestResource.getResource("R_dbNameIsCurrentDB")));
                }
            }
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.toString());
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

        if (expectedRowCount != rowCount) {
            assertEquals(expectedRowCount, rowCount, TestResource.getResource("R_numKeysIncorrect"));
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
                {4, 3}, {2, 0}, {0, 2}, {3, 4}};

        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString)) {

            SQLServerDatabaseMetaData dmd = (SQLServerDatabaseMetaData) conn.getMetaData();

            for (int i = 0; i < tableNames.length; i++) {
                String pkTable = tableNames[i];
                try (SQLServerResultSet rs1 = (SQLServerResultSet) dmd.getExportedKeys(null, null, pkTable);
                        SQLServerResultSet rs2 = (SQLServerResultSet) dmd.getExportedKeys(catalog, schema, pkTable);
                        SQLServerResultSet rs3 = (SQLServerResultSet) dmd.getExportedKeys(catalog, "", pkTable)) {

                    rs1.next();
                    assertEquals(values[i][0], rs1.getInt("UPDATE_RULE"));
                    assertEquals(values[i][1], rs1.getInt("DELETE_RULE"));

                    rs2.next();
                    assertEquals(values[i][0], rs2.getInt("UPDATE_RULE"));
                    assertEquals(values[i][1], rs2.getInt("DELETE_RULE"));

                    rs3.next();
                    assertEquals(values[i][0], rs3.getInt("UPDATE_RULE"));
                    assertEquals(values[i][1], rs3.getInt("DELETE_RULE"));

                    try {
                        dmd.getExportedKeys("", schema, pkTable);
                        fail(TestResource.getResource("R_expectedExceptionNotThrown"));
                    } catch (SQLException e) {
                        assertTrue(e.getMessage().startsWith(TestResource.getResource("R_dbNameIsCurrentDB")));
                    }
                }
            }
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.toString());
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
                {4, 3}, {2, 0}, {0, 2}, {3, 4}};

        SQLServerDatabaseMetaData dmd = (SQLServerDatabaseMetaData) connection.getMetaData();

        for (int i = 0; i < tableNames.length; i++) {
            String pkTable = tableNames[i];
            try (SQLServerResultSet rs1 = (SQLServerResultSet) dmd.getCrossReference(null, null, pkTable, null, null,
                    fkTable);
                    SQLServerResultSet rs2 = (SQLServerResultSet) dmd.getCrossReference(catalog, schema, pkTable,
                            catalog, schema, fkTable);
                    SQLServerResultSet rs3 = (SQLServerResultSet) dmd.getCrossReference(catalog, "", pkTable, catalog,
                            "", fkTable)) {

                rs1.next();
                assertEquals(values[i][0], rs1.getInt("UPDATE_RULE"));
                assertEquals(values[i][1], rs1.getInt("DELETE_RULE"));

                rs2.next();
                assertEquals(values[i][0], rs2.getInt("UPDATE_RULE"));
                assertEquals(values[i][1], rs2.getInt("DELETE_RULE"));

                rs3.next();
                assertEquals(values[i][0], rs3.getInt("UPDATE_RULE"));
                assertEquals(values[i][1], rs3.getInt("DELETE_RULE"));

                try {
                    dmd.getCrossReference("", schema, pkTable, "", schema, fkTable);
                    fail(TestResource.getResource("R_expectedExceptionNotThrown"));
                } catch (SQLException e) {
                    assertEquals(TestResource.getResource("R_dbNameIsCurrentDB"), e.getMessage());
                }
            }
        }
    }
}
