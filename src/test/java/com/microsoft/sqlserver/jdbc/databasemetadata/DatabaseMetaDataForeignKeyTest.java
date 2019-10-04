/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.databasemetadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


/**
 * Test class for testing DatabaseMetaData with foreign keys.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDW)
public class DatabaseMetaDataForeignKeyTest extends AbstractTest {

    private static String table1 = RandomUtil.getIdentifier("DatabaseMetaDataForeignKeyTest_table_1");
    private static String table2 = RandomUtil.getIdentifier("DatabaseMetaDataForeignKeyTest_table_2");
    private static String table3 = RandomUtil.getIdentifier("DatabaseMetaDataForeignKeyTest_table_3");
    private static String table4 = RandomUtil.getIdentifier("DatabaseMetaDataForeignKeyTest_table_4");
    private static String table5 = RandomUtil.getIdentifier("DatabaseMetaDataForeignKeyTest_table_5");
    private static String PKTable1 = RandomUtil.getIdentifier("DatabaseMetaDataForeignKeyTest_PKTable1");
    private static String FKTable1 = RandomUtil.getIdentifier("DatabaseMetaDataForeignKeyTest_FKTable1");
    private static String PKTable2 = RandomUtil.getIdentifier("DatabaseMetaDataForeignKeyTest_PKTable2");
    private static String FKTable2 = RandomUtil.getIdentifier("DatabaseMetaDataForeignKeyTest_FKTable2");

    private static String schema = null;
    private static String anotherSchema = RandomUtil.getIdentifier("anotherSchema");
    private static String catalog = null;

    @BeforeAll
    public static void setupVariation() throws SQLException {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            catalog = conn.getCatalog();
            schema = conn.getSchema();

            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(table1), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(table2), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(table3), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(table4), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(table5), stmt);

            stmt.execute("Create table " + AbstractSQLGenerator.escapeIdentifier(table2)
                    + " (c21 int NOT NULL PRIMARY KEY)");

            stmt.execute("Create table " + AbstractSQLGenerator.escapeIdentifier(table3)
                    + " (c31 int NOT NULL PRIMARY KEY)");

            stmt.execute("Create table " + AbstractSQLGenerator.escapeIdentifier(table4)
                    + " (c41 int NOT NULL PRIMARY KEY)");

            stmt.execute("Create table " + AbstractSQLGenerator.escapeIdentifier(table5)
                    + " (c51 int NOT NULL PRIMARY KEY)");

            stmt.execute("Create table " + AbstractSQLGenerator.escapeIdentifier(table1) + " (c11 int primary key,"
                    + " c12 int FOREIGN KEY REFERENCES " + AbstractSQLGenerator.escapeIdentifier(table2)
                    + "(c21) ON DELETE no action ON UPDATE set default," + " c13 int FOREIGN KEY REFERENCES "
                    + AbstractSQLGenerator.escapeIdentifier(table3) + "(c31) ON DELETE cascade ON UPDATE set null,"
                    + " c14 int FOREIGN KEY REFERENCES " + AbstractSQLGenerator.escapeIdentifier(table4)
                    + "(c41) ON DELETE set null ON UPDATE cascade," + " c15 int FOREIGN KEY REFERENCES "
                    + AbstractSQLGenerator.escapeIdentifier(table5) + "(c51) ON DELETE set default ON UPDATE no action,"
                    + ")");

            stmt.execute("CREATE SCHEMA " + AbstractSQLGenerator.escapeIdentifier(anotherSchema));

            stmt.execute("Create table " + AbstractSQLGenerator.escapeIdentifier(PKTable1)
                    + " (col int NOT NULL PRIMARY KEY)");

            stmt.execute("Create table " + AbstractSQLGenerator.escapeIdentifier(anotherSchema) + "."
                    + AbstractSQLGenerator.escapeIdentifier(PKTable2) + " (col int NOT NULL PRIMARY KEY)");

            stmt.execute("Create table " + AbstractSQLGenerator.escapeIdentifier(schema) + "."
                    + AbstractSQLGenerator.escapeIdentifier(FKTable1)
                    + " (col int, CONSTRAINT fk_DuplicateName FOREIGN KEY ([col]) REFERENCES "
                    + AbstractSQLGenerator.escapeIdentifier(schema) + "."
                    + AbstractSQLGenerator.escapeIdentifier(PKTable1) + "([col])" + ")");

            stmt.execute("Create table " + AbstractSQLGenerator.escapeIdentifier(anotherSchema) + "."
                    + AbstractSQLGenerator.escapeIdentifier(FKTable2)
                    + " (col int, CONSTRAINT fk_DuplicateName FOREIGN KEY ([col]) REFERENCES "
                    + AbstractSQLGenerator.escapeIdentifier(anotherSchema) + "."
                    + AbstractSQLGenerator.escapeIdentifier(PKTable2) + "([col])" + ")");

        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
        }
    }

    @AfterAll
    public static void terminateVariation() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(table1), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(table2), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(table3), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(table4), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(table5), stmt);

            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(FKTable1), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(anotherSchema) + "."
                    + AbstractSQLGenerator.escapeIdentifier(FKTable2), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(anotherSchema) + "."
                    + AbstractSQLGenerator.escapeIdentifier(PKTable2), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(PKTable1), stmt);

            TestUtils.dropSchemaIfExists(anotherSchema, stmt);

        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
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
        try (Connection conn = getConnection()) {
            DatabaseMetaData dmd = conn.getMetaData();

            try (ResultSet rs1 = dmd.getImportedKeys(null, null, table1);
                    ResultSet rs2 = dmd.getImportedKeys(catalog, schema, table1);
                    ResultSet rs3 = dmd.getImportedKeys(catalog, "", table1)) {

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
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
        }
    }

    @Test
    /**
     * test getImportedKeys does not return duplicate row if multiple FKs have same name
     * 
     * @throws SQLException
     * @throws SQLTimeoutException
     */
    public void validateDuplicateForeignKeys() throws SQLException {
        int expectedRowCount = 1;
        int rowCount = 0;

        try (Connection conn = getConnection()) {
            DatabaseMetaData dmd = conn.getMetaData();

            try (ResultSet rs = dmd.getImportedKeys(null, null, FKTable1)) {
                while (rs.next()) {
                    rowCount++;
                }
            }
            if (expectedRowCount != rowCount) {
                assertEquals(expectedRowCount, rowCount, TestResource.getResource("R_numKeysIncorrect"));
            }
        }
    }

    private void validateGetImportedKeysResults(ResultSet rs) throws SQLException {
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

        try (Connection conn = getConnection()) {
            DatabaseMetaData dmd = conn.getMetaData();

            for (int i = 0; i < tableNames.length; i++) {
                String pkTable = tableNames[i];
                try (ResultSet rs1 = dmd.getExportedKeys(null, null, pkTable);
                        ResultSet rs2 = dmd.getExportedKeys(catalog, schema, pkTable);
                        ResultSet rs3 = dmd.getExportedKeys(catalog, "", pkTable)) {

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
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
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

        DatabaseMetaData dmd = connection.getMetaData();

        for (int i = 0; i < tableNames.length; i++) {
            String pkTable = tableNames[i];
            try (ResultSet rs1 = dmd.getCrossReference(null, null, pkTable, null, null, fkTable);
                    ResultSet rs2 = dmd.getCrossReference(catalog, schema, pkTable, catalog, schema, fkTable);
                    ResultSet rs3 = dmd.getCrossReference(catalog, "", pkTable, catalog, "", fkTable)) {

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
