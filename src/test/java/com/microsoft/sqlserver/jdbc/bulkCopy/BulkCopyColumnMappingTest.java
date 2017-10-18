/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import static org.junit.jupiter.api.Assertions.fail;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.DBResultSet;
import com.microsoft.sqlserver.testframework.DBStatement;
import com.microsoft.sqlserver.testframework.DBTable;
import com.microsoft.sqlserver.testframework.sqlType.SqlType;
import com.microsoft.sqlserver.testframework.util.ComparisonUtil;

/**
 * Test BulkCopy Column Mapping
 */
@RunWith(JUnitPlatform.class)
@DisplayName("BulkCopy Column Mapping Test")
public class BulkCopyColumnMappingTest extends BulkCopyTestSetUp {

    static DBConnection con = null;
    static DBStatement stmt = null;

    /**
     * Create connection, statement and generate path of resource file
     */
    @BeforeAll
    static void setUpConnection() {
        con = new DBConnection(connectionString);
        stmt = con.createStatement();
    }

    @AfterAll
    static void closeConnection() throws SQLException {
        stmt.close();
        con.close();
    }

    @Test
    @DisplayName("BulkCopy:test no explicit column mapping")
    void testNoExplicitCM() {

        // create dest table
        DBTable destTable = sourceTable.cloneSchema();
        stmt.createTable(destTable);

        // set up bulkCopy without explicit column mapping
        BulkCopyTestWrapper bulkWrapper = new BulkCopyTestWrapper(connectionString);
        bulkWrapper.setUsingConnection((0 == ThreadLocalRandom.current().nextInt(2)) ? true : false);
        BulkCopyTestUtil.performBulkCopy(bulkWrapper, sourceTable, destTable);
    }

    @Test
    @DisplayName("BulkCopy:test explicit column mapping")
    void testExplicitCM() {

        // create dest table
        DBTable destTable = sourceTable.cloneSchema();
        stmt.createTable(destTable);

        // set up bulkCopy with explicit column mapping
        BulkCopyTestWrapper bulkWrapper = new BulkCopyTestWrapper(connectionString);
        bulkWrapper.setUsingConnection((0 == ThreadLocalRandom.current().nextInt(2)) ? true : false);
        for (int i = 1; i <= destTable.totalColumns(); i++) {
            int select = i % 4;

            switch (select) {
                case 0:
                    bulkWrapper.setColumnMapping(i, i);
                    break;

                case 1:
                    bulkWrapper.setColumnMapping(i, destTable.getColumnName(i - 1));
                    break;

                case 2:
                    bulkWrapper.setColumnMapping(sourceTable.getColumnName(i - 1), destTable.getColumnName(i - 1));
                    break;

                case 3:
                    bulkWrapper.setColumnMapping(sourceTable.getColumnName(i - 1), i);
                    break;
            }
        }
        BulkCopyTestUtil.performBulkCopy(bulkWrapper, sourceTable, destTable);
    }

    @Test
    @DisplayName("BulkCopy:test unicode column mapping")
    void testUnicodeCM() {

        // create source unicode table
        DBTable sourceTableUnicode = new DBTable(true, true);
        stmt.createTable(sourceTableUnicode);

        // create destication unicode table with same schema as source
        DBTable destTableUnicode = sourceTableUnicode.cloneSchema();
        stmt.createTable(destTableUnicode);

        // set up bulkCopy with explicit column mapping
        BulkCopyTestWrapper bulkWrapper = new BulkCopyTestWrapper(connectionString);
        bulkWrapper.setUsingConnection((0 == ThreadLocalRandom.current().nextInt(2)) ? true : false);
        for (int i = 1; i <= destTableUnicode.totalColumns(); i++) {
            int select = i % 4;

            switch (select) {
                case 0:
                    bulkWrapper.setColumnMapping(i, i);
                    break;

                case 1:
                    bulkWrapper.setColumnMapping(i, destTableUnicode.getColumnName(i - 1));
                    break;

                case 2:
                    bulkWrapper.setColumnMapping(sourceTableUnicode.getColumnName(i - 1), destTableUnicode.getColumnName(i - 1));
                    break;

                case 3:
                    bulkWrapper.setColumnMapping(sourceTableUnicode.getColumnName(i - 1), i);
                    break;
            }
        }
        BulkCopyTestUtil.performBulkCopy(bulkWrapper, sourceTableUnicode, destTableUnicode);
        dropTable(sourceTableUnicode.getEscapedTableName());
    }

    @Test
    @DisplayName("BulkCopy:test repetative column mapping")
    void testRepetativeCM() {

        // create source table
        DBTable sourceTable1 = new DBTable(true);
        stmt.createTable(sourceTable1);
        stmt.populateTable(sourceTable1);

        // create destication table with same shcema as source
        DBTable destTable = sourceTable1.cloneSchema();

        // add 1 column to destination which will be duplicate of first source column
        SqlType sqlType = sourceTable1.getSqlType(0);
        destTable.addColumn(sqlType);
        stmt.createTable(destTable);

        // set up bulkCopy with explicit column mapping
        BulkCopyTestWrapper bulkWrapper = new BulkCopyTestWrapper(connectionString);
        bulkWrapper.setUsingConnection((0 == ThreadLocalRandom.current().nextInt(2)) ? true : false);
        for (int i = 1; i <= sourceTable1.totalColumns(); i++) {
            int select = i % 4;

            switch (select) {
                case 0:
                    bulkWrapper.setColumnMapping(i, i);
                    break;

                case 1:
                    bulkWrapper.setColumnMapping(i, destTable.getColumnName(i - 1));
                    break;

                case 2:
                    bulkWrapper.setColumnMapping(sourceTable1.getColumnName(i - 1), destTable.getColumnName(i - 1));
                    break;

                case 3:
                    bulkWrapper.setColumnMapping(sourceTable1.getColumnName(i - 1), i);
                    break;
            }
        }

        // add column mapping for duplicate column in destination
        bulkWrapper.setColumnMapping(1, 24);

        // perform bulkCopy without validating results or dropping destination table
        BulkCopyTestUtil.performBulkCopy(bulkWrapper, sourceTable1, destTable, false, false, false);
        try {
            validateValuesRepetativeCM(con, sourceTable1, destTable);
        }
        catch (SQLException e) {
            fail("failed to validate values in " + sourceTable1.getTableName() + " and " + destTable.getTableName() + "\n" + e.getMessage());
        }
        dropTable(sourceTable1.getEscapedTableName());
        dropTable(destTable.getEscapedTableName());
    }

    @Test
    @DisplayName("BulkCopy:test implicit mismatched column mapping")
    void testImplicitMismatchCM() {

        // create non unicode dest table with different schema from source table
        DBTable destTable = new DBTable(true, false, true);
        stmt.createTable(destTable);

        // set up bulkCopy with explicit column mapping
        BulkCopyTestWrapper bulkWrapper = new BulkCopyTestWrapper(connectionString);
        bulkWrapper.setUsingConnection((0 == ThreadLocalRandom.current().nextInt(2)) ? true : false);
        for (int i = 1; i <= destTable.totalColumns(); i++) {
            int select = i % 4;

            switch (select) {
                case 0:
                    bulkWrapper.setColumnMapping(i, i);
                    break;

                case 1:
                    bulkWrapper.setColumnMapping(i, destTable.getColumnName(i - 1));
                    break;

                case 2:
                    bulkWrapper.setColumnMapping(sourceTable.getColumnName(i - 1), destTable.getColumnName(i - 1));
                    break;

                case 3:
                    bulkWrapper.setColumnMapping(sourceTable.getColumnName(i - 1), i);
                    break;
            }
        }
        BulkCopyTestUtil.performBulkCopy(bulkWrapper, sourceTable, destTable, true, true);
    }

    @Test
    @DisplayName("BulkCopy:test invalid column mapping")
    void testInvalidCM() {

        // create dest table
        DBTable destTable = sourceTable.cloneSchema();
        stmt.createTable(destTable);

        // set up bulkCopy with wrong column names
        BulkCopyTestWrapper bulkWrapper = new BulkCopyTestWrapper(connectionString);
        bulkWrapper.setUsingConnection((0 == ThreadLocalRandom.current().nextInt(2)) ? true : false);
        bulkWrapper.setColumnMapping("wrongFirst", "wrongSecond");
        BulkCopyTestUtil.performBulkCopy(bulkWrapper, sourceTable, destTable, true, true);

        // create dest table
        destTable = sourceTable.cloneSchema();
        stmt.createTable(destTable);

        // set up bulkCopy with invalid ordinal, column no 65 does not exist
        bulkWrapper = new BulkCopyTestWrapper(connectionString);
        bulkWrapper.setUsingConnection((0 == ThreadLocalRandom.current().nextInt(2)) ? true : false);
        bulkWrapper.setColumnMapping(sourceTable.getColumnName(1), 65);
        BulkCopyTestUtil.performBulkCopy(bulkWrapper, sourceTable, destTable, true, true);

        // create dest table
        destTable = sourceTable.cloneSchema();
        stmt.createTable(destTable);

        // set up bulkCopy with invalid ordinal, column no 42 does not exist
        bulkWrapper = new BulkCopyTestWrapper(connectionString);
        bulkWrapper.setUsingConnection((0 == ThreadLocalRandom.current().nextInt(2)) ? true : false);
        bulkWrapper.setColumnMapping(42, destTable.getColumnName(1));
        BulkCopyTestUtil.performBulkCopy(bulkWrapper, sourceTable, destTable, true, true);

        // create dest table
        destTable = sourceTable.cloneSchema();
        stmt.createTable(destTable);

        // set up bulkCopy with invalid ordinal, column no 42 and 65 do not exist
        bulkWrapper = new BulkCopyTestWrapper(connectionString);
        bulkWrapper.setUsingConnection((0 == ThreadLocalRandom.current().nextInt(2)) ? true : false);
        bulkWrapper.setColumnMapping(42, 65);
        BulkCopyTestUtil.performBulkCopy(bulkWrapper, sourceTable, destTable, true, true);

        // create dest table
        destTable = sourceTable.cloneSchema();
        stmt.createTable(destTable);

        // set up bulkCopy while passing empty string as column mapping
        bulkWrapper = new BulkCopyTestWrapper(connectionString);
        bulkWrapper.setUsingConnection((0 == ThreadLocalRandom.current().nextInt(2)) ? true : false);
        bulkWrapper.setColumnMapping(sourceTable.getColumnName(1), "     ");
        BulkCopyTestUtil.performBulkCopy(bulkWrapper, sourceTable, destTable, true, true);

        // create dest table
        destTable = sourceTable.cloneSchema();
        stmt.createTable(destTable);

        // set up bulkCopy with 0 ordinal column mapping
        bulkWrapper = new BulkCopyTestWrapper(connectionString);
        bulkWrapper.setUsingConnection((0 == ThreadLocalRandom.current().nextInt(2)) ? true : false);
        bulkWrapper.setColumnMapping(0, 0);
        BulkCopyTestUtil.performBulkCopy(bulkWrapper, sourceTable, destTable, true, true);

        // create dest table
        destTable = sourceTable.cloneSchema();
        stmt.createTable(destTable);

        // set up bulkCopy with negative ordinal column mapping
        bulkWrapper = new BulkCopyTestWrapper(connectionString);
        bulkWrapper.setUsingConnection((0 == ThreadLocalRandom.current().nextInt(2)) ? true : false);
        bulkWrapper.setColumnMapping(-3, -6);
        BulkCopyTestUtil.performBulkCopy(bulkWrapper, sourceTable, destTable, true, true);

        // create dest table
        destTable = sourceTable.cloneSchema();
        stmt.createTable(destTable);

        // set up bulkCopy with Integer.MIN_VALUE and Integer.MAX_VALUE column mapping
        bulkWrapper = new BulkCopyTestWrapper(connectionString);
        bulkWrapper.setUsingConnection((0 == ThreadLocalRandom.current().nextInt(2)) ? true : false);
        bulkWrapper.setColumnMapping(Integer.MIN_VALUE, Integer.MAX_VALUE);
        BulkCopyTestUtil.performBulkCopy(bulkWrapper, sourceTable, destTable, true, true);

    }

    /**
     * validate if same values are in both source and destination table taking into account 1 extra column in destination which should be a copy of
     * first column of source.
     * 
     * @param con
     * @param sourceTable
     * @param destinationTable
     * @throws SQLException
     */
    private void validateValuesRepetativeCM(DBConnection con,
            DBTable sourceTable,
            DBTable destinationTable) throws SQLException {
        try(DBStatement srcStmt = con.createStatement();
	        DBStatement dstStmt = con.createStatement();
	        DBResultSet srcResultSet = srcStmt.executeQuery("SELECT * FROM " + sourceTable.getEscapedTableName() + ";");
	        DBResultSet dstResultSet = dstStmt.executeQuery("SELECT * FROM " + destinationTable.getEscapedTableName() + ";")) {
	        ResultSetMetaData sourceMeta = ((ResultSet) srcResultSet.product()).getMetaData();
	        int totalColumns = sourceMeta.getColumnCount();
	
	        // verify data from sourceType and resultSet
	        while (srcResultSet.next() && dstResultSet.next()) {
	            for (int i = 1; i <= totalColumns; i++) {
	                // TODO: check row and column count in both the tables
	
	                Object srcValue, dstValue;
	                srcValue = srcResultSet.getObject(i);
	                dstValue = dstResultSet.getObject(i);
	                ComparisonUtil.compareExpectedAndActual(sourceMeta.getColumnType(i), srcValue, dstValue);
	
	                // compare value of first column of source with extra column in destination
	                if (1 == i) {
	                    Object srcValueFirstCol = srcResultSet.getObject(i);
	                    Object dstValLastCol = dstResultSet.getObject(totalColumns + 1);
	                    ComparisonUtil.compareExpectedAndActual(sourceMeta.getColumnType(i), srcValueFirstCol, dstValLastCol);
	                }
	            }
	        }
        }
    }

    private void dropTable(String tableName) {

        String dropSQL = "DROP TABLE [dbo]." + tableName;
        try {
            stmt.execute(dropSQL);
        }
        catch (SQLException e) {
            fail("table " + tableName + " not dropped\n" + e.getMessage());
        }
    }

}
