/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ComparisonUtil;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.DBResultSet;
import com.microsoft.sqlserver.testframework.DBStatement;
import com.microsoft.sqlserver.testframework.DBTable;
import com.microsoft.sqlserver.testframework.sqlType.SqlType;


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
    public static void setUpConnection() {
        con = new DBConnection(connectionString);
        stmt = con.createStatement();
    }

    @AfterAll
    public static void closeConnection() throws SQLException {
        stmt.close();
        con.close();
    }

    @Test
    @DisplayName("BulkCopy:test no explicit column mapping")
    public void testNoExplicitCM() throws SQLException {
        DBTable destTable = null;
        try {
            // create dest table
            destTable = sourceTable.cloneSchema();
            stmt.createTable(destTable);

            // set up bulkCopy without explicit column mapping
            BulkCopyTestWrapper bulkWrapper = new BulkCopyTestWrapper(connectionString);
            bulkWrapper.setUsingConnection((0 == ThreadLocalRandom.current().nextInt(2)) ? true : false);
            BulkCopyTestUtil.performBulkCopy(bulkWrapper, sourceTable, destTable);
        } finally {
            TestUtils.dropTableIfExists(destTable.getEscapedTableName(), (Statement) stmt.product());
        }
    }

    @Test
    @DisplayName("BulkCopy:test explicit column mapping")
    public void testExplicitCM() throws SQLException {
        DBTable destTable = null;
        try {
            // create dest table
            destTable = sourceTable.cloneSchema();
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
        } finally {
            TestUtils.dropTableIfExists(destTable.getEscapedTableName(), (Statement) stmt.product());
        }
    }

    @Test
    @DisplayName("BulkCopy:test unicode column mapping")
    public void testUnicodeCM() throws SQLException {
        DBTable sourceTableUnicode = null;
        DBTable destTableUnicode = null;
        try {
            // create source unicode table
            sourceTableUnicode = new DBTable(true, true);
            stmt.createTable(sourceTableUnicode);

            // create destination unicode table with same schema as source
            destTableUnicode = sourceTableUnicode.cloneSchema();
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
                        bulkWrapper.setColumnMapping(sourceTableUnicode.getColumnName(i - 1),
                                destTableUnicode.getColumnName(i - 1));
                        break;

                    case 3:
                        bulkWrapper.setColumnMapping(sourceTableUnicode.getColumnName(i - 1), i);
                        break;
                }
            }
            BulkCopyTestUtil.performBulkCopy(bulkWrapper, sourceTableUnicode, destTableUnicode);
        } finally {
            TestUtils.dropTableIfExists(sourceTableUnicode.getEscapedTableName(), (Statement) stmt.product());
            TestUtils.dropTableIfExists(destTableUnicode.getEscapedTableName(), (Statement) stmt.product());
        }
    }

    @Test
    @DisplayName("BulkCopy:test repetitive column mapping")
    public void testRepetitiveCM() throws SQLException {
        DBTable sourceTable1 = null;
        DBTable destTable = null;
        try {
            // create source table
            sourceTable1 = new DBTable(true);
            stmt.createTable(sourceTable1);
            stmt.populateTable(sourceTable1);

            // create destination table with same schema as source
            destTable = sourceTable1.cloneSchema();

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
            bulkWrapper.setColumnMapping(1, 25);

            // perform bulkCopy without validating results or dropping destination table
            BulkCopyTestUtil.performBulkCopy(bulkWrapper, sourceTable1, destTable, false, false, false);
            try {
                validateValuesRepetitiveCM(con, sourceTable1, destTable);
            } catch (SQLException e) {
                MessageFormat form = new MessageFormat(TestResource.getResource("R_failedValidate"));
                Object[] msgArgs = {sourceTable1.getTableName() + " and" + destTable.getTableName()};

                fail(form.format(msgArgs) + "\n" + destTable.getTableName() + "\n" + e.getMessage());
            }
        } finally {
            TestUtils.dropTableIfExists(sourceTable1.getEscapedTableName(), (Statement) stmt.product());
            TestUtils.dropTableIfExists(destTable.getEscapedTableName(), (Statement) stmt.product());
        }
    }

    @Test
    @DisplayName("BulkCopy:test implicit mismatched column mapping")
    public void testImplicitMismatchCM() throws SQLException {
        DBTable destTable = null;
        try {
            // create non unicode dest table with different schema from source table
            destTable = new DBTable(true, false, true);
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
        } finally {
            TestUtils.dropTableIfExists(destTable.getEscapedTableName(), (Statement) stmt.product());
        }
    }

    @Test
    @DisplayName("BulkCopy:test invalid column mapping")
    public void testInvalidCM() throws SQLException {
        DBTable destTable = null;
        try {
            // create dest table
            destTable = sourceTable.cloneSchema();
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
        } finally {
            TestUtils.dropTableIfExists(destTable.getEscapedTableName(), (Statement) stmt.product());
        }
    }

    /**
     * validate if same values are in both source and destination table taking into account 1 extra column in
     * destination which should be a copy of first column of source.
     * 
     * @param con
     * @param sourceTable
     * @param destinationTable
     * @throws SQLException
     */
    private void validateValuesRepetitiveCM(DBConnection con, DBTable sourceTable,
            DBTable destinationTable) throws SQLException {
        try (DBStatement srcStmt = con.createStatement(); DBStatement dstStmt = con.createStatement();
                DBResultSet srcResultSet = srcStmt
                        .executeQuery("SELECT * FROM " + sourceTable.getEscapedTableName() + ";");
                DBResultSet dstResultSet = dstStmt
                        .executeQuery("SELECT * FROM " + destinationTable.getEscapedTableName() + ";")) {
            ResultSetMetaData sourceMeta = ((ResultSet) srcResultSet.product()).getMetaData();
            int totalColumns = sourceMeta.getColumnCount();

            // verify data from sourceType and resultSet
            int numRows = 0;
            while (srcResultSet.next() && dstResultSet.next()) {
                numRows++;
                for (int i = 1; i <= totalColumns; i++) {
                    Object srcValue, dstValue;
                    srcValue = srcResultSet.getObject(i);
                    dstValue = dstResultSet.getObject(i);
                    ComparisonUtil.compareExpectedAndActual(sourceMeta.getColumnType(i), srcValue, dstValue);

                    // compare value of first column of source with extra column in destination
                    if (1 == i) {
                        Object srcValueFirstCol = srcResultSet.getObject(i);
                        Object dstValLastCol = dstResultSet.getObject(totalColumns + 1);
                        ComparisonUtil.compareExpectedAndActual(sourceMeta.getColumnType(i), srcValueFirstCol,
                                dstValLastCol);
                    }
                }
            }

            // verify number of rows and columns
            assertTrue(((ResultSet) dstResultSet.product()).getMetaData().getColumnCount() == totalColumns + 1);
            assertTrue(sourceTable.getTotalRows() == numRows);
            assertTrue(destinationTable.getTotalRows() == numRows);
        }
    }
}
