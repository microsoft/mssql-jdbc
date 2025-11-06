/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import com.microsoft.sqlserver.jdbc.*;
import com.microsoft.sqlserver.testframework.*;
import com.microsoft.sqlserver.testframework.sqlType.SqlVarChar;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.fail;


/**
 * Test BulkCopy Column Order Hints
 */
@RunWith(JUnitPlatform.class)
@DisplayName("BulkCopy Column Order Hints Test")
public class BulkCopyColumnOrderHintTest extends BulkCopyTestSetUp {
    private String prcdb = RandomUtil.getIdentifier("BulkCopy_PRC_DB");

    @Test
    @DisplayName("BulkCopy:test no column order hints")
    @Tag(Constants.xAzureSQLDW)
    public void testNoCOH() throws SQLException {
        try (DBConnection con = new DBConnection(connectionString); DBStatement stmt = con.createStatement()) {
            DBTable destTable = null;
            try {
                // create destination table
                destTable = sourceTable.cloneSchema();
                stmt.createTable(destTable);

                BulkCopyTestWrapper bulkWrapper = new BulkCopyTestWrapper(connectionString);
                bulkWrapper.setUsingConnection((0 == Constants.RANDOM.nextInt(2)) ? true : false, ds);
                bulkWrapper.setUsingXAConnection((0 == Constants.RANDOM.nextInt(2)) ? true : false, dsXA);
                bulkWrapper.setUsingPooledConnection((0 == Constants.RANDOM.nextInt(2)) ? true : false, dsPool);
                BulkCopyTestUtil.performBulkCopy(bulkWrapper, sourceTable, destTable);
            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                TestUtils.dropTableIfExists(destTable.getEscapedTableName(), (Statement) stmt.product());
            }
        }
    }

    @Test
    @DisplayName("BulkCopy:test column order hints")
    @Tag(Constants.xAzureSQLDW)
    public void testCOH() throws SQLException {
        try (DBConnection con = new DBConnection(connectionString); DBStatement stmt = con.createStatement()) {
            DBTable destTable = null;
            try {
                // create destination table
                destTable = sourceTable.cloneSchema();
                stmt.createTable(destTable);

                BulkCopyTestWrapper bulkWrapper = new BulkCopyTestWrapper(connectionString);
                bulkWrapper.setUsingConnection((0 == Constants.RANDOM.nextInt(2)) ? true : false, ds);
                bulkWrapper.setUsingXAConnection((0 == Constants.RANDOM.nextInt(2)) ? true : false, dsXA);
                bulkWrapper.setUsingPooledConnection((0 == Constants.RANDOM.nextInt(2)) ? true : false, dsPool);

                bulkWrapper.setColumnOrderHint(destTable.getColumnName(0), SQLServerSortOrder.ASCENDING);

                BulkCopyTestUtil.performBulkCopy(bulkWrapper, sourceTable, destTable);
            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                TestUtils.dropTableIfExists(destTable.getEscapedTableName(), (Statement) stmt.product());
            }
        }
    }

    @Test
    @DisplayName("BulkCopy:test column order hints with multiple columns")
    @Tag(Constants.xAzureSQLDW)
    public void testMultiColumnsCOH() throws SQLException {
        try (DBConnection con = new DBConnection(connectionString); DBStatement stmt = con.createStatement()) {
            DBTable destTable = null;
            try {
                // create destination table
                destTable = sourceTable.cloneSchema();
                stmt.createTable(destTable);

                BulkCopyTestWrapper bulkWrapper = new BulkCopyTestWrapper(connectionString);
                bulkWrapper.setUsingConnection((0 == Constants.RANDOM.nextInt(2)) ? true : false, ds);
                bulkWrapper.setUsingXAConnection((0 == Constants.RANDOM.nextInt(2)) ? true : false, dsXA);
                bulkWrapper.setUsingPooledConnection((0 == Constants.RANDOM.nextInt(2)) ? true : false, dsPool);

                bulkWrapper.setColumnOrderHint(destTable.getColumnName(0), SQLServerSortOrder.ASCENDING);
                bulkWrapper.setColumnOrderHint(destTable.getColumnName(1), SQLServerSortOrder.ASCENDING);

                BulkCopyTestUtil.performBulkCopy(bulkWrapper, sourceTable, destTable);
            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                TestUtils.dropTableIfExists(destTable.getEscapedTableName(), (Statement) stmt.product());
            }
        }
    }

    @Test
    @DisplayName("BulkCopy:test unicode column name")
    public void testUnicodeCOH() throws SQLException {
        try (DBConnection con = new DBConnection(connectionString); DBStatement stmt = con.createStatement()) {
            DBTable sourceTableUnicode = null;
            DBTable destTableUnicode = null;
            try {
                // create source unicode table
                sourceTableUnicode = new DBTable(true, true);
                stmt.createTable(sourceTableUnicode);

                // create destination unicode table with same schema as source
                destTableUnicode = sourceTableUnicode.cloneSchema();
                stmt.createTable(destTableUnicode);

                BulkCopyTestWrapper bulkWrapper = new BulkCopyTestWrapper(connectionString);
                bulkWrapper.setUsingConnection((0 == Constants.RANDOM.nextInt(2)) ? true : false, ds);
                bulkWrapper.setUsingXAConnection((0 == Constants.RANDOM.nextInt(2)) ? true : false, dsXA);
                bulkWrapper.setUsingPooledConnection((0 == Constants.RANDOM.nextInt(2)) ? true : false, dsPool);

                bulkWrapper.setColumnOrderHint(destTableUnicode.getColumnName(1), SQLServerSortOrder.ASCENDING);

                BulkCopyTestUtil.performBulkCopy(bulkWrapper, sourceTableUnicode, destTableUnicode);
            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                TestUtils.dropTableIfExists(sourceTableUnicode.getEscapedTableName(), (Statement) stmt.product());
                TestUtils.dropTableIfExists(destTableUnicode.getEscapedTableName(), (Statement) stmt.product());
            }
        }
    }

    @Test
    @DisplayName("BulkCopy:test column name with escape character")
    public void testCOHWithEscapeChar() throws SQLException {
        try (DBConnection con = new DBConnection(connectionString); DBStatement stmt = con.createStatement()) {
            DBTable sourceTableEscape = null;
            DBTable destTableEscape = null;
            try {
                // create source unicode table
                sourceTableEscape = new DBTable(true);
                sourceTableEscape.addColumn(new SqlVarChar(), "COLUMN_NAME]");
                stmt.createTable(sourceTableEscape);

                // create destination unicode table with same schema as source
                destTableEscape = sourceTableEscape.cloneSchema();
                stmt.createTable(destTableEscape);

                BulkCopyTestWrapper bulkWrapper = new BulkCopyTestWrapper(connectionString);
                bulkWrapper.setUsingConnection((0 == Constants.RANDOM.nextInt(2)) ? true : false, ds);
                bulkWrapper.setUsingXAConnection((0 == Constants.RANDOM.nextInt(2)) ? true : false, dsXA);
                bulkWrapper.setUsingPooledConnection((0 == Constants.RANDOM.nextInt(2)) ? true : false, dsPool);

                bulkWrapper.setColumnOrderHint(destTableEscape.getColumnName(destTableEscape.totalColumns() - 1), SQLServerSortOrder.ASCENDING);

                BulkCopyTestUtil.performBulkCopy(bulkWrapper, sourceTableEscape, destTableEscape);
            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                TestUtils.dropTableIfExists(sourceTableEscape.getEscapedTableName(), (Statement) stmt.product());
                TestUtils.dropTableIfExists(destTableEscape.getEscapedTableName(), (Statement) stmt.product());
            }
        }
    }

    @Test
    @DisplayName("BulkCopy:test column order hints in descending order")
    @Tag(Constants.xAzureSQLDW)
    public void testCOHDescOrder() throws SQLException {
        try (DBConnection con = new DBConnection(connectionString); DBStatement stmt = con.createStatement()) {
            DBTable destTable = null;
            try {
                // create destination table
                destTable = sourceTable.cloneSchema();
                stmt.createTable(destTable);

                BulkCopyTestWrapper bulkWrapper = new BulkCopyTestWrapper(connectionString);
                bulkWrapper.setUsingConnection((0 == Constants.RANDOM.nextInt(2)) ? true : false, ds);
                bulkWrapper.setUsingXAConnection((0 == Constants.RANDOM.nextInt(2)) ? true : false, dsXA);
                bulkWrapper.setUsingPooledConnection((0 == Constants.RANDOM.nextInt(2)) ? true : false, dsPool);

                bulkWrapper.setColumnOrderHint(destTable.getColumnName(0), SQLServerSortOrder.DESCENDING);

                BulkCopyTestUtil.performBulkCopy(bulkWrapper, sourceTable, destTable);
            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                TestUtils.dropTableIfExists(destTable.getEscapedTableName(), (Statement) stmt.product());
            }
        }
    }



    @Test
    @DisplayName("BulkCopy:test colmun order hints with unspecified sort order")
    public void testInvalidCOHWithUnspecifiedSortOrder() throws SQLException {
        try (DBConnection con = new DBConnection(connectionString); DBStatement stmt = con.createStatement()) {
            DBTable destTable = null;
            try {
                // create destination table
                destTable = sourceTable.cloneSchema();
                stmt.createTable(destTable);

                BulkCopyTestWrapper bulkWrapper = new BulkCopyTestWrapper(connectionString);
                bulkWrapper.setUsingConnection((0 == Constants.RANDOM.nextInt(2)) ? true : false, ds);
                bulkWrapper.setUsingXAConnection((0 == Constants.RANDOM.nextInt(2)) ? true : false, dsXA);
                bulkWrapper.setUsingPooledConnection((0 == Constants.RANDOM.nextInt(2)) ? true : false, dsPool);

                bulkWrapper.setColumnOrderHint(destTable.getColumnName(0), SQLServerSortOrder.UNSPECIFIED);

                BulkCopyTestUtil.performBulkCopy(bulkWrapper, sourceTable, destTable, true, true);
            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                TestUtils.dropTableIfExists(destTable.getEscapedTableName(), (Statement) stmt.product());
            }
        }
    }

    @Test
    @DisplayName("BulkCopy:test  colmun order hints with invalid column name")
    public void testInvalidCOHWithInvalidColumnName() throws SQLException {
        try (DBConnection con = new DBConnection(connectionString); DBStatement stmt = con.createStatement()) {
            DBTable destTable = null;
            try {
                // create destination table
                destTable = sourceTable.cloneSchema();
                stmt.createTable(destTable);

                BulkCopyTestWrapper bulkWrapper = new BulkCopyTestWrapper(connectionString);
                bulkWrapper.setUsingConnection((0 == Constants.RANDOM.nextInt(2)) ? true : false, ds);
                bulkWrapper.setUsingXAConnection((0 == Constants.RANDOM.nextInt(2)) ? true : false, dsXA);
                bulkWrapper.setUsingPooledConnection((0 == Constants.RANDOM.nextInt(2)) ? true : false, dsPool);

                bulkWrapper.setColumnOrderHint("INVALID_COLUMN_NAME", SQLServerSortOrder.ASCENDING);

                BulkCopyTestUtil.performBulkCopy(bulkWrapper, sourceTable, destTable, true, true);
            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                TestUtils.dropTableIfExists(destTable.getEscapedTableName(), (Statement) stmt.product());
            }
        }
    }
}
