/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.bulkCopy.BulkCopyTestWrapper.ColumnMap;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.DBResultSet;
import com.microsoft.sqlserver.testframework.DBStatement;
import com.microsoft.sqlserver.testframework.DBTable;

/**
 * Utility class
 */
class BulkCopyTestUtil {

    /**
     * perform bulk copy using source table and validate bulkcopy
     * 
     * @param wrapper
     * @param sourceTable
     */
    static void performBulkCopy(BulkCopyTestWrapper wrapper,
            DBTable sourceTable) {
        performBulkCopy(wrapper, sourceTable, true);
    }

    /**
     * perform bulk copy using source and destination tables and validate bulkcopy
     * 
     * @param wrapper
     * @param sourceTable
     * @param destTable
     */
    static void performBulkCopy(BulkCopyTestWrapper wrapper,
            DBTable sourceTable,
            DBTable destTable) {
        performBulkCopy(wrapper, sourceTable, destTable, true);
    }

    /**
     * perform bulk copy using source table
     * 
     * @param wrapper
     * @param sourceTable
     * @param validateResult
     */
    static void performBulkCopy(BulkCopyTestWrapper wrapper,
            DBTable sourceTable,
            boolean validateResult) {
        DBConnection con = null;
        DBStatement stmt = null;
        DBTable destinationTable = null;
        try {
            con = new DBConnection(wrapper.getConnectionString());
            stmt = con.createStatement();

            destinationTable = sourceTable.cloneSchema();
            stmt.createTable(destinationTable);
            DBResultSet srcResultSet = stmt.executeQuery("SELECT * FROM " + sourceTable.getEscapedTableName() + ";");
            SQLServerBulkCopy bulkCopy;
            if (wrapper.isUsingConnection()) {
                bulkCopy = new SQLServerBulkCopy((Connection) con.product());
            }
            else {
                bulkCopy = new SQLServerBulkCopy(wrapper.getConnectionString());
            }
            if (wrapper.isUsingBulkCopyOptions()) {
                bulkCopy.setBulkCopyOptions(wrapper.getBulkOptions());
            }
            bulkCopy.setDestinationTableName(destinationTable.getEscapedTableName());
            if (wrapper.isUsingColumnMapping()) {
                for (int i = 0; i < wrapper.cm.size(); i++) {
                    ColumnMap currentMap = wrapper.cm.get(i);
                    if (currentMap.sourceIsInt && currentMap.destIsInt) {
                        bulkCopy.addColumnMapping(currentMap.srcInt, currentMap.destInt);
                    }
                    else if (currentMap.sourceIsInt && (!currentMap.destIsInt)) {
                        bulkCopy.addColumnMapping(currentMap.srcInt, currentMap.destString);
                    }
                    else if ((!currentMap.sourceIsInt) && currentMap.destIsInt) {
                        bulkCopy.addColumnMapping(currentMap.srcString, currentMap.destInt);
                    }
                    else if ((!currentMap.sourceIsInt) && (!currentMap.destIsInt)) {
                        bulkCopy.addColumnMapping(currentMap.srcString, currentMap.destString);
                    }
                }
            }
            bulkCopy.writeToServer((ResultSet) srcResultSet.product());
            bulkCopy.close();
            if (validateResult) {
                validateValues(con, sourceTable, destinationTable);
            }
        }
        catch (SQLException ex) {
            fail(ex.getMessage());
        }
        finally {
            stmt.dropTable(destinationTable);
            con.close();
        }
    }

    /**
     * perform bulk copy using source and destination tables
     * 
     * @param wrapper
     * @param sourceTable
     * @param destTable
     * @param validateResult
     */
    static void performBulkCopy(BulkCopyTestWrapper wrapper,
            DBTable sourceTable,
            DBTable destinationTable,
            boolean validateResult) {
        DBConnection con = null;
        DBStatement stmt = null;
        try {
            con = new DBConnection(wrapper.getConnectionString());
            stmt = con.createStatement();

            DBResultSet srcResultSet = stmt.executeQuery("SELECT * FROM " + sourceTable.getEscapedTableName() + ";");
            SQLServerBulkCopy bulkCopy;
            if (wrapper.isUsingConnection()) {
                bulkCopy = new SQLServerBulkCopy((Connection) con.product());
            }
            else {
                bulkCopy = new SQLServerBulkCopy(wrapper.getConnectionString());
            }
            if (wrapper.isUsingBulkCopyOptions()) {
                bulkCopy.setBulkCopyOptions(wrapper.getBulkOptions());
            }
            bulkCopy.setDestinationTableName(destinationTable.getEscapedTableName());
            if (wrapper.isUsingColumnMapping()) {
                for (int i = 0; i < wrapper.cm.size(); i++) {
                    ColumnMap currentMap = wrapper.cm.get(i);
                    if (currentMap.sourceIsInt && currentMap.destIsInt) {
                        bulkCopy.addColumnMapping(currentMap.srcInt, currentMap.destInt);
                    }
                    else if (currentMap.sourceIsInt && (!currentMap.destIsInt)) {
                        bulkCopy.addColumnMapping(currentMap.srcInt, currentMap.destString);
                    }
                    else if ((!currentMap.sourceIsInt) && currentMap.destIsInt) {
                        bulkCopy.addColumnMapping(currentMap.srcString, currentMap.destInt);
                    }
                    else if ((!currentMap.sourceIsInt) && (!currentMap.destIsInt)) {
                        bulkCopy.addColumnMapping(currentMap.srcString, currentMap.destString);
                    }
                }
            }
            bulkCopy.writeToServer((ResultSet) srcResultSet.product());
            bulkCopy.close();
            if (validateResult) {
                validateValues(con, sourceTable, destinationTable);
            }
        }
        catch (SQLException ex) {
            fail(ex.getMessage());
        }
        finally {
            stmt.dropTable(destinationTable);
            con.close();
        }
    }

    /**
     * perform bulk copy using source and destination tables
     * 
     * @param wrapper
     * @param sourceTable
     * @param destTable
     * @param validateResult
     * @param fail
     */
    static void performBulkCopy(BulkCopyTestWrapper wrapper,
            DBTable sourceTable,
            DBTable destinationTable,
            boolean validateResult,
            boolean fail) {
        DBConnection con = null;
        DBStatement stmt = null;
        try {
            con = new DBConnection(wrapper.getConnectionString());
            stmt = con.createStatement();

            DBResultSet srcResultSet = stmt.executeQuery("SELECT * FROM " + sourceTable.getEscapedTableName() + ";");
            SQLServerBulkCopy bulkCopy;
            if (wrapper.isUsingConnection()) {
                bulkCopy = new SQLServerBulkCopy((Connection) con.product());
            }
            else {
                bulkCopy = new SQLServerBulkCopy(wrapper.getConnectionString());
            }
            if (wrapper.isUsingBulkCopyOptions()) {
                bulkCopy.setBulkCopyOptions(wrapper.getBulkOptions());
            }
            bulkCopy.setDestinationTableName(destinationTable.getEscapedTableName());
            if (wrapper.isUsingColumnMapping()) {
                for (int i = 0; i < wrapper.cm.size(); i++) {
                    ColumnMap currentMap = wrapper.cm.get(i);
                    if (currentMap.sourceIsInt && currentMap.destIsInt) {
                        bulkCopy.addColumnMapping(currentMap.srcInt, currentMap.destInt);
                    }
                    else if (currentMap.sourceIsInt && (!currentMap.destIsInt)) {
                        bulkCopy.addColumnMapping(currentMap.srcInt, currentMap.destString);
                    }
                    else if ((!currentMap.sourceIsInt) && currentMap.destIsInt) {
                        bulkCopy.addColumnMapping(currentMap.srcString, currentMap.destInt);
                    }
                    else if ((!currentMap.sourceIsInt) && (!currentMap.destIsInt)) {
                        bulkCopy.addColumnMapping(currentMap.srcString, currentMap.destString);
                    }
                }
            }
            bulkCopy.writeToServer((ResultSet) srcResultSet.product());
            if (fail)
                fail("bulkCopy.writeToServer did not fail when it should have");
            bulkCopy.close();
            if (validateResult) {
                validateValues(con, sourceTable, destinationTable);
            }
        }
        catch (SQLException ex) {
            if (!fail) {
                fail(ex.getMessage());
            }
        }
        finally {
            stmt.dropTable(destinationTable);
            con.close();
        }
    }

    /**
     * perform bulk copy using source and destination tables
     * 
     * @param wrapper
     * @param sourceTable
     * @param destTable
     * @param validateResult
     * @param fail
     * @param dropDest
     */
    static void performBulkCopy(BulkCopyTestWrapper wrapper,
            DBTable sourceTable,
            DBTable destinationTable,
            boolean validateResult,
            boolean fail,
            boolean dropDest) {
        DBConnection con = null;
        DBStatement stmt = null;
        try {
            con = new DBConnection(wrapper.getConnectionString());
            stmt = con.createStatement();

            DBResultSet srcResultSet = stmt.executeQuery("SELECT * FROM " + sourceTable.getEscapedTableName() + ";");
            SQLServerBulkCopy bulkCopy;
            if (wrapper.isUsingConnection()) {
                bulkCopy = new SQLServerBulkCopy((Connection) con.product());
            }
            else {
                bulkCopy = new SQLServerBulkCopy(wrapper.getConnectionString());
            }
            if (wrapper.isUsingBulkCopyOptions()) {
                bulkCopy.setBulkCopyOptions(wrapper.getBulkOptions());
            }
            bulkCopy.setDestinationTableName(destinationTable.getEscapedTableName());
            if (wrapper.isUsingColumnMapping()) {
                for (int i = 0; i < wrapper.cm.size(); i++) {
                    ColumnMap currentMap = wrapper.cm.get(i);
                    if (currentMap.sourceIsInt && currentMap.destIsInt) {
                        bulkCopy.addColumnMapping(currentMap.srcInt, currentMap.destInt);
                    }
                    else if (currentMap.sourceIsInt && (!currentMap.destIsInt)) {
                        bulkCopy.addColumnMapping(currentMap.srcInt, currentMap.destString);
                    }
                    else if ((!currentMap.sourceIsInt) && currentMap.destIsInt) {
                        bulkCopy.addColumnMapping(currentMap.srcString, currentMap.destInt);
                    }
                    else if ((!currentMap.sourceIsInt) && (!currentMap.destIsInt)) {
                        bulkCopy.addColumnMapping(currentMap.srcString, currentMap.destString);
                    }
                }
            }
            bulkCopy.writeToServer((ResultSet) srcResultSet.product());
            if (fail)
                fail("bulkCopy.writeToServer did not fail when it should have");
            bulkCopy.close();
            if (validateResult) {
                validateValues(con, sourceTable, destinationTable);
            }
        }
        catch (SQLException ex) {
            if (!fail) {
                fail(ex.getMessage());
            }
        }
        finally {
            if (dropDest) {
                stmt.dropTable(destinationTable);
            }
            con.close();
        }
    }

    /**
     * validate if same values are in both source and destination table
     * 
     * @param con
     * @param sourceTable
     * @param destinationTable
     * @throws SQLException
     */
    static void validateValues(DBConnection con,
            DBTable sourceTable,
            DBTable destinationTable) throws SQLException {
        DBStatement srcStmt = con.createStatement();
        DBStatement dstStmt = con.createStatement();
        DBResultSet srcResultSet = srcStmt.executeQuery("SELECT * FROM " + sourceTable.getEscapedTableName() + ";");
        DBResultSet dstResultSet = dstStmt.executeQuery("SELECT * FROM " + destinationTable.getEscapedTableName() + ";");
        ResultSetMetaData destMeta = ((ResultSet) dstResultSet.product()).getMetaData();
        int totalColumns = destMeta.getColumnCount();

        // verify data from sourceType and resultSet
        while (srcResultSet.next() && dstResultSet.next())
            for (int i = 1; i <= totalColumns; i++) {
                // TODO: check row and column count in both the tables

                Object srcValue, dstValue;
                srcValue = srcResultSet.getObject(i);
                dstValue = dstResultSet.getObject(i);

                comapreSourceDest(destMeta.getColumnType(i), srcValue, dstValue);
            }
    }

    /**
     * validate if both expected and actual value are same
     * 
     * @param dataType
     * @param expectedValue
     * @param actualValue
     */
    static void comapreSourceDest(int dataType,
            Object expectedValue,
            Object actualValue) {
        // Bulkcopy doesn't guarantee order of insertion - if we need to test several rows either use primary key or
        // validate result based on sql JOIN

        if ((null == expectedValue) || (null == actualValue)) {
            // if one value is null other should be null too
            assertEquals(expectedValue, actualValue, "Expected null in source and destination");
        }
        else
            switch (dataType) {
                case java.sql.Types.BIGINT:
                    assertTrue((((Long) expectedValue).longValue() == ((Long) actualValue).longValue()), "Unexpected bigint value");
                    break;

                case java.sql.Types.INTEGER:
                    assertTrue((((Integer) expectedValue).intValue() == ((Integer) actualValue).intValue()), "Unexpected int value");
                    break;

                case java.sql.Types.SMALLINT:
                case java.sql.Types.TINYINT:
                    assertTrue((((Short) expectedValue).shortValue() == ((Short) actualValue).shortValue()), "Unexpected smallint/tinyint value");
                    break;

                case java.sql.Types.BIT:
                    assertTrue((((Boolean) expectedValue).booleanValue() == ((Boolean) actualValue).booleanValue()), "Unexpected bit value");
                    break;

                case java.sql.Types.DECIMAL:
                case java.sql.Types.NUMERIC:
                    assertTrue(0 == (((BigDecimal) expectedValue).compareTo((BigDecimal) actualValue)),
                            "Unexpected decimal/numeric/money/smallmoney value");
                    break;

                case java.sql.Types.DOUBLE:
                    assertTrue((((Double) expectedValue).doubleValue() == ((Double) actualValue).doubleValue()), "Unexpected float value");
                    break;

                case java.sql.Types.REAL:
                    assertTrue((((Float) expectedValue).floatValue() == ((Float) actualValue).floatValue()), "Unexpected real value");
                    break;

                case java.sql.Types.VARCHAR:
                case java.sql.Types.NVARCHAR:
                    assertTrue((((String) expectedValue).equals((String) actualValue)), "Unexpected varchar/nvarchar value ");
                    break;

                case java.sql.Types.CHAR:
                case java.sql.Types.NCHAR:
                    assertTrue((((String) expectedValue).equals((String) actualValue)), "Unexpected char/nchar value ");
                    break;

                case java.sql.Types.BINARY:
                case java.sql.Types.VARBINARY:
                    assertTrue(Arrays.equals(((byte[]) expectedValue), ((byte[]) actualValue)), "Unexpected bianry/varbinary value ");
                    break;

                case java.sql.Types.TIMESTAMP:
                    assertTrue((((Timestamp) expectedValue).getTime() == (((Timestamp) actualValue).getTime())),
                            "Unexpected datetime/smalldatetime/datetime2 value");
                    break;

                case java.sql.Types.DATE:
                    assertTrue((((Date) expectedValue).getTime() == (((Date) actualValue).getTime())), "Unexpected datetime value");
                    break;

                case java.sql.Types.TIME:
                    assertTrue(((Time) expectedValue).getTime() == ((Time) actualValue).getTime(), "Unexpected time value ");
                    break;

                case microsoft.sql.Types.DATETIMEOFFSET:
                    assertTrue(0 == ((microsoft.sql.DateTimeOffset) expectedValue).compareTo((microsoft.sql.DateTimeOffset) actualValue),
                            "Unexpected time value ");
                    break;

                default:
                    fail("Unhandled JDBCType " + JDBCType.valueOf(dataType));
                    break;
            }
    }
}