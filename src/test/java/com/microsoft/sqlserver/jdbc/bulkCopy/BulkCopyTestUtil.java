/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) 2016 Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

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

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
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
    static void performBulkCopy(BulkCopyTestWrapper wrapper, DBTable sourceTable) {
        performBulkCopy(wrapper, sourceTable, false);
    }

    /**
     * perform bulk copy using source table
     * 
     * @param wrapper
     * @param sourceTable
     * @param validateResult
     */
    static void performBulkCopy(BulkCopyTestWrapper wrapper, DBTable sourceTable, boolean validateResult) {
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
     * validate if same values are in both source and destination table
     * 
     * @param con
     * @param sourceTable
     * @param destinationTable
     * @throws SQLException
     */
    static void validateValues(DBConnection con, DBTable sourceTable, DBTable destinationTable) throws SQLException {
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
                // Bulkcopy doesn't guarantee order of insertion -
                // if we need to test several rows either use primary key or
                // validate result based on sql JOIN
                switch (destMeta.getColumnType(i)) {
                    case java.sql.Types.BIGINT:
                        assertTrue((((Long) srcValue).longValue() == ((Long) dstValue).longValue()), "Unexpected bigint value");
                        break;

                    case java.sql.Types.INTEGER:
                        assertTrue((((Integer) srcValue).intValue() == ((Integer) dstValue).intValue()), "Unexpected int value");
                        break;

                    case java.sql.Types.SMALLINT:
                    case java.sql.Types.TINYINT:
                        assertTrue((((Short) srcValue).shortValue() == ((Short) dstValue).shortValue()), "Unexpected smallint/tinyint value");
                        break;

                    case java.sql.Types.BIT:
                        assertTrue((((Boolean) srcValue).booleanValue() == ((Boolean) dstValue).booleanValue()), "Unexpected bit value");
                        break;

                    case java.sql.Types.DECIMAL:
                    case java.sql.Types.NUMERIC:
                        assertTrue(0 == (((BigDecimal) srcValue).compareTo((BigDecimal) dstValue)),
                                "Unexpected decimal/numeric/money/smallmoney value");
                        break;

                    case java.sql.Types.DOUBLE:
                        assertTrue((((Double) srcValue).doubleValue() == ((Double) dstValue).doubleValue()), "Unexpected float value");
                        break;

                    case java.sql.Types.REAL:
                        assertTrue((((Float) srcValue).floatValue() == ((Float) dstValue).floatValue()), "Unexpected real value");
                        break;

                    case java.sql.Types.VARCHAR:
                    case java.sql.Types.NVARCHAR:
                        assertTrue((((String) srcValue).equals((String) dstValue)), "Unexpected varchar/nvarchar value ");
                        break;

                    case java.sql.Types.CHAR:
                    case java.sql.Types.NCHAR:
                        assertTrue((((String) srcValue).equals((String) dstValue)), "Unexpected char/nchar value ");
                        break;

                    case java.sql.Types.TIMESTAMP:
                        assertTrue((((Timestamp) srcValue).getTime() == (((Timestamp) dstValue).getTime())),
                                "Unexpected datetime/smalldatetime/datetime2 value");
                        break;

                    case java.sql.Types.DATE:
                        assertTrue((((Date) srcValue).getTime() == (((Date) dstValue).getTime())), "Unexpected datetime value");
                        break;

                    case java.sql.Types.TIME:
                        assertTrue(((Time) srcValue).getTime() == ((Time) dstValue).getTime(), "Unexpected time value ");
                        break;

                    case microsoft.sql.Types.DATETIMEOFFSET:
                        assertTrue(0 == ((microsoft.sql.DateTimeOffset) srcValue).compareTo((microsoft.sql.DateTimeOffset) dstValue),
                                "Unexpected time value ");
                        break;

                    default:
                        fail("Unhandled JDBCType " + JDBCType.valueOf(destMeta.getColumnType(i)));
                        break;
                }
            }
    }
}