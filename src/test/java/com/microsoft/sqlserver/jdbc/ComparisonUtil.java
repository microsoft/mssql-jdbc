/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.DBResultSet;
import com.microsoft.sqlserver.testframework.DBStatement;
import com.microsoft.sqlserver.testframework.DBTable;


public class ComparisonUtil {

    /**
     * test if source table and destination table are the same
     * 
     * @param con
     * @param srcTable
     * @param destTable
     * @throws SQLException
     */
    public static void compareSrcTableAndDestTableIgnoreRowOrder(DBConnection con, DBTable srcTable,
            DBTable destTable) throws SQLException {
        try (DBStatement stmt = con.createStatement(); DBStatement stmt2 = con.createStatement();
                DBResultSet srcResultSetCount = stmt
                        .executeQuery("SELECT COUNT(*) FROM " + srcTable.getEscapedTableName() + ";");
                DBResultSet dstResultSetCount = stmt2
                        .executeQuery("SELECT COUNT(*) FROM " + destTable.getEscapedTableName() + ";")) {
            srcResultSetCount.next();
            dstResultSetCount.next();
            int srcRows = srcResultSetCount.getInt(1);
            int destRows = dstResultSetCount.getInt(1);

            if (srcRows != destRows) {
                fail("Souce table and Destination table have different number of rows.");
            }

            if (srcTable.getColumns().size() != destTable.getColumns().size()) {
                fail("Souce table and Destination table have different number of columns.");
            }

            try (DBResultSet srcResultSet = stmt.executeQuery(
                    "SELECT * FROM " + srcTable.getEscapedTableName() + " ORDER BY [" + srcTable.getColumnName(1)
                            + "], [" + srcTable.getColumnName(2) + "],[" + srcTable.getColumnName(3) + "];");
                    DBResultSet dstResultSet = stmt2.executeQuery("SELECT * FROM " + destTable.getEscapedTableName()
                            + " ORDER BY [" + destTable.getColumnName(1) + "], [" + destTable.getColumnName(2) + "],["
                            + destTable.getColumnName(3) + "];")) {

                while (srcResultSet.next() && dstResultSet.next()) {
                    for (int i = 0; i < destTable.getColumns().size(); i++) {
                        SQLServerResultSetMetaData srcMeta = (SQLServerResultSetMetaData) ((ResultSet) srcResultSet
                                .product()).getMetaData();
                        SQLServerResultSetMetaData destMeta = (SQLServerResultSetMetaData) ((ResultSet) dstResultSet
                                .product()).getMetaData();

                        int srcJDBCTypeInt = srcMeta.getColumnType(i + 1);
                        int destJDBCTypeInt = destMeta.getColumnType(i + 1);

                        // verify column types
                        if (srcJDBCTypeInt != destJDBCTypeInt) {
                            fail("Souce table and Destination table have different number of columns.");
                        }

                        Object expectedValue = srcResultSet.getObject(i + 1);
                        Object actualValue = dstResultSet.getObject(i + 1);

                        compareExpectedAndActual(destJDBCTypeInt, expectedValue, actualValue);
                    }
                }
            }
        }
    }

    /**
     * validate if both expected and actual value are same
     * 
     * @param dataType
     * @param expectedValue
     * @param actualValue
     */
    public static void compareExpectedAndActual(int dataType, Object expectedValue, Object actualValue) {
        // Bulkcopy doesn't guarantee order of insertion - if we need to test several rows either use primary key or
        // validate result based on sql JOIN

        if ((null == expectedValue) || (null == actualValue)) {
            // if one value is null other should be null too
            assertEquals(expectedValue, actualValue, "Expected null in source and destination");
        } else
            switch (dataType) {
                case java.sql.Types.BIGINT:
                    assertEquals((Long) expectedValue, (Long) actualValue, "Unexpected bigint value.");
                    break;

                case java.sql.Types.INTEGER:
                    assertEquals((Integer) expectedValue, (Integer) actualValue, "Unexpected int value.");
                    break;

                case java.sql.Types.SMALLINT:
                case java.sql.Types.TINYINT:
                    assertEquals((Short) expectedValue, (Short) actualValue, "Unexpected smallint/tinyint value.");
                    break;

                case java.sql.Types.BIT:
                    assertEquals((Boolean) expectedValue, (Boolean) actualValue, "Unexpected bit value");
                    break;

                case java.sql.Types.DECIMAL:
                case java.sql.Types.NUMERIC:
                    assertTrue(((BigDecimal) expectedValue).compareTo((BigDecimal) actualValue) == 0,
                            "Unexpected decimal/numeric/money/smallmoney value. Expected:" + expectedValue + " Actual:"
                                    + actualValue);
                    break;

                case java.sql.Types.DOUBLE:
                    assertEquals((Double) expectedValue, (Double) actualValue, "Unexpected double value.");
                    break;

                case java.sql.Types.REAL:
                    assertEquals((Float) expectedValue, (Float) actualValue, "Unexpected real/float value.");
                    break;

                case java.sql.Types.VARCHAR:
                case java.sql.Types.NVARCHAR:
                    assertEquals(((String) expectedValue).trim(), ((String) actualValue).trim(),
                            "Unexpected varchar/nvarchar value ");
                    break;

                case java.sql.Types.CHAR:
                case java.sql.Types.NCHAR:
                    assertEquals(((String) expectedValue).trim(), ((String) actualValue).trim(),
                            "Unexpected char/nchar value ");
                    break;

                case java.sql.Types.BINARY:
                case java.sql.Types.VARBINARY:
                    assertTrue(TestUtils.parseByte((byte[]) expectedValue, (byte[]) actualValue),
                            "Unexpected bianry/varbinary value ");
                    break;

                case java.sql.Types.TIMESTAMP:
                    assertEquals((Timestamp) expectedValue, (Timestamp) actualValue,
                            "Unexpected datetime/smalldatetime/datetime2 value");
                    break;

                case java.sql.Types.DATE:
                    Calendar expC = Calendar.getInstance();
                    expC.setTime((Date) expectedValue);
                    Calendar actC = Calendar.getInstance();
                    actC.setTime((Date) actualValue);
                    assertEquals(expC.get(Calendar.DAY_OF_MONTH), actC.get(Calendar.DAY_OF_MONTH),
                            "Unexpected date value. Expected:" + expectedValue + " Actual:" + actualValue);
                    break;

                case java.sql.Types.TIME:
                    assertEquals((Time) expectedValue, (Time) actualValue, "Unexpected time value ");
                    break;

                case microsoft.sql.Types.DATETIMEOFFSET:
                    assertTrue(
                            0 == ((microsoft.sql.DateTimeOffset) expectedValue)
                                    .compareTo((microsoft.sql.DateTimeOffset) actualValue),
                            "Unexpected datetimeoffset value. Expected:" + expectedValue + " Actual:" + actualValue);
                    break;

                default:
                    fail("Unhandled JDBCType " + JDBCType.valueOf(dataType));
                    break;
            }
    }
}
