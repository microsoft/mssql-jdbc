package com.microsoft.sqlserver.testframework.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.DBResultSet;
import com.microsoft.sqlserver.testframework.DBTable;
import com.microsoft.sqlserver.testframework.Utils;

public class ComparisonUtil {

    /**
     * test if source table and destination table are the same
     * 
     * @param con
     * @param srcTable
     * @param destTable
     * @throws SQLException
     */
    public static void compareSrcTableAndDestTable(DBConnection con,
            DBTable srcTable,
            DBTable destTable) throws SQLException {
        DBResultSet srcResultSet = con.createStatement().executeQuery("SELECT * FROM " + srcTable.getEscapedTableName() + ";");
        DBResultSet dstResultSet = con.createStatement().executeQuery("SELECT * FROM " + destTable.getEscapedTableName() + ";");

        if (srcTable.getTotalRows() != destTable.getTotalRows()) {
            fail("Souce table and Destination table have different number of rows.");
        }

        if (srcTable.getColumns().size() != destTable.getColumns().size()) {
            fail("Souce table and Destination table have different number of columns.");
        }

        while (srcResultSet.next() && dstResultSet.next()) {
            for (int i = 0; i < destTable.getColumns().size(); i++) {
                int srcJDBCTypeInt = srcTable.getColumns().get(i).getSqlType().getJdbctype().getVendorTypeNumber();
                int destJDBCTypeInt = destTable.getColumns().get(i).getSqlType().getJdbctype().getVendorTypeNumber();

                // varify column types
                if (srcJDBCTypeInt != destJDBCTypeInt) {
                    fail("Souce table and Destination table have different number of columns.");
                }

                Object expectedValue = srcResultSet.getObject(i + 1);
                Object actualValue = dstResultSet.getObject(i + 1);

                int precision = destTable.getColumns().get(i).getSqlType().getPrecision();

                compareExpectedAndActual(destJDBCTypeInt, precision, expectedValue, actualValue);
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
    public static void compareExpectedAndActual(int dataType,
            Object expectedValue,
            Object actualValue) {

        compareExpectedAndActual(dataType, null, expectedValue, actualValue);
    }

    /**
     * validate if both expected and actual value are same
     * 
     * @param dataType
     * @param expectedValue
     * @param actualValue
     */
    public static void compareExpectedAndActual(int dataType,
            Integer precision,
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

                    if (null != precision) {
                        if (24 >= precision) {
                            assertTrue((((Float) expectedValue).floatValue() == ((Float) actualValue).floatValue()), "Unexpected float value");
                        }
                        else {
                            assertTrue((((Double) expectedValue).doubleValue() == ((Double) actualValue).doubleValue()), "Unexpected float value");
                        }
                    }
                    else {
                        assertTrue((((Double) expectedValue).doubleValue() == ((Double) actualValue).doubleValue()), "Unexpected float value");
                    }
                    break;

                case java.sql.Types.REAL:
                    assertTrue((((Float) expectedValue).floatValue() == ((Float) actualValue).floatValue()), "Unexpected real value");
                    break;

                case java.sql.Types.VARCHAR:
                case java.sql.Types.NVARCHAR:
                    assertTrue(((((String) expectedValue).trim()).equals(((String) actualValue).trim())), "Unexpected varchar/nvarchar value ");
                    break;

                case java.sql.Types.CHAR:
                case java.sql.Types.NCHAR:
                    assertTrue(((((String) expectedValue).trim()).equals(((String) actualValue).trim())), "Unexpected char/nchar value ");
                    break;

                case java.sql.Types.BINARY:
                case java.sql.Types.VARBINARY:
                    assertTrue(Utils.parseByte((byte[]) expectedValue, (byte[]) actualValue), "Unexpected bianry/varbinary value ");
                    break;

                case java.sql.Types.TIMESTAMP:
                    assertTrue((((Timestamp) expectedValue).getTime() == (((Timestamp) actualValue).getTime())),
                            "Unexpected datetime/smalldatetime/datetime2 value");
                    break;

                case java.sql.Types.DATE:
                    assertTrue((((Date) expectedValue).getDate() == (((Date) actualValue).getDate())), "Unexpected datetime value");
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
