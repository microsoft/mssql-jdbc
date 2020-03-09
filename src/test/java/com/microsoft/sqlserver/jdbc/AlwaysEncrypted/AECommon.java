package com.microsoft.sqlserver.jdbc.AlwaysEncrypted;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.jdbc.TestResource;


public class AECommon {
    protected static void testGetString(ResultSet rs, int numberOfColumns, String[] values) throws SQLException {

        int index = 0;
        for (int i = 1; i <= numberOfColumns; i = i + 3) {
            String stringValue1 = ("" + rs.getString(i)).trim();
            String stringValue2 = ("" + rs.getString(i + 1)).trim();
            String stringValue3 = ("" + rs.getString(i + 2)).trim();

            if (stringValue1.equalsIgnoreCase("0") && (values[index].equalsIgnoreCase(Boolean.TRUE.toString())
                    || values[index].equalsIgnoreCase(Boolean.FALSE.toString()))) {
                stringValue1 = Boolean.FALSE.toString();
                stringValue2 = Boolean.FALSE.toString();
                stringValue3 = Boolean.FALSE.toString();
            } else if (stringValue1.equalsIgnoreCase("1") && (values[index].equalsIgnoreCase(Boolean.TRUE.toString())
                    || values[index].equalsIgnoreCase(Boolean.FALSE.toString()))) {
                stringValue1 = Boolean.TRUE.toString();
                stringValue2 = Boolean.TRUE.toString();
                stringValue3 = Boolean.TRUE.toString();
            }
            try {

                boolean matches = stringValue1.equalsIgnoreCase("" + values[index])
                        && stringValue2.equalsIgnoreCase("" + values[index])
                        && stringValue3.equalsIgnoreCase("" + values[index]);

                if (("" + values[index]).length() >= 1000) {
                    assertTrue(matches, TestResource.getResource("R_decryptionFailed") + " getString():" + i + ", "
                            + (i + 1) + ", " + (i + 2) + ".\n" + TestResource.getResource("R_expectedValue") + index);
                } else {
                    assertTrue(matches,
                            TestResource.getResource("R_decryptionFailed") + " getString(): " + stringValue1 + ", "
                                    + stringValue2 + ", " + stringValue3 + ".\n"
                                    + TestResource.getResource("R_expectedValue") + values[index]);
                }
            } finally {
                index++;
            }
        }
    }

    protected static void testGetObject(ResultSet rs, int numberOfColumns, String[] values) throws SQLException {
        int index = 0;
        for (int i = 1; i <= numberOfColumns; i = i + 3) {
            try {
                String objectValue1 = ("" + rs.getObject(i)).trim();
                String objectValue2 = ("" + rs.getObject(i + 1)).trim();
                String objectValue3 = ("" + rs.getObject(i + 2)).trim();

                boolean matches = objectValue1.equalsIgnoreCase("" + values[index])
                        && objectValue2.equalsIgnoreCase("" + values[index])
                        && objectValue3.equalsIgnoreCase("" + values[index]);

                if (("" + values[index]).length() >= 1000) {
                    assertTrue(matches,
                            TestResource.getResource("R_decryptionFailed") + "getObject(): " + i + ", " + (i + 1) + ", "
                                    + (i + 2) + ".\n" + TestResource.getResource("R_expectedValueAtIndex") + index);
                } else {
                    assertTrue(matches,
                            TestResource.getResource("R_decryptionFailed") + "getObject(): " + objectValue1 + ", "
                                    + objectValue2 + ", " + objectValue3 + ".\n"
                                    + TestResource.getResource("R_expectedValue") + values[index]);
                }
            } finally {
                index++;
            }
        }
    }

    protected static void testGetBigDecimal(ResultSet rs, int numberOfColumns, String[] values) throws SQLException {
        int index = 0;
        for (int i = 1; i <= numberOfColumns; i = i + 3) {

            String decimalValue1 = "" + rs.getBigDecimal(i);
            String decimalValue2 = "" + rs.getBigDecimal(i + 1);
            String decimalValue3 = "" + rs.getBigDecimal(i + 2);
            String value = values[index];

            if (decimalValue1.equalsIgnoreCase("0") && (value.equalsIgnoreCase(Boolean.TRUE.toString())
                    || value.equalsIgnoreCase(Boolean.FALSE.toString()))) {
                decimalValue1 = Boolean.FALSE.toString();
                decimalValue2 = Boolean.FALSE.toString();
                decimalValue3 = Boolean.FALSE.toString();
            } else if (decimalValue1.equalsIgnoreCase("1") && (value.equalsIgnoreCase(Boolean.TRUE.toString())
                    || value.equalsIgnoreCase(Boolean.FALSE.toString()))) {
                decimalValue1 = Boolean.TRUE.toString();
                decimalValue2 = Boolean.TRUE.toString();
                decimalValue3 = Boolean.TRUE.toString();
            }

            if (null != value) {
                if (value.equalsIgnoreCase("1.79E308")) {
                    value = "1.79E+308";
                } else if (value.equalsIgnoreCase("3.4E38")) {
                    value = "3.4E+38";
                }

                if (value.equalsIgnoreCase("-1.79E308")) {
                    value = "-1.79E+308";
                } else if (value.equalsIgnoreCase("-3.4E38")) {
                    value = "-3.4E+38";
                }
            }

            try {
                assertTrue(
                        decimalValue1.equalsIgnoreCase("" + value) && decimalValue2.equalsIgnoreCase("" + value)
                                && decimalValue3.equalsIgnoreCase("" + value),
                        TestResource.getResource("R_decryptionFailed") + "getBigDecimal(): " + decimalValue1 + ", "
                                + decimalValue2 + ", " + decimalValue3 + ".\n"
                                + TestResource.getResource("R_expectedValue") + value);
            } finally {
                index++;
            }
        }
    }

    protected static void testWithSpecifiedtype(SQLServerResultSet rs, int numberOfColumns,
            String[] values) throws SQLException {

        String value1, value2, value3, expectedValue = null;
        int index = 0;

        // bit
        value1 = "" + rs.getBoolean(1);
        value2 = "" + rs.getBoolean(2);
        value3 = "" + rs.getBoolean(3);

        expectedValue = values[index];
        compare(expectedValue, value1, value2, value3);
        index++;

        // tiny
        value1 = "" + rs.getShort(4);
        value2 = "" + rs.getShort(5);
        value3 = "" + rs.getShort(6);

        expectedValue = values[index];
        compare(expectedValue, value1, value2, value3);
        index++;

        // smallint
        value1 = "" + rs.getShort(7);
        value2 = "" + rs.getShort(8);
        value3 = "" + rs.getShort(8);

        expectedValue = values[index];
        compare(expectedValue, value1, value2, value3);
        index++;

        // int
        value1 = "" + rs.getInt(10);
        value2 = "" + rs.getInt(11);
        value3 = "" + rs.getInt(12);

        expectedValue = values[index];
        compare(expectedValue, value1, value2, value3);
        index++;

        // bigint
        value1 = "" + rs.getLong(13);
        value2 = "" + rs.getLong(14);
        value3 = "" + rs.getLong(15);

        expectedValue = values[index];
        compare(expectedValue, value1, value2, value3);
        index++;

        // float
        value1 = "" + rs.getDouble(16);
        value2 = "" + rs.getDouble(17);
        value3 = "" + rs.getDouble(18);

        expectedValue = values[index];
        compare(expectedValue, value1, value2, value3);
        index++;

        // float(30)
        value1 = "" + rs.getDouble(19);
        value2 = "" + rs.getDouble(20);
        value3 = "" + rs.getDouble(21);

        expectedValue = values[index];
        compare(expectedValue, value1, value2, value3);
        index++;

        // real
        value1 = "" + rs.getFloat(22);
        value2 = "" + rs.getFloat(23);
        value3 = "" + rs.getFloat(24);

        expectedValue = values[index];
        compare(expectedValue, value1, value2, value3);
        index++;

        // decimal
        value1 = "" + rs.getBigDecimal(25);
        value2 = "" + rs.getBigDecimal(26);
        value3 = "" + rs.getBigDecimal(27);

        expectedValue = values[index];
        compare(expectedValue, value1, value2, value3);
        index++;

        // decimal (10,5)
        value1 = "" + rs.getBigDecimal(28);
        value2 = "" + rs.getBigDecimal(29);
        value3 = "" + rs.getBigDecimal(30);

        expectedValue = values[index];
        compare(expectedValue, value1, value2, value3);
        index++;

        // numeric
        value1 = "" + rs.getBigDecimal(31);
        value2 = "" + rs.getBigDecimal(32);
        value3 = "" + rs.getBigDecimal(33);

        expectedValue = values[index];
        compare(expectedValue, value1, value2, value3);
        index++;

        // numeric (8,2)
        value1 = "" + rs.getBigDecimal(34);
        value2 = "" + rs.getBigDecimal(35);
        value3 = "" + rs.getBigDecimal(36);

        expectedValue = values[index];
        compare(expectedValue, value1, value2, value3);
        index++;

        // smallmoney
        value1 = "" + rs.getSmallMoney(37);
        value2 = "" + rs.getSmallMoney(38);
        value3 = "" + rs.getSmallMoney(39);

        expectedValue = values[index];
        compare(expectedValue, value1, value2, value3);
        index++;

        // money
        value1 = "" + rs.getMoney(40);
        value2 = "" + rs.getMoney(41);
        value3 = "" + rs.getMoney(42);

        expectedValue = values[index];
        compare(expectedValue, value1, value2, value3);
        index++;

        // decimal(28,4)
        value1 = "" + rs.getBigDecimal(43);
        value2 = "" + rs.getBigDecimal(44);
        value3 = "" + rs.getBigDecimal(45);

        expectedValue = values[index];
        compare(expectedValue, value1, value2, value3);
        index++;

        // numeric(28,4)
        value1 = "" + rs.getBigDecimal(46);
        value2 = "" + rs.getBigDecimal(47);
        value3 = "" + rs.getBigDecimal(48);

        expectedValue = values[index];
        compare(expectedValue, value1, value2, value3);
        index++;
    }

    static void compare(String expectedValue, String value1, String value2, String value3) {

        if (null != expectedValue) {
            if (expectedValue.equalsIgnoreCase("1.79E+308")) {
                expectedValue = "1.79E308";
            } else if (expectedValue.equalsIgnoreCase("3.4E+38")) {
                expectedValue = "3.4E38";
            }

            if (expectedValue.equalsIgnoreCase("-1.79E+308")) {
                expectedValue = "-1.79E308";
            } else if (expectedValue.equalsIgnoreCase("-3.4E+38")) {
                expectedValue = "-3.4E38";
            }
        }

        assertTrue(
                value1.equalsIgnoreCase("" + expectedValue) && value2.equalsIgnoreCase("" + expectedValue)
                        && value3.equalsIgnoreCase("" + expectedValue),
                TestResource.getResource("R_decryptionFailed") + "getBigDecimal(): " + value1 + ", " + value2 + ", "
                        + value3 + ".\n" + TestResource.getResource("R_expectedValue"));
    }
}
