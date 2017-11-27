/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.AlwaysEncrypted;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.TimeZone;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.testframework.util.Util;

/**
 * Tests datatypes that have precision and/or scale.
 *
 */
@RunWith(JUnitPlatform.class)
public class PrecisionScaleTest extends AESetup {
    private static SQLServerPreparedStatement pstmt = null;

    private static java.util.Date date = null;
    private static int offsetFromGMT = 0;
    private static final int offset = 60000;
    private static String GMTDate = "";
    private static String GMTDateWithoutDate = "";
    private static String dateTimeOffsetExpectedValue = "";

    static {
        TimeZone tz = TimeZone.getDefault();
        offsetFromGMT = tz.getOffset(1450812362177L);

        // since the Date object already accounts for timezone, subtracting the timezone difference will always give us the
        // GMT version of the Date object. I can't make this PST because there are datetimeoffset tests, so I have to use GMT.
        date = new Date(1450812362177L - offsetFromGMT);

        // Cannot use date.toGMTString() here directly since the date object is used elsewhere for population of data.
        GMTDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
        GMTDateWithoutDate = new SimpleDateFormat("HH:mm:ss").format(date);

        // datetimeoffset is aware of timezone as well as Date, so we must apply the timezone value twice.
        dateTimeOffsetExpectedValue = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                .format(new Date(1450812362177L - offsetFromGMT - offsetFromGMT + offset));
    }

    @Test
    public void testNumericPrecision8Scale2() throws Exception {
        dropTables(stmt);

        String[] numeric = {"1.12345", "12345.12", "567.70"};

        createNumericPrecisionTable(30, 8, 2);
        populateNumericNormalCase(numeric, 8, 2);
        populateNumericSetObject(numeric, 8, 2);

        testNumeric(numeric);
    }

    @Test
    public void testDateScale2() throws Exception {
        dropTables(stmt);

        String[] dateNormalCase = {GMTDate + ".18", GMTDate + ".1770000", dateTimeOffsetExpectedValue + ".1770000 +00:01",
                GMTDateWithoutDate + ".1770000", GMTDateWithoutDate + ".18", dateTimeOffsetExpectedValue + ".18 +00:01"};
        String[] dateSetObject = {GMTDate + ".18", GMTDate + ".177", dateTimeOffsetExpectedValue + ".177 +00:01", GMTDateWithoutDate,
                GMTDateWithoutDate, dateTimeOffsetExpectedValue + ".18 +00:01"};

        createDatePrecisionTable(2);
        populateDateNormalCase(2);
        populateDateSetObject(2);

        testDate(dateNormalCase, dateSetObject);
    }

    @Test
    public void testNumericPrecision8Scale0() throws Exception {
        dropTables(stmt);

        String[] numeric2 = {"1.12345", "12345", "567"};

        createNumericPrecisionTable(30, 8, 0);
        populateNumericNormalCase(numeric2, 8, 0);
        populateNumericSetObject(numeric2, 8, 0);

        testNumeric(numeric2);
    }

    @Test
    public void testDateScale0() throws Exception {
        dropTables(stmt);

        String[] dateNormalCase2 = {GMTDate, GMTDate + ".1770000", dateTimeOffsetExpectedValue + ".1770000 +00:01", GMTDateWithoutDate + ".1770000",
                GMTDateWithoutDate, dateTimeOffsetExpectedValue + " +00:01"};
        String[] dateSetObject2 = {GMTDate + ".0", GMTDate + ".177", dateTimeOffsetExpectedValue + ".177 +00:01", GMTDateWithoutDate,
                GMTDateWithoutDate, dateTimeOffsetExpectedValue + " +00:01"};

        createDatePrecisionTable(0);
        populateDateNormalCase(0);
        populateDateSetObject(0);

        testDate(dateNormalCase2, dateSetObject2);
    }

    @Test
    public void testNumericPrecision8Scale2Null() throws Exception {
        dropTables(stmt);

        String[] numericNull = {"null", "null", "null"};

        createNumericPrecisionTable(30, 8, 2);
        populateNumericSetObjectNull(8, 2);

        testNumeric(numericNull);
    }

    @Test
    public void testDateScale2Null() throws Exception {
        dropTables(stmt);

        String[] dateSetObjectNull = {"null", "null", "null", "null", "null", "null"};

        createDatePrecisionTable(2);
        populateDateSetObjectNull(2);

        testDate(dateSetObjectNull, dateSetObjectNull);
    }

    @Test
    public void testDateScale5Null() throws Exception {
        dropTables(stmt);

        String[] dateSetObjectNull = {"null", "null", "null", "null", "null", "null"};

        createDatePrecisionTable(5);
        populateDateNormalCaseNull(5);
        testDate(dateSetObjectNull, dateSetObjectNull);
    }

    private void testNumeric(String[] numeric) throws SQLException {

        try(ResultSet rs = stmt.executeQuery("select * from " + numericTable)) {
	        int numberOfColumns = rs.getMetaData().getColumnCount();
	
	        ArrayList<Integer> skipMax = new ArrayList<>();
	
	        while (rs.next()) {
	            testGetString(rs, numberOfColumns, skipMax, numeric);
	            testGetBigDecimal(rs, numberOfColumns, numeric);
	            testGetObject(rs, numberOfColumns, skipMax, numeric);
	        }
        }
    }

    private void testDate(String[] dateNormalCase,
            String[] dateSetObject) throws Exception {

        try(ResultSet rs = stmt.executeQuery("select * from " + dateTable)) {
	        int numberOfColumns = rs.getMetaData().getColumnCount();
	
	        ArrayList<Integer> skipMax = new ArrayList<>();
	
	        while (rs.next()) {
	            testGetString(rs, numberOfColumns, skipMax, dateNormalCase);
	            testGetObject(rs, numberOfColumns, skipMax, dateSetObject);
	            testGetDate(rs, numberOfColumns, dateSetObject);
	        }
        }
    }

    private void testGetString(ResultSet rs,
            int numberOfColumns,
            ArrayList<Integer> skipMax,
            String[] values) throws SQLException {
        int index = 0;
        for (int i = 1; i <= numberOfColumns; i = i + 3) {
            if (skipMax.contains(i)) {
                continue;
            }

            String stringValue1 = "" + rs.getString(i);
            String stringValue2 = "" + rs.getString(i + 1);
            String stringValue3 = "" + rs.getString(i + 2);

            try {
                if (rs.getMetaData().getColumnTypeName(i).equalsIgnoreCase("time")) {
                    assertTrue(stringValue2.equalsIgnoreCase("" + values[index]) && stringValue3.equalsIgnoreCase("" + values[index]),
                            "\nDecryption failed with getString(): " + stringValue1 + ", " + stringValue2 + ", " + stringValue3
                                    + ".\nExpected Value: " + values[index]);
                }
                else {
                    assertTrue(
                            values[index].contains(stringValue1) && stringValue2.equalsIgnoreCase("" + values[index])
                                    && stringValue3.equalsIgnoreCase("" + values[index]),
                            "\nDecryption failed with getString(): " + stringValue1 + ", " + stringValue2 + ", " + stringValue3
                                    + ".\nExpected Value: " + values[index]);
                }
            }
            finally {
                index++;
            }
        }
    }

    private void testGetBigDecimal(ResultSet rs,
            int numberOfColumns,
            String[] values) throws SQLException {
        int index = 0;
        for (int i = 1; i <= numberOfColumns; i = i + 3) {

            String decimalValue1 = "" + rs.getBigDecimal(i);
            String decimalValue2 = "" + rs.getBigDecimal(i + 1);
            String decimalValue3 = "" + rs.getBigDecimal(i + 2);

            try {
                assertTrue(
                        decimalValue1.equalsIgnoreCase(values[index]) && decimalValue2.equalsIgnoreCase(values[index])
                                && decimalValue3.equalsIgnoreCase(values[index]),
                        "Decryption failed with getBigDecimal(): " + decimalValue1 + ", " + decimalValue2 + ", " + decimalValue3
                                + "\nExpected value: " + values[index]);

            }
            finally {
                index++;
            }
        }
    }

    private void testGetObject(ResultSet rs,
            int numberOfColumns,
            ArrayList<Integer> skipMax,
            String[] values) throws SQLException {
        int index = 0;
        for (int i = 1; i <= numberOfColumns; i = i + 3) {
            if (skipMax.contains(i)) {
                continue;
            }

            try {
                String objectValue1 = "" + rs.getObject(i);
                String objectValue2 = "" + rs.getObject(i + 1);
                String objectValue3 = "" + rs.getObject(i + 2);

                assertTrue(
                        objectValue1.equalsIgnoreCase(values[index]) && objectValue2.equalsIgnoreCase(values[index])
                                && objectValue3.equalsIgnoreCase(values[index]),
                        "Decryption failed with getObject(): " + objectValue1 + ", " + objectValue2 + ", " + objectValue3 + "\nExpected value: "
                                + values[index]);

            }
            finally {
                index++;
            }
        }
    }

    private void testGetDate(ResultSet rs,
            int numberOfColumns,
            String[] dates) throws Exception {
        int index = 0;
        for (int i = 1; i <= numberOfColumns; i = i + 3) {

            if (rs instanceof SQLServerResultSet) {

                String stringValue1 = null;
                String stringValue2 = null;
                String stringValue3 = null;

                switch (i) {

                    case 1:
                        stringValue1 = "" + ((SQLServerResultSet) rs).getTimestamp(i);
                        stringValue2 = "" + ((SQLServerResultSet) rs).getTimestamp(i + 1);
                        stringValue3 = "" + ((SQLServerResultSet) rs).getTimestamp(i + 2);
                        break;

                    case 4:
                        stringValue1 = "" + ((SQLServerResultSet) rs).getTimestamp(i);
                        stringValue2 = "" + ((SQLServerResultSet) rs).getTimestamp(i + 1);
                        stringValue3 = "" + ((SQLServerResultSet) rs).getTimestamp(i + 2);
                        break;

                    case 7:
                        stringValue1 = "" + ((SQLServerResultSet) rs).getDateTimeOffset(i);
                        stringValue2 = "" + ((SQLServerResultSet) rs).getDateTimeOffset(i + 1);
                        stringValue3 = "" + ((SQLServerResultSet) rs).getDateTimeOffset(i + 2);
                        break;

                    case 10:
                        stringValue1 = "" + ((SQLServerResultSet) rs).getTime(i);
                        stringValue2 = "" + ((SQLServerResultSet) rs).getTime(i + 1);
                        stringValue3 = "" + ((SQLServerResultSet) rs).getTime(i + 2);
                        break;

                    case 13:
                        stringValue1 = "" + ((SQLServerResultSet) rs).getTime(i);
                        stringValue2 = "" + ((SQLServerResultSet) rs).getTime(i + 1);
                        stringValue3 = "" + ((SQLServerResultSet) rs).getTime(i + 2);
                        break;

                    case 16:
                        stringValue1 = "" + ((SQLServerResultSet) rs).getDateTimeOffset(i);
                        stringValue2 = "" + ((SQLServerResultSet) rs).getDateTimeOffset(i + 1);
                        stringValue3 = "" + ((SQLServerResultSet) rs).getDateTimeOffset(i + 2);
                        break;

                    default:
                        fail("Switch case is not matched with data");
                }

                try {
                    assertTrue(
                            stringValue1.equalsIgnoreCase(dates[index]) && stringValue2.equalsIgnoreCase(dates[index])
                                    && stringValue3.equalsIgnoreCase(dates[index]),
                            "Decryption failed with getString(): " + stringValue1 + ", " + stringValue2 + ", " + stringValue3 + "\nExpected value: "
                                    + dates[index]);
                }
                finally {
                    index++;
                }
            }

            else {
                throw new Exception("Result set is not instance of SQLServerResultSet");
            }
        }
    }

    private void populateDateNormalCase(int scale) throws SQLException {
        String sql = "insert into " + dateTable + " values( " + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?" + ")";

        try(SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) Util.getPreparedStmt(con, sql, stmtColEncSetting)) {

	        // datetime2(5)
	        for (int i = 1; i <= 3; i++) {
	            pstmt.setTimestamp(i, new Timestamp(date.getTime()), scale);
	        }
	
	        // datetime2 default
	        for (int i = 4; i <= 6; i++) {
	            pstmt.setTimestamp(i, new Timestamp(date.getTime()));
	        }
	
	        // datetimeoffset default
	        for (int i = 7; i <= 9; i++) {
	            pstmt.setDateTimeOffset(i, microsoft.sql.DateTimeOffset.valueOf(new Timestamp(date.getTime()), 1));
	        }
	
	        // time default
	        for (int i = 10; i <= 12; i++) {
	            pstmt.setTime(i, new Time(date.getTime()));
	        }
	
	        // time(3)
	        for (int i = 13; i <= 15; i++) {
	            pstmt.setTime(i, new Time(date.getTime()), scale);
	        }
	
	        // datetimeoffset(2)
	        for (int i = 16; i <= 18; i++) {
	            pstmt.setDateTimeOffset(i, microsoft.sql.DateTimeOffset.valueOf(new Timestamp(date.getTime()), 1), scale);
	        }
	
	        pstmt.execute();
        }
    }

    private void populateDateNormalCaseNull(int scale) throws SQLException {
        String sql = "insert into " + dateTable + " values( " + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?" + ")";

        try(SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) Util.getPreparedStmt(con, sql, stmtColEncSetting)) {

	        // datetime2(5)
	        for (int i = 1; i <= 3; i++) {
	            pstmt.setTimestamp(i, null, scale);
	        }
	
	        // datetime2 default
	        for (int i = 4; i <= 6; i++) {
	            pstmt.setTimestamp(i, null);
	        }
	
	        // datetimeoffset default
	        for (int i = 7; i <= 9; i++) {
	            pstmt.setDateTimeOffset(i, null);
	        }
	
	        // time default
	        for (int i = 10; i <= 12; i++) {
	            pstmt.setTime(i, null);
	        }
	
	        // time(3)
	        for (int i = 13; i <= 15; i++) {
	            pstmt.setTime(i, null, scale);
	        }
	
	        // datetimeoffset(2)
	        for (int i = 16; i <= 18; i++) {
	            pstmt.setDateTimeOffset(i, null, scale);
	        }
	
	        pstmt.execute();
        }
    }

    private void populateNumericNormalCase(String[] numeric,
            int precision,
            int scale) throws SQLException {
        String sql = "insert into " + numericTable + " values( " + "?,?,?," + "?,?,?," + "?,?,?" + ")";

        try(SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) Util.getPreparedStmt(con, sql, stmtColEncSetting)) {

	        // float(30)
	        for (int i = 1; i <= 3; i++) {
	            pstmt.setDouble(i, Double.valueOf(numeric[0]));
	        }
	
	        // decimal(10,5)
	        for (int i = 4; i <= 6; i++) {
	            pstmt.setBigDecimal(i, new BigDecimal(numeric[1]), precision, scale);
	        }
	
	        // numeric(8,2)
	        for (int i = 7; i <= 9; i++) {
	            pstmt.setBigDecimal(i, new BigDecimal(numeric[2]), precision, scale);
	        }
	
	        pstmt.execute();
        }
    }

    private void populateNumericSetObject(String[] numeric,
            int precision,
            int scale) throws SQLException {
        String sql = "insert into " + numericTable + " values( " + "?,?,?," + "?,?,?," + "?,?,?" + ")";

        try(SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) Util.getPreparedStmt(con, sql, stmtColEncSetting)) {
	
	        // float(30)
	        for (int i = 1; i <= 3; i++) {
	            pstmt.setObject(i, Double.valueOf(numeric[0]));
	
	        }
	
	        // decimal(10,5)
	        for (int i = 4; i <= 6; i++) {
	            pstmt.setObject(i, new BigDecimal(numeric[1]), java.sql.Types.DECIMAL, precision, scale);
	        }
	
	        // numeric(8,2)
	        for (int i = 7; i <= 9; i++) {
	            pstmt.setObject(i, new BigDecimal(numeric[2]), java.sql.Types.NUMERIC, precision, scale);
	        }
	
	        pstmt.execute();
        }
    }

    private void populateNumericSetObjectNull(int precision,
            int scale) throws SQLException {
        String sql = "insert into " + numericTable + " values( " + "?,?,?," + "?,?,?," + "?,?,?" + ")";

        try(SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) Util.getPreparedStmt(con, sql, stmtColEncSetting)) {

	        // float(30)
	        for (int i = 1; i <= 3; i++) {
	            pstmt.setObject(i, null, java.sql.Types.DOUBLE);
	
	        }
	
	        // decimal(10,5)
	        for (int i = 4; i <= 6; i++) {
	            pstmt.setObject(i, null, java.sql.Types.DECIMAL, precision, scale);
	        }
	
	        // numeric(8,2)
	        for (int i = 7; i <= 9; i++) {
	            pstmt.setObject(i, null, java.sql.Types.NUMERIC, precision, scale);
	        }
	
	        pstmt.execute();
        }
    }

    private void populateDateSetObject(int scale) throws SQLException {
        String sql = "insert into " + dateTable + " values( " + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?" + ")";

        try(SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) Util.getPreparedStmt(con, sql, stmtColEncSetting)) {

	        // datetime2(5)
	        for (int i = 1; i <= 3; i++) {
	            pstmt.setObject(i, new Timestamp(date.getTime()), java.sql.Types.TIMESTAMP, scale);
	        }
	
	        // datetime2 default
	        for (int i = 4; i <= 6; i++) {
	            pstmt.setObject(i, new Timestamp(date.getTime()), java.sql.Types.TIMESTAMP);
	        }
	
	        // datetimeoffset default
	        for (int i = 7; i <= 9; i++) {
	            pstmt.setObject(i, microsoft.sql.DateTimeOffset.valueOf(new Timestamp(date.getTime()), 1), microsoft.sql.Types.DATETIMEOFFSET);
	        }
	
	        // time default
	        for (int i = 10; i <= 12; i++) {
	            pstmt.setObject(i, new Time(date.getTime()), java.sql.Types.TIME);
	        }
	
	        // time(3)
	        for (int i = 13; i <= 15; i++) {
	            pstmt.setObject(i, new Time(date.getTime()), java.sql.Types.TIME, scale);
	        }
	
	        // datetimeoffset(2)
	        for (int i = 16; i <= 18; i++) {
	            pstmt.setObject(i, microsoft.sql.DateTimeOffset.valueOf(new Timestamp(date.getTime()), 1), microsoft.sql.Types.DATETIMEOFFSET, scale);
	        }
	
	        pstmt.execute();
        }
    }

    private void populateDateSetObjectNull(int scale) throws SQLException {
        String sql = "insert into " + dateTable + " values( " + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?" + ")";

        try(SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) Util.getPreparedStmt(con, sql, stmtColEncSetting)) {

	        // datetime2(5)
	        for (int i = 1; i <= 3; i++) {
	            pstmt.setObject(i, null, java.sql.Types.TIMESTAMP, scale);
	        }
	
	        // datetime2 default
	        for (int i = 4; i <= 6; i++) {
	            pstmt.setObject(i, null, java.sql.Types.TIMESTAMP);
	        }
	
	        // datetimeoffset default
	        for (int i = 7; i <= 9; i++) {
	            pstmt.setObject(i, null, microsoft.sql.Types.DATETIMEOFFSET);
	        }
	
	        // time default
	        for (int i = 10; i <= 12; i++) {
	            pstmt.setObject(i, null, java.sql.Types.TIME);
	        }
	
	        // time(3)
	        for (int i = 13; i <= 15; i++) {
	            pstmt.setObject(i, null, java.sql.Types.TIME, scale);
	        }
	
	        // datetimeoffset(2)
	        for (int i = 16; i <= 18; i++) {
	            pstmt.setObject(i, null, microsoft.sql.Types.DATETIMEOFFSET, scale);
	        }
	
	        pstmt.execute();
        }
    }
}
