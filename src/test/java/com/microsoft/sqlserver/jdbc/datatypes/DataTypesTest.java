package com.microsoft.sqlserver.jdbc.datatypes;

import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.Calendar;
import java.util.EnumSet;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.SimpleTimeZone;
import java.util.TimeZone;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;

import microsoft.sql.DateTimeOffset;


/*
 * This test suite tests all kinds of temporal data types for Katmai or later versions. It has tests for
 * date/time/datetime2/datetimeoffset data types. Also includes tests for data type mappings.
 */
@RunWith(JUnitPlatform.class)
public class DataTypesTest extends AbstractTest {

    final static String tableName = RandomUtil.getIdentifier("KatmaiDataTypesTable");
    final static String escapedTableName = AbstractSQLGenerator.escapeIdentifier(tableName);

    final static String procName = RandomUtil.getIdentifier("KatmaiDataTypesTableProc");
    final static String escapedProcName = AbstractSQLGenerator.escapeIdentifier(procName);

    enum SQLType {
        date("yyyy-mm-dd", 0, java.sql.Types.DATE, "java.sql.Date"),

        time("hh:mm:ss", 7, java.sql.Types.TIME, "java.sql.Time"),

        datetime("yyyy-mm-dd hh:mm:ss", 3, java.sql.Types.TIMESTAMP, "java.sql.Timestamp"),

        datetime2("yyyy-mm-dd hh:mm:ss", 7, java.sql.Types.TIMESTAMP, "java.sql.Timestamp"),

        datetimeoffset("yyyy-mm-dd hh:mm:ss +hh:mm", 7, microsoft.sql.Types.DATETIMEOFFSET, "microsoft.sql.DateTimeOffset");

        final int basePrecision;
        final int maxPrecision;
        final int maxFractionalSecondsDigits;
        final int jdbcType;
        final String className;

        SQLType(String format, int maxFractionalSecondsDigits, int jdbcType, String className) {
            assert maxFractionalSecondsDigits >= 0;

            this.basePrecision = format.length();
            this.maxFractionalSecondsDigits = maxFractionalSecondsDigits;
            this.maxPrecision = basePrecision
                    + ((maxFractionalSecondsDigits > 0) ? (1 + maxFractionalSecondsDigits) : 0);
            this.jdbcType = jdbcType;
            this.className = className;
        }
    };

    abstract static class SQLValue {
        private final SQLType sqlType;
        private final String stringValue;

        final String getString() {
            return stringValue;
        }

        private final String sqlTypeExpression;
        private final int precision;
        private final int scale;

        SQLValue(SQLType sqlType, String stringValue) {
            this.sqlType = sqlType;
            this.stringValue = stringValue;
            this.sqlTypeExpression = sqlType.toString();
            this.precision = sqlType.maxPrecision;
            this.scale = sqlType.maxFractionalSecondsDigits;
        }

        SQLValue(SQLType sqlType, String stringValue, int fractionalSecondsDigits) {
            this.sqlType = sqlType;
            this.stringValue = stringValue;
            this.sqlTypeExpression = sqlType.toString() + "(" + fractionalSecondsDigits + ")";
            this.precision = sqlType.basePrecision + (1 + fractionalSecondsDigits);
            this.scale = fractionalSecondsDigits;
        }

        /*
         * For testing the setObject and setNull methods in PreparedStatement, use the verifySetter* methods. These
         * methods prepare a single statement and execute it for all different data types by calling the appropriate
         * 'setObject' methods for each data type and/or type conversion.
         */
        abstract void verifySetters(PreparedStatement ps) throws Exception;

        abstract void verifySettersUtilDate(PreparedStatement ps) throws Exception;

        abstract void verifySettersCalendar(PreparedStatement ps) throws Exception;

        abstract void verifyRSGetters(ResultSet rs) throws Exception;

        abstract void verifyRSUpdaters(ResultSet rs) throws Exception;

        abstract void verifyCSGetters(CallableStatement cs) throws Exception;

        private String sqlCastExpression() {
            return "CAST('" + stringValue + "' AS " + sqlTypeExpression + ")";
        }

        void verifyResultSetMetaData(Connection conn) throws Exception {
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT TOP 0 " + sqlCastExpression())) {

                ResultSetMetaData metadata = rs.getMetaData();

                assertEquals(sqlType.jdbcType, metadata.getColumnType(1), "getColumnType() of " + sqlCastExpression());
                assertEquals(sqlType.toString(), metadata.getColumnTypeName(1),
                        "getColumnTypeName() of " + sqlCastExpression());
                assertEquals(precision, metadata.getPrecision(1), "getPrecision() of " + sqlCastExpression());

                // Display size of temporal types is the precision per JDBC spec
                assertEquals(precision, metadata.getColumnDisplaySize(1),
                        "getColumnDisplaySize() of " + sqlCastExpression());

                // Scale is interpreted as number of fractional seconds precision
                assertEquals(scale, metadata.getScale(1), "getScale() of " + sqlCastExpression());
                assertEquals(sqlType.className, metadata.getColumnClassName(1),
                        "getColumnClassName() of " + sqlCastExpression());

                // Katmai temporal types are not signed
                assertEquals(false, metadata.isSigned(1), "isSigned() of " + sqlCastExpression());

                // Katmai temporal types are searchable (i.e. usable in a WHERE clause)
                assertEquals(true, metadata.isSearchable(1), "isSearchable() of " + sqlCastExpression());
            }
        }

        void verifyParameterMetaData(Connection conn) throws Exception {
            try (Statement stmt = conn.createStatement()) {
                // Create the stored proc
                stmt.executeUpdate(
                        "CREATE PROCEDURE " + escapedProcName + " @arg " + sqlTypeExpression + " AS SELECT @arg");

                try (PreparedStatement pstmt = conn.prepareStatement("{call " + escapedProcName + "(?)}")) {
                    ParameterMetaData metadata = pstmt.getParameterMetaData();

                    assertEquals(sqlType.jdbcType, metadata.getParameterType(1),
                            "getParameterType() of " + sqlCastExpression());
                    assertEquals(sqlType.toString(), metadata.getParameterTypeName(1),
                            "getParameterTypeName() of " + sqlCastExpression());
                    assertEquals(precision, metadata.getPrecision(1), "getPrecision() of " + sqlCastExpression());

                    // Scale is interpreted as number of fractional seconds precision
                    assertEquals(scale, metadata.getScale(1), "getScale() of " + sqlCastExpression());
                    assertEquals(sqlType.className, metadata.getParameterClassName(1),
                            "getParameterClassName() of " + sqlCastExpression());

                    // Katmai temporal types are not signed
                    assertEquals(false, metadata.isSigned(1), "isSigned() of " + sqlCastExpression());
                }
            } finally {
                try (Statement stmt = conn.createStatement()) {
                    TestUtils.dropProcedureIfExists(escapedProcName, stmt);
                }
            }
        }

        void verifyRSGetters(Connection conn) throws Exception {
            try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(
                    "SELECT " + sqlCastExpression() + ", CAST(" + sqlCastExpression() + " AS VARCHAR(60))")) {
                rs.next();
                verifyRSGetters(rs);
            }
        }

        void verifyRSUpdaters(Connection conn) throws Exception {
            assumeTrue(!isSqlAzureDW(), TestResource.getResource("R_skipAzure"));

            try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
                TestUtils.dropTableIfExists(escapedTableName, stmt);

                stmt.executeUpdate("CREATE TABLE " + escapedTableName + " (col1 " + sqlTypeExpression
                        + ", col2 int identity(1,1) primary key)");

                stmt.executeUpdate("INSERT INTO " + escapedTableName + " VALUES (" + sqlCastExpression() + ")");

                try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + escapedTableName)) {
                    rs.next();
                    verifyRSUpdaters(rs);
                } finally {
                    TestUtils.dropTableIfExists(escapedTableName, stmt);
                }
            }
        }

        void verifySetters(Connection conn) throws Exception {
            assumeTrue(!isSqlAzureDW(), TestResource.getResource("R_skipAzure"));

            try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
                TestUtils.dropTableIfExists(escapedTableName, stmt);
                stmt.executeUpdate("CREATE TABLE " + escapedTableName + " (col1 " + sqlTypeExpression
                        + ", col2 int identity(1,1) primary key)");

                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + escapedTableName + " VALUES (?) SELECT * FROM " + escapedTableName,
                        ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
                    verifySetters(ps);
                    // Verify setObject function for the mapping java.util.Date to TIMESTAMP
                    stmt.executeUpdate("TRUNCATE TABLE " + escapedTableName);

                    verifySettersUtilDate(ps);
                    // Verify setObject function for the mapping java.util.Calendar to TIMESTAMP
                    stmt.executeUpdate("TRUNCATE TABLE " + escapedTableName);
                    verifySettersCalendar(ps);
                } finally {
                    TestUtils.dropTableIfExists(escapedTableName, stmt);
                }
            }
        }

        void verifyCSGetters(Connection conn) throws Exception {
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropProcedureIfExists(escapedProcName, stmt);
                stmt.executeUpdate("CREATE PROCEDURE " + escapedProcName + " @argIn " + sqlTypeExpression + ","
                        + " @argOut " + sqlTypeExpression + " OUTPUT" + " AS " + " SET @argOut=@argIn");

                try (CallableStatement cs = conn.prepareCall("{call " + escapedProcName + "(?,?)}")) {
                    verifyCSGetters(cs);
                } finally {
                    TestUtils.dropProcedureIfExists(escapedProcName, stmt);
                }
            }
        }
    }

    static final class DateValue extends SQLValue {
        private final Date expected;

        DateValue(String stringValue) {
            super(SQLType.date, stringValue);
            this.expected = Date.valueOf(stringValue);
        }

        private Timestamp expectedTimestampMillisPrecision() {
            return new Timestamp(expected.getTime());
        }

        private java.util.Date expectedUtilDate() {
            return (java.util.Date) expected;
        }

        private java.util.Calendar expectedCalendar() {
            Calendar cal = Calendar.getInstance();
            cal.clear();
            cal.setTimeInMillis(expected.getTime());
            return cal;
        }

        void verifyRSGetters(ResultSet rs) throws Exception {
            assertEquals(expected, rs.getDate(1));
            assertEquals(new Timestamp(expected.getTime()), rs.getTimestamp(1));
            assertEquals(expected.toString(), rs.getString(1));
        }

        void verifyRSUpdaters(ResultSet rs) throws Exception {
            rs.updateDate(1, expected);
            rs.updateRow();

            assertEquals(expected, rs.getDate(1));
        }

        void verifySetters(PreparedStatement ps) throws Exception {
            ps.setDate(1, expected);
            ps.execute();
            ps.getMoreResults();
            try (ResultSet rs = ps.getResultSet()) {
                rs.next();
                assertEquals(expected, rs.getDate(1));
            }
        }

        void verifySettersUtilDate(PreparedStatement ps) throws Exception {
            int currentRow = 0;
            ps.setObject(1, expectedUtilDate());
            ps.execute();
            ps.getMoreResults();
            try (ResultSet rs = ps.getResultSet()) {
                rs.next();
                assertEquals(expected, rs.getDate(1));
                assertEquals(expectedUtilDate(), (java.util.Date) rs.getObject(1));
            }

            // Test datetime2 column with target type TIMESTAMP
            ps.setObject(1, expectedUtilDate(), java.sql.Types.DATE);
            ps.execute();
            ps.getMoreResults();
            try (ResultSet rs = ps.getResultSet()) {
                // Go to the first row
                rs.next();
                // Go to the row just inserted
                rs.relative(++currentRow);
                assertEquals(expected, rs.getDate(1));
            }

            ps.setObject(1, expectedUtilDate(), java.sql.Types.TIMESTAMP);
            ps.execute();
            ps.getMoreResults();
            try (ResultSet rs = ps.getResultSet()) {
                // Go to the first row
                rs.next();
                // Go to the row just inserted
                rs.relative(++currentRow);
                assertEquals(expectedTimestampMillisPrecision(), rs.getTimestamp(1));
            }

            // Test the setNull() methods for different data type conversions
            ps.setNull(1, java.sql.Types.DATE);
            ps.execute();
            ps.getMoreResults();
            try (ResultSet rs = ps.getResultSet()) {
                // Go to the next row
                rs.next();
                rs.relative(++currentRow);
                // Read the column, first before calling rs.wasNull()
                rs.getTimestamp(1);
                assertEquals(true, rs.wasNull());
            }

            ps.setNull(1, java.sql.Types.TIMESTAMP);
            ps.execute();
            ps.getMoreResults();
            try (ResultSet rs = ps.getResultSet()) {
                // Go to the next row
                rs.next();
                rs.relative(++currentRow);
                // Read the column, first before calling rs.wasNull()
                rs.getTimestamp(1);
                assertEquals(true, rs.wasNull());
            }
        }

        void verifySettersCalendar(PreparedStatement ps) throws Exception {
            int currentRow = 0;
            ps.setObject(1, expectedCalendar());
            ps.execute();
            ps.getMoreResults();
            try (ResultSet rs = ps.getResultSet()) {
                rs.next();
                assertEquals(expected, rs.getDate(1));
            }

            // test datetime2 column with target type TIMESTAMP
            ps.setObject(1, expectedCalendar(), java.sql.Types.DATE);
            ps.execute();
            ps.getMoreResults();
            try (ResultSet rs = ps.getResultSet()) {
                // Go to the first row
                rs.next();
                // Go to the row just inserted
                rs.relative(++currentRow);
                assertEquals(expected, rs.getDate(1));
            }

            ps.setObject(1, expectedCalendar(), java.sql.Types.TIMESTAMP);
            ps.execute();
            ps.getMoreResults();
            try (ResultSet rs = ps.getResultSet()) {
                // Go to the first row
                rs.next();
                // Go to the row just inserted
                rs.relative(++currentRow);
                assertEquals(expectedTimestampMillisPrecision(), rs.getTimestamp(1));
            }
        }

        void verifyCSGetters(CallableStatement cs) throws Exception {
            cs.setDate(1, expected);
            cs.registerOutParameter(2, java.sql.Types.DATE);
            cs.execute();
            assertEquals(expected, cs.getDate(2));
            assertEquals(expected, cs.getObject(2));
        }
    }

    static final class TimeValue extends SQLValue {
        private int hour;
        private int minute;
        private int second;
        private int nanos;
        private TimeZone tz;

        TimeValue(String stringValue) {
            super(SQLType.time, stringValue);
            initExpected(stringValue, TimeZone.getDefault());
        }

        TimeValue(String stringValue, int fractionalSecondsDigits) {
            super(SQLType.time, stringValue, fractionalSecondsDigits);
            initExpected(stringValue, TimeZone.getDefault());
        }

        private void initExpected(String stringValue, TimeZone tz) {
            // "hh:mm:ss[.nnnnnnn]"
            this.hour = Integer.valueOf(stringValue.substring(0, 2));
            this.minute = Integer.valueOf(stringValue.substring(3, 5));
            this.second = Integer.valueOf(stringValue.substring(6, 8));

            this.nanos = (8 == stringValue.indexOf('.')) ? (new BigDecimal(stringValue.substring(8)))
                    .scaleByPowerOfTen(9).intValue() : 0;

            this.tz = tz;
        }

        private Timestamp expectedTimestamp() {
            Calendar cal = Calendar.getInstance(tz);
            cal.clear();
            cal.set(1900, Calendar.JANUARY, 1, hour, minute, second);

            Timestamp timestamp = new Timestamp(cal.getTimeInMillis());
            timestamp.setNanos(nanos);

            return timestamp;
        }

        private java.sql.Time expectedTime() {
            Calendar cal = Calendar.getInstance(tz);
            cal.clear();
            cal.set(1970, Calendar.JANUARY, 1, hour, minute, second);

            cal.set(Calendar.MILLISECOND, (nanos + 500000) / 1000000);

            return new java.sql.Time(cal.getTimeInMillis());
        }

        private Timestamp expectedTimestampMillisPrecision() {
            Calendar cal = Calendar.getInstance(tz);
            cal.clear();
            cal.set(1900, Calendar.JANUARY, 1, hour, minute, second);
            cal.set(Calendar.MILLISECOND, (nanos + 500000) / 1000000);

            Timestamp timestamp = new Timestamp(cal.getTimeInMillis());
            return timestamp;
        }

        private java.util.Date expectedUtilDate() {
            Calendar cal = Calendar.getInstance(tz);
            cal.clear();
            cal.set(1970, Calendar.JANUARY, 1, hour, minute, second);

            cal.set(Calendar.MILLISECOND, (nanos + 500000) / 1000000);
            java.util.Date udate = new java.util.Date(cal.getTimeInMillis());
            return udate;

        }

        private java.util.Calendar expectedCalendar() {
            Calendar cal = Calendar.getInstance(tz);
            cal.clear();
            cal.set(1970, Calendar.JANUARY, 1, hour, minute, second);

            cal.set(Calendar.MILLISECOND, (nanos + 500000) / 1000000);
            return cal;
        }

        void verifyRSGetters(ResultSet rs) throws Exception {
            assertEquals(expectedTime(), rs.getTime(1));
            assertEquals(expectedTimestamp(), rs.getTimestamp(1));
            assertEquals(this.getString(), rs.getString(1));
        }

        void verifyRSUpdaters(ResultSet rs) throws Exception {
            rs.updateTime(1, expectedTime());
            rs.updateRow();

            assertEquals(expectedTime(), rs.getTime(1));
        }

        void verifySetters(PreparedStatement ps) throws Exception {
            ps.setTime(1, expectedTime());
            ps.execute();
            ps.getMoreResults();
            try (ResultSet rs = ps.getResultSet()) {
                rs.next();
                assertEquals(expectedTime(), rs.getTime(1));
            }
        }

        void verifySettersUtilDate(PreparedStatement ps) throws Exception {
            int currentRow = 0;
            ps.setObject(1, expectedUtilDate());
            ps.execute();
            ps.getMoreResults();
            try (ResultSet rs = ps.getResultSet()) {
                rs.next();
                assertEquals(expectedTime(), rs.getTime(1));
                assertEquals(expectedTimestampMillisPrecision(), rs.getTimestamp(1));
                assertEquals(expectedUtilDate(), (java.util.Date) rs.getObject(1));
            }

            // Test datetime2 column with target type TIMESTAMP
            ps.setObject(1, expectedUtilDate(), java.sql.Types.TIME);
            ps.execute();
            ps.getMoreResults();
            try (ResultSet rs = ps.getResultSet()) {
                // Go to the first row
                rs.next();
                // Go to the row just inserted
                rs.relative(++currentRow);
                assertEquals(expectedTime(), rs.getTime(1));
            }

            ps.setObject(1, expectedUtilDate(), java.sql.Types.TIMESTAMP);
            ps.execute();
            ps.getMoreResults();
            try (ResultSet rs = ps.getResultSet()) {
                // Go to the first row
                rs.next();
                // Go to the row just inserted
                rs.relative(++currentRow);
                assertEquals(expectedTimestampMillisPrecision(), rs.getTimestamp(1));
            }

            // Test the setNull() methods for different data type conversions
            ps.setNull(1, java.sql.Types.TIME);
            ps.execute();
            ps.getMoreResults();
            try (ResultSet rs = ps.getResultSet()) {
                // Go to the next row
                rs.next();
                rs.relative(++currentRow);
                // Read the column, first before calling rs.wasNull()
                rs.getTimestamp(1);
                assertEquals(true, rs.wasNull());
            }

            ps.setNull(1, java.sql.Types.TIMESTAMP);
            ps.execute();
            ps.getMoreResults();
            try (ResultSet rs = ps.getResultSet()) {
                // Go to the next row
                rs.next();
                rs.relative(++currentRow);
                // Read the column, first before calling rs.wasNull()
                rs.getTimestamp(1);
                assertEquals(true, rs.wasNull());
            }
        }

        void verifySettersCalendar(PreparedStatement ps) throws Exception {
            int currentRow = 0;
            ps.setObject(1, expectedCalendar());
            ps.execute();
            ps.getMoreResults();
            try (ResultSet rs = ps.getResultSet()) {
                rs.next();
                assertEquals(expectedTime(), rs.getTime(1));
                assertEquals(expectedTimestampMillisPrecision(), rs.getTimestamp(1));
            }

            // test datetime2 column with target type TIMESTAMP
            ps.setObject(1, expectedCalendar(), java.sql.Types.TIME);
            ps.execute();
            ps.getMoreResults();
            try (ResultSet rs = ps.getResultSet()) {
                // Go to the first row
                rs.next();
                // Go to the row just inserted
                rs.relative(++currentRow);
                assertEquals(expectedTime(), rs.getTime(1));
            }

            ps.setObject(1, expectedCalendar(), java.sql.Types.TIMESTAMP);
            ps.execute();
            ps.getMoreResults();
            try (ResultSet rs = ps.getResultSet()) {
                // Go to the first row
                rs.next();
                // Go to the row just inserted
                rs.relative(++currentRow);
                assertEquals(expectedTimestampMillisPrecision(), rs.getTimestamp(1));
            }
        }

        void verifyCSGetters(CallableStatement cs) throws Exception {
            cs.setTime(1, expectedTime());
            cs.registerOutParameter(2, java.sql.Types.TIME);
            cs.execute();
            assertEquals(expectedTime(), cs.getTime(2));
            assertEquals(expectedTime(), cs.getObject(2));
        }
    }

    static final class DateTime2Value extends SQLValue {
        private String stringValue;
        private long utcMillis;
        private int nanos;
        private TimeZone tz;

        DateTime2Value(String stringValue) {
            super(SQLType.datetime2, stringValue);
            initExpected(stringValue, TimeZone.getDefault());
        }

        DateTime2Value(String stringValue, int fractionalSecondsDigits) {
            super(SQLType.datetime2, stringValue, fractionalSecondsDigits);
            initExpected(stringValue, TimeZone.getDefault());
        }

        DateTime2Value(String stringValue, String timeZone) {
            super(SQLType.datetime2, stringValue);
            initExpected(stringValue, TimeZone.getTimeZone(timeZone));
        }

        DateTime2Value(String stringValue, int fractionalSecondsDigits, String timeZone) {
            super(SQLType.datetime2, stringValue, fractionalSecondsDigits);
            initExpected(stringValue, TimeZone.getTimeZone(timeZone));
        }

        private void initExpected(String timestampString, TimeZone tz) {
            // "yyyy-MM-dd hh:mm:ss[.nnnnnnn]"
            int year = Integer.valueOf(timestampString.substring(0, 4));
            int month = Integer.valueOf(timestampString.substring(5, 7));
            int day = Integer.valueOf(timestampString.substring(8, 10));
            int hour = Integer.valueOf(timestampString.substring(11, 13));
            int minute = Integer.valueOf(timestampString.substring(14, 16));
            int second = Integer.valueOf(timestampString.substring(17, 19));

            int nanos = (19 == timestampString.indexOf('.')) ? (new BigDecimal(timestampString.substring(19)))
                    .scaleByPowerOfTen(9).intValue() : 0;

            Calendar cal = Calendar.getInstance(tz);
            cal.set(Calendar.YEAR, year);
            cal.set(Calendar.MONTH, month - 1);
            cal.set(Calendar.DAY_OF_MONTH, day);
            cal.set(Calendar.HOUR_OF_DAY, hour);
            cal.set(Calendar.MINUTE, minute);
            cal.set(Calendar.SECOND, second);
            cal.set(Calendar.MILLISECOND, nanos / 1000000);

            this.stringValue = timestampString;
            this.utcMillis = cal.getTimeInMillis();
            this.nanos = nanos;
            this.tz = tz;
        }

        private Date expectedDate() {
            Calendar cal = Calendar.getInstance(tz);
            cal.setTimeInMillis(utcMillis);
            cal.set(Calendar.HOUR_OF_DAY, 0);
            cal.set(Calendar.MINUTE, 0);
            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            return new Date(cal.getTimeInMillis());
        }

        private java.sql.Time expectedTime() {
            Calendar cal = Calendar.getInstance(tz);
            cal.setTimeInMillis(utcMillis);
            if (nanos % 1000000 >= 500000)
                cal.add(Calendar.MILLISECOND, 1);
            cal.set(Calendar.YEAR, 1970);
            cal.set(Calendar.MONTH, Calendar.JANUARY);
            cal.set(Calendar.DAY_OF_MONTH, 1);
            return new java.sql.Time(cal.getTimeInMillis());
        }

        private Timestamp expectedTimestamp() {
            Timestamp timestamp = new Timestamp(utcMillis);
            timestamp.setNanos(nanos);
            return timestamp;
        }

        private Timestamp expectedTimestampMillisPrecision() {
            Timestamp timestamp = new Timestamp(utcMillis);
            return timestamp;
        }

        private java.util.Date expectedUtilDate() {
            return new java.util.Date(utcMillis);
        }

        private java.util.Calendar expectedCalendar() {
            Calendar cal = Calendar.getInstance(tz);
            cal.setTimeInMillis(utcMillis);
            return cal;
        }

        void verifyRSGetters(ResultSet rs) throws Exception {
            assertEquals(expectedDate(), rs.getDate(1, Calendar.getInstance(tz)));
            assertEquals(expectedTime(), rs.getTime(1, Calendar.getInstance(tz)));
            assertEquals(expectedTimestamp(), rs.getTimestamp(1, Calendar.getInstance(tz)));
            assertEquals(stringValue, rs.getString(1));
        }

        void verifyRSUpdaters(ResultSet rs) throws Exception {
            TimeZone tzDefault = TimeZone.getDefault();
            try {
                // temporary change default time zone while doing the update
                TimeZone.setDefault(tz);

                // Update the timestamp value with this value's time zone (set as the VM default above)
                rs.updateTimestamp(1, expectedTimestamp());
                rs.updateRow();

                // Verify the update (this value's time zone is still the default)
                assertEquals(expectedTimestamp(), rs.getTimestamp(1));
            } finally {
                // Restore the original default time zone
                TimeZone.setDefault(tzDefault);
            }

            /*
             * Verify the update (after restoring the default time zone) using the getTimestamp variant that takes a
             * time zone argument (as a Calendar)
             */
            assertEquals(expectedTimestamp(), rs.getTimestamp(1, Calendar.getInstance(tz)));
        }

        void verifySetters(PreparedStatement ps) throws Exception {
            TimeZone tzDefault = TimeZone.getDefault();
            try {
                // temporary change default time zone
                TimeZone.setDefault(tz);

                ps.setTimestamp(1, expectedTimestamp());
                ps.execute();
                ps.getMoreResults();
                try (ResultSet rs = ps.getResultSet()) {
                    rs.next();

                    assertEquals(expectedTimestamp(), rs.getTimestamp(1));
                }
            } finally {
                // Restore the original default time zone
                TimeZone.setDefault(tzDefault);
            }

            // Verify PreparedStatement.setTimestamp(..., Calendar) as well (don't change the VM default)
            ps.setTimestamp(1, expectedTimestamp(), Calendar.getInstance(tz));
            ps.execute();
            ps.getMoreResults();
            try (ResultSet rs = ps.getResultSet()) {
                rs.next();
                assertEquals(expectedTimestamp(), rs.getTimestamp(1, Calendar.getInstance(tz)));
            }
        }

        void verifySettersUtilDate(PreparedStatement ps) throws Exception {
            int currentRow = 0;
            ps.setObject(1, expectedUtilDate());
            ps.execute();
            ps.getMoreResults();
            try (ResultSet rs = ps.getResultSet()) {
                rs.next();
                assertEquals(expectedTimestampMillisPrecision(), rs.getTimestamp(1));
                assertEquals(expectedUtilDate(), (java.util.Date) rs.getObject(1));
            }

            // Test datetime2 column with target type TIMESTAMP
            ps.setObject(1, expectedUtilDate(), java.sql.Types.TIMESTAMP);
            ps.execute();
            ps.getMoreResults();
            try (ResultSet rs = ps.getResultSet()) {
                // Go to the next row
                rs.next();
                rs.relative(++currentRow);
                assertEquals(expectedTimestampMillisPrecision(), rs.getTimestamp(1));
            }

            // Test the setNull() methods for different data type conversions
            ps.setNull(1, java.sql.Types.TIME);
            ps.execute();
            ps.getMoreResults();
            try (ResultSet rs = ps.getResultSet()) {
                // Go to the next row
                rs.next();
                rs.relative(++currentRow);
                // Read the column, first before calling rs.wasNull()
                rs.getTimestamp(1);
                assertEquals(true, rs.wasNull());
            }

            ps.setNull(1, java.sql.Types.DATE);
            ps.execute();
            ps.getMoreResults();
            try (ResultSet rs = ps.getResultSet()) {
                // Go to the next row
                rs.next();
                rs.relative(++currentRow);
                // Read the column, first before calling rs.wasNull()
                rs.getTimestamp(1);
                assertEquals(true, rs.wasNull());
            }

            ps.setNull(1, java.sql.Types.TIMESTAMP);
            ps.execute();
            ps.getMoreResults();
            try (ResultSet rs = ps.getResultSet()) {
                // Go to the next row
                rs.next();
                rs.relative(++currentRow);
                // Read the column, first before calling rs.wasNull()
                rs.getTimestamp(1);
                assertEquals(true, rs.wasNull());
            }
        }

        void verifySettersCalendar(PreparedStatement ps) throws Exception {
            int currentRow = 0;
            ps.setObject(1, expectedCalendar());
            ps.execute();
            ps.getMoreResults();
            try (ResultSet rs = ps.getResultSet()) {
                rs.next();
                assertEquals(expectedTimestampMillisPrecision(), rs.getTimestamp(1));
            }

            // Test datetime2 column with target type TIMESTAMP
            ps.setObject(1, expectedCalendar(), java.sql.Types.TIMESTAMP);
            ps.execute();
            ps.getMoreResults();
            try (ResultSet rs = ps.getResultSet()) {
                // Go to the next row
                rs.next();
                rs.relative(++currentRow);
                assertEquals(expectedTimestampMillisPrecision(), rs.getTimestamp(1));
            }
        }

        void verifyCSGetters(CallableStatement cs) throws Exception {
            cs.setTimestamp(1, expectedTimestamp(), Calendar.getInstance(tz));
            cs.registerOutParameter(2, java.sql.Types.TIMESTAMP);
            cs.execute();

            // Verify typed getter (with time zone argument)
            assertEquals(expectedTimestamp(), cs.getTimestamp(2, Calendar.getInstance(tz)));

            // Verify getObject (no provision for time zone argument - need to push/pop the default)
            TimeZone tzDefault = TimeZone.getDefault();
            try {
                TimeZone.setDefault(tz);

                assertEquals(expectedTimestamp(), cs.getObject(2));
            } finally {
                TimeZone.setDefault(tzDefault);
            }
        }
    }

    static final class DateTimeOffsetValue extends SQLValue {
        private final DateTimeOffset dto;

        DateTimeOffset get() {
            return dto;
        }

        private final DateTimeOffset initExpected(String stringValue, int fractionalSecondsDigits) {
            int lastColon = stringValue.lastIndexOf(':');

            String offsetString = stringValue.substring(lastColon - 3);
            int minutesOffset = 60 * Integer.valueOf(offsetString.substring(1, 3))
                    + Integer.valueOf(offsetString.substring(4, 6));

            if (offsetString.startsWith("-"))
                minutesOffset = -minutesOffset;

            String timestampString = stringValue.substring(0, lastColon - 4);
            int year = Integer.valueOf(timestampString.substring(0, 4));
            int month = Integer.valueOf(timestampString.substring(5, 7));
            int day = Integer.valueOf(timestampString.substring(8, 10));
            int hour = Integer.valueOf(timestampString.substring(11, 13));
            int minute = Integer.valueOf(timestampString.substring(14, 16));
            int second = Integer.valueOf(timestampString.substring(17, 19));

            int nanos = (19 == timestampString.indexOf('.')) ? (new BigDecimal(timestampString.substring(19)))
                    .scaleByPowerOfTen(9).intValue() : 0;

            Calendar cal = Calendar.getInstance(Locale.US);
            cal.set(Calendar.ZONE_OFFSET, 1000 * 60 * minutesOffset);
            cal.set(Calendar.DST_OFFSET, 0);
            cal.set(Calendar.YEAR, year);
            cal.set(Calendar.MONTH, month - 1);
            cal.set(Calendar.DAY_OF_MONTH, day);
            cal.set(Calendar.HOUR_OF_DAY, hour);
            cal.set(Calendar.MINUTE, minute);
            cal.set(Calendar.SECOND, second);

            Timestamp timestamp = new Timestamp(cal.getTimeInMillis());
            timestamp.setNanos(nanos);

            return DateTimeOffset.valueOf(timestamp, minutesOffset);
        }

        DateTimeOffsetValue(String stringValue) {
            super(SQLType.datetimeoffset, stringValue);
            dto = initExpected(stringValue, -1);
        }

        DateTimeOffsetValue(String stringValue, int fractionalSecondsDigits) {
            super(SQLType.datetimeoffset, stringValue);
            dto = initExpected(stringValue, fractionalSecondsDigits);
        }

        private Date expectedDate() {
            Calendar cal = Calendar.getInstance(new SimpleTimeZone(1000 * 60 * dto.getMinutesOffset(), ""), Locale.US);
            cal.setTimeInMillis(dto.getTimestamp().getTime());
            cal.set(Calendar.HOUR_OF_DAY, 0);
            cal.set(Calendar.MINUTE, 0);
            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            return new Date(cal.getTimeInMillis());
        }

        private java.sql.Time expectedTime() {
            Calendar cal = Calendar.getInstance(new SimpleTimeZone(1000 * 60 * dto.getMinutesOffset(), ""), Locale.US);
            cal.set(Calendar.ZONE_OFFSET, 1000 * 60 * dto.getMinutesOffset());
            cal.setTimeInMillis(dto.getTimestamp().getTime());
            if (dto.getTimestamp().getNanos() % 1000000 >= 500000)
                cal.add(Calendar.MILLISECOND, 1);
            cal.set(Calendar.YEAR, 1970);
            cal.set(Calendar.MONTH, Calendar.JANUARY);
            cal.set(Calendar.DAY_OF_MONTH, 1);
            return new java.sql.Time(cal.getTimeInMillis());
        }

        void verifyRSGetters(ResultSet rs) throws Exception {
            assertEquals(expectedDate(), rs.getDate(1));
            assertEquals(expectedTime(), rs.getTime(1));
            assertEquals(dto.getTimestamp(), rs.getTimestamp(1));
            assertEquals(dto, ((SQLServerResultSet) rs).getDateTimeOffset(1));
            assertEquals(this.getString(), rs.getString(1));
        }

        void verifyRSUpdaters(ResultSet rs) throws Exception {
            ((SQLServerResultSet) rs).updateDateTimeOffset(1, dto);
            rs.updateRow();

            assertEquals(dto, ((SQLServerResultSet) rs).getDateTimeOffset(1));
        }

        void verifySetters(PreparedStatement ps) throws Exception {
            ((SQLServerPreparedStatement) ps).setDateTimeOffset(1, dto);
            ps.execute();
            ps.getMoreResults();
            try (ResultSet rs = ps.getResultSet()) {
                rs.next();
                assertEquals(dto, ((SQLServerResultSet) rs).getDateTimeOffset(1));
            }
        }

        void verifySettersUtilDate(PreparedStatement ps) throws Exception {
            /*
             * Cannot test by setObject(...,java.util.Date) with the back end data type as datetimeoffset because by
             * design anything but datetimeoffset is normalized to UTC time zone in the source code (in sendTemporal
             * function in dtv.java) setObject(...,Date) and setObject(...,java.utl.date) gets same exception
             */
        }

        void verifySettersCalendar(PreparedStatement ps) throws Exception {
            /*
             * Cannot test by setObject(...,java.util.Date) with the back end data type as datetimeoffset because by
             * design anything but datetimeoffset is normalized to UTC time zone in the source code (in sendTemporal
             * function in dtv.java)
             */
        }

        void verifyCSGetters(CallableStatement cs) throws Exception {
            ((SQLServerCallableStatement) cs).setDateTimeOffset(1, dto);
            cs.registerOutParameter(2, microsoft.sql.Types.DATETIMEOFFSET);
            cs.execute();
            assertEquals(dto, ((SQLServerCallableStatement) cs).getDateTimeOffset(2));
            assertEquals(dto, cs.getObject(2));
        }
    }

    enum TestValue {
        POST_GREGORIAN_DATETIME2(new DateTime2Value("1582-10-25 15:07:09.0810000")),

        PRE_GREGORIAN_DTO_VALUE(new DateTimeOffsetValue("1414-01-05 00:00:00.0000000 -08:00")),

        PRE_GREGORIAN_DATETIME2_VALUE(new DateTime2Value("1414-01-05 00:00:00.0000000")),

        ANOTHER_TEST(new DateTimeOffsetValue("3431-04-13 15:23:32.7954829 -05:32")),

        BOA_VISTA(new DateTime2Value("6854-01-27 04:39:54.86772", 5, "America/Boa_Vista")),

        NEGATIVE_OFFSET(new DateTimeOffsetValue("3431-04-13 21:42:14.7954829 -05:32")),

        SOMETIME(new TimeValue("11:58:31.456789", 6)),

        THE_LAST_MILLISECOND_WITH_TIME_ZONE(new DateTimeOffsetValue("9999-12-31 23:59:59.9999999 +14:00")),

        COMMON_ERA_FIRST_DAY(new DateValue("0001-01-01")),

        PRE_CUTOVER(new DateValue("1582-10-04")),

        /*
         * Dates in the Gregorian cutover date range appear as 10 days later than what they should. This behavior is
         * consistent with other JDBC drivers, such as IBM
         */
        CUTOVER_START(new DateValue("1582-10-05")),

        CUTOVER_END(new DateValue("1582-10-14")),

        POST_CUTOVER(new DateValue("1582-10-15")),

        // Last "slow path" date
        POST_CUTOVER_PLUS_1(new DateValue("1582-10-16")),

        // First "fast path" date
        POST_CUTOVER_PLUS_2(new DateValue("1582-10-17")),

        /*
         * Post-cutover date requiring preservation of "wall calendar" date in computing Calendar.DAY_OF_YEAR
         */
        POST_CUTOVER_NOVEMBER(new DateTime2Value("1582-11-15 15:07:09.0810000")),

        A_RECENT_DATE(new DateValue("2009-10-20")),

        A_FRACTION_OF_A_SECOND_PAST_MIDNIGHT(new TimeValue("00:00:00.007", 3)),

        TIME_7(new TimeValue("11:58:31.9999999", 7)),

        DATETIME2_4(new DateTime2Value("2009-10-20 11:58:31.1234", 4)),

        DATETIMEOFFSET(new DateTimeOffsetValue("2009-10-20 11:58:31.1230000 +07:00"));

        final SQLValue sqlValue;

        TestValue(SQLValue sqlValue) {
            this.sqlValue = sqlValue;
        }
    };

    @Test
    public void testResultSetGetters() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            for (TestValue value : TestValue.values())
                value.sqlValue.verifyRSGetters(conn);
        }
    }

    @Test
    public void testResultSetUpdaters() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            for (TestValue value : TestValue.values())
                value.sqlValue.verifyRSUpdaters(conn);
        }
    }

    @Test
    public void testSetters() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString + ";sendTimeAsDateTime=true")) {
            for (TestValue value : TestValue.values())
                value.sqlValue.verifySetters(conn);
        }
    }

    @Test
    public void testCallableStatementGetters() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            for (TestValue value : TestValue.values())
                value.sqlValue.verifyCSGetters(conn);
        }
    }

    @Test
    public void testResultSetMetaData() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            TestValue v[] = TestValue.values();
            for (TestValue value : TestValue.values())
                value.sqlValue.verifyResultSetMetaData(conn);
        }
    }

    @Test
    public void testParameterMetaData() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            for (TestValue value : TestValue.values())
                value.sqlValue.verifyParameterMetaData(conn);
        } ;
    }

    /*
     * test CS.setObject(timestamp, TIME)/registerOutParam(TIME) with sendTimeAsDatetime
     */
    @Test
    public void testSendTimestampAsTimeAsDatetime() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString + ";sendTimeAsDatetime=true")) {
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropProcedureIfExists(escapedProcName, stmt);
                stmt.executeUpdate("CREATE PROCEDURE " + escapedProcName + " @argIn time(7), "
                        + " @argOut time(7) OUTPUT " + " AS " + " SET @argOut=@argIn");

                try (CallableStatement cs = conn.prepareCall("{call " + escapedProcName + "(?,?)}")) {

                    // Set up a timestamp with a time component that is the last millisecond of the day
                    Timestamp ts = Timestamp.valueOf("2010-02-15 23:59:59.999");

                    /*
                     * send the timestamp to the server using the TIME SQL type rather than TIMESTAMP. The driver will
                     * strip the date portion and, because sendTimeAsDatetime=true, round the resulting time value to
                     * midnight because it should be sending a DATETIME which has only 1/300s accuracy
                     */
                    cs.setObject(1, ts, java.sql.Types.TIME);
                    cs.registerOutParameter(2, java.sql.Types.TIME);
                    cs.execute();

                    // Fetch the OUT parameter and verify that we have a date-normalized TIME of midnight
                    java.sql.Time timeOut = cs.getTime(2);
                    Timestamp tsOut = new Timestamp(timeOut.getTime());
                    assertEquals("1970-01-01 00:00:00.0", tsOut.toString());
                }
            } finally {
                try (Statement stmt = conn.createStatement()) {
                    TestUtils.dropProcedureIfExists(escapedProcName, stmt);
                }
            }
        }
    }

    /*
     * test sending Timestamp to the server via an updater does not result in the same behavior as a setter wrt
     * double-rounding of fractional seconds
     */
    @Test
    public void testDoubleRounding() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString)) {

            // create a table with a datetimeoffset column and insert a value in it
            assumeTrue(!isSqlAzureDW(), TestResource.getResource("R_skipAzure"));

            String sql;
            try (Statement stmt = conn.createStatement()) {
                // SQL Azure requires each table to have a clustered index, so change col1 to the primary key
                sql = "CREATE TABLE " + escapedTableName + " (col1 int primary key, col2 datetimeoffset(6))";
                stmt.executeUpdate(sql);
                sql = "INSERT INTO " + escapedTableName + " VALUES(1, '2010-04-29 10:51:12.123456 +00:00')";
                stmt.executeUpdate(sql);

            }

            try (Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
                DateTimeOffset dto;
                DateTimeOffset actualDto;

                // create select query and update the datetimeoffset
                String query = "SELECT * FROM " + escapedTableName;

                try (ResultSet rs = stmt.executeQuery(query)) {
                    rs.next();

                    // when double-rounding to scale of 6, nanos should be 832909
                    Timestamp ts = Timestamp.valueOf("3432-12-21 15:22:58.832908453");
                    dto = DateTimeOffset.valueOf(ts, 0);

                    // ts value that goes to driver is 3432-12-21 15:22:58.832908453
                    rs.updateTimestamp(2, ts);
                    rs.updateRow(); // update row
                }

                // get the datetime offset from server
                sql = "SELECT col2 FROM " + escapedTableName;
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    rs.next();
                    Object value = rs.getObject(1);
                    // 3432-12-21 15:22:58.832909
                    actualDto = (DateTimeOffset) value;
                }

                // compare after doing a cast on the server to datetimeoffset(6)
                sql = "SELECT CAST('" + dto.getTimestamp().toString() + "' as datetimeoffset(6))";
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    rs.next();
                    // 3432-12-21 23:22:58.832909 +00:00
                    DateTimeOffset expectedDto = (DateTimeOffset) rs.getObject(1);
                    assertEquals(expectedDto, actualDto);
                }
            } finally {
                try (Statement stmt = conn.createStatement()) {
                    TestUtils.dropTableIfExists(escapedTableName, stmt);
                } catch (SQLException e) {
                    fail(TestResource.getResource("R_createDropTableFailed") + e.toString());
                }
            }
        }
    }

    /*
     * Tests "fail fast" SQLException path when a Japanese imperial calendar is used with values representing the first
     * year of an imperial era. See for more details:
     * http://java.sun.com/javase/6/docs/technotes/guides/intl/calendar.doc.html
     */
    @Test
    public void testWithJapaneseImperialCalendar() throws Exception {
        /*
         * From http://java.sun.com/javase/6/docs/api/java/util/Locale.html : "Note: When you ask for a resource for a
         * particular locale, you get back the best available match, not necessarily precisely what you asked for. For
         * more information, look at ResourceBundle." Japanese Imperial locale does not exist with some VMs. In these
         * VMs, Locale.US is substituted as "best available match".
         */
        Locale japaneseImperialLocale = new Locale("ja", "JP", "JP");
        Calendar japaneseImperialCalendar = Calendar.getInstance(japaneseImperialLocale);
        MessageFormat cal = new MessageFormat(TestResource.getResource("R_noJRESupport"));
        Object[] msgsArgs = {japaneseImperialLocale.toString()};
        assumeTrue(GregorianCalendar.class.isInstance(japaneseImperialCalendar), cal.format(msgsArgs));

        Locale defaultLocale = Locale.getDefault();
        Locale.setDefault(japaneseImperialLocale);

        try (Connection conn = DriverManager.getConnection(connectionString + ";sendTimeAsDatetime=true")) {

            // Get Gregorian date using Japanese imperial calendar
            try (ResultSet rs = conn.createStatement().executeQuery("SELECT CAST('0821-01-04' AS DATE)")) {
                rs.next();
                Date date = rs.getDate(1, japaneseImperialCalendar);

                // check pre-Meiji
                assertEquals("0821-01-04", date.toString());
            }

            try (PreparedStatement ps = conn.prepareStatement("SELECT CAST(? AS VARCHAR(40))")) {
                Timestamp ts;

                /*
                 * Set second day of first year of Taisho era (1912 Gregorian) Note: Taisho era began July 30, 1912
                 * Gregorian; second day of that year was July 31.
                 */
                japaneseImperialCalendar.clear();
                japaneseImperialCalendar.set(Calendar.ERA, 2); // Taisho -> ERA 2
                japaneseImperialCalendar.set(Calendar.YEAR, 1);
                japaneseImperialCalendar.set(Calendar.DAY_OF_YEAR, 2);
                ts = new Timestamp(japaneseImperialCalendar.getTimeInMillis());
                ps.setTimestamp(1, ts, japaneseImperialCalendar);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    // check Taisho 1
                    assertEquals("1912-07-31 00:00:00.0000000", rs.getString(1));
                }

                // Set second year of Showa era (1927 Gregorian)
                japaneseImperialCalendar.clear();
                japaneseImperialCalendar.set(Calendar.ERA, 3); // Showa -> ERA 3
                japaneseImperialCalendar.set(Calendar.YEAR, 2);
                japaneseImperialCalendar.set(Calendar.MONTH, Calendar.FEBRUARY);
                japaneseImperialCalendar.set(Calendar.DATE, 15);
                japaneseImperialCalendar.set(Calendar.HOUR_OF_DAY, 8);
                japaneseImperialCalendar.set(Calendar.MINUTE, 49);
                japaneseImperialCalendar.set(Calendar.SECOND, 3);
                japaneseImperialCalendar.set(Calendar.MILLISECOND, 87);
                ts = new Timestamp(japaneseImperialCalendar.getTimeInMillis());
                ps.setTimestamp(1, ts, japaneseImperialCalendar);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    // check Showa 2
                    assertEquals("1927-02-15 08:49:03.0870000", rs.getString(1));
                }
            }
        } finally {
            Locale.setDefault(defaultLocale);
        }
    }

    enum StringFormatTestValue {
        TIME_ZEROS_FILL("02:34:56.1", "TIME(3)"),

        TIME_NO_FRACTIONAL_SECONDS_WITH_SCALE("02:34:56", "TIME(3)"),

        TIME_ROUNDED_WITH_SCALE("02:34:56.777", "TIME(2)"),

        TIME_WITH_ZERO_SCALE("02:34:56", "TIME(0)"),

        DATETIME2_ZEROS_FILL("2010-03-10 02:34:56.1", "DATETIME2(3)"),

        DATETIME2_NO_FRACTIONAL_SECONDS_WITH_SCALE("2010-03-10 02:34:56", "DATETIME2(3)"),

        DATETIME2_ROUNDED_WITH_SCALE("2010-03-10 02:34:56.777", "DATETIME2(2)"),

        DATETIME2_WITH_ZERO_SCALE("2010-03-10 02:34:56", "DATETIME2(0)"),

        DATETIMEOFFSET_ZEROS_FILL("2010-03-10 02:34:56.1 -08:00", "DATETIMEOFFSET(3)"),

        DATETIMEOFFSET_NO_FRACTIONAL_SECONDS_WITH_SCALE("2010-03-10 02:34:56 -08:00", "DATETIMEOFFSET(3)"),

        DATETIMEOFFSET_ROUNDED_WITH_SCALE("2010-03-10 02:34:56.777 -08:00", "DATETIMEOFFSET(2)"),

        DATETIMEOFFSET_WITH_ZERO_SCALE("2010-03-10 02:34:56 -08:00", "DATETIMEOFFSET(0)");

        final String sqlLiteral;
        final String sqlType;

        StringFormatTestValue(String sqlLiteral, String sqlType) {
            this.sqlLiteral = sqlLiteral;
            this.sqlType = sqlType;
        }
    }

    @Test
    public void testGetString() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {

            for (StringFormatTestValue testValue : EnumSet.allOf(StringFormatTestValue.class)) {
                String query = "SELECT " + "CAST('" + testValue.sqlLiteral + "' AS " + testValue.sqlType + "), "
                        + "CAST(CAST('" + testValue.sqlLiteral + "' AS " + testValue.sqlType + ") AS VARCHAR)";

                try (ResultSet rs = stmt.executeQuery(query)) {
                    rs.next();
                    assertEquals(rs.getString(1), rs.getString(2));
                }
            }
        }
    }

    @Test
    public void testWithThaiLocale() throws Exception {
        java.text.SimpleDateFormat tsFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS0000");
        java.text.SimpleDateFormat timeFormat = new java.text.SimpleDateFormat("h:mmaa");
        java.text.SimpleDateFormat dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd");
        java.text.SimpleDateFormat dtoFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS0000 XXX");

        Locale locale = Locale.getDefault();
        Locale.setDefault(new Locale("th", "TH"));

        try (Connection conn = DriverManager.getConnection(connectionString + ";sendTimeAsDatetime=true")) {
            // Test setter conversions
            try (PreparedStatement ps = conn.prepareStatement("SELECT CAST(? AS VARCHAR(40))")) {

                /*
                 * Need to use the following constructor for running against IBM JVM. Here, year should be year-1900, //
                 * month // is from 0-11.
                 */
                Timestamp ts = new Timestamp(System.currentTimeMillis());

                /*
                 * Test PreparedStatement with Timestamp Value sent as DATETIME2; result should have 7 digits of
                 * subsecond precision)
                 */
                ps.setTimestamp(1, ts);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    assertEquals(tsFormat.format(ts), rs.getString(1));
                }

                /*
                 * Test PreparedStatement with Time Value sent as DATETIME w/Unix Epoch as base date when
                 * sendTimeAsDatetime=true
                 */
                Time time = new Time(ts.getTime());
                ps.setTime(1, time);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();

                    // compare these separately since there may be an extra space between the 2
                    assertEquals("Jan  1 1970", rs.getString(1).substring(0, 11));
                    assertEquals(timeFormat.format(ts.getTime()),
                            rs.getString(1).substring(rs.getString(1).length() - 7).trim());
                }

                // Test PreparedStatement with Date
                Date date = new Date(ts.getTime());
                ps.setDate(1, date);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    assertEquals(dateFormat.format(ts), rs.getString(1));
                }

                // Test PreparedStatement with Date (using Buddhist calendar)
                date = new Date(ts.getTime());
                ps.setDate(1, date, Calendar.getInstance());
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    assertEquals(dateFormat.format(ts), rs.getString(1));
                }

                // Test PreparedStatement with DateTimeOffset (using Buddhist calendar)
                // Note: Expected value does not reflect Buddhist year, even though a Buddhist calendar is used.
                DateTimeOffset dto = DateTimeOffset.valueOf(ts,
                        Calendar.getInstance(TimeZone.getTimeZone("America/Los_Angeles")));

                ((SQLServerPreparedStatement) ps).setDateTimeOffset(1, dto);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();

                    dtoFormat.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
                    assertEquals(dtoFormat.format(ts), rs.getString(1));
                }
            }
        } finally {
            Locale.setDefault(locale);
        }
    }

    // test base date compatibility for Time to DATETIMEx conversions
    @Test
    public void testBaseDate() throws Exception {
        Timestamp ts;

        // Test Java base date (1/1/1970)
        try (Connection conn = DriverManager.getConnection(connectionString + ";sendTimeAsDatetime=true");
                PreparedStatement ps = conn.prepareStatement("SELECT CAST(? AS DATETIME)")) {
            ps.setTime(1, java.sql.Time.valueOf("12:34:56"));

            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                ts = rs.getTimestamp(1);
                // java base date
                assertEquals("1970-01-01 12:34:56.0", ts.toString());
            }
        }

        // Test SQL Server base date (1/1/1900)
        try (Connection conn = DriverManager.getConnection(connectionString + ";sendTimeAsDatetime=false");
                PreparedStatement ps = conn.prepareStatement("SELECT CAST(? AS DATETIME)")) {
            ps.setTime(1, java.sql.Time.valueOf("12:34:56"));

            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                ts = rs.getTimestamp(1);
                // SQL Server base date
                assertEquals("1900-01-01 12:34:56.0", ts.toString());
            }
        }
    }

    // test setTimestamp to DATETIMEOFFSET yields a value in local time with UTC time zone offset (+00:00)
    @Test
    public void testTimestampToDateTimeOffset() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString);
                PreparedStatement ps = conn.prepareStatement("SELECT CAST(? AS DATETIMEOFFSET)")) {
            ps.setTimestamp(1, Timestamp.valueOf("2010-01-06 12:34:56"));

            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                DateTimeOffset dto = ((SQLServerResultSet) rs).getDateTimeOffset(1);
                assertEquals("2010-01-06 12:34:56 +00:00", dto.toString());
            }
        }
    }

    /*
     * test PS.setObject() on a datetime2 with values on or after 0700-02-29 have a value one day ahead stored in the
     * server
     */
    @Test
    public void testJulianLeapYear() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString + ";sendTimeAsDatetime=true");
                // PreparedStatement ps = conn.prepareStatement("SELECT CAST(? AS VARCHAR)");
                PreparedStatement ps = conn.prepareStatement("SELECT CAST(? AS DATE)")) {

            // Julian date from string
            Date julianDate = Date.valueOf("0700-03-01");
            ps.setDate(1, julianDate);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                assertEquals("0700-03-01", rs.getString(1));
            }
        }
    }

    @Test
    public void testGetTimeRounding() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString + ";sendTimeAsDatetime=true");
                Statement stmt = conn.createStatement()) {

            // Test getTime() rounding from TIME(6) SQL type
            try (ResultSet rs = stmt.executeQuery("SELECT CAST('12:34:56.999500' AS TIME)")) {
                rs.next();
                assertEquals("12:34:57", rs.getTime(1).toString());
            }

            // Test getTime() rounding from character data
            try (ResultSet rs = stmt.executeQuery("SELECT '12:34:56.999500'")) {
                rs.next();
                assertEquals("12:34:57", rs.getTime(1).toString());
            }
        }
    }

    @Test
    public void testGregorianCutoverDateTime2() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString + ";sendTimeAsDatetime=true");
                PreparedStatement ps = conn.prepareStatement("SELECT CAST(? AS VARCHAR)")) {
            Timestamp ts;

            // Test setting value during the Gregorian cutover via Timestamp constructed from String
            ts = Timestamp.valueOf("1582-10-24 15:07:09.081");
            ps.setTimestamp(1, ts);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                assertEquals("1582-10-24 15:07:09.0810000", rs.getString(1));
            }

            // Test setting value during the Gregorian cutover via Timestamp constructed from Calendar
            Calendar cal = Calendar.getInstance();
            cal.set(1582, Calendar.NOVEMBER, 1, 15, 7, 9);
            cal.set(Calendar.MILLISECOND, 81);
            ts = new Timestamp(cal.getTimeInMillis());
            ps.setTimestamp(1, ts);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                assertEquals("1582-11-01 15:07:09.0810000", rs.getString(1));
            }
        }
    }

    /*
     * setTimestamp to DATETIMEOFFSET yields a value in local time with UTC time zone offset (+00:00) In this case,
     * verify that SELECT with a WHERE clause does not fail due to mapping the Timestamp value to a SQL Server type that
     * does not compare equal. For example, a DATETIMEOFFSET and DATETIME only compare equal if the DATETIMEOFFSET
     * offset is 0 (UTC).
     */
    @Test
    public void testTimestampToDateTime() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString); PreparedStatement ps = conn
                .prepareStatement("SELECT 1 WHERE ?=CAST('2009-12-17 17:00:29' AS DATETIME)")) {
            ps.setTimestamp(1, Timestamp.valueOf("2009-12-17 17:00:29"));
            try (ResultSet rs = ps.executeQuery()) {
                assertEquals(true, rs.next());
            }
        }
    }

    @Test
    public void testUpdateMisc() throws Exception {
        try (SQLServerConnection conn = (SQLServerConnection) DriverManager
                .getConnection(connectionString + ";sendTimeAsDatetime=true")) {

            assumeTrue(!isSqlAzureDW(), TestResource.getResource("R_skipAzure"));

            try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
                TestUtils.dropTableIfExists(escapedTableName, stmt);

                stmt.executeUpdate("CREATE TABLE " + escapedTableName + " (col1 datetimeoffset(2)," + "  col2 datetime,"
                        + "  col3 time(5)," + "  col4 datetime2(5)," + "  col5 smalldatetime," + "  col6 time(2),"
                        + "  col7 int identity(1,1) primary key)");
                stmt.executeUpdate("INSERT INTO " + escapedTableName + " VALUES (" + " '2010-01-12 09:00:23.17 -08:00',"
                        + " '2010-01-12 10:20:23'," + " '10:20:23'," + " '2010-01-12 10:20:23',"
                        + " '2010-01-12 11:45:17'," + " '10:20:23')");
                DateTimeOffset dto;

                try (ResultSet rs = stmt.executeQuery("SELECT *, CAST(col1 AS VARCHAR) FROM " + escapedTableName)) {
                    rs.next();

                    // Update datetimeoffset(2) from pre-Gregorian Date
                    rs.updateDate(1, Date.valueOf("0814-02-18"));
                    rs.updateRow();
                    assertEquals("0814-02-18 00:00:00 +00:00",
                            ((SQLServerResultSet) rs).getDateTimeOffset(1).toString());

                    // Update datetimeoffset(2) from "last" Time
                    rs.updateTime(1, new java.sql.Time(Timestamp.valueOf("1970-01-01 23:59:59.998").getTime()));
                    rs.updateRow();
                    assertEquals("1970-01-02 00:00:00 +00:00",
                            ((SQLServerResultSet) rs).getDateTimeOffset(1).toString());

                    // Update datetimeoffset(2) from the "last" Timestamp
                    rs.updateTimestamp(1, Timestamp.valueOf("9999-12-31 23:59:59.998"));
                    rs.updateRow();
                    dto = ((SQLServerResultSet) rs).getDateTimeOffset(1);
                    assertEquals("9999-12-31 23:59:59.99 +00:00", dto.toString());

                    // Attempt to update datetimeoffset(2) from the first out of range value
                    // Verify that an exception is thrown and that the statement/connection is still usable after
                    boolean exceptionThrown = true;
                    try {
                        Timestamp tsInvalid = Timestamp.valueOf("9999-12-31 23:59:59.999999999");
                        tsInvalid = new Timestamp(tsInvalid.getTime() + 1);
                        rs.updateTimestamp(1, tsInvalid);
                        rs.updateRow();
                        exceptionThrown = false;
                    } catch (SQLServerException e) {
                        // data exception - datetime field overflow (ISO/IEC 9075-2:1999
                        assertEquals("22008", e.getSQLState());
                    }

                    if (!exceptionThrown) {
                        fail(TestResource.getResource("R_expectedExceptionNotThrown"));
                    }

                    // Update time(5) from Timestamp with nanos more precise than 100ns
                    Timestamp ts = Timestamp.valueOf("2010-01-12 11:05:23");
                    ts.setNanos(987659999);
                    rs.updateTimestamp(3, ts);
                    rs.updateRow();
                    assertEquals("1900-01-01 11:05:23.98766", rs.getTimestamp(3).toString());

                    // Update time(5) from Timestamp to max value in a day. The value should not be rounded
                    ts = Timestamp.valueOf("2010-01-12 23:59:59");
                    ts.setNanos(999999999);
                    Time time = new java.sql.Time(ts.getTime());
                    rs.updateTimestamp(3, ts);
                    rs.updateRow();
                    assertEquals("1900-01-01 23:59:59.99999", rs.getTimestamp(3).toString());

                    // Update time(2) from Time to max value in a day. The value should not be rounded
                    rs.updateTime(6, time);
                    rs.updateRow();
                    // conversion to timestamp is necessary to see fractional secs
                    assertEquals("1970-01-01 23:59:59.99", new Timestamp(rs.getTime(6).getTime()).toString());

                    // Update time(5) from Timestamp to max value in a second. The value should be rounded
                    ts = Timestamp.valueOf("2010-01-12 23:59:58");
                    ts.setNanos(999999999);
                    time = new java.sql.Time(ts.getTime());
                    rs.updateTimestamp(3, ts);
                    rs.updateRow();
                    assertEquals("1900-01-01 23:59:59.0", rs.getTimestamp(3).toString());

                    // Update time(2) from Time to max value in a second. The value should be rounded
                    rs.updateTime(6, time);
                    rs.updateRow();
                    // conversion to timestamp is necessary to see fractional secs
                    assertEquals("1970-01-01 23:59:59.0", new Timestamp(rs.getTime(6).getTime()).toString());

                    // Update datetime w/expected rounding of nanos to DATETIME's 1/300second resolution
                    ts = Timestamp.valueOf("6289-04-22 05:13:57.6745106");
                    rs.updateTimestamp(2, ts);
                    rs.updateRow();
                    assertEquals("6289-04-22 05:13:57.677", rs.getTimestamp(2).toString());

                    // Update datetime with rounding-induced overflow from Time (should roll date part to 1/2/1970)
                    ts = Timestamp.valueOf("2010-01-18 23:59:59.999");
                    rs.updateTime(2, new java.sql.Time(ts.getTime()));
                    rs.updateRow();
                    assertEquals("1970-01-02 00:00:00.0", rs.getTimestamp(2).toString());

                } finally {
                    TestUtils.dropTableIfExists(escapedTableName, stmt);
                }
            }
        }
    }
}
