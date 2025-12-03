package com.microsoft.sqlserver.jdbc.preparedStatement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Random;
import java.util.UUID;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.Geography;
import com.microsoft.sqlserver.jdbc.Geometry;
import com.microsoft.sqlserver.jdbc.RandomData;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.AzureDB;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;

import microsoft.sql.DateTimeOffset;
import microsoft.sql.Vector;
import microsoft.sql.Vector.VectorDimensionType;

@RunWith(JUnitPlatform.class)
public class BatchExecutionWithBulkCopyTest extends AbstractTest {

    static String tableName = RandomUtil.getIdentifier("BulkCopyParseTest");
    static String tableNameBulk = RandomUtil.getIdentifier("BulkCopyParseTest");
    static String unsupportedTableName = RandomUtil.getIdentifier("BulkCopyUnsupportedTable'");
    static String squareBracketTableName = RandomUtil.getIdentifier("BulkCopy]]]]test'");
    static String doubleQuoteTableName = RandomUtil.getIdentifier("\"BulkCopy\"\"\"\"test\"");
    static String schemaTableName = "\"dbo\"         . /*some comment */     " + squareBracketTableName;
    static String tableNameBulkComputedCols = RandomUtil.getIdentifier("BulkCopyComputedCols");
    static String tableNameBulkString = RandomUtil.getIdentifier("BulkInsertTable");

    private Object[] generateExpectedValues() {
        float randomFloat = RandomData.generateReal(false);
        int ramdonNum = RandomData.generateInt(false);
        short randomShort = RandomData.generateTinyint(false);
        String randomString = RandomData.generateCharTypes("6", false, false);
        String randomChar = RandomData.generateCharTypes("1", false, false);
        byte[] randomBinary = RandomData.generateBinaryTypes("5", false, false);
        BigDecimal randomBigDecimal = new BigDecimal(ramdonNum);
        BigDecimal randomMoney = RandomData.generateMoney(false);
        BigDecimal randomSmallMoney = RandomData.generateSmallMoney(false);
        String randomUuid = UUID.randomUUID().toString().toUpperCase();
        // Temporal datatypes
        Date randomDate = Date.valueOf(Constants.NOW.toLocalDate());
        Time randomTime = new Time(Constants.NOW.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
        Timestamp randomTimestamp = new Timestamp(
                Constants.NOW.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());

        // Datetime can only end in 0,3,7 and will be rounded to those numbers on the server. Manually set nanos
        Timestamp roundedDatetime = randomTimestamp;
        roundedDatetime.setNanos(0);

        // Smalldatetime does not have seconds. Manually set nanos and seconds.
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(randomTimestamp.getTime());
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Timestamp smallTimestamp = new Timestamp(cal.getTimeInMillis());

        Object[] expected = new Object[24];
        expected[0] = Constants.RANDOM.nextLong();
        expected[1] = randomBinary;
        expected[2] = true;
        expected[3] = randomChar;
        expected[4] = randomDate;
        expected[5] = roundedDatetime;
        expected[6] = randomTimestamp;
        expected[7] = microsoft.sql.DateTimeOffset.valueOf(randomTimestamp, 0);
        expected[8] = randomBigDecimal.setScale(0, RoundingMode.HALF_UP);
        expected[9] = (double) ramdonNum;
        expected[10] = ramdonNum;
        expected[11] = randomMoney;
        expected[12] = randomChar;
        expected[13] = BigDecimal.valueOf(Constants.RANDOM.nextInt());
        expected[14] = randomString;
        expected[15] = randomFloat;
        expected[16] = smallTimestamp;
        expected[17] = randomShort;
        expected[18] = randomSmallMoney;
        expected[19] = randomTime;
        expected[20] = randomShort;
        expected[21] = randomBinary;
        expected[22] = randomString;
        expected[23] = randomUuid;

        return expected;
    }

    @Test
    public void testIsInsert() throws Exception {
        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                Statement stmt = (SQLServerStatement) connection.createStatement()) {
            String valid1 = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableNameBulk) + " values (1, 2)";
            String valid2 = " insert into " + AbstractSQLGenerator.escapeIdentifier(tableNameBulk) + " values (1, 2)";
            String valid3 = "/* asdf */ insert into " + AbstractSQLGenerator.escapeIdentifier(tableNameBulk)
                    + " values (1, 2)";
            String invalid = "select * from " + AbstractSQLGenerator.escapeIdentifier(tableNameBulk);

            Method method = stmt.getClass().getDeclaredMethod("isInsert", String.class);
            method.setAccessible(true);
            assertTrue((boolean) method.invoke(stmt, valid1));
            assertTrue((boolean) method.invoke(stmt, valid2));
            assertTrue((boolean) method.invoke(stmt, valid3));
            assertFalse((boolean) method.invoke(stmt, invalid));
        }
    }

    @Test
    public void testComments() throws Exception {
        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                PreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement("");) {
            String valid = "/* rando comment *//* rando comment */ INSERT /* rando comment */ INTO /* rando comment *//*rando comment*/ "
                    + AbstractSQLGenerator.escapeIdentifier(tableNameBulk) + " /*rando comment */"
                    + " /* rando comment */values/* rando comment */ (1, 2)";

            Field f1 = pstmt.getClass().getDeclaredField("localUserSQL");
            f1.setAccessible(true);
            f1.set(pstmt, valid);

            Method method = pstmt.getClass().getDeclaredMethod("parseUserSQLForTableNameDW", boolean.class,
                    boolean.class, boolean.class, boolean.class);
            method.setAccessible(true);

            assertEquals(AbstractSQLGenerator.escapeIdentifier(tableNameBulk),
                    (String) method.invoke(pstmt, false, false, false, false));
        }
    }

    @Test
    public void testBrackets() throws Exception {
        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                PreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement("");) {
            String valid = "/* rando comment *//* rando comment */ INSERT /* rando comment */ INTO /* rando comment *//*rando comment*/ [BulkCopy[]]Table] /*rando comment */"
                    + " /* rando comment */values/* rando comment */ (1, 2)";

            Field f1 = pstmt.getClass().getDeclaredField("localUserSQL");
            f1.setAccessible(true);
            f1.set(pstmt, valid);

            Method method = pstmt.getClass().getDeclaredMethod("parseUserSQLForTableNameDW", boolean.class,
                    boolean.class, boolean.class, boolean.class);
            method.setAccessible(true);

            assertEquals("[BulkCopy[]]Table]", (String) method.invoke(pstmt, false, false, false, false));
        }
    }

    @Test
    public void testDoubleQuotes() throws Exception {
        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                PreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement("");) {
            String valid = "/* rando comment *//* rando comment */ INSERT /* rando comment */ INTO /* rando comment *//*rando comment*/ \"Bulk\"\"\"\"Table\" /*rando comment */"
                    + " /* rando comment */values/* rando comment */ (1, 2)";

            Field f1 = pstmt.getClass().getDeclaredField("localUserSQL");
            f1.setAccessible(true);
            f1.set(pstmt, valid);

            Method method = pstmt.getClass().getDeclaredMethod("parseUserSQLForTableNameDW", boolean.class,
                    boolean.class, boolean.class, boolean.class);
            method.setAccessible(true);

            assertEquals("\"Bulk\"\"\"\"Table\"", (String) method.invoke(pstmt, false, false, false, false));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testAll() throws Exception {
        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                PreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement("");) {
            String valid = "/* rando comment *//* rando comment */ INSERT /* rando comment */ INTO /* rando comment *//*rando comment*/ \"Bulk\"\"\"\"Table\" /*rando comment */"
                    + " /* rando comment */ (\"c1\"/* rando comment */, /* rando comment */[c2]/* rando comment */, /* rando comment */ /* rando comment */c3/* rando comment */, c4)"
                    + "values/* rando comment */ (/* rando comment */1/* rando comment */, /* rando comment */2/* rando comment */ , '?', ?)/* rando comment */";

            Field f1 = pstmt.getClass().getDeclaredField("localUserSQL");
            f1.setAccessible(true);
            f1.set(pstmt, valid);

            Method method = pstmt.getClass().getDeclaredMethod("parseUserSQLForTableNameDW", boolean.class,
                    boolean.class, boolean.class, boolean.class);
            method.setAccessible(true);

            assertEquals("\"Bulk\"\"\"\"Table\"", (String) method.invoke(pstmt, false, false, false, false));

            method = pstmt.getClass().getDeclaredMethod("parseUserSQLForColumnListDW");
            method.setAccessible(true);

            ArrayList<String> columnList = (ArrayList<String>) method.invoke(pstmt);
            ArrayList<String> columnListExpected = new ArrayList<String>();
            columnListExpected.add("c1");
            columnListExpected.add("c2");
            columnListExpected.add("c3");
            columnListExpected.add("c4");

            for (int i = 0; i < columnListExpected.size(); i++) {
                assertEquals(columnListExpected.get(i), columnList.get(i));
            }
        }
    }

    @Test
    public void testAllcolumns() throws Exception {
        String valid = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values " + "(" + "?, "
                + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, "
                + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?," + "?" + ")";

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(valid);
                Statement stmt = (SQLServerStatement) connection.createStatement();) {
            Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
            f1.setAccessible(true);
            f1.set(connection, true);

            Object[] expected = generateExpectedValues();

            pstmt.setLong(1, (long) expected[0]); // bigint
            pstmt.setBytes(2, (byte[]) expected[1]); // binary(5)
            pstmt.setBoolean(3, (boolean) expected[2]); // bit
            pstmt.setString(4, (String) expected[3]); // char
            pstmt.setDate(5, (Date) expected[4]); // date
            pstmt.setDateTime(6, (Timestamp) expected[5]);// datetime
            pstmt.setDateTime(7, (Timestamp) expected[6]); // datetime2
            pstmt.setDateTimeOffset(8, (DateTimeOffset) expected[7]); // datetimeoffset
            pstmt.setBigDecimal(9, (BigDecimal) expected[8]); // decimal
            pstmt.setDouble(10, (double) expected[9]); // float
            pstmt.setInt(11, (int) expected[10]); // int
            pstmt.setMoney(12, (BigDecimal) expected[11]); // money
            pstmt.setString(13, (String) expected[12]); // nchar
            pstmt.setBigDecimal(14, (BigDecimal) expected[13]); // numeric
            pstmt.setString(15, (String) expected[14]); // nvarchar(20)
            pstmt.setFloat(16, (float) expected[15]); // real
            pstmt.setSmallDateTime(17, (Timestamp) expected[16]); // smalldatetime
            pstmt.setShort(18, (short) expected[17]); // smallint
            pstmt.setSmallMoney(19, (BigDecimal) expected[18]); // smallmoney
            pstmt.setTime(20, (Time) expected[19]); // time
            pstmt.setShort(21, (short) expected[20]); // tinyint
            pstmt.setBytes(22, (byte[]) expected[21]); // varbinary(5)
            pstmt.setString(23, (String) expected[22]); // varchar(20)
            pstmt.setString(24, (String) expected[23]); // uniqueidentifier

            pstmt.addBatch();
            pstmt.executeBatch();

            try (ResultSet rs = stmt
                    .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                for (int i = 0; i < expected.length; i++) {
                    if (rs.getObject(i + 1) instanceof byte[]) {
                        assertTrue(Arrays.equals((byte[]) expected[i], (byte[]) rs.getObject(i + 1)));
                    } else {
                        assertEquals(expected[i].toString(), rs.getObject(i + 1).toString());
                    }
                }
            }
        }
    }

    @Test
    public void testMixColumns() throws Exception {
        String valid = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (c1, c3, c5, c8) values "
                + "(" + "?, " + "?, " + "?, " + "? " + ")";

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(valid);
                Statement stmt = (SQLServerStatement) connection.createStatement();) {
            Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
            f1.setAccessible(true);
            f1.set(connection, true);

            Timestamp randomTimestamp = new Timestamp(
                    Constants.NOW.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
            Date randomDate = Date.valueOf(Constants.NOW.toLocalDate());
            long randomLong = Constants.RANDOM.nextLong();

            pstmt.setLong(1, randomLong); // bigint
            pstmt.setBoolean(2, true); // bit
            pstmt.setDate(3, randomDate); // date
            pstmt.setDateTimeOffset(4, microsoft.sql.DateTimeOffset.valueOf(randomTimestamp, 0)); // datetimeoffset
            pstmt.addBatch();

            pstmt.executeBatch();

            try (ResultSet rs = stmt
                    .executeQuery("select c1, c3, c5, c8 from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {

                Object[] expected = new Object[4];

                expected[0] = randomLong;
                expected[1] = true;
                expected[2] = randomDate;
                expected[3] = microsoft.sql.DateTimeOffset.valueOf(randomTimestamp, 0);
                rs.next();

                for (int i = 0; i < expected.length; i++) {
                    if (null != rs.getObject(i + 1)) {
                        assertEquals(expected[i].toString(), rs.getObject(i + 1).toString());
                    }
                }
            }
        }
    }

    @Test
    @Tag((Constants.xAzureSQLDW))
    public void testNullGuid() throws Exception {
        String valid = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (c24) values (?)";
        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(valid);
                Statement stmt = (SQLServerStatement) connection.createStatement();) {
            pstmt.setNull(1, microsoft.sql.Types.GUID);
            pstmt.addBatch();
            pstmt.executeBatch();

            try (ResultSet rs = stmt
                    .executeQuery("select c24 from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                Object[] expected = new Object[1];

                expected[0] = null;
                rs.next();

                assertEquals(expected[0], rs.getObject(1));
            }
        }
    }

    @Test
    public void testNullOrEmptyColumns() throws Exception {
        String valid = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName)
                + " (c1, c2, c3, c4, c5, c6, c7) values " + "(" + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "? "
                + ")";
        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(valid);
                Statement stmt = (SQLServerStatement) connection.createStatement();) {
            Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
            f1.setAccessible(true);
            f1.set(connection, true);

            long randomLong = Constants.RANDOM.nextLong();
            String randomChar = RandomData.generateCharTypes("1", false, false);

            pstmt.setLong(1, randomLong); // bigint
            pstmt.setBytes(2, null); // binary(5)
            pstmt.setBoolean(3, true); // bit
            pstmt.setString(4, randomChar); // char
            pstmt.setDate(5, null); // date
            pstmt.setDateTime(6, null);// datetime
            pstmt.setDateTime(7, null); // datetime2
            pstmt.addBatch();

            pstmt.executeBatch();

            try (ResultSet rs = stmt
                    .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {

                Object[] expected = new Object[7];

                expected[0] = randomLong;
                expected[1] = null;
                expected[2] = true;
                expected[3] = randomChar;
                expected[4] = null;
                expected[5] = null;
                expected[6] = null;

                rs.next();
                for (int i = 0; i < expected.length; i++) {
                    if (null != rs.getObject(i + 1)) {
                        assertEquals(expected[i], rs.getObject(i + 1));
                    }
                }
            }
        }
    }

    @Test
    public void testSquareBracketAgainstDB() throws Exception {
        String valid = "insert into " + AbstractSQLGenerator.escapeIdentifier(squareBracketTableName) + " values (?)";

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(valid);
                Statement stmt = (SQLServerStatement) connection.createStatement();) {
            Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
            f1.setAccessible(true);
            f1.set(connection, true);

            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(squareBracketTableName), stmt);
            String createTable = "create table " + AbstractSQLGenerator.escapeIdentifier(squareBracketTableName)
                    + " (c1 int)";
            stmt.execute(createTable);

            pstmt.setInt(1, 1);
            pstmt.addBatch();

            pstmt.executeBatch();

            try (ResultSet rs = stmt
                    .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(squareBracketTableName))) {
                rs.next();

                assertEquals(1, rs.getObject(1));
            }
        }
    }

    @Test
    public void testDoubleQuoteAgainstDB() throws Exception {
        String valid = "insert into " + AbstractSQLGenerator.escapeIdentifier(doubleQuoteTableName) + " values (?)";

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(valid);
                Statement stmt = (SQLServerStatement) connection.createStatement();) {
            Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
            f1.setAccessible(true);
            f1.set(connection, true);

            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(doubleQuoteTableName), stmt);
            String createTable = "create table " + AbstractSQLGenerator.escapeIdentifier(doubleQuoteTableName)
                    + " (c1 int)";
            stmt.execute(createTable);

            pstmt.setInt(1, 1);
            pstmt.addBatch();

            pstmt.executeBatch();

            try (ResultSet rs = stmt
                    .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(doubleQuoteTableName))) {
                rs.next();

                assertEquals(1, rs.getObject(1));
            }
        }
    }

    @Test
    public void testSchemaAgainstDB() throws Exception {
        String valid = "insert into " + AbstractSQLGenerator.escapeIdentifier(schemaTableName) + " values (?)";

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(valid);
                Statement stmt = (SQLServerStatement) connection.createStatement();) {
            Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
            f1.setAccessible(true);
            f1.set(connection, true);

            TestUtils.dropTableIfExists("[dbo]." + AbstractSQLGenerator.escapeIdentifier(squareBracketTableName), stmt);
            String createTable = "create table " + AbstractSQLGenerator.escapeIdentifier(schemaTableName) + " (c1 int)";
            stmt.execute(createTable);

            pstmt.setInt(1, 1);
            pstmt.addBatch();

            pstmt.executeBatch();

            try (ResultSet rs = stmt
                    .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(schemaTableName))) {
                rs.next();

                assertEquals(1, rs.getObject(1));
            }
        }
    }

    @Test
    public void testColumnNameMixAgainstDB() throws Exception {
        String valid = "insert into " + AbstractSQLGenerator.escapeIdentifier(squareBracketTableName)
                + " ([c]]]]1], [c]]]]2]) values (?, 1)";

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(valid);
                Statement stmt = (SQLServerStatement) connection.createStatement();) {
            Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
            f1.setAccessible(true);
            f1.set(connection, true);

            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(squareBracketTableName), stmt);
            String createTable = "create table " + AbstractSQLGenerator.escapeIdentifier(squareBracketTableName)
                    + " ([c]]]]1] int, [c]]]]2] int)";
            stmt.execute(createTable);

            pstmt.setInt(1, 1);
            pstmt.addBatch();

            pstmt.executeBatch();

            try (ResultSet rs = stmt
                    .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(squareBracketTableName))) {
                rs.next();

                assertEquals(1, rs.getObject(1));
            }
        }
    }

    @Test
    public void testAllColumnsLargeBatch() throws Exception {
        String valid = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values " + "(" + "?, "
                + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, "
                + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?," + "?" + ")";

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(valid);
                Statement stmt = (SQLServerStatement) connection.createStatement();) {
            Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
            f1.setAccessible(true);
            f1.set(connection, true);

            Object[] expected = generateExpectedValues();

            pstmt.setLong(1, (long) expected[0]); // bigint
            pstmt.setBytes(2, (byte[]) expected[1]); // binary(5)
            pstmt.setBoolean(3, (boolean) expected[2]); // bit
            pstmt.setString(4, (String) expected[3]); // char
            pstmt.setDate(5, (Date) expected[4]); // date
            pstmt.setDateTime(6, (Timestamp) expected[5]);// datetime
            pstmt.setDateTime(7, (Timestamp) expected[6]); // datetime2
            pstmt.setDateTimeOffset(8, (DateTimeOffset) expected[7]); // datetimeoffset
            pstmt.setBigDecimal(9, (BigDecimal) expected[8]); // decimal
            pstmt.setDouble(10, (double) expected[9]); // float
            pstmt.setInt(11, (int) expected[10]); // int
            pstmt.setMoney(12, (BigDecimal) expected[11]); // money
            pstmt.setString(13, (String) expected[12]); // nchar
            pstmt.setBigDecimal(14, (BigDecimal) expected[13]); // numeric
            pstmt.setString(15, (String) expected[14]); // nvarchar(20)
            pstmt.setFloat(16, (float) expected[15]); // real
            pstmt.setSmallDateTime(17, (Timestamp) expected[16]); // smalldatetime
            pstmt.setShort(18, (short) expected[17]); // smallint
            pstmt.setSmallMoney(19, (BigDecimal) expected[18]); // smallmoney
            pstmt.setTime(20, (Time) expected[19]); // time
            pstmt.setShort(21, (short) expected[20]); // tinyint
            pstmt.setBytes(22, (byte[]) expected[21]); // varbinary(5)
            pstmt.setString(23, (String) expected[22]); // varchar(20)
            pstmt.setString(24, (String) expected[23]); // uniqueidentifier

            pstmt.addBatch();
            pstmt.executeLargeBatch();

            try (ResultSet rs = stmt
                    .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                for (int i = 0; i < expected.length; i++) {
                    if (rs.getObject(i + 1) instanceof byte[]) {
                        assertTrue(Arrays.equals((byte[]) expected[i], (byte[]) rs.getObject(i + 1)));
                    } else {
                        assertEquals(expected[i].toString(), rs.getObject(i + 1).toString());
                    }
                }
            }
        }
    }

    @Test
    public void testIllegalNumberOfArgNoColumnList() throws Exception {
        String invalid = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values (?, ?,? ,?) ";

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(invalid);
                Statement stmt = (SQLServerStatement) connection.createStatement();) {
            Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
            f1.setAccessible(true);
            f1.set(connection, true);

            pstmt.setInt(1, 1);
            pstmt.setInt(2, 1);
            pstmt.setInt(3, 1);
            pstmt.setInt(4, 1);
            pstmt.addBatch();

            pstmt.executeBatch();
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (BatchUpdateException | SQLServerException e) {
            assertEquals(TestResource.getResource("R_incorrectColumnNum"), e.getMessage());
        }

        invalid = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName)
                + " (c1, c2, c3) values (?, ?,? ,?) ";

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(invalid);
                Statement stmt = (SQLServerStatement) connection.createStatement();) {
            Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
            f1.setAccessible(true);
            f1.set(connection, true);

            pstmt.setInt(1, 1);
            pstmt.setInt(2, 1);
            pstmt.setInt(3, 1);
            pstmt.setInt(4, 1);
            pstmt.addBatch();

            pstmt.executeBatch();
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (BatchUpdateException | SQLServerException e) {
            if (isSqlAzureDW()) {
                assertEquals(TestResource.getResource("R_incorrectColumnNumInsertDW"), e.getMessage());
            } else {
                assertEquals(TestResource.getResource("R_incorrectColumnNumInsert"), e.getMessage());
            }
        }
    }

    @Test
    public void testNonParameterizedQuery() throws Exception {
        String invalid = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName)
                + " values ((SELECT * from table where c1=?), ?,? ,?) ";

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(invalid);
                Statement stmt = (SQLServerStatement) connection.createStatement();) {

            Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
            f1.setAccessible(true);
            f1.set(connection, true);

            pstmt.setInt(1, 1);
            pstmt.setInt(2, 1);
            pstmt.setInt(3, 1);
            pstmt.setInt(4, 1);
            pstmt.addBatch();

            pstmt.executeBatch();
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (BatchUpdateException | SQLServerException e) {
            if (isSqlAzureDW()) {
                assertTrue(e.getMessage().contains(TestResource.getResource("R_incorrectSyntaxTableDW")));
            } else {
                assertTrue(e.getMessage().contains(TestResource.getResource("R_incorrectSyntaxTable")));
            }
        }

        invalid = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values ('?', ?,? ,?) ";

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(invalid);
                Statement stmt = (SQLServerStatement) connection.createStatement();) {
            Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
            f1.setAccessible(true);
            f1.set(connection, true);

            pstmt.setInt(1, 1);
            pstmt.setInt(2, 1);
            pstmt.setInt(3, 1);
            pstmt.addBatch();

            pstmt.executeBatch();
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (BatchUpdateException | SQLServerException e) {
            assertEquals(TestResource.getResource("R_incorrectColumnNum"), e.getMessage());
        }
    }

    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testNonSupportedColumns() throws Exception {
        String valid = "insert into " + AbstractSQLGenerator.escapeIdentifier(unsupportedTableName)
                + " values (?, ?, ?, ?)";

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(valid);
                Statement stmt = (SQLServerStatement) connection.createStatement()) {
            Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
            f1.setAccessible(true);
            f1.set(connection, true);

            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(unsupportedTableName), stmt);

            String createTable = "create table " + AbstractSQLGenerator.escapeIdentifier(unsupportedTableName)
                    + " (c1 geometry, c2 geography, c3 datetime, c4 smalldatetime)";
            stmt.execute(createTable);

            Timestamp myTimestamp = new Timestamp(1200000L);
            Geometry g1 = Geometry.STGeomFromText("POINT(1 2 3 4)", 0);
            Geography g2 = Geography.STGeomFromText("POINT(1 2 3 4)", 4326);

            pstmt.setGeometry(1, g1);
            pstmt.setGeography(2, g2);
            pstmt.setDateTime(3, myTimestamp);
            pstmt.setSmallDateTime(4, myTimestamp);
            pstmt.addBatch();

            pstmt.executeBatch();

            try (ResultSet rs = stmt
                    .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(unsupportedTableName))) {
                rs.next();
                assertEquals(g1.toString(), Geometry.STGeomFromWKB((byte[]) rs.getObject(1)).toString());
                assertEquals(g2.toString(), Geography.STGeomFromWKB((byte[]) rs.getObject(2)).toString());
                assertEquals(myTimestamp, rs.getObject(3));
                assertEquals(myTimestamp, rs.getObject(4));
            }
        }
    }

    @Test
    public void testReverseColumnOrder() throws Exception {
        String valid = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (c2, c1) values " + "("
                + "?, " + "? " + ")";

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(valid);
                Statement stmt = connection.createStatement()) {
            Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
            f1.setAccessible(true);
            f1.set(connection, true);

            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            String createTable = "create table " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (c1 varchar(1), c2 varchar(3))";
            stmt.execute(createTable);

            pstmt.setString(1, "One");
            pstmt.setString(2, "1");
            pstmt.addBatch();

            pstmt.executeBatch();

            try (ResultSet rs = stmt
                    .executeQuery("select c1, c2 from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {

                Object[] expected = new Object[2];

                expected[0] = "1";
                expected[1] = "One";
                rs.next();

                for (int i = 0; i < expected.length; i++) {
                    assertEquals(expected[i], rs.getObject(i + 1));
                }
            }
        }
    }

    @Test
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.xSQLv11)
    @Tag(Constants.xSQLv12)
    public void testComputedCols() throws Exception {
        String valid = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableNameBulkComputedCols) + " (id, json)"
                + " values (?, ?)";

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(valid);
                Statement stmt = (SQLServerStatement) connection.createStatement();) {
            Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
            f1.setAccessible(true);
            f1.set(connection, true);

            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableNameBulkComputedCols), stmt);
            String createTable = "create table " + AbstractSQLGenerator.escapeIdentifier(tableNameBulkComputedCols)
                    + " (id nvarchar(100) not null, json nvarchar(max) not null,"
                    + " vcol1 as json_value([json], '$.vcol1'), vcol2 as json_value([json], '$.vcol2'))";
            stmt.execute(createTable);

            String jsonValue = "{\"vcol1\":\"" + UUID.randomUUID().toString() + "\",\"vcol2\":\""
                    + UUID.randomUUID().toString() + "\" }";
            String idValue = UUID.randomUUID().toString();
            pstmt.setString(1, idValue);
            pstmt.setString(2, jsonValue);
            pstmt.addBatch();
            pstmt.executeBatch();

            try (ResultSet rs = stmt.executeQuery(
                    "select * from " + AbstractSQLGenerator.escapeIdentifier(tableNameBulkComputedCols))) {
                rs.next();

                assertEquals(idValue, rs.getObject(1));
                assertEquals(jsonValue, rs.getObject(2));
            }
        }
    }

    /**
     * Test inserting complex JSON data using prepared statement with bulk copy enabled.
     */
    @Test
    @AzureDB
    @Tag(Constants.JSONTest)
    public void testInsertJsonWithBulkCopy() throws Exception {
        String tableName = RandomUtil.getIdentifier("BulkCopyComplexJsonTest");
        String valid = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (jsonCol) values (?)";

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
             SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(valid);
             Statement stmt = (SQLServerStatement) connection.createStatement()) {

            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            String createTable = "create table " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (jsonCol JSON)";
            stmt.execute(createTable);

            String complexJsonData = "{"
                    + "\"name\":\"John\","
                    + "\"age\":30,"
                    + "\"address\":{"
                    + "\"street\":\"123 Main St\","
                    + "\"city\":\"New York\","
                    + "\"zipcode\":\"10001\""
                    + "},"
                    + "\"phoneNumbers\":["
                    + "{\"type\":\"home\",\"number\":\"212-555-1234\"},"
                    + "{\"type\":\"work\",\"number\":\"646-555-4567\"}"
                    + "],"
                    + "\"children\":["
                    + "{\"name\":\"Jane\",\"age\":10},"
                    + "{\"name\":\"Doe\",\"age\":8}"
                    + "]"
                    + "}";

            pstmt.setString(1, complexJsonData);
            pstmt.addBatch();
            pstmt.executeBatch();

            try (ResultSet rs = stmt.executeQuery("select jsonCol from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next());
                assertEquals(complexJsonData, rs.getString(1));
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            }
        }
    }

    /**
     * Test select, update, create, and delete operations on JSON data and verify at each step.
     */
    @Test
    @AzureDB
    @Tag(Constants.JSONTest)
    public void testCRUDOperationsWithJson() throws Exception {
        String tableName = RandomUtil.getIdentifier("CRUDJsonTest");
        String createTableSQL = "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (id INT PRIMARY KEY, jsonCol JSON)";
        String insertSQL = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (id, jsonCol) VALUES (?, ?)";
        String selectSQL = "SELECT jsonCol FROM " + AbstractSQLGenerator.escapeIdentifier(tableName) + " WHERE id = ?";
        String updateSQL = "UPDATE " + AbstractSQLGenerator.escapeIdentifier(tableName) + " SET jsonCol = ? WHERE id = ?";
        String deleteSQL = "DELETE FROM " + AbstractSQLGenerator.escapeIdentifier(tableName) + " WHERE id = ?";

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
             Statement stmt = (SQLServerStatement) connection.createStatement()) {

            stmt.execute(createTableSQL);

            String initialJsonData = "{\"name\":\"John\",\"age\":30}";
            try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) {
                pstmt.setInt(1, 1);
                pstmt.setString(2, initialJsonData);
                pstmt.executeUpdate();
            }

            try (PreparedStatement pstmt = connection.prepareStatement(selectSQL)) {
                pstmt.setInt(1, 1);
                try (ResultSet rs = pstmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(initialJsonData, rs.getString(1));
                }
            }

            String updatedJsonData = "{\"name\":\"Jane\",\"age\":25}";
            try (PreparedStatement pstmt = connection.prepareStatement(updateSQL)) {
                pstmt.setString(1, updatedJsonData);
                pstmt.setInt(2, 1);
                pstmt.executeUpdate();
            }

            try (PreparedStatement pstmt = connection.prepareStatement(selectSQL)) {
                pstmt.setInt(1, 1);
                try (ResultSet rs = pstmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(updatedJsonData, rs.getString(1));
                }
            }

            try (PreparedStatement pstmt = connection.prepareStatement(deleteSQL)) {
                pstmt.setInt(1, 1);
                pstmt.executeUpdate();
            }

            try (PreparedStatement pstmt = connection.prepareStatement(selectSQL)) {
                pstmt.setInt(1, 1);
                try (ResultSet rs = pstmt.executeQuery()) {
                    assertFalse(rs.next());
                }
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            }
        }
    }

    /**
     * Test bulk insert with no space after table name
     * 
     * @throws Exception
     */
    @Test
    public void testNoSpaceInsert() throws Exception {
        // table name with valid alphanumeric chars that don't need to be escaped, since escaping the table name would not test the space issue
        String testNoSpaceInsertTableName = "testNoSpaceInsertTable" + (new Random()).nextInt(Integer.MAX_VALUE);
        String valid = "insert into " + testNoSpaceInsertTableName + "(col)" + " values(?)";

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(valid);
                Statement stmt = (SQLServerStatement) connection.createStatement();) {

            TestUtils.dropTableIfExists(testNoSpaceInsertTableName, stmt);
            String createTable = "create table " + testNoSpaceInsertTableName + " (col varchar(4))";
            stmt.execute(createTable);

            pstmt.setString(1, "test");
            pstmt.addBatch();
            pstmt.executeBatch();
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(testNoSpaceInsertTableName), stmt);
            }
        }
    }

    /**
     * Test bulk insert with all temporal types and money as varchar when useBulkCopyForBatchInsert is true.
     * sendTemporalDataTypesAsStringForBulkCopy is set to true by default.
     * Temporal types are sent as varchar, and money/smallMoney are sent as their respective types.
     * 
     * @throws Exception
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testBulkInsertWithAllTemporalTypesAndMoneyAsVarchar() throws Exception {
        String tableName = RandomUtil.getIdentifier("BulkTable");
        String insertSQL = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) +
                " (dateTimeColumn, smallDateTimeColumn, dateTime2Column, dateColumn, timeColumn, dateTimeOffsetColumn, moneyColumn, smallMoneyColumn) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        String selectSQL = "SELECT dateTimeColumn, smallDateTimeColumn, dateTime2Column, dateColumn, timeColumn, dateTimeOffsetColumn, moneyColumn, smallMoneyColumn FROM "
                + AbstractSQLGenerator.escapeIdentifier(tableName);

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                Statement stmt = connection.createStatement();
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(insertSQL)) {

            getCreateTableTemporalSQL(tableName);

            Timestamp dateTimeVal = Timestamp.valueOf(LocalDateTime.of(2025, 5, 13, 14, 30, 45));
            String expectedDateTimeString = "2025-05-13 14:30:45.0"; 

            Timestamp smallDateTimeVal = Timestamp.valueOf(LocalDateTime.of(2025, 5, 13, 14, 30, 45));
            String expectedSmallDateTimeString = "2025-05-13 14:31:00.0"; 

            Timestamp dateTime2Val = Timestamp.valueOf(LocalDateTime.of(2025, 5, 13, 14, 30, 25, 123000000));
            String expectedDateTime2String = "2025-05-13 14:30:25.1230000"; 

            Date dateVal = Date.valueOf("2025-06-02");
            String expectedDateString = "2025-06-02";

            Time timeVal = Time.valueOf("14:30:00");
            String expectedTimeString = "14:30:00";

            OffsetDateTime offsetDateTimeVal = OffsetDateTime.of(2025, 5, 13, 14, 30, 0, 0, ZoneOffset.UTC);
            DateTimeOffset dateTimeOffsetVal = DateTimeOffset.valueOf(offsetDateTimeVal);
            String expectedDateTimeOffsetString = "2025-05-13 14:30:00 +00:00";

            BigDecimal moneyVal = new BigDecimal("12345.6789");
            String expectedMoneyString = "12345.6789";

            BigDecimal smallMoneyVal = new BigDecimal("1234.5611");
            String expectedSmallMoneyString = "1234.5611";

            pstmt.setTimestamp(1, dateTimeVal); // DATETIME
            pstmt.setSmallDateTime(2, smallDateTimeVal); // SMALLDATETIME
            pstmt.setObject(3, dateTime2Val); // DATETIME2
            pstmt.setDate(4, dateVal); // DATE
            pstmt.setObject(5, timeVal); // TIME
            pstmt.setDateTimeOffset(6, dateTimeOffsetVal); // DATETIMEOFFSET
            pstmt.setMoney(7, moneyVal); // MONEY
            pstmt.setSmallMoney(8, smallMoneyVal); // SMALLMONEY

            pstmt.addBatch();
            pstmt.executeBatch();

            // Validate inserted data
            try (ResultSet rs = stmt.executeQuery(selectSQL)) {
                assertTrue(rs.next());

                assertEquals(dateTimeVal, rs.getTimestamp(1));
                assertEquals(expectedDateTimeString, rs.getString(1));

                assertEquals(Timestamp.valueOf(LocalDateTime.of(2025, 5, 13, 14, 31, 0)), rs.getTimestamp(2));
                assertEquals(expectedSmallDateTimeString, rs.getString(2));

                assertEquals(dateTime2Val, rs.getTimestamp(3));
                assertEquals(expectedDateTime2String, rs.getString(3));

                assertEquals(dateVal, rs.getDate(4));
                assertEquals(expectedDateString, rs.getString(4));

                assertEquals(timeVal, rs.getObject(5));
                assertEquals(expectedTimeString, rs.getObject(5).toString());

                assertEquals(dateTimeOffsetVal, rs.getObject(6, DateTimeOffset.class));
                assertEquals(expectedDateTimeOffsetString, rs.getObject(6).toString());

                assertEquals(moneyVal, rs.getBigDecimal(7));
                assertEquals(expectedMoneyString, rs.getBigDecimal(7).toString());

                assertEquals(smallMoneyVal, rs.getBigDecimal(8));
                assertEquals(expectedSmallMoneyString,rs.getBigDecimal(8).toString());
                
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            }
        }
    }

    /**
     * Test bulk insert with null data for all temporal types and money as varchar when useBulkCopyForBatchInsert is true.
     * sendTemporalDataTypesAsStringForBulkCopy is set to true by default.
     * Temporal types are sent as varchar, and money/smallMoney are sent as their respective types.
     * 
     * @throws Exception
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testBulkInsertWithNullDataForAllTemporalTypesAndMoneyAsVarchar() throws Exception {
        String tableName = RandomUtil.getIdentifier("BulkTable");
        String insertSQL = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) +
                " (dateTimeColumn, smallDateTimeColumn, dateTime2Column, dateColumn, timeColumn, dateTimeOffsetColumn, moneyColumn, smallMoneyColumn) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        String selectSQL = "SELECT dateTimeColumn, smallDateTimeColumn, dateTime2Column, dateColumn, timeColumn, dateTimeOffsetColumn, moneyColumn, smallMoneyColumn FROM "
                + AbstractSQLGenerator.escapeIdentifier(tableName);

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                Statement stmt = connection.createStatement();
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(insertSQL)) {

            getCreateTableTemporalSQL(tableName);

            pstmt.setTimestamp(1, null); // DATETIME
            pstmt.setSmallDateTime(2, null); // SMALLDATETIME
            pstmt.setObject(3, null); // DATETIME2
            pstmt.setDate(4, null); // DATE
            pstmt.setObject(5, null); // TIME
            pstmt.setDateTimeOffset(6, null); // DATETIMEOFFSET
            pstmt.setMoney(7, null); // MONEY
            pstmt.setSmallMoney(8, null); // SMALLMONEY

            pstmt.addBatch();
            pstmt.executeBatch();

            // Validate inserted data
            try (ResultSet rs = stmt.executeQuery(selectSQL)) {
                assertTrue(rs.next());

                assertEquals(null, rs.getTimestamp(1));
                assertEquals(null, rs.getTimestamp(2));
                assertEquals(null, rs.getTimestamp(3));
                assertEquals(null, rs.getDate(4));
                assertEquals(null, rs.getObject(5));
                assertEquals(null, rs.getObject(6));
                assertEquals(null, rs.getBigDecimal(7));
                assertEquals(null, rs.getBigDecimal(8));
                
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            }
        }
    }

    /**
     * Test bulk insert with all temporal types and money as varchar when useBulkCopyForBatchInsert is true.
     * and sendTemporalDataTypesAsStringForBulkCopy is set to false explicitly.
     * In this case all data types are sent as their respective types, including temporal types and money/smallMoney.
     * 
     * @throws Exception
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testBulkInsertWithAllTemporalTypesAndMoney() throws Exception {
        String tableName = RandomUtil.getIdentifier("BulkTable");
        String insertSQL = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) +
                " (dateTimeColumn, smallDateTimeColumn, dateTime2Column, dateColumn, timeColumn, dateTimeOffsetColumn, moneyColumn, smallMoneyColumn) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        String selectSQL = "SELECT dateTimeColumn, smallDateTimeColumn, dateTime2Column, dateColumn, timeColumn, dateTimeOffsetColumn, moneyColumn, smallMoneyColumn FROM "
                + AbstractSQLGenerator.escapeIdentifier(tableName);

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;sendTemporalDataTypesAsStringForBulkCopy=false;");
                Statement stmt = connection.createStatement();
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(insertSQL)) {

            getCreateTableTemporalSQL(tableName);

            Timestamp dateTimeVal = Timestamp.valueOf(LocalDateTime.of(2025, 5, 13, 14, 30, 45));
            String expectedDateTimeString = "2025-05-13 14:30:45.0"; 

            Timestamp smallDateTimeVal = Timestamp.valueOf(LocalDateTime.of(2025, 5, 13, 14, 30, 45));
            String expectedSmallDateTimeString = "2025-05-13 14:31:00.0"; 

            Timestamp dateTime2Val = Timestamp.valueOf(LocalDateTime.of(2025, 5, 13, 14, 30, 25, 123000000));
            String expectedDateTime2String = "2025-05-13 14:30:25.1230000"; 

            Date dateVal = Date.valueOf("2025-06-02");
            String expectedDateString = "2025-06-02";

            LocalTime time = LocalTime.of(14, 30, 0);
            Timestamp timeVal = Timestamp.valueOf(
                    LocalDateTime.of(1970, 1, 1, time.getHour(), time.getMinute(), time.getSecond()));
            pstmt.setTimestamp(5, timeVal);
            String expectedTimeString = "14:30:00";

            OffsetDateTime offsetDateTimeVal = OffsetDateTime.of(2025, 5, 13, 14, 30, 0, 0, ZoneOffset.UTC);
            DateTimeOffset dateTimeOffsetVal = DateTimeOffset.valueOf(offsetDateTimeVal);
            String expectedDateTimeOffsetString = "2025-05-13 14:30:00 +00:00";

            BigDecimal moneyVal = new BigDecimal("12345.6789");
            String expectedMoneyString = "12345.6789";

            BigDecimal smallMoneyVal = new BigDecimal("1234.5611");
            String expectedSmallMoneyString = "1234.5611";

            pstmt.setTimestamp(1, dateTimeVal); // DATETIME
            pstmt.setSmallDateTime(2, smallDateTimeVal); // SMALLDATETIME
            pstmt.setObject(3, dateTime2Val); // DATETIME2
            pstmt.setDate(4, dateVal); // DATE
            pstmt.setTimestamp(5, timeVal); // TIME
            pstmt.setDateTimeOffset(6, dateTimeOffsetVal); // DATETIMEOFFSET
            pstmt.setMoney(7, moneyVal); // MONEY
            pstmt.setSmallMoney(8, smallMoneyVal); // SMALLMONEY

            pstmt.addBatch();
            pstmt.executeBatch();

            // Validate inserted data
            try (ResultSet rs = stmt.executeQuery(selectSQL)) {
                assertTrue(rs.next());

                assertEquals(dateTimeVal, rs.getTimestamp(1));
                assertEquals(expectedDateTimeString, rs.getString(1));

                assertEquals(Timestamp.valueOf(LocalDateTime.of(2025, 5, 13, 14, 31, 0)), rs.getTimestamp(2));
                assertEquals(expectedSmallDateTimeString, rs.getString(2));

                assertEquals(dateTime2Val, rs.getTimestamp(3));
                assertEquals(expectedDateTime2String, rs.getString(3));

                assertEquals(dateVal, rs.getDate(4));
                assertEquals(expectedDateString, rs.getString(4));

                assertEquals(Time.valueOf(time), rs.getObject(5));
                assertEquals(expectedTimeString, rs.getObject(5).toString());

                assertEquals(dateTimeOffsetVal, rs.getObject(6, DateTimeOffset.class));
                assertEquals(expectedDateTimeOffsetString, rs.getObject(6).toString());

                assertEquals(moneyVal, rs.getBigDecimal(7));
                assertEquals(expectedMoneyString, rs.getBigDecimal(7).toString());

                assertEquals(smallMoneyVal, rs.getBigDecimal(8));
                assertEquals(expectedSmallMoneyString,rs.getBigDecimal(8).toString());
                
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            }
        }
    }

    /**
     * Test bulk insert with null data for all temporal types and money as varchar when useBulkCopyForBatchInsert is true.
     * and sendTemporalDataTypesAsStringForBulkCopy is set to false explicitly.
     * In this case all data types are sent as their respective types, including temporal types and money/smallMoney.
     * 
     * @throws Exception
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testBulkInsertWithNullDataForAllTemporalTypesAndMoney() throws Exception {
        String tableName = RandomUtil.getIdentifier("BulkTable");
        String insertSQL = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) +
                " (dateTimeColumn, smallDateTimeColumn, dateTime2Column, dateColumn, timeColumn, dateTimeOffsetColumn, moneyColumn, smallMoneyColumn) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        String selectSQL = "SELECT dateTimeColumn, smallDateTimeColumn, dateTime2Column, dateColumn, timeColumn, dateTimeOffsetColumn, moneyColumn, smallMoneyColumn FROM "
                + AbstractSQLGenerator.escapeIdentifier(tableName);

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;sendTemporalDataTypesAsStringForBulkCopy=false;");
                Statement stmt = connection.createStatement();
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(insertSQL)) {

            getCreateTableTemporalSQL(tableName);

            pstmt.setTimestamp(1, null); // DATETIME
            pstmt.setSmallDateTime(2, null); // SMALLDATETIME
            pstmt.setObject(3, null); // DATETIME2
            pstmt.setDate(4, null); // DATE
            pstmt.setObject(5, null); // TIME
            pstmt.setDateTimeOffset(6, null); // DATETIMEOFFSET
            pstmt.setMoney(7, null); // MONEY
            pstmt.setSmallMoney(8, null); // SMALLMONEY

            pstmt.addBatch();
            pstmt.executeBatch();

            // Validate inserted data
            try (ResultSet rs = stmt.executeQuery(selectSQL)) {
                assertTrue(rs.next());

                assertEquals(null, rs.getTimestamp(1));
                assertEquals(null, rs.getTimestamp(2));
                assertEquals(null, rs.getTimestamp(3));
                assertEquals(null, rs.getDate(4));
                assertEquals(null, rs.getObject(5));
                assertEquals(null, rs.getObject(6));
                assertEquals(null, rs.getBigDecimal(7));
                assertEquals(null, rs.getBigDecimal(8));
                
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            }
        }
    }

    /**
     * Test inserting vector data using prepared statement with bulk copy enabled.
     */
    @Test
    @AzureDB
    @Tag(Constants.vectorTest)
    public void testInsertVectorWithBulkCopy() throws Exception {
        String tableName = RandomUtil.getIdentifier("BulkCopyVectorTest");
        String sqlString = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (vectorCol) values (?)";

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
             SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(sqlString);
             Statement stmt = (SQLServerStatement) connection.createStatement()) {

            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            String createTable = "create table " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (vectorCol VECTOR(3))";
            stmt.execute(createTable);

            Object[] vectorData = new Float[] { 4.0f, 5.0f, 6.0f };
            Vector vector = new Vector(vectorData.length, VectorDimensionType.FLOAT32, vectorData);

            pstmt.setObject(1, vector, microsoft.sql.Types.VECTOR);
            pstmt.addBatch();
            pstmt.executeBatch();

            try (ResultSet rs = stmt.executeQuery("select vectorCol from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next());
                Vector resultVector = rs.getObject("vectorCol", Vector.class);
                assertNotNull(resultVector, "Retrieved vector is null.");
                assertEquals(3, resultVector.getDimensionCount());
                assertArrayEquals(vectorData, resultVector.getData(), "Vector data mismatch.");
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            }
        }
    }

    /**
     * Test inserting null vector data using prepared statement with bulk copy enabled.
     */
    @Test
    @AzureDB
    @Tag(Constants.vectorTest)
    public void testInsertNullVectorWithBulkCopy() throws Exception {
        String tableName = RandomUtil.getIdentifier("BulkCopyVectorTest");
        String sqlString = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (vectorCol) values (?)";

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
             SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(sqlString);
             Statement stmt = (SQLServerStatement) connection.createStatement()) {

            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            String createTable = "create table " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (vectorCol VECTOR(3))";
            stmt.execute(createTable);

            Vector vector = new Vector(3, VectorDimensionType.FLOAT32, null);

            pstmt.setObject(1, vector, microsoft.sql.Types.VECTOR);
            pstmt.addBatch();
            pstmt.executeBatch();

            try (ResultSet rs = stmt.executeQuery("select vectorCol from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                int rowCount = 0;
                    while (rs.next()) {
                        Vector vectorObject = rs.getObject("vectorCol", Vector.class);
                        assertEquals(null, vectorObject.getData());
                        rowCount++;
                    }
                assertEquals(1, rowCount);
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            }
        }
    }

    /**
     * Test inserting vector data using prepared statement with bulk copy enabled for performance.
     */
    @Test
    @AzureDB
    @Tag(Constants.vectorTest)
    public void testInsertWithBulkCopyPerformance() throws SQLException {
        String tableName = AbstractSQLGenerator.escapeIdentifier("BulkCopyVectorPerformanceTest");
        // For testing, we can use a smaller set of records to avoid long execution time
        int recordCount = 100; // Number of records to insert
        int dimensionCount = 1998; // Dimension count for the vector
        Object[] vectorData = new Float[dimensionCount];

        // Initialize vector data
        for (int i = 0; i < dimensionCount; i++) {
            vectorData[i] = i + 0.5f;
        }

        // Drop the table if it already exists
        try (Connection conn = PrepUtil.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyForBatchInsertBatchSize=1000001;");
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("IF OBJECT_ID('" + tableName + "', 'U') IS NOT NULL DROP TABLE " + tableName);
        }

        // Create the destination table with a single VECTOR column
        try (Connection conn = PrepUtil.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyForBatchInsertBatchSize=1000001;");
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + tableName + " (vectorCol VECTOR(" + dimensionCount + "))");
        }

        long startTime = System.nanoTime();
        try (Connection conn = PrepUtil.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyForBatchInsertBatchSize=1000001")) {

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn.prepareStatement(
                    "INSERT INTO " + tableName + " (vectorCol) VALUES (?)")) {

                for (int i = 1; i <= recordCount; i++) {
                    Vector vector = new Vector(dimensionCount, VectorDimensionType.FLOAT32, vectorData);
                    pstmt.setObject(1, vector, microsoft.sql.Types.VECTOR);
                    pstmt.addBatch();
                }
                // Execute the batch
                pstmt.executeBatch();

            }
        }
        long endTime = System.nanoTime();
        long durationMs = (endTime - startTime) / 1_000_000;
        System.out.println("Insert for " + recordCount + " records in " + durationMs + " ms.");
    }

    /**
     * GitHub issue 2847 : Persisted Computed Columns Break useBulkCopyForBatchInsert
     * Test bulk copy batch insert with persisted computed column.
     * 
     * @throws Exception
     */ 
    @Test
    public void testBulkCopyBatchInsertWithPersistedComputedColumn() throws Exception {

        String tableName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("BulkCopyPersistedComputed"));
        String insertSQL = "INSERT INTO " + tableName + " (QtyAvailable, UnitPrice) VALUES (?, ?)";

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(insertSQL);
                Statement stmt = connection.createStatement()) {

            TestUtils.dropTableIfExists(tableName, stmt);

            // Create table with persisted computed column
            String createTable = "CREATE TABLE " + tableName + " ("
                    + "ProductID int IDENTITY(1,1) NOT NULL, "
                    + "QtyAvailable SMALLINT, "
                    + "InventoryValue AS QtyAvailable * UnitPrice PERSISTED, "
                    + "UnitPrice MONEY)";
            stmt.execute(createTable);

            // Test batch insert
            pstmt.setInt(1, 10);
            pstmt.setBigDecimal(2, new BigDecimal("15.99"));
            pstmt.addBatch();

            pstmt.setInt(1, 20);
            pstmt.setBigDecimal(2, new BigDecimal("25.50"));
            pstmt.addBatch();

            int[] updateCounts = pstmt.executeBatch();
            assertEquals(2, updateCounts.length);

            // Verify data was inserted correctly
            try (ResultSet rs = stmt.executeQuery("SELECT ProductID, QtyAvailable, InventoryValue, UnitPrice FROM "
                    + tableName + " ORDER BY ProductID")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals(10, rs.getInt(2));
                assertEquals(new BigDecimal("159.9000"), rs.getBigDecimal(3));
                assertEquals(new BigDecimal("15.9900"), rs.getBigDecimal(4));

                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertEquals(20, rs.getInt(2));
                assertEquals(new BigDecimal("510.0000"), rs.getBigDecimal(3));
                assertEquals(new BigDecimal("25.5000"), rs.getBigDecimal(4));
            }
        } finally {
            try (Connection connection = getConnection(); Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(tableName, stmt);
            }
        }
    }

    private void getCreateTableTemporalSQL(String tableName) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            String createTableSQL = "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (" +
                    "dateTimeColumn DATETIME, " +
                    "smallDateTimeColumn SMALLDATETIME, " +
                    "dateTime2Column DATETIME2, " +
                    "dateColumn DATE, " +
                    "timeColumn TIME, " +
                    "dateTimeOffsetColumn DATETIMEOFFSET, " +
                    "moneyColumn MONEY, " +
                    "smallMoneyColumn SMALLMONEY" + ")";

            stmt.execute(createTableSQL);

        }
    }

    class FallbackWatcherLogHandler extends Handler implements AutoCloseable {

        Logger stmtLogger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerStatement");
        boolean gotFallbackMessage = false;

        public FallbackWatcherLogHandler() {
            stmtLogger.addHandler(this);
        }

        @Override
        public void publish(LogRecord record) {
            if (record.getMessage().contains("Falling back to the original implementation for Batch Insert.")) {
                gotFallbackMessage = true;
            }
        }

        @Override
        public void flush() {}

        @Override
        public void close() throws SecurityException {
            stmtLogger.removeHandler(this);
        }
    }

    /**
     * Test string values using prepared statement using accented and unicode characters.
     * This test covers all combinations of useBulkCopyForBatchInsert and sendStringParametersAsUnicode.
     * 
     * @throws Exception
     */
    @Test
    public void testBulkInsertStringAllCombinations() throws Exception {
        boolean[] bulkCopyOptions = { true, false };
        boolean[] unicodeOptions = { true, false };
        for (boolean useBulkCopy : bulkCopyOptions) {
            for (boolean sendUnicode : unicodeOptions) {
                runBulkInsertStringTest(useBulkCopy, sendUnicode);
                runBulkInsertStringTestForceFallback(useBulkCopy, sendUnicode);
            }
        }
    }

    /**
     * Test batch insert using accented and unicode characters.
     */
    public void runBulkInsertStringTest(boolean useBulkCopy, boolean sendUnicode) throws Exception {
        String insertSQL = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableNameBulkString)
                + " (charCol, varcharCol, longvarcharCol, ncharCol1, nvarcharCol1, longnvarcharCol1, "
                + "ncharCol2, nvarcharCol2, longnvarcharCol2) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        String selectSQL = "SELECT charCol, varcharCol, longvarcharCol, ncharCol1, nvarcharCol1, "
                + "longnvarcharCol1, ncharCol2, nvarcharCol2, longnvarcharCol2 FROM "
                + AbstractSQLGenerator.escapeIdentifier(tableNameBulkString);

        try (Connection connection = PrepUtil.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=" + useBulkCopy + ";sendStringParametersAsUnicode="
                        + sendUnicode + ";");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(insertSQL);
                Statement stmt = (SQLServerStatement) connection.createStatement()) {

            getCreateTableWithStringData();

            String charValue = "Anaïs_Ni";
            String varcharValue = "café";
            String longVarcharValue = "Sørén Kierkégaard";
            String ncharValue1 = "José Müll";
            String nvarcharValue1 = "José Müller";
            String longNvarcharValue1 = "François Saldaña";
            String ncharValue2 = "Test1汉字😀";
            String nvarcharValue2 = "汉字";
            String longNvarcharValue2 = "日本語";

            pstmt.setString(1, charValue);
            pstmt.setString(2, varcharValue);
            pstmt.setString(3, longVarcharValue);
            pstmt.setString(4, ncharValue1);
            pstmt.setString(5, nvarcharValue1);
            pstmt.setString(6, longNvarcharValue1);
            pstmt.setNString(7, ncharValue2);
            pstmt.setNString(8, nvarcharValue2);
            pstmt.setNString(9, longNvarcharValue2);
            pstmt.addBatch();
            pstmt.executeBatch();

            // Validate inserted data
            try (ResultSet rs = stmt.executeQuery(selectSQL)) {
                assertTrue(rs.next(), "Expected at least one row in result set");
                assertEquals(charValue, rs.getString("charCol"));
                assertEquals(varcharValue, rs.getString("varcharCol"));
                assertEquals(longVarcharValue, rs.getString("longvarcharCol"));
                assertEquals(ncharValue1, rs.getString("ncharCol1"));
                assertEquals(nvarcharValue1, rs.getString("nvarcharCol1"));
                assertEquals(longNvarcharValue1, rs.getString("longnvarcharCol1"));
                assertEquals(ncharValue2, rs.getString("ncharCol2"));
                assertEquals(nvarcharValue2, rs.getString("nvarcharCol2"));
                assertEquals(longNvarcharValue2, rs.getString("longnvarcharCol2"));
                assertFalse(rs.next());
            }
        }
    }

    /**
     * Test batch insert using an unsupported statement (falls back to batch mode) with accented and Unicode characters.
     */
    public void runBulkInsertStringTestForceFallback(boolean useBulkCopy, boolean sendUnicode) throws Exception {
        String insertSQL = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableNameBulkString)
                + " (charCol, varcharCol, longvarcharCol, ncharCol1, nvarcharCol1, longnvarcharCol1, "
                + "ncharCol2, nvarcharCol2, longnvarcharCol2) VALUES ('Anaïs_Ni', ?, ?, ?, ?, ?, ?, ?, ?)";

        String selectSQL = "SELECT charCol, varcharCol, longvarcharCol, ncharCol1, nvarcharCol1, "
                + "longnvarcharCol1, ncharCol2, nvarcharCol2, longnvarcharCol2 FROM "
                + AbstractSQLGenerator.escapeIdentifier(tableNameBulkString);

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert="
                + useBulkCopy + ";sendStringParametersAsUnicode=" + sendUnicode + ";");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(insertSQL);
                Statement stmt = (SQLServerStatement) connection.createStatement()) {

            getCreateTableWithStringData();

            String charValue = "Anaïs_Ni";
            String varcharValue = "café";
            String longVarcharValue = "Sørén Kierkégaard";
            String ncharValue1 = "José Müll";
            String nvarcharValue1 = "José Müller";
            String longNvarcharValue1 = "François Saldaña";
            String ncharValue2 = "Test1汉字😀";
            String nvarcharValue2 = "汉字";
            String longNvarcharValue2 = "日本語";

            pstmt.setString(1, varcharValue);
            pstmt.setString(2, longVarcharValue);
            pstmt.setString(3, ncharValue1);
            pstmt.setString(4, nvarcharValue1);
            pstmt.setString(5, longNvarcharValue1);
            pstmt.setNString(6, ncharValue2);
            pstmt.setNString(7, nvarcharValue2);
            pstmt.setNString(8, longNvarcharValue2);
            pstmt.addBatch();

            try (FallbackWatcherLogHandler handler = new FallbackWatcherLogHandler()) {
                pstmt.executeBatch();
                if (useBulkCopy) {
                    assertTrue(handler.gotFallbackMessage);
                }
            }

            // Validate inserted data
            try (ResultSet rs = stmt.executeQuery(selectSQL)) {
                assertTrue(rs.next(), "Expected at least one row in result set");
                assertEquals(charValue, rs.getString("charCol"));
                assertEquals(varcharValue, rs.getString("varcharCol"));
                assertEquals(longVarcharValue, rs.getString("longvarcharCol"));
                assertEquals(ncharValue1, rs.getString("ncharCol1"));
                assertEquals(nvarcharValue1, rs.getString("nvarcharCol1"));
                assertEquals(longNvarcharValue1, rs.getString("longnvarcharCol1"));
                assertEquals(ncharValue2, rs.getString("ncharCol2"));
                assertEquals(nvarcharValue2, rs.getString("nvarcharCol2"));
                assertEquals(longNvarcharValue2, rs.getString("longnvarcharCol2"));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testIssue2669Repro() throws Exception {
        // Original repro
        testIssue2669Variation(false, true);
        // Variations
        testIssue2669Variation(false, false);
        testIssue2669Variation(true, true);
        testIssue2669Variation(true, false);

        // Test the same combos except falling back to batch insert
        testIssue2669VariationForceFallback(false, true);
        testIssue2669VariationForceFallback(false, false);
        testIssue2669VariationForceFallback(true, true);
        testIssue2669VariationForceFallback(true, false);
    }

    public void testIssue2669Variation(boolean sendStringsAsUnicode,
            boolean useBulkCopyForBatchInsert) throws Exception {
        String statesTable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("states"));
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(statesTable, stmt);
            String createTableSQL = "CREATE TABLE " + statesTable + " (" + "StateCode nvarchar(50) NOT NULL " + ")";

            stmt.execute(createTableSQL);
        }

        String insertSQL = "INSERT INTO " + statesTable + " (StateCode) VALUES (?)";

        String selectSQL = "SELECT StateCode FROM " + statesTable;

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert="
                + useBulkCopyForBatchInsert + ";sendStringParametersAsUnicode=" + sendStringsAsUnicode + ";");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(insertSQL);
                Statement stmt = (SQLServerStatement) connection.createStatement()) {

            pstmt.setString(1, "OH");
            pstmt.addBatch();
            pstmt.executeBatch();

            // Validate inserted data
            try (ResultSet rs = stmt.executeQuery(selectSQL)) {
                assertTrue(rs.next(), "Expected at least one row in result set");
                assertEquals("OH", rs.getString("StateCode"));
                assertFalse(rs.next());
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(statesTable, stmt);
            }
        }
    }

    public void testIssue2669VariationForceFallback(boolean sendStringsAsUnicode,
            boolean useBulkCopyForBatchInsert) throws SQLException {
        String statesTable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("states"));
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(statesTable, stmt);
            String createTableSQL = "CREATE TABLE " + statesTable
                    + " (StateCode nvarchar(50), StateCode2 nvarchar(50) NOT NULL " + ")";

            stmt.execute(createTableSQL);
        }

        // Use an INSERT that forces fall back to plain batch insert
        String insertSQL = "INSERT INTO " + statesTable + " (StateCode, StateCode2) VALUES ('NA', ?)";

        String selectSQL = "SELECT StateCode, StateCode2 FROM " + statesTable;

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert="
                + useBulkCopyForBatchInsert + ";sendStringParametersAsUnicode=" + sendStringsAsUnicode + ";");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(insertSQL);
                Statement stmt = (SQLServerStatement) connection.createStatement()) {

                pstmt.setString(1, "OH");
                pstmt.addBatch();

                try (FallbackWatcherLogHandler handler = new FallbackWatcherLogHandler()) {
                    pstmt.executeBatch();
                    if (useBulkCopyForBatchInsert) {
                        assertTrue(handler.gotFallbackMessage);
                    }
                }

            // Validate inserted data
            try (ResultSet rs = stmt.executeQuery(selectSQL)) {
                assertTrue(rs.next(), "Expected at least one row in result set");
                assertEquals("NA", rs.getString("StateCode"));
                assertEquals("OH", rs.getString("StateCode2"));
                assertFalse(rs.next());
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(statesTable, stmt);
            }
        }
    }

    private void getCreateTableWithStringData() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableNameBulkString), stmt);
            String createTableSQL = "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableNameBulkString) + " (" +
                    "charCol CHAR(8), " +
                    "varcharCol VARCHAR(50), " +
                    "longvarcharCol VARCHAR(MAX), " +
                    "ncharCol1 NCHAR(9), " +
                    "nvarcharCol1 NVARCHAR(50), " +
                    "longnvarcharCol1 NVARCHAR(MAX), " +
                    "ncharCol2 NCHAR(9), " +
                    "nvarcharCol2 NVARCHAR(50), " +
                    "longnvarcharCol2 NVARCHAR(MAX)" + ")";

            stmt.execute(createTableSQL);
        }
    }

    /**
     * Test bulk insert with InputStream data when useBulkCopyForBatchInsert is true.
     * Added support for InputStream data type with bulk copy batch insert.
     * 
     * @throws Exception
     */
    @Test
    public void testBulkCopyBatchInsertForInputStreamData() throws Exception {
        String tableName = RandomUtil.getIdentifier("BulkTableForInputStream");
        String insertSQL = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) +
                " (Id, Data) VALUES (?, ?)";
        String selectSQL = "SELECT Id, Data FROM " + AbstractSQLGenerator.escapeIdentifier(tableName);

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                Statement stmt = connection.createStatement();
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(insertSQL)) {

            getCreateTableInputStream(tableName);

            String data = "Sample string to be inserted as binary data.";
            byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
            InputStream inputStream = new ByteArrayInputStream(bytes);

            pstmt.setObject(1, 1);
            pstmt.setBinaryStream(2, inputStream);
            
            pstmt.addBatch();
            pstmt.executeBatch();

            // Validate inserted data
            try (ResultSet rs = stmt.executeQuery(selectSQL)) {
                assertTrue(rs.next());

                assertEquals(1, rs.getInt(1));
                byte[] retrievedBytes = rs.getBytes(2);
                String retrievedData = new String(retrievedBytes, StandardCharsets.UTF_8);
                assertEquals(data, retrievedData);
                
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            }
        }
    }

    private void getCreateTableInputStream(String tableName) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            String createTableSQL = "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (" +
                    "Id INT PRIMARY KEY, " +
                    "Data VARBINARY(MAX) NULL" + ")";

            stmt.execute(createTableSQL);
        }
    }

    /**
     * Test for GitHub Issue#2825 - PreparedStatement.executeBatch() fails for insert statements 
     * with SQL functions when useBulkCopyForBatchInsert is true.
     * This test verifies that SQL functions cause fallback from bulk copy to regular batch execution and succeed.
     */
    @Test
    public void testBulkCopyWithSQLFunctionFallback() throws Exception {
        String tableName = RandomUtil.getIdentifier("Table_BulkCopy_API_SQLFunction");

        // Insert with sql function as last parameter - this should trigger fallback from bulk copy to regular batch
        String insertSQL = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                + " (Id, Data) VALUES (?, len(?))";

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(insertSQL);
                Statement stmt = (SQLServerStatement) connection.createStatement()) {

            createTable_SQLFunction(tableName);

            pstmt.setObject(1, 1);
            pstmt.setObject(2, "MySecretData123");
            pstmt.addBatch();

            // Execute with fallback monitoring using existing handler
            try (FallbackWatcherLogHandler handler = new FallbackWatcherLogHandler()) {
                pstmt.executeBatch();

                // Verify fallback occurred due to SQL function
                assertTrue(handler.gotFallbackMessage);
            }

            // Verify the data was inserted correctly
            try (ResultSet rs = stmt.executeQuery("SELECT Id, Data FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next(), "Expected at least one row");
                assertEquals(1, rs.getInt("Id"));
                assertEquals(15, rs.getInt("Data"));
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            }
        }
    }

    /**
     * Test for GitHub Issue#2825 - PreparedStatement.executeBatch() fails for insert statements 
     * with SQL functions when useBulkCopyForBatchInsert is true.
     * This test verifies that SQL functions cause fallback from bulk copy to regular batch execution and succeed.
     */
    @Test
    public void testBulkCopyWithSQLFunctionFallback_FirstParameter() throws Exception {
        String tableName = RandomUtil.getIdentifier("Table_BulkCopy_API_SQLFunction");

        // Insert with sql function as first parameter - this should trigger fallback from bulk copy to regular batch
        String insertSQL = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                + " (Id, Data) VALUES (len(?), ?)";

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(insertSQL);
                Statement stmt = (SQLServerStatement) connection.createStatement()) {

            createTable_SQLFunction(tableName);

            pstmt.setObject(1, "MySecretData123");
            pstmt.setObject(2, 1);
            pstmt.addBatch();

            // Execute with fallback monitoring using existing handler
            try (FallbackWatcherLogHandler handler = new FallbackWatcherLogHandler()) {
                pstmt.executeBatch();

                // Verify fallback occurred due to SQL function
                assertTrue(handler.gotFallbackMessage);
            }

            // Verify the data was inserted correctly
            try (ResultSet rs = stmt.executeQuery("SELECT Id, Data FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next(), "Expected at least one row");
                assertEquals(15, rs.getInt("Id"));
                assertEquals(1, rs.getInt("Data"));
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            }
        }
    }

    private void createTable_SQLFunction(String tableName) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            String createTableSQL = "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName) +
                    " (Id INT PRIMARY KEY, Data INT)";
            stmt.execute(createTableSQL);
        }
    }

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @BeforeEach
    public void testSetup() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            String sql1 = "create table " + AbstractSQLGenerator.escapeIdentifier(tableName) + " " + "(" + "c1 bigint, "
                    + "c2 binary(5), " + "c3 bit, " + "c4 char, " + "c5 date, " + "c6 datetime, " + "c7 datetime2, "
                    + "c8 datetimeoffset, " + "c9 decimal, " + "c10 float, " + "c11 int, " + "c12 money, "
                    + "c13 nchar, " + "c14 numeric, " + "c15 nvarchar(20), " + "c16 real, " + "c17 smalldatetime, "
                    + "c18 smallint, " + "c19 smallmoney, " + "c20 time, " + "c21 tinyint, " + "c22 varbinary(5), "
                    + "c23 varchar(20), " + "c24 UNIQUEIDENTIFIER" + ")";

            stmt.execute(sql1);
        }
    }

    @AfterAll
    public static void terminateVariation() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableNameBulk), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(unsupportedTableName), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(squareBracketTableName), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(doubleQuoteTableName), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(schemaTableName), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableNameBulkString), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableNameBulkComputedCols), stmt);
        }
    }
}
