package com.microsoft.sqlserver.jdbc.preparedStatement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.opentest4j.TestAbortedException;

import com.microsoft.sqlserver.jdbc.Geography;
import com.microsoft.sqlserver.jdbc.Geometry;
import com.microsoft.sqlserver.jdbc.RandomData;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;

import microsoft.sql.DateTimeOffset;


@RunWith(JUnitPlatform.class)
@Tag("AzureDWTest")
public class BatchExecutionWithBulkCopyTest extends AbstractTest {

    static String tableName = RandomUtil.getIdentifier("BulkCopyParseTest");
    static String tableNameBulk = RandomUtil.getIdentifier("BulkCopyParseTest");
    static String unsupportedTableName = RandomUtil.getIdentifier("BulkCopyUnsupportedTable'");
    static String squareBracketTableName = RandomUtil.getIdentifier("BulkCopy]]]]test'");
    static String doubleQuoteTableName = RandomUtil.getIdentifier("\"BulkCopy\"\"\"\"test\"");
    static String schemaTableName = "\"dbo\"         . /*some comment */     " + squareBracketTableName;

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

        // Temporal datatypes
        Date randomDate = Date.valueOf(LocalDateTime.now().toLocalDate());
        Time randomTime = new Time(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
        Timestamp randomTimestamp = new Timestamp(
                LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());

        // Datetime can only end in 0,3,7 and will be rounded to those numbers on the server. Manually set nanos
        Timestamp roundedDatetime = randomTimestamp;
        roundedDatetime.setNanos(0);
        // Smalldatetime does not have seconds. Manually set nanos and seconds.
        Timestamp smallTimestamp = randomTimestamp;
        smallTimestamp.setNanos(0);
        smallTimestamp.setSeconds(0);

        Object[] expected = new Object[23];
        expected[0] = ThreadLocalRandom.current().nextLong();
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
        expected[13] = BigDecimal.valueOf(ThreadLocalRandom.current().nextInt());
        expected[14] = randomString;
        expected[15] = randomFloat;
        expected[16] = smallTimestamp;
        expected[17] = randomShort;
        expected[18] = randomSmallMoney;
        expected[19] = randomTime;
        expected[20] = randomShort;
        expected[21] = randomBinary;
        expected[22] = randomString;

        return expected;
    }

    @Test
    public void testIsInsert() throws Exception {
        try (Connection connection = DriverManager.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
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
        try (Connection connection = DriverManager.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
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
        try (Connection connection = DriverManager.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
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
        try (Connection connection = DriverManager.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
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
        try (Connection connection = DriverManager.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
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

            assertEquals((String) method.invoke(pstmt, false, false, false, false), "\"Bulk\"\"\"\"Table\"");

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
                + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?" + ")";

        try (Connection connection = DriverManager.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
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

        try (Connection connection = DriverManager.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(valid);
                Statement stmt = (SQLServerStatement) connection.createStatement();) {
            Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
            f1.setAccessible(true);
            f1.set(connection, true);

            Timestamp randomTimestamp = new Timestamp(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
            Date randomDate = Date.valueOf(LocalDateTime.now().toLocalDate());
            long randomLong = ThreadLocalRandom.current().nextLong();
            
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
    public void testNullOrEmptyColumns() throws Exception {
        String valid = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName)
                + " (c1, c2, c3, c4, c5, c6, c7) values " + "(" + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "? "
                + ")";

        try (Connection connection = DriverManager.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(valid);
                Statement stmt = (SQLServerStatement) connection.createStatement();) {
            Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
            f1.setAccessible(true);
            f1.set(connection, true);

            long randomLong = ThreadLocalRandom.current().nextLong();
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

        try (Connection connection = DriverManager.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
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

        try (Connection connection = DriverManager.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
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

        try (Connection connection = DriverManager.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
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

        try (Connection connection = DriverManager.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
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
                + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?" + ")";

        try (Connection connection = DriverManager.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
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

        try (Connection connection = DriverManager.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
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
            throw new Exception(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (BatchUpdateException e) {
            assertEquals(TestResource.getResource("R_incorrectColumnNum"), e.getMessage());
        }

        invalid = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName)
                + " (c1, c2, c3) values (?, ?,? ,?) ";

        try (Connection connection = DriverManager.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
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
            throw new Exception(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (BatchUpdateException e) {
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

        try (Connection connection = DriverManager.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
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
            throw new Exception(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (BatchUpdateException e) {
            if (isSqlAzureDW()) {
                assertEquals(TestResource.getResource("R_incorrectSyntaxTableDW"), e.getMessage());
            } else {
                assertEquals(TestResource.getResource("R_incorrectSyntaxTable"), e.getMessage());
            }
        }

        invalid = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values ('?', ?,? ,?) ";

        try (Connection connection = DriverManager.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
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
            throw new Exception(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (BatchUpdateException e) {
            assertEquals(TestResource.getResource("R_incorrectColumnNum"), e.getMessage());
        }
    }

    @Test
    public void testNonSupportedColumns() throws Exception {
        assumeFalse(isSqlAzureDW(), TestResource.getResource("R_spatialDWNotSupported"));
        String valid = "insert into " + AbstractSQLGenerator.escapeIdentifier(unsupportedTableName)
                + " values (?, ?, ?, ?)";

        try (Connection connection = DriverManager.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
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

    @BeforeEach
    public void testSetup() throws TestAbortedException, Exception {
        try (Connection connection = DriverManager
                .getConnection(connectionString + ";useBulkCopyForBatchInsert=true;")) {
            try (Statement stmt = (SQLServerStatement) connection.createStatement()) {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
                String sql1 = "create table " + AbstractSQLGenerator.escapeIdentifier(tableName) + " " + "("
                        + "c1 bigint, " + "c2 binary(5), " + "c3 bit, " + "c4 char, " + "c5 date, " + "c6 datetime, "
                        + "c7 datetime2, " + "c8 datetimeoffset, " + "c9 decimal, " + "c10 float, " + "c11 int, "
                        + "c12 money, " + "c13 nchar, " + "c14 numeric, " + "c15 nvarchar(20), " + "c16 real, "
                        + "c17 smalldatetime, " + "c18 smallint, " + "c19 smallmoney, " + "c20 time, " + "c21 tinyint, "
                        + "c22 varbinary(5), " + "c23 varchar(20) " + ")";

                stmt.execute(sql1);
            }
        }
    }

    @AfterAll
    public static void terminateVariation() throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionString)) {
            try (Statement stmt = (SQLServerStatement) connection.createStatement()) {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableNameBulk), stmt);
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(unsupportedTableName), stmt);
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(squareBracketTableName), stmt);
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(doubleQuoteTableName), stmt);
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(schemaTableName), stmt);
            }
        }
    }
}
