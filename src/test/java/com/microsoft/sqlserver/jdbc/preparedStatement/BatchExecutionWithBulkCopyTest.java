package com.microsoft.sqlserver.jdbc.preparedStatement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
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
import java.util.ArrayList;
import java.util.Arrays;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.opentest4j.TestAbortedException;

import com.microsoft.sqlserver.jdbc.Geography;
import com.microsoft.sqlserver.jdbc.Geometry;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;


@RunWith(JUnitPlatform.class)
@Tag("AzureDWTest")
public class BatchExecutionWithBulkCopyTest extends AbstractTest {

    static String tableName = RandomUtil.getIdentifier("BulkCopyParseTest");
    static String tableNameBulk = RandomUtil.getIdentifier("BulkCopyParseTest");
    static String unsupportedTableName = RandomUtil.getIdentifier("BulkCopyUnsupportedTable'");
    static String squareBracketTableName = RandomUtil.getIdentifier("BulkCopy]]]]test'");
    static String doubleQuoteTableName = RandomUtil.getIdentifier("\"BulkCopy\"\"\"\"test\"");
    static String schemaTableName = "\"dbo\"         . /*some comment */     " + squareBracketTableName;

    @Test
    public void testIsInsert() throws Exception {
        try (Connection connection = DriverManager.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                Statement stmt = (SQLServerStatement) connection.createStatement()) {
            String valid1 = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableNameBulk) + " values (1, 2)";
            String valid2 = " INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableNameBulk) + " values (1, 2)";
            String valid3 = "/* asdf */ INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableNameBulk)
                    + " values (1, 2)";
            String invalid = "Select * from " + AbstractSQLGenerator.escapeIdentifier(tableNameBulk);

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
        // TODO: VSO-5432
        String valid = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values "
                + "("
                + "?, "+ "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, "
                + "?, "+ "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, "
                + "?, "+ "?, " + "?"
                + ")";

        try (Connection connection = DriverManager.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(valid);
                Statement stmt = (SQLServerStatement) connection.createStatement();) {
            Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
            f1.setAccessible(true);
            f1.set(connection, true);

            Long timeMilis = 114550L;
            byte[] binaryData = "11234".getBytes();
            Timestamp testTimestamp = new Timestamp(timeMilis);
            BigDecimal testBigDecimal = new BigDecimal(123.456);

            pstmt.setLong(1, 123); // bigint
            pstmt.setBytes(2, binaryData); // binary(5)
            pstmt.setBoolean(3, true); // bit
            pstmt.setString(4, "s"); // char
            pstmt.setDate(5, new Date(timeMilis));  // date
            pstmt.setDateTime(6, testTimestamp);// datetime
            pstmt.setDateTime(7, testTimestamp); // datetime2
            pstmt.setDateTimeOffset(8, microsoft.sql.DateTimeOffset.valueOf(testTimestamp, 0)); // datetimeoffset
            pstmt.setBigDecimal(9, testBigDecimal); // decimal
            pstmt.setDouble(10, 123.45); // float
            pstmt.setInt(11, 1); // int
            pstmt.setMoney(12, testBigDecimal); // money
            pstmt.setString(13, "s"); // nchar
            pstmt.setBigDecimal(14, testBigDecimal); // numeric
            pstmt.setString(15, "somenvarchar"); // nvarchar(20)
            pstmt.setFloat(16, 1); // real
            pstmt.setSmallDateTime(17, testTimestamp); // smalldatetime
            pstmt.setShort(18, (short) 1); // smallint
            pstmt.setSmallMoney(19, testBigDecimal); // smallmoney
            pstmt.setTime(20, new Time(114550L)); // time
            pstmt.setShort(21, (short) 1); // tinyint
            pstmt.setBytes(22, binaryData); // varbinary(5)
            pstmt.setString(23, "somevarchar"); // varchar(20)

            pstmt.addBatch();
            pstmt.executeBatch();

            try (ResultSet rs = stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {

                Object[] expected = new Object[23];

                expected[0] = 123;
                expected[1] = binaryData;
                expected[2] = true;
                expected[3] = "s";
                expected[4] = new Date(timeMilis);
                expected[5] = testTimestamp;
                expected[6] = testTimestamp;
                expected[7] = microsoft.sql.DateTimeOffset.valueOf(testTimestamp, 0);
                expected[8] = testBigDecimal.intValue();
                expected[9] = 123.45;
                expected[10] = 1;
                expected[11] = "123.4560";
                expected[12] = "s";
                expected[13] = testBigDecimal.intValue();
                expected[14] = "somenvarchar";
                expected[15] = "1.0";
                expected[16] = "1969-12-31 16:02:00.0";
                expected[17] = (short) 1;
                expected[18] = "123.4560";
                expected[19] = new Time(114550L);
                expected[20] = (short) 1;
                expected[21] = binaryData;
                expected[22] = "somevarchar";
                
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
        // TODO: VSO-5432
        String valid = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (c1, c3, c5, c8) values "
                + "(" + "?, " + "?, " + "?, " + "? " + ")";

        try (Connection connection = DriverManager.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(valid);
                Statement stmt = (SQLServerStatement) connection.createStatement();) {
            Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
            f1.setAccessible(true);
            f1.set(connection, true);

            Long timeMilis = 114550L;
            Timestamp testTimestamp = new Timestamp(timeMilis);

            pstmt.setLong(1, 123); // bigint
            pstmt.setBoolean(2, true); // bit
            pstmt.setDate(3, new Date(timeMilis));  // date
            pstmt.setDateTimeOffset(4, microsoft.sql.DateTimeOffset.valueOf(testTimestamp, 0)); // datetimeoffset
            pstmt.addBatch();

            pstmt.executeBatch();

            try (ResultSet rs = stmt
                    .executeQuery("SELECT c1, c3, c5, c8 FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {

                Object[] expected = new Object[4];

                expected[0] = 123;
                expected[1] = true;
                expected[2] = new Date(timeMilis);
                expected[3] = microsoft.sql.DateTimeOffset.valueOf(testTimestamp, 0);
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
        // TODO: VSO-5432
        String valid = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                + " (c1, c2, c3, c4, c5, c6, c7) values " + "(" + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "? "
                + ")";

        try (Connection connection = DriverManager.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(valid);
                Statement stmt = (SQLServerStatement) connection.createStatement();) {
            Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
            f1.setAccessible(true);
            f1.set(connection, true);

            pstmt.setLong(1, 123); // bigint
            pstmt.setBytes(2, null); // binary(5)
            pstmt.setBoolean(3, true); // bit
            pstmt.setString(4, " "); // char
            pstmt.setDate(5, null);  // date
            pstmt.setDateTime(6, null);// datetime
            pstmt.setDateTime(7, null); // datetime2
            pstmt.addBatch();

            pstmt.executeBatch();

            try (ResultSet rs = stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {

                Object[] expected = new Object[7];

                expected[0] = 123L;
                expected[1] = null;
                expected[2] = true;
                expected[3] = " ";
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

    // Non-parameterized queries are not supported anymore.
    // @Test
    public void testAllFilledColumns() throws Exception {
        String valid = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values " + "(" + "1234, "
                + "false, " + "a, " + "null, " + "null, " + "123.45, " + "b, " + "varc, " + "sadf, " + ")";

        try (Connection connection = DriverManager.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(valid);
                Statement stmt = (SQLServerStatement) connection.createStatement();) {
            Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
            f1.setAccessible(true);
            f1.set(connection, true);

            pstmt.addBatch();

            pstmt.executeBatch();

            try (ResultSet rs = stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {

                Object[] expected = new Object[9];

                expected[0] = 1234;
                expected[1] = false;
                expected[2] = "a";
                expected[3] = null;
                expected[4] = null;
                expected[5] = 123.45;
                expected[6] = "b";
                expected[7] = "varc";
                expected[8] = "sadf";

                rs.next();
                for (int i = 0; i < expected.length; i++) {
                    assertEquals(expected[i], rs.getObject(i + 1));
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
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(squareBracketTableName))) {
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
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(doubleQuoteTableName))) {
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
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(schemaTableName))) {
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
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(squareBracketTableName))) {
                rs.next();

                assertEquals(1, rs.getObject(1));
            }
        }
    }

    @Test
    public void testAllColumnsLargeBatch() throws Exception {
        // TODO: VSO-5432
        String valid = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values "
                + "("
                + "?, "+ "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, "
                + "?, "+ "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, " + "?, "
                + "?, "+ "?, " + "?"
                + ")";

        try (Connection connection = DriverManager.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(valid);
                Statement stmt = (SQLServerStatement) connection.createStatement();) {
            Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
            f1.setAccessible(true);
            f1.set(connection, true);

            Long timeMilis = 114550L;
            byte[] binaryData = "11234".getBytes();
            Timestamp testTimestamp = new Timestamp(timeMilis);
            BigDecimal testBigDecimal = new BigDecimal(123.456);

            pstmt.setLong(1, 123); // bigint
            pstmt.setBytes(2, binaryData); // binary(5)
            pstmt.setBoolean(3, true); // bit
            pstmt.setString(4, "s"); // char
            pstmt.setDate(5, new Date(timeMilis));  // date
            pstmt.setDateTime(6, testTimestamp);// datetime
            pstmt.setDateTime(7, testTimestamp); // datetime2
            pstmt.setDateTimeOffset(8, microsoft.sql.DateTimeOffset.valueOf(testTimestamp, 0)); // datetimeoffset
            pstmt.setBigDecimal(9, testBigDecimal); // decimal
            pstmt.setDouble(10, 123.45); // float
            pstmt.setInt(11, 1); // int
            pstmt.setMoney(12, testBigDecimal); // money
            pstmt.setString(13, "s"); // nchar
            pstmt.setBigDecimal(14, testBigDecimal); // numeric
            pstmt.setString(15, "somenvarchar"); // nvarchar(20)
            pstmt.setFloat(16, 1); // real
            pstmt.setSmallDateTime(17, testTimestamp); // smalldatetime
            pstmt.setShort(18, (short) 1); // smallint
            pstmt.setSmallMoney(19, testBigDecimal); // smallmoney
            pstmt.setTime(20, new Time(114550L)); // time
            pstmt.setShort(21, (short) 1); // tinyint
            pstmt.setBytes(22, binaryData); // varbinary(5)
            pstmt.setString(23, "somevarchar"); // varchar(20)
            
            pstmt.addBatch();
            pstmt.executeLargeBatch();

            try (ResultSet rs = stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {

                Object[] expected = new Object[23];

                expected[0] = 123;
                expected[1] = binaryData;
                expected[2] = true;
                expected[3] = "s";
                expected[4] = new Date(timeMilis);
                expected[5] = testTimestamp;
                expected[6] = testTimestamp;
                expected[7] = microsoft.sql.DateTimeOffset.valueOf(testTimestamp, 0);
                expected[8] = testBigDecimal.intValue();
                expected[9] = 123.45;
                expected[10] = 1;
                expected[11] = "123.4560";
                expected[12] = "s";
                expected[13] = testBigDecimal.intValue();
                expected[14] = "somenvarchar";
                expected[15] = "1.0";
                expected[16] = "1969-12-31 16:02:00.0";
                expected[17] = (short) 1;
                expected[18] = "123.4560";
                expected[19] = new Time(114550L);
                expected[20] = (short) 1;
                expected[21] = binaryData;
                expected[22] = "somevarchar";

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
            throw new Exception("Test did not throw an exception when it was expected.");
        } catch (BatchUpdateException e) {
            assertEquals("Column name or number of supplied values does not match table definition.", e.getMessage());
        }

        invalid = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName)
                + " (c1, c2, c3) values (?, ?,? ,?) ";

        String expected = "";
        
        try (Connection connection = DriverManager.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(invalid);
                Statement stmt = (SQLServerStatement) connection.createStatement();) {
            
            if (TestUtils.isSqlAzureDW(connection)) {
                expected = "Column name or number of supplied values does not match table definition.";
            } else {
                expected = "There are fewer columns in the INSERT statement than values specified in the VALUES clause. The number of values in the VALUES clause must match the number of columns specified in the INSERT statement.";
            }
            
            Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
            f1.setAccessible(true);
            f1.set(connection, true);

            pstmt.setInt(1, 1);
            pstmt.setInt(2, 1);
            pstmt.setInt(3, 1);
            pstmt.setInt(4, 1);
            pstmt.addBatch();

            pstmt.executeBatch();
            throw new Exception("Test did not throw an exception when it was expected.");
        } catch (BatchUpdateException e) {
            assertEquals(expected, e.getMessage());
        }
    }

    @Test
    public void testNonParameterizedQuery() throws Exception {
        String invalid = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName)
                + " values ((SELECT * from table where c1=?), ?,? ,?) ";

        String expected = "";
        
        try (Connection connection = DriverManager.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(invalid);
                Statement stmt = (SQLServerStatement) connection.createStatement();) {
            
            if (TestUtils.isSqlAzureDW(connection)) {
                expected = "Parse error at line: 1, column: 106: Incorrect syntax near 'table'.";
            } else {
                expected = "Incorrect syntax near the keyword 'table'.";
            }
            
            Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
            f1.setAccessible(true);
            f1.set(connection, true);

            pstmt.setInt(1, 1);
            pstmt.setInt(2, 1);
            pstmt.setInt(3, 1);
            pstmt.setInt(4, 1);
            pstmt.addBatch();

            pstmt.executeBatch();
            throw new Exception("Test did not throw an exception when it was expected.");
        } catch (BatchUpdateException e) {
            assertEquals(expected, e.getMessage());
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
            throw new Exception("Test did not throw an exception when it was expected.");
        } catch (BatchUpdateException e) {
            assertEquals("Column name or number of supplied values does not match table definition.", e.getMessage());
        }
    }

    @Test
    public void testNonSupportedColumns() throws Exception {
        assumeFalse(TestUtils.isSqlAzureDW(connection), "Geometry/Geography is not supported for DW.");
        String valid = "insert into " + AbstractSQLGenerator.escapeIdentifier(unsupportedTableName)
                + " values (?, ?, ?, ?)";

        try (Connection connection = DriverManager.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(valid);
                Statement stmt = (SQLServerStatement) connection.createStatement();) {
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
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(unsupportedTableName))) {
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
                        + "c1 bigint, "
                        + "c2 binary(5), "
                        + "c3 bit, "
                        + "c4 char, "
                        + "c5 date, "
                        + "c6 datetime, "
                        + "c7 datetime2, "
                        + "c8 datetimeoffset, "
                        + "c9 decimal, "
                        + "c10 float, "
                        + "c11 int, "
                        + "c12 money, "
                        + "c13 nchar, "
                        + "c14 numeric, "
                        + "c15 nvarchar(20), "
                        + "c16 real, "
                        + "c17 smalldatetime, "
                        + "c18 smallint, "
                        + "c19 smallmoney, "
                        + "c20 time, "
                        + "c21 tinyint, "
                        + "c22 varbinary(5), "
                        + "c23 varchar(20) "
                        + ")";

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
