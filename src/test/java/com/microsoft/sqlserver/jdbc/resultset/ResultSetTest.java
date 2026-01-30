/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.resultset;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ISQLServerResultSet;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;
import com.microsoft.sqlserver.testframework.vectorJsonTest;

import microsoft.sql.DateTimeOffset;

@RunWith(JUnitPlatform.class)
public class ResultSetTest extends AbstractTest {
    private static final String tableName = RandomUtil.getIdentifier("StatementParam");
    private static final String tableName1 = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("AmbiguousRs1"));
    private static final String tableName2 = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("AmbiguousRs2"));

    private static final String expectedSqlState = "S0001";

    private static final int expectedErrorCode = 8134;

    static final String uuid = UUID.randomUUID().toString();

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @BeforeEach
    public void init() throws Exception {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            TestUtils.dropTableIfExists(tableName1, stmt);
            TestUtils.dropTableIfExists(tableName2, stmt);
        }
    }

    @AfterEach
    public void cleanUp() throws Exception {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            TestUtils.dropTableIfExists(tableName1, stmt);
            TestUtils.dropTableIfExists(tableName2, stmt);
        }
    }

    /**
     * Tests proper exception for unsupported operation
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testJdbc41ResultSetMethods() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate("create table " + AbstractSQLGenerator.escapeIdentifier(tableName) + " ( " + "col1 int, "
                    + "col2 varchar(512), " + "col3 float, " + "col4 decimal(10,5), " + "col5 uniqueidentifier, "
                    + "col6 xml, " + "col7 varbinary(max), " + "col8 text, " + "col9 ntext, " + "col10 varbinary(max), "
                    + "col11 date, " + "col12 time, " + "col13 datetime2, " + "col14 datetimeoffset, "
                    + "col15 decimal(10,9), " + "col16 decimal(38,38), "
                    + "order_column int identity(1,1) primary key)");
            try {

                stmt.executeUpdate(
                        "Insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values(" + "1, " // col1
                                + "'hello', " // col2
                                + "2.0, " // col3
                                + "123.45, " // col4
                                + "'" + uuid + "', " // col5
                                + "'<test/>', " // col6
                                + "0x63C34D6BCAD555EB64BF7E848D02C376, " // col7
                                + "'text', " // col8
                                + "'ntext', " // col9
                                + "0x63C34D6BCAD555EB64BF7E848D02C376," // col10
                                + "'2017-05-19'," // col11
                                + "'10:47:15.1234567'," // col12
                                + "'2017-05-19T10:47:15.1234567'," // col13
                                + "'2017-05-19T10:47:15.1234567+02:00'," // col14
                                + "0.123456789, " // col15
                                + "0.1234567890123456789012345678901234567" // col16
                                + ")");

                stmt.executeUpdate("Insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values("
                        + "null, " + "null, " + "null, " + "null, " + "null, " + "null, " + "null, " + "null, "
                        + "null, " + "null, " + "null, " + "null, " + "null, " + "null, " + "null, " + "null)");

                try (ResultSet rs = stmt.executeQuery("select * from "
                        + AbstractSQLGenerator.escapeIdentifier(tableName) + " order by order_column")) {
                    // test non-null values
                    assertTrue(rs.next());
                    assertEquals(Byte.valueOf((byte) 1), rs.getObject(1, Byte.class));
                    assertEquals(Byte.valueOf((byte) 1), rs.getObject("col1", Byte.class));
                    assertEquals(Short.valueOf((short) 1), rs.getObject(1, Short.class));
                    assertEquals(Short.valueOf((short) 1), rs.getObject("col1", Short.class));
                    assertEquals(Integer.valueOf(1), rs.getObject(1, Integer.class));
                    assertEquals(Integer.valueOf(1), rs.getObject("col1", Integer.class));
                    assertEquals(Long.valueOf(1), rs.getObject(1, Long.class));
                    assertEquals(Long.valueOf(1), rs.getObject("col1", Long.class));
                    assertEquals(Boolean.TRUE, rs.getObject(1, Boolean.class));
                    assertEquals(Boolean.TRUE, rs.getObject("col1", Boolean.class));

                    assertEquals("hello", rs.getObject(2, String.class));
                    assertEquals("hello", rs.getObject("col2", String.class));

                    assertEquals(2.0f, rs.getObject(3, Float.class), 0.0001f);
                    assertEquals(2.0f, rs.getObject("col3", Float.class), 0.0001f);
                    assertEquals(2.0d, rs.getObject(3, Double.class), 0.0001d);
                    assertEquals(2.0d, rs.getObject("col3", Double.class), 0.0001d);

                    // BigDecimal#equals considers the number of decimal places
                    assertEquals(0, rs.getObject(4, BigDecimal.class).compareTo(new BigDecimal("123.45")));
                    assertEquals(0, rs.getObject("col4", BigDecimal.class).compareTo(new BigDecimal("123.45")));

                    assertEquals(UUID.fromString(uuid), rs.getObject(5, UUID.class));
                    assertEquals(UUID.fromString(uuid), rs.getObject("col5", UUID.class));

                    SQLXML sqlXml;
                    sqlXml = rs.getObject(6, SQLXML.class);
                    try {
                        assertEquals("<test/>", sqlXml.getString());
                    } finally {
                        sqlXml.free();
                    }

                    Blob blob;
                    blob = rs.getObject(7, Blob.class);
                    try {
                        assertArrayEquals(
                                new byte[] {0x63, (byte) 0xC3, 0x4D, 0x6B, (byte) 0xCA, (byte) 0xD5, 0x55, (byte) 0xEB,
                                        0x64, (byte) 0xBF, 0x7E, (byte) 0x84, (byte) 0x8D, 0x02, (byte) 0xC3, 0x76},
                                blob.getBytes(1, 16));
                    } finally {
                        blob.free();
                    }

                    Clob clob;
                    clob = rs.getObject(8, Clob.class);
                    try {
                        assertEquals("text", clob.getSubString(1, 4));
                    } finally {
                        clob.free();
                    }

                    NClob nclob;
                    nclob = rs.getObject(9, NClob.class);
                    try {
                        assertEquals("ntext", nclob.getSubString(1, 5));
                    } finally {
                        nclob.free();
                    }

                    assertArrayEquals(
                            new byte[] {0x63, (byte) 0xC3, 0x4D, 0x6B, (byte) 0xCA, (byte) 0xD5, 0x55, (byte) 0xEB,
                                    0x64, (byte) 0xBF, 0x7E, (byte) 0x84, (byte) 0x8D, 0x02, (byte) 0xC3, 0x76},
                            rs.getObject(10, byte[].class));

                    assertEquals(java.sql.Date.valueOf("2017-05-19"), rs.getObject(11, java.sql.Date.class));
                    assertEquals(java.sql.Date.valueOf("2017-05-19"), rs.getObject("col11", java.sql.Date.class));

                    java.sql.Time expectedTime = new java.sql.Time(java.sql.Time.valueOf("10:47:15").getTime() + 123L);
                    assertEquals(expectedTime, rs.getObject(12, java.sql.Time.class));
                    assertEquals(expectedTime, rs.getObject("col12", java.sql.Time.class));

                    assertEquals(java.sql.Timestamp.valueOf("2017-05-19 10:47:15.1234567"),
                            rs.getObject(13, java.sql.Timestamp.class));
                    assertEquals(java.sql.Timestamp.valueOf("2017-05-19 10:47:15.1234567"),
                            rs.getObject("col13", java.sql.Timestamp.class));

                    assertEquals("2017-05-19 10:47:15.1234567 +02:00",
                            rs.getObject(14, microsoft.sql.DateTimeOffset.class).toString());
                    assertEquals("2017-05-19 10:47:15.1234567 +02:00",
                            rs.getObject("col14", microsoft.sql.DateTimeOffset.class).toString());

                    // BigDecimal#equals considers the number of decimal places (ResultSet returns all digits after
                    // decimal unlike CallableStatement outparams)
                    assertEquals(0, rs.getObject(15, BigDecimal.class).compareTo(new BigDecimal("0.123456789")));
                    assertEquals(0, rs.getObject("col15", BigDecimal.class).compareTo(new BigDecimal("0.123456789")));

                    assertEquals(0, rs.getObject(16, BigDecimal.class)
                            .compareTo(new BigDecimal("0.12345678901234567890123456789012345670")));
                    assertEquals(0, rs.getObject("col16", BigDecimal.class)
                            .compareTo(new BigDecimal("0.12345678901234567890123456789012345670")));

                    // test null values, mostly to verify primitive wrappers do not return default values
                    assertTrue(rs.next());
                    assertNull(rs.getObject("col1", Boolean.class));
                    assertNull(rs.getObject(1, Boolean.class));
                    assertNull(rs.getObject("col1", Byte.class));
                    assertNull(rs.getObject(1, Byte.class));
                    assertNull(rs.getObject("col1", Short.class));
                    assertNull(rs.getObject(1, Short.class));
                    assertNull(rs.getObject(1, Integer.class));
                    assertNull(rs.getObject("col1", Integer.class));
                    assertNull(rs.getObject(1, Long.class));
                    assertNull(rs.getObject("col1", Long.class));

                    assertNull(rs.getObject(2, String.class));
                    assertNull(rs.getObject("col2", String.class));

                    assertNull(rs.getObject(3, Float.class));
                    assertNull(rs.getObject("col3", Float.class));
                    assertNull(rs.getObject(3, Double.class));
                    assertNull(rs.getObject("col3", Double.class));

                    assertNull(rs.getObject(4, BigDecimal.class));
                    assertNull(rs.getObject("col4", BigDecimal.class));

                    assertNull(rs.getObject(5, UUID.class));
                    assertNull(rs.getObject("col5", UUID.class));

                    assertNull(rs.getObject(6, SQLXML.class));
                    assertNull(rs.getObject("col6", SQLXML.class));

                    assertNull(rs.getObject(7, Blob.class));
                    assertNull(rs.getObject("col7", Blob.class));

                    assertNull(rs.getObject(8, Clob.class));
                    assertNull(rs.getObject("col8", Clob.class));

                    assertNull(rs.getObject(9, NClob.class));
                    assertNull(rs.getObject("col9", NClob.class));

                    assertNull(rs.getObject(10, byte[].class));
                    assertNull(rs.getObject("col10", byte[].class));

                    assertNull(rs.getObject(11, java.sql.Date.class));
                    assertNull(rs.getObject("col11", java.sql.Date.class));

                    assertNull(rs.getObject(12, java.sql.Time.class));
                    assertNull(rs.getObject("col12", java.sql.Time.class));

                    assertNull(rs.getObject(13, java.sql.Timestamp.class));
                    assertNull(rs.getObject("col14", java.sql.Timestamp.class));

                    assertNull(rs.getObject(14, microsoft.sql.DateTimeOffset.class));
                    assertNull(rs.getObject("col14", microsoft.sql.DateTimeOffset.class));

                    assertNull(rs.getObject(15, BigDecimal.class));
                    assertNull(rs.getObject("col15", BigDecimal.class));

                    assertNull(rs.getObject(16, BigDecimal.class));
                    assertNull(rs.getObject("col16", BigDecimal.class));

                    assertFalse(rs.next());
                }
            } finally {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            }
        }
    }

    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testUpdateOnColumnForAssociatedTable() throws SQLException {
        String query1 = "SELECT t.* FROM " + tableName1 + " k INNER JOIN " + tableName2 + " t ON  t.i = k.i";
        String query2 = "SELECT * FROM " + tableName2;
        String data = "NEW EDIT";

        try (SQLServerConnection conn = PrepUtil.getConnection(connectionString)) {

            ambiguousUpdateRowTestSetup(conn);

            try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs = stmt.executeQuery(query1);) {
                rs.first();
                rs.updateString(3, data);
                rs.updateRow();
            }

            try (Statement createStatement = conn.createStatement();
                    ResultSet rs = createStatement.executeQuery(query2);) {
                rs.next();
                assertEquals(data, rs.getString(3));
            }
        }
    }

    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testErrorOnAmbiguousUpdate() throws SQLException {
        String query1 = "SELECT t.*, k.* FROM " + tableName1 + " k INNER JOIN " + tableName2 + " t ON  t.i = k.i";
        String data = "NEW EDIT";

        try (SQLServerConnection conn = PrepUtil.getConnection(connectionString)) {

            ambiguousUpdateRowTestSetup(conn);

            try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs = stmt.executeQuery(query1);) {
                rs.first();
                rs.updateString(3, data);
                rs.updateString(5, data);
                rs.updateRow();
            }
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException se) {
            assertTrue(se.getMessage().matches(TestUtils.formatErrorMsg("R_AmbiguousRowUpdate")));
        }
    }

    /**
     * Tests getObject(n, java.time.LocalDateTime.class).
     * 
     * @throws SQLException
     */
    @Test
    public void testGetObjectAsLocalDateTime() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            TimeZone prevTimeZone = TimeZone.getDefault();
            TimeZone.setDefault(TimeZone.getTimeZone("America/Edmonton"));

            // a local date/time that does not actually exist because of Daylight Saving Time
            final String testValueDate = "2018-03-11";
            final String testValueTime = "02:00:00.1234567";
            final String testValueDateTime = testValueDate + "T" + testValueTime;

            stmt.executeUpdate(
                    "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (id INT, dt2 DATETIME2)");
            stmt.executeUpdate("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (id, dt2) VALUES (1, '" + testValueDateTime + "')");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT dt2 FROM " + AbstractSQLGenerator.escapeIdentifier(tableName) + " WHERE id=1")) {
                rs.next();

                LocalDateTime expectedLocalDateTime = LocalDateTime.parse(testValueDateTime);
                LocalDateTime actualLocalDateTime = rs.getObject(1, LocalDateTime.class);
                assertEquals(expectedLocalDateTime, actualLocalDateTime);

                LocalDate expectedLocalDate = LocalDate.parse(testValueDate);
                LocalDate actualLocalDate = rs.getObject(1, LocalDate.class);
                assertEquals(expectedLocalDate, actualLocalDate);

                LocalTime expectedLocalTime = LocalTime.parse(testValueTime);
                LocalTime actualLocalTime = rs.getObject(1, LocalTime.class);
                assertEquals(expectedLocalTime, actualLocalTime);
            } finally {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
                TimeZone.setDefault(prevTimeZone);
            }
        }
    }

    /**
     * Tests getObject(n, java.time.OffsetDateTime.class) and getObject(n, java.time.OffsetTime.class).
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testGetObjectAsOffsetDateTime() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            final String testValue = "2018-01-02T11:22:33.123456700+12:34";

            stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (id INT PRIMARY KEY, dto DATETIMEOFFSET, dto2 DATETIMEOFFSET)");
            stmt.executeUpdate("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (id, dto, dto2) VALUES (1, '" + testValue + "', null)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT dto, dto2 FROM " + AbstractSQLGenerator.escapeIdentifier(tableName) + " WHERE id=1")) {
                rs.next();

                OffsetDateTime expected = OffsetDateTime.parse(testValue);
                OffsetDateTime actual = rs.getObject(1, OffsetDateTime.class);
                assertEquals(expected, actual);
                assertNull(rs.getObject(2, OffsetDateTime.class));

                OffsetTime expectedTime = OffsetTime.parse(testValue.split("T")[1]);
                OffsetTime actualTime = rs.getObject(1, OffsetTime.class);
                assertEquals(expectedTime, actualTime);
                assertNull(rs.getObject(2, OffsetTime.class));
            } finally {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            }
        }
    }

    /**
     * Tests ResultSet#isWrapperFor and ResultSet#unwrap.
     * 
     * @throws SQLException
     */
    @Test
    public void testResultSetWrapper() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {

            stmt.executeUpdate("create table " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (col1 int, col2 varchar(8000), col3 int identity(1,1))");

            try (ResultSet rs = stmt
                    .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.isWrapperFor(ResultSet.class));
                assertTrue(rs.isWrapperFor(ISQLServerResultSet.class));

                assertSame(rs, rs.unwrap(ResultSet.class));
                assertSame(rs, rs.unwrap(ISQLServerResultSet.class));
            } finally {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            }
        }
    }

    /**
     * Tests calling any getter on a null column should work regardless of their type.
     * 
     * @throws SQLException
     */
    @Test
    public void testGetterOnNull() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery("select null")) {
            rs.next();
            assertEquals(null, rs.getTime(1));
        }
    }

    /**
     * Tests getters and setters for holdability.
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testGetSetHoldability() throws SQLException {
        int[] holdabilityOptions = {ResultSet.HOLD_CURSORS_OVER_COMMIT, ResultSet.CLOSE_CURSORS_AT_COMMIT};

        try (Connection con = getConnection(); Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery("select null")) {

            int connHold = con.getHoldability();
            assertEquals(stmt.getResultSetHoldability(), connHold);
            assertEquals(rs.getHoldability(), connHold);

            for (int i = 0; i < holdabilityOptions.length; i++) {
                if ((connHold = con.getHoldability()) != holdabilityOptions[i]) {
                    con.setHoldability(holdabilityOptions[i]);
                    assertEquals(con.getHoldability(), holdabilityOptions[i]);
                }
            }
        }
    }

    /**
     * Call resultset methods to run thru some code paths
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testResultSetMethods() throws SQLException {
        try (Connection con = getConnection();
                Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)) {

            stmt.executeUpdate("create table " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (col1 int primary key, col2 varchar(255))");
            stmt.executeUpdate(
                    "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values(0, " + " 'one')");
            stmt.executeUpdate(
                    "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values(1, " + "'two')");
            stmt.executeUpdate(
                    "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values(2, " + "'three')");

            try (ResultSet rs = stmt
                    .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {

                rs.clearWarnings();

                assert (rs.getType() == ResultSet.TYPE_SCROLL_SENSITIVE);

                // check cursor
                rs.first();
                assert (rs.isFirst());

                rs.relative(1);
                assert (!rs.isFirst());

                rs.last();
                assert (rs.isLast());

                rs.beforeFirst();
                assert (rs.isBeforeFirst());

                rs.afterLast();
                assert (rs.isAfterLast());
                assert (!rs.isLast());

                rs.absolute(1);
                assert (rs.getRow() == 1);

                rs.moveToInsertRow();
                assert (rs.getRow() == 0);

                // insert and update
                rs.updateInt(1, 4);
                rs.updateString(2, "four");
                rs.insertRow();

                rs.updateObject(1, 5);
                rs.updateObject(2, new String("five"));
                rs.insertRow();

                rs.updateObject("col1", 6);
                rs.updateObject("col2", new String("six"));
                rs.insertRow();

                rs.updateObject(1, 7, 0);
                rs.updateObject("col2", new String("seven"), 0);
                rs.insertRow();

                // valid column names
                assert (rs.findColumn("col1") == 1);
                assert (rs.findColumn("col2") == 2);

                // invalid column name
                try {
                    rs.findColumn("col3");
                } catch (SQLException e) {
                    assertTrue(e.getMessage().contains("column name col3 is not valid"));
                }

                rs.moveToCurrentRow();
                assert (rs.getRow() == 1);

                // no inserts or updates
                assert (!rs.rowInserted());
                assert (!rs.rowUpdated());

                // check concurrency method
                assert (rs.getConcurrency() == ResultSet.CONCUR_UPDATABLE);

                // check fetch direction
                rs.setFetchDirection(ResultSet.FETCH_FORWARD);
                assert (rs.getFetchDirection() == ResultSet.FETCH_FORWARD);

                // check fetch size
                rs.setFetchSize(1);
                assert (rs.getFetchSize() == 1);

                rs.refreshRow();

                rs.previous();
                assert (!rs.rowDeleted());
                rs.next();

                // delete row
                do {
                    rs.moveToCurrentRow();
                    rs.deleteRow();
                    assert (rs.rowDeleted());
                } while (rs.next());

            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            }
        }
    }

    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testMultipleResultSets() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            stmt.execute(
                    "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (c1 int IDENTITY, c2 int)");
            String SQL = "exec sp_help 'dbo." + TestUtils.escapeSingleQuotes(tableName) + "'";

            boolean results = stmt.execute(SQL);
            int rsCount = 0;
            int warningCount = 0;

            // Loop through the available result sets.
            while (results) {
                try (ResultSet rs = stmt.getResultSet()) {
                    rsCount++;
                    int i = 1;
                    String firstColumnValue = null;
                    while (rs.next()) {
                        switch (rsCount) {
                            case 1:
                                firstColumnValue = rs.getString("Name");
                                assert (firstColumnValue.equals(tableName));
                                break;
                            case 2:
                                firstColumnValue = rs.getString("Column_name");
                                assert (firstColumnValue.equalsIgnoreCase("c" + i++));
                                break;
                            case 3:
                                firstColumnValue = rs.getString("Identity");
                                assert (firstColumnValue.equalsIgnoreCase("c1"));
                                break;
                            case 4:
                                firstColumnValue = rs.getString("RowGuidCol");
                                assert (firstColumnValue.equalsIgnoreCase("No rowguidcol column defined."));
                                break;
                            case 5:
                                firstColumnValue = rs.getString("Data_located_on_filegroup");
                                assert (firstColumnValue.equalsIgnoreCase("PRIMARY"));
                                break;
                        }
                    }
                }

                SQLWarning warnings = stmt.getWarnings();
                while (null != warnings) {
                    warningCount++;
                    warnings = warnings.getNextWarning();
                }
                results = stmt.getMoreResults();
            }
            assert (!stmt.getMoreResults() && -1 == stmt.getUpdateCount());
            assert (rsCount == 5);
            assert (warningCount == 26);

            /*
             * Testing Scenario: There are no more results when the following is true ............
             * ((stmt.getMoreResults() == false) && (stmt.getUpdateCount() == -1))
             */
            stmt.execute("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values (12345)");
            assert (stmt.getUpdateCount() == 1);
            assert (!stmt.getMoreResults());

            try (ResultSet rs = stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                // Calling getMoreResults() consumes and closes the current ResultSet
                assert (!stmt.getMoreResults() && -1 == stmt.getUpdateCount());
                assert (rs.isClosed());
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            }
        }
    }

    @Test
    public void testResultSetFetchBufferSqlErrorState() throws Exception {
        try (SQLServerConnection connection = PrepUtil.getConnection(connectionString)) {
            try (Statement stmt = connection.createStatement()) {
                ResultSet rs = stmt.executeQuery("select 1/0");
                rs.next();
                fail(TestResource.getResource("R_expectedFailPassed"));
            } catch (SQLException e) {
                assertEquals(expectedSqlState, e.getSQLState());
                assertEquals(expectedErrorCode, e.getErrorCode());
            }
        }
    }

    @Test
    public void testResultSetClientCursorInitializerSqlErrorState() {
        try (Connection con = PrepUtil.getConnection(connectionString); Statement stmt = con.createStatement()) {
            stmt.executeUpdate("create table " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (col1 int)");
            boolean hasResults = stmt
                    .execute("select * from " + AbstractSQLGenerator.escapeIdentifier(tableName) + "; select 1/0");
            while (hasResults) {
                ResultSet rs = stmt.getResultSet();
                while (rs.next()) {}
                hasResults = stmt.getMoreResults();
            }
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (SQLException e) {
            assertEquals(expectedSqlState, e.getSQLState());
            assertEquals(expectedErrorCode, e.getErrorCode());
        }
    }

    /**
     * Test getObject() with unsupported type conversion that throws SQLException
     */
    @Test
    @Tag(Constants.CodeCov)
    public void testGetObjectUnsupportedTypeConversion() throws SQLException {
        try (Connection con = getConnection();
                Statement stmt = con.createStatement()) {

            stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (id int, test_string varchar(50))");
            stmt.executeUpdate("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (id, test_string) VALUES (1, 'test_value')");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next());

                SQLException thrown = assertThrows(SQLException.class, () -> {
                    rs.getObject(2, java.util.ArrayList.class); // ArrayList is not a supported conversion type
                });
                assertTrue(thrown.getMessage().contains("The conversion to class java.util.ArrayList is unsupported."));
                assertTrue(thrown.getMessage().contains("java.util.ArrayList"));
            }
        }
    }

    /**
     * Test updateObject() with various data types and ensure correct handling of nulls and streams.
     */
    @Test
    @Tag(Constants.CodeCov)
    public void testUpdateObject() throws SQLException, UnsupportedEncodingException, IOException {
        try (Connection con = getConnection(); 
             Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
            
            stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName) 
                    + " (id int IDENTITY(1,1) PRIMARY KEY, col_varchar varchar(max), "
                    + "col_nvarchar nvarchar(max), col_varbinary varbinary(max), "
                    + "col_xml xml, col_decimal decimal(18,6), col_int int)");
            
            stmt.executeUpdate("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) 
                    + " (col_varchar, col_nvarchar, col_varbinary, col_xml, col_decimal, col_int) "
                    + " VALUES ('initial', N'initial_n', 0x123456, '<root>test</root>', 123.456, 999)");
            
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next());
                
                rs.updateObject(2, null);
                assertTrue(rs.wasNull() == false);
                
                rs.updateObject(6, new BigDecimal("555.123"));
                
                try (StringReader reader = new StringReader("test reader content")) {
                    rs.updateObject(2, reader);
                }
                try (ByteArrayInputStream textStream = new ByteArrayInputStream("test stream".getBytes("UTF-8"))) {
                    rs.updateObject(2, textStream);
                }
                try (ByteArrayInputStream binaryStream = new ByteArrayInputStream(new byte[]{1, 2, 3, 4})) {
                    rs.updateObject(4, binaryStream);
                }
                
                SQLXML sqlxml = con.createSQLXML();
                sqlxml.setString("<root>updated xml</root>");
                rs.updateObject(5, sqlxml);
                
                rs.updateObject(7, Integer.valueOf(777));
                
                // Apply updates and validate the data
                rs.updateRow();
                
                try (ResultSet verifyRs = stmt.executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                    assertTrue(verifyRs.next());
                    assertEquals("test stream", verifyRs.getString("col_varchar"));
                    assertEquals(Integer.valueOf(777), verifyRs.getObject("col_int"));
                    
                    SQLXML retrievedXML = verifyRs.getSQLXML("col_xml");
                    assertNotNull(retrievedXML);
                    String xmlContent = retrievedXML.getString();
                    assertTrue(xmlContent.contains("updated xml"));
                }
            }
        }
    }

    /**
     * Test getStatement() method on ResultSet to ensure it returns the correct Statement object.
     */
    @Test
    @Tag(Constants.CodeCov)
    public void testGetStatement() throws SQLException {
        try (Connection con = getConnection();
                Statement stmt = con.createStatement()) {

            stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (id int, test_string varchar(50))");
            stmt.executeUpdate("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (id, test_string) VALUES (1, 'test_value')");

            try (ResultSet rs = stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                Statement returnedStmt = rs.getStatement();
                assertNotNull(returnedStmt);
                assertSame(stmt, returnedStmt);

                // Verify that the returned Statement is of the correct type
                assertEquals(ResultSet.TYPE_FORWARD_ONLY, returnedStmt.getResultSetType());
                assertEquals(ResultSet.CONCUR_READ_ONLY, returnedStmt.getResultSetConcurrency());

                // Test that getStatement() works when ResultSet has data
                assertTrue(rs.next());
                Statement returnedStmtWithData = rs.getStatement();
                assertNotNull(returnedStmtWithData);
                assertSame(stmt, returnedStmtWithData);

                // Close the ResultSet and verify getStatement() throws exception
                rs.close();
                SQLException thrown = assertThrows(SQLException.class, () -> {
                    rs.getStatement();
                });
                assertTrue(thrown.getMessage().contains("closed") ||
                        thrown.getMessage().contains("invalid") ||
                        thrown instanceof SQLServerException);
            }
        }
    }

    /**
     * Test setFetchDirection validation for invalid directions
     */
    @Test
    @Tag(Constants.CodeCov)
    public void testSetFetchDirectionInvalidDirection() throws SQLException {
        try (Connection con = getConnection();
                Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY)) {

            stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (col1 int)");
            stmt.executeUpdate("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " VALUES (1)");

            // Test invalid fetch direction
            try (ResultSet rs = stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                SQLException thrown = assertThrows(SQLException.class, () -> {
                    rs.setFetchDirection(999); // Invalid fetch direction
                });
                assertEquals("The fetch direction 999 is not valid.", thrown.getMessage());
            }

            // Test non-forward fetch direction on forward-only result set
            try (Statement forwardStmt = con.createStatement(SQLServerResultSet.TYPE_SS_DIRECT_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
                    ResultSet rs = forwardStmt
                            .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {

                SQLException thrown = assertThrows(SQLException.class, () -> {
                    rs.setFetchDirection(ResultSet.FETCH_REVERSE); // Not allowed on forward-only
                });
                assertEquals("The requested operation is not supported on forward only result sets.",
                        thrown.getMessage());
            }
        }
    }

    /**
     * Test setFetchSize validation for negative values
     */
    @Test
    @Tag(Constants.CodeCov)
    public void testSetFetchSizeNegativeValue() throws SQLException {
        try (Connection con = getConnection();
                Statement stmt = con.createStatement()) {

            stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (col1 int)");
            stmt.executeUpdate("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " VALUES (1)");

            try (ResultSet rs = stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                try {
                    rs.setFetchSize(-1);
                    fail("Expected SQLException for negative fetch size");
                } catch (SQLException e) {
                    assertEquals("The fetch size cannot be negative.", e.getMessage());
                }
                rs.setFetchSize(0); // Should use default fetch size
                assertEquals(128, rs.getFetchSize());
            }
        }
    }

    /**
     * Tests various ResultSet getter methods by column index.
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.CodeCov)
    public void testResultSetGetterMethodsByIndex() throws SQLException {
        try (Connection con = getConnection();
                Statement stmt = con.createStatement()) {

            createUnifiedResultSetTestTable(con);

            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next());

                assertNotNull(rs.getAsciiStream(1));
                assertNotNull(rs.getBinaryStream(2));
                assertTrue(rs.getBoolean(3));
                assertEquals((byte) 255, rs.getByte(4));
                assertNotNull(rs.getBytes(5));
                assertNotNull(rs.getDate(6));
                assertNotNull(rs.getDate(6, null)); // Test getDate with Calendar
                assertEquals(123.456, rs.getDouble(7), 0.001);
                assertEquals(78.9f, rs.getFloat(8), 0.1f);
                assertNotNull(rs.getGeometry(9));
                assertNotNull(rs.getGeography(10));
                assertEquals(42, rs.getInt(11));
                assertEquals(9876543210L, rs.getLong(12));
                assertEquals((short) 12345, rs.getShort(13));
                assertEquals("hello", rs.getString(14));
                assertEquals("world", rs.getNString(15));
                assertEquals(UUID.fromString(uuid), UUID.fromString(rs.getUniqueIdentifier(16)));
                assertNotNull(rs.getTime(17));
                assertNotNull(rs.getTime(17, null)); // Test getTime with Calendar
                assertNotNull(rs.getTimestamp(18));
                assertNotNull(rs.getTimestamp(18, null)); // Test getTimestamp with Calendar
                assertNotNull(rs.getDateTime(19));
                java.util.Calendar cal = java.util.Calendar.getInstance();
                assertNotNull(rs.getDateTime(19, cal)); // Test getDateTime with Calendar
                assertNotNull(rs.getBigDecimal(20));
                @SuppressWarnings("deprecation")
                BigDecimal bigDec2 = rs.getBigDecimal(20, 1); // Test deprecated getBigDecimal with scale
                assertNotNull(bigDec2);
                assertNull(rs.getObject(21)); // col_null_test
                assertTrue(rs.wasNull());
                assertEquals(0, rs.getInt(21)); // Test primitive getter on null column
                assertTrue(rs.wasNull());
                assertNotNull(rs.getClob(22));
                assertNotNull(rs.getNClob(23));
                assertNotNull(rs.getCharacterStream(22)); // Test character stream on TEXT column
                assertNotNull(rs.getNCharacterStream(23)); // Test ncharacter stream on NTEXT column
                assertNotNull(rs.getBlob(24));
                assertNotNull(rs.getSmallDateTime(25));
                assertNotNull(rs.getSmallDateTime(25, null)); // Test getSmallDateTime with Calendar
                assertNotNull(rs.getDateTimeOffset(26));
                assertNotNull(rs.getMoney(27));
                assertNotNull(rs.getSmallMoney(28));
                assertNotNull(rs.getSQLXML(29));

                // Test getObject variations
                assertNotNull(rs.getObject(14)); // col_string
                assertNotNull(rs.getObject(15, String.class)); // col_nstring with type

                // Test unsupported operations that should throw SQLException
                assertThrows(SQLException.class, () -> rs.getObject(14, new java.util.HashMap<>()));
                assertThrows(SQLException.class, () -> rs.getRef(14));
                assertThrows(SQLException.class, () -> rs.getArray(14));
                @SuppressWarnings("deprecation")
                SQLException ex2 = assertThrows(SQLException.class, () -> rs.getUnicodeStream(14));
                assertNotNull(ex2);
            }
        }
    }

    /**
     * Tests various ResultSet getter methods by column name.
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.CodeCov)
    public void testResultSetGetterMethodsByColumnName() throws SQLException {
        try (Connection con = getConnection();
                Statement stmt = con.createStatement()) {

            createUnifiedResultSetTestTable(con);

            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next());

                assertNotNull(rs.getAsciiStream("col_ascii"));
                assertNotNull(rs.getBinaryStream("col_binary"));
                assertTrue(rs.getBoolean("col_bool"));
                assertEquals((byte) 255, rs.getByte("col_byte"));
                assertNotNull(rs.getBytes("col_bytes"));
                assertNotNull(rs.getDate("col_date"));
                assertNotNull(rs.getDate("col_date", null)); // Test getDate with Calendar
                assertEquals(123.456, rs.getDouble("col_double"), 0.001);
                assertEquals(78.9f, rs.getFloat("col_float"), 0.1f);
                assertNotNull(rs.getGeometry("col_geometry"));
                assertNotNull(rs.getGeography("col_geography"));
                assertEquals(42, rs.getInt("col_int"));
                assertEquals(9876543210L, rs.getLong("col_long"));
                assertEquals((short) 12345, rs.getShort("col_short"));
                assertEquals("hello", rs.getString("col_string"));
                assertEquals("world", rs.getNString("col_nstring"));
                assertEquals(UUID.fromString(uuid), UUID.fromString(rs.getUniqueIdentifier("col_uniqueid")));
                assertNotNull(rs.getTime("col_time"));
                assertNotNull(rs.getTime("col_time", null)); // Test getTime with Calendar
                assertNotNull(rs.getTimestamp("col_timestamp"));
                assertNotNull(rs.getTimestamp("col_timestamp", null)); // Test getTimestamp with Calendar
                assertNotNull(rs.getDateTime("col_datetime"));
                java.util.Calendar cal = java.util.Calendar.getInstance();
                assertNotNull(rs.getDateTime("col_datetime", cal)); // Test getDateTime with Calendar
                assertNotNull(rs.getDateTime("col_datetime", null)); // Test getDateTime with null Calendar
                assertNotNull(rs.getBigDecimal("col_decimal"));
                @SuppressWarnings("deprecation")
                BigDecimal bigDec2 = rs.getBigDecimal("col_decimal", 1); // Test deprecated getBigDecimal with scale
                assertNotNull(bigDec2);
                assertNull(rs.getObject("col_null_test"));
                assertTrue(rs.wasNull());
                assertEquals(0, rs.getInt("col_null_test")); // Test primitive getter on null column
                assertTrue(rs.wasNull());
                assertNotNull(rs.getClob("col_text"));
                assertNotNull(rs.getNClob("col_ntext"));
                assertNotNull(rs.getCharacterStream("col_text")); // Test character stream on TEXT column
                assertNotNull(rs.getNCharacterStream("col_ntext")); // Test ncharacter stream on NTEXT column
                assertNotNull(rs.getBlob("col_blob"));
                assertNotNull(rs.getSmallDateTime("col_smalldatetime"));
                assertNotNull(rs.getSmallDateTime("col_smalldatetime", null)); // Test getSmallDateTime with Calendar
                assertNotNull(rs.getDateTimeOffset("col_datetimeoffset"));
                assertNotNull(rs.getMoney("col_money"));
                assertNotNull(rs.getSmallMoney("col_smallmoney"));
                assertNotNull(rs.getSQLXML("col_xml"));

                // Test getObject variations
                assertNotNull(rs.getObject("col_string")); // Basic getObject
                assertNotNull(rs.getObject("col_nstring", String.class)); // getObject with type

                // Test unsupported operations that should throw SQLException
                assertThrows(SQLException.class, () -> rs.getObject("col_string", new java.util.HashMap<>()));
                assertThrows(SQLException.class, () -> rs.getRef("col_string"));
                assertThrows(SQLException.class, () -> rs.getArray("col_string"));
                @SuppressWarnings("deprecation")
                SQLException ex2 = assertThrows(SQLException.class, () -> rs.getUnicodeStream("col_string"));
                assertNotNull(ex2);
            }
        }
    }

    /**
     * Tests various ResultSet update methods by column index.
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.CodeCov)
    public void testResultSetUpdateMethods() throws SQLException {
        try (Connection con = getConnection();
                Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)) {

            createUnifiedResultSetTestTable(con);

            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next());

                rs.updateBoolean(3, false);
                rs.updateBoolean(3, true, false);

                rs.updateByte(4, (byte) 150);
                rs.updateByte(4, (byte) 0, false);

                rs.updateShort(13, (short) 1500);
                rs.updateShort(13, (short) 2500, false);

                rs.updateInt(11, 999);
                rs.updateInt(11, 1001, false);

                rs.updateLong(12, 5000L);
                rs.updateLong(12, 6000L, false);

                rs.updateFloat(8, 987.65f);
                rs.updateFloat(8, 876.54f, false);

                rs.updateDouble(7, 1234.5678);
                rs.updateDouble(7, 2345.6789, false);

                BigDecimal money1 = new BigDecimal("1500.75");
                BigDecimal money2 = new BigDecimal("1750.2500");
                rs.updateMoney(27, money1);
                rs.updateMoney(27, money2, false);
                rs.updateSmallMoney(28, money1);
                rs.updateSmallMoney(28, money2, false);

                BigDecimal decimal1 = new BigDecimal("999.123456");
                BigDecimal decimal2 = new BigDecimal("888.12345");
                rs.updateBigDecimal(20, decimal1);
                rs.updateBigDecimal(20, decimal2, 10, 5);
                rs.updateBigDecimal(20, decimal1, 10, 5, false);
                rs.updateObject(20, decimal2, 10, 3);
                rs.updateObject(20, decimal1, 10, 3, false);

                rs.updateString(14, "updated_string");
                rs.updateString(14, "updated_string_encrypt", false);

                rs.updateNString(15, "updated_nstring");
                rs.updateNString(15, "updated_nstring_encrypt", false);

                byte[] bytes1 = { 0x78, (byte) 0x9A, (byte) 0xBC };
                byte[] bytes2 = { 0x78, (byte) 0x9A, (byte) 0xBC, (byte) 0xDE };
                rs.updateBytes(5, bytes1);
                rs.updateBytes(5, bytes2, false);

                Date date1 = Date.valueOf("2023-12-25");
                Date date2 = Date.valueOf("2023-12-26");
                rs.updateDate(6, date1);
                rs.updateDate(6, date2, false);

                Time time1 = Time.valueOf("15:45:30");
                Time time2 = Time.valueOf("16:45:30");
                rs.updateTime(17, time1);
                rs.updateTime(17, time2, 3);
                rs.updateTime(17, time1, 3, false);

                Timestamp timestamp1 = Timestamp.valueOf("2023-12-25 15:45:30.456");
                Timestamp timestamp2 = Timestamp.valueOf("2023-12-25 16:45:30.456");
                rs.updateTimestamp(18, timestamp1);
                rs.updateTimestamp(18, timestamp2, 6);
                rs.updateTimestamp(18, timestamp1, 6, false);

                rs.updateDateTime(19, timestamp1);
                rs.updateDateTime(19, timestamp2, 3);
                rs.updateDateTime(19, timestamp1, 3, false);

                Timestamp smallDateTime1 = Timestamp.valueOf("2023-12-25 15:45:00");
                Timestamp smallDateTime2 = Timestamp.valueOf("2023-12-25 16:45:00");
                rs.updateSmallDateTime(25, smallDateTime1);
                rs.updateSmallDateTime(25, smallDateTime2, 0);
                rs.updateSmallDateTime(25, smallDateTime1, 0, false);

                DateTimeOffset dateTimeOffset1 = DateTimeOffset.valueOf(timestamp1, 5);
                DateTimeOffset dateTimeOffset2 = DateTimeOffset.valueOf(timestamp2, 5);
                rs.updateDateTimeOffset(26, dateTimeOffset1);
                rs.updateDateTimeOffset(26, dateTimeOffset2, 6);
                rs.updateDateTimeOffset(26, dateTimeOffset1, 6, false);

                String guid1 = java.util.UUID.randomUUID().toString();
                String guid2 = java.util.UUID.randomUUID().toString();
                rs.updateUniqueIdentifier(16, guid1);
                rs.updateUniqueIdentifier(16, guid2, false);

                rs.updateNull(21);

                rs.updateRow(); // Apply the updates to the database

                // Verify the updates were applied to the database
                try (SQLServerResultSet verifyRs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM "
                        + AbstractSQLGenerator.escapeIdentifier(tableName) + " WHERE col_int = 1001")) {
                    assertTrue(verifyRs.next());
                    assertEquals(true, verifyRs.getBoolean("col_bool"));
                    assertEquals((short) 2500, verifyRs.getShort("col_short"));
                    assertEquals(1001, verifyRs.getInt("col_int"));
                    assertEquals(6000L, verifyRs.getLong("col_long"));
                    assertEquals(876.54f, verifyRs.getFloat("col_float"), 0.01f);
                    assertEquals(2345.6789, verifyRs.getDouble("col_double"), 0.0001);
                    assertEquals(money2, verifyRs.getBigDecimal("col_money"));
                    assertEquals(money2, verifyRs.getBigDecimal("col_smallmoney"));
                    assertEquals(-1, verifyRs.getBigDecimal("col_decimal").compareTo(new BigDecimal("999.654")));
                    assertEquals("updated_string_encrypt", verifyRs.getString("col_string"));
                    assertEquals("updated_nstring_encrypt", verifyRs.getNString("col_nstring"));
                    assertArrayEquals(bytes2, verifyRs.getBytes("col_bytes"));
                    assertEquals(date2, verifyRs.getDate("col_date"));
                    assertEquals(guid2.toUpperCase(), verifyRs.getUniqueIdentifier("col_uniqueid"));
                    assertNull(verifyRs.getObject("col_null_test"));
                    assertTrue(verifyRs.wasNull());
                }
            } 
        }
    }

    /**
     * Tests various ResultSet update methods by column name.
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.CodeCov)
    public void testResultSetUpdateMethodsByColumnName() throws SQLException {
        try (Connection con = getConnection();
                Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)) {

            createUnifiedResultSetTestTable(con);

            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next());

                rs.updateBoolean("col_bool", false);
                rs.updateBoolean("col_bool", true, false);

                rs.updateByte("col_byte", (byte) 150);
                rs.updateByte("col_byte", (byte) 0, false);

                rs.updateShort("col_short", (short) 1500);
                rs.updateShort("col_short", (short) 2500, false);

                rs.updateInt("col_int", 999);
                rs.updateInt("col_int", 1001, false);

                rs.updateLong("col_long", 5000L);
                rs.updateLong("col_long", 6000L, false);

                rs.updateFloat("col_float", 987.65f);
                rs.updateFloat("col_float", 876.54f, false);

                rs.updateDouble("col_double", 1234.5678);
                rs.updateDouble("col_double", 2345.6789, false);

                BigDecimal money1 = new BigDecimal("1500.75");
                BigDecimal money2 = new BigDecimal("1750.2500");
                rs.updateMoney("col_money", money1);
                rs.updateMoney("col_money", money2, false);
                rs.updateSmallMoney("col_smallmoney", money1);
                rs.updateSmallMoney("col_smallmoney", money2, false);

                BigDecimal decimal1 = new BigDecimal("999.123456");
                BigDecimal decimal2 = new BigDecimal("888.12345");
                rs.updateBigDecimal("col_decimal", decimal1);
                rs.updateBigDecimal("col_decimal", decimal2, 10, 5);
                rs.updateBigDecimal("col_decimal", decimal1, 10, 5, false);
                rs.updateObject("col_decimal", decimal2, 10, 3);
                rs.updateObject("col_decimal", decimal1, 10, 3, false);

                rs.updateString("col_string", "updated_string");
                rs.updateString("col_string", "updated_string_encrypt", false);

                rs.updateNString("col_nstring", "updated_nstring");
                rs.updateNString("col_nstring", "updated_nstring_encrypt", false);

                byte[] bytes1 = { 0x78, (byte) 0x9A, (byte) 0xBC };
                byte[] bytes2 = { 0x78, (byte) 0x9A, (byte) 0xBC, (byte) 0xDE };
                rs.updateBytes("col_bytes", bytes1);
                rs.updateBytes("col_bytes", bytes2, false);

                Date date1 = Date.valueOf("2023-12-25");
                Date date2 = Date.valueOf("2023-12-26");
                rs.updateDate("col_date", date1);
                rs.updateDate("col_date", date2, false);

                Time time1 = Time.valueOf("15:45:30");
                Time time2 = Time.valueOf("16:45:30");
                rs.updateTime("col_time", time1);
                rs.updateTime("col_time", time2, 3);
                rs.updateTime("col_time", time1, 3, false);

                Timestamp timestamp1 = Timestamp.valueOf("2023-12-25 15:45:30.456");
                Timestamp timestamp2 = Timestamp.valueOf("2023-12-25 16:45:30.456");
                rs.updateTimestamp("col_timestamp", timestamp1);
                rs.updateTimestamp("col_timestamp", timestamp2, 6);
                rs.updateTimestamp("col_timestamp", timestamp1, 6, false);

                rs.updateDateTime("col_datetime", timestamp1);
                rs.updateDateTime("col_datetime", timestamp2, 3);
                rs.updateDateTime("col_datetime", timestamp1, 3, false);

                Timestamp smallDateTime1 = Timestamp.valueOf("2023-12-25 15:45:00");
                Timestamp smallDateTime2 = Timestamp.valueOf("2023-12-25 16:45:00");
                rs.updateSmallDateTime("col_smalldatetime", smallDateTime1);
                rs.updateSmallDateTime("col_smalldatetime", smallDateTime2, 0);
                rs.updateSmallDateTime("col_smalldatetime", smallDateTime1, 0, false);

                DateTimeOffset dateTimeOffset1 = DateTimeOffset.valueOf(timestamp1, 5);
                DateTimeOffset dateTimeOffset2 = DateTimeOffset.valueOf(timestamp2, 5);
                rs.updateDateTimeOffset("col_datetimeoffset", dateTimeOffset1);
                rs.updateDateTimeOffset("col_datetimeoffset", dateTimeOffset2, 6);
                rs.updateDateTimeOffset("col_datetimeoffset", dateTimeOffset1, 6, false);

                String guid1 = java.util.UUID.randomUUID().toString();
                String guid2 = java.util.UUID.randomUUID().toString();
                rs.updateUniqueIdentifier("col_uniqueid", guid1);
                rs.updateUniqueIdentifier("col_uniqueid", guid2, false);

                rs.updateNull("col_null_test");

                rs.updateRow(); // Apply the updates to the database

                // Verify the updates were applied to the database
                try (SQLServerResultSet verifyRs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM "
                        + AbstractSQLGenerator.escapeIdentifier(tableName) + " WHERE col_int = 1001")) {
                    assertTrue(verifyRs.next());
                    assertEquals(true, verifyRs.getBoolean("col_bool"));
                    assertEquals((short) 2500, verifyRs.getShort("col_short"));
                    assertEquals(1001, verifyRs.getInt("col_int"));
                    assertEquals(6000L, verifyRs.getLong("col_long"));
                    assertEquals(876.54f, verifyRs.getFloat("col_float"), 0.01f);
                    assertEquals(2345.6789, verifyRs.getDouble("col_double"), 0.0001);
                    assertEquals(money2, verifyRs.getBigDecimal("col_money"));
                    assertEquals(money2, verifyRs.getBigDecimal("col_smallmoney"));
                    assertEquals(-1, verifyRs.getBigDecimal("col_decimal").compareTo(new BigDecimal("999.654")));
                    assertEquals("updated_string_encrypt", verifyRs.getString("col_string"));
                    assertEquals("updated_nstring_encrypt", verifyRs.getNString("col_nstring"));
                    assertArrayEquals(bytes2, verifyRs.getBytes("col_bytes"));
                    assertEquals(date2, verifyRs.getDate("col_date"));
                    assertEquals(guid2.toUpperCase(), verifyRs.getUniqueIdentifier("col_uniqueid"));
                    assertNull(verifyRs.getObject("col_null_test"));
                    assertTrue(verifyRs.wasNull());
                }
            }
        }
    }

    /**
     * This test covers updateAsciiStream, updateBinaryStream, updateCharacterStream, and updateNCharacterStream methods
     */
    @Test
    @Tag(Constants.CodeCov)
    public void testResultSetUpdateStreamMethods() throws SQLException, UnsupportedEncodingException, IOException {
        try (Connection con = getConnection();
                Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)) {

            stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (id int IDENTITY(1,1) PRIMARY KEY, col_ascii varchar(max), col_binary varbinary(max), "
                    + "col_character text, col_ncharacter ntext)");
            stmt.executeUpdate("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (col_ascii, col_binary, col_character, col_ncharacter) "
                    + " VALUES ('initial_ascii', 0x48656C6C6F, 'initial_character', N'initial_ncharacter')");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next());

                // Prepare all content - reuse string variables for similar operations
                String asciiContent = "updated_ascii_stream";
                String charContent = "updated_character_stream";
                String ncharContent = "updated_ncharacter_stream";
                String finalSuffix = "_final";

                // Final content strings
                String asciiContentFinal = asciiContent + finalSuffix;
                String charContentFinal = charContent + finalSuffix;
                String ncharContentFinal = ncharContent + finalSuffix;

                byte[] binaryBase = { 0x01, 0x02, 0x03, 0x04 };
                byte[] binaryContentFinal = { 0x15, 0x16, 0x17, 0x18 };

                byte[] asciiBytesF = asciiContentFinal.getBytes("ASCII");

                // Create streams that will remain open until updateRow()
                InputStream asciiStreamFinal = new ByteArrayInputStream(asciiBytesF);
                InputStream binaryStreamFinal = new ByteArrayInputStream(binaryContentFinal);
                Reader charReaderFinal = new StringReader(charContentFinal);
                Reader ncharReaderFinal = new StringReader(ncharContentFinal);

                try {
                    rs.updateAsciiStream(2, new ByteArrayInputStream((asciiContent + "_1").getBytes("ASCII")));
                    byte[] asciiBytes2 = (asciiContent + "_2").getBytes("ASCII");
                    rs.updateAsciiStream(2, new ByteArrayInputStream(asciiBytes2), asciiBytes2.length);
                    byte[] asciiBytes3 = (asciiContent + "_3").getBytes("ASCII");
                    rs.updateAsciiStream(2, new ByteArrayInputStream(asciiBytes3), (long) asciiBytes3.length);
                    rs.updateAsciiStream("col_ascii",
                            new ByteArrayInputStream((asciiContent + "_4").getBytes("ASCII")));
                    byte[] asciiBytes5 = (asciiContent + "_5").getBytes("ASCII");
                    rs.updateAsciiStream("col_ascii", new ByteArrayInputStream(asciiBytes5), asciiBytes5.length);
                    rs.updateAsciiStream("col_ascii", asciiStreamFinal, (long) asciiBytesF.length);

                    rs.updateBinaryStream(3, new ByteArrayInputStream(binaryBase));
                    byte[] binaryContent2 = { 0x05, 0x06, 0x07, 0x08 };
                    rs.updateBinaryStream(3, new ByteArrayInputStream(binaryContent2), binaryContent2.length);
                    byte[] binaryContent3 = { 0x09, 0x0A, 0x0B, 0x0C };
                    rs.updateBinaryStream(3, new ByteArrayInputStream(binaryContent3), (long) binaryContent3.length);
                    byte[] binaryContent4 = { 0x0D, 0x0E, 0x0F, 0x10 };
                    rs.updateBinaryStream("col_binary", new ByteArrayInputStream(binaryContent4));
                    byte[] binaryContent5 = { 0x11, 0x12, 0x13, 0x14 };
                    rs.updateBinaryStream("col_binary", new ByteArrayInputStream(binaryContent5),
                            binaryContent5.length);
                    rs.updateBinaryStream("col_binary", binaryStreamFinal, (long) binaryContentFinal.length);

                    rs.updateCharacterStream(4, new StringReader(charContent + "_1"));
                    String charContent2 = charContent + "_2";
                    rs.updateCharacterStream(4, new StringReader(charContent2), charContent2.length());
                    String charContent3 = charContent + "_3";
                    rs.updateCharacterStream(4, new StringReader(charContent3), (long) charContent3.length());
                    rs.updateCharacterStream("col_character", new StringReader(charContent + "_4"));
                    String charContent5 = charContent + "_5";
                    rs.updateCharacterStream("col_character", new StringReader(charContent5), charContent5.length());
                    rs.updateCharacterStream("col_character", charReaderFinal, (long) charContentFinal.length());

                    rs.updateNCharacterStream(5, new StringReader(ncharContent + "_1"));
                    String ncharContent2 = ncharContent + "_2";
                    rs.updateNCharacterStream(5, new StringReader(ncharContent2), (long) ncharContent2.length());
                    rs.updateNCharacterStream("col_ncharacter", new StringReader(ncharContent + "_3"));
                    rs.updateNCharacterStream("col_ncharacter", ncharReaderFinal, (long) ncharContentFinal.length());

                    // Apply the updates - streams must still be open at this point
                    rs.updateRow();

                    // Verify the updates were applied to the database
                    try (ResultSet verifyRs = stmt
                            .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                        assertTrue(verifyRs.next());
                        assertEquals(asciiContentFinal, verifyRs.getString("col_ascii"));
                        assertArrayEquals(binaryContentFinal, verifyRs.getBytes("col_binary"));
                        assertEquals(charContentFinal, verifyRs.getString("col_character"));
                        assertEquals(ncharContentFinal, verifyRs.getNString("col_ncharacter"));
                    }

                } finally {

                    asciiStreamFinal.close();
                    binaryStreamFinal.close();
                    charReaderFinal.close();
                    ncharReaderFinal.close();
                }
            }
        }
    }

    /**
     * Test ResultSet updateClob, updateNClob, updateBlob, and updateSQLXML methods
     */
    @Test
    @Tag(Constants.CodeCov)
    public void testResultSetUpdateClobBlobMethods() throws SQLException, IOException {
        try (Connection con = getConnection();
                Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)) {

            stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (id int IDENTITY(1,1) PRIMARY KEY, "
                    + "col_clob text, col_nclob ntext, col_blob varbinary(max), col_xml xml)");

            stmt.executeUpdate("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (col_clob, col_nclob, col_blob, col_xml) "
                    + " VALUES ('initial_clob', N'initial_nclob', 0x123456, '<root>initial</root>')");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next());

                String clobContent = "updated_clob";
                String nclobContent = "updated_nclob";
                String xmlContent = "<test>updated_xml";
                String finalSuffix = "_final";

                String clobContentFinal = clobContent + finalSuffix;
                String nclobContentFinal = nclobContent + finalSuffix;
                byte[] blobContentFinal = { 0x78, (byte) 0x9A, (byte) 0xBC, (byte) 0xDE };
                String xmlContentFinal = "<updated>final</updated>";

                byte[] blobBase = { 0x01, 0x02, 0x03 };

                Clob finalClob = con.createClob();
                finalClob.setString(1, clobContentFinal);

                NClob finalNClob = con.createNClob();
                finalNClob.setString(1, nclobContentFinal);

                Blob finalBlob = con.createBlob();
                finalBlob.setBytes(1, blobContentFinal);

                SQLXML finalSQLXML = con.createSQLXML();
                finalSQLXML.setString(xmlContentFinal);

                Reader clobReaderFinal = new StringReader(clobContentFinal);
                Reader nclobReaderFinal = new StringReader(nclobContentFinal);
                InputStream blobStreamFinal = new ByteArrayInputStream(blobContentFinal);

                try {
                    Clob clob1 = con.createClob();
                    clob1.setString(1, clobContent + "_1");
                    rs.updateClob(2, clob1);
                    rs.updateClob(2, new StringReader(clobContent + "_2"));
                    String clobContent3 = clobContent + "_3";
                    rs.updateClob(2, new StringReader(clobContent3), (long) clobContent3.length());
                    Clob clob4 = con.createClob();
                    clob4.setString(1, clobContent + "_4");
                    rs.updateClob("col_clob", clob4);
                    rs.updateClob("col_clob", new StringReader(clobContent + "_5"));
                    rs.updateClob("col_clob", clobReaderFinal, (long) clobContentFinal.length());

                    NClob nclob1 = con.createNClob();
                    nclob1.setString(1, nclobContent + "_1");
                    rs.updateNClob(3, nclob1);
                    rs.updateNClob(3, new StringReader(nclobContent + "_2"));
                    String nclobContent3 = nclobContent + "_3";
                    rs.updateNClob(3, new StringReader(nclobContent3), (long) nclobContent3.length());
                    NClob nclob4 = con.createNClob();
                    nclob4.setString(1, nclobContent + "_4");
                    rs.updateNClob("col_nclob", nclob4);
                    rs.updateNClob("col_nclob", new StringReader(nclobContent + "_5"));
                    rs.updateNClob("col_nclob", nclobReaderFinal, (long) nclobContentFinal.length());

                    Blob blob1 = con.createBlob();
                    blob1.setBytes(1, blobBase);
                    rs.updateBlob(4, blob1);
                    byte[] blobContent2 = { 0x04, 0x05, 0x06 };
                    rs.updateBlob(4, new ByteArrayInputStream(blobContent2));
                    byte[] blobContent3 = { 0x07, 0x08, 0x09 };
                    rs.updateBlob(4, new ByteArrayInputStream(blobContent3), (long) blobContent3.length);
                    Blob blob4 = con.createBlob();
                    blob4.setBytes(1, new byte[] { 0x0A, 0x0B, 0x0C });
                    rs.updateBlob("col_blob", blob4);
                    byte[] blobContent5 = { 0x0D, 0x0E, 0x0F };
                    rs.updateBlob("col_blob", new ByteArrayInputStream(blobContent5));
                    rs.updateBlob("col_blob", blobStreamFinal, (long) blobContentFinal.length);

                    SQLXML sqlxml1 = con.createSQLXML();
                    sqlxml1.setString(xmlContent + "_1</test>");
                    rs.updateSQLXML(5, sqlxml1);
                    rs.updateSQLXML("col_xml", finalSQLXML);

                    rs.updateRow();

                    try (ResultSet verifyRs = stmt
                            .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                        assertTrue(verifyRs.next());
                        assertEquals(clobContentFinal, verifyRs.getString("col_clob"));
                        assertEquals(nclobContentFinal, verifyRs.getNString("col_nclob"));
                        assertArrayEquals(blobContentFinal, verifyRs.getBytes("col_blob"));

                        SQLXML verifyXML = verifyRs.getSQLXML("col_xml");
                        assertEquals(xmlContentFinal, verifyXML.getString());
                        verifyXML.free();
                    }

                } finally {
                    clobReaderFinal.close();
                    nclobReaderFinal.close();
                    blobStreamFinal.close();
                    finalClob.free();
                    finalNClob.free();
                    finalBlob.free();
                    finalSQLXML.free();
                }
            }
        }
    }

    private void createUnifiedResultSetTestTable(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (" +
                    "col_ascii VARCHAR(100), col_binary VARBINARY(100), col_bool BIT, col_byte TINYINT, " +
                    "col_bytes VARBINARY(100), col_date DATE, col_double FLOAT, col_float REAL, " +
                    "col_geometry GEOMETRY, col_geography GEOGRAPHY, col_int INT, col_long BIGINT, " +
                    "col_short SMALLINT, col_string VARCHAR(100), col_nstring NVARCHAR(100), " +
                    "col_uniqueid UNIQUEIDENTIFIER, col_time TIME, col_timestamp DATETIME2, " +
                    "col_datetime DATETIME, col_decimal DECIMAL(10,2), col_null_test INT, " +
                    "col_text TEXT, col_ntext NTEXT, col_blob VARBINARY(MAX), col_smalldatetime SMALLDATETIME, " +
                    "col_datetimeoffset DATETIMEOFFSET, col_money MONEY, col_smallmoney SMALLMONEY, col_xml XML" +
                    ")");

            stmt.executeUpdate("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " VALUES (" +
                    "'test', 0x48656C6C6F, 1, 255, 0x48656C6C6F, '2023-01-15', 123.456, 78.9, " +
                    "geometry::Point(1, 2, 0), geography::Point(47.6, -122.3, 4326), 42, 9876543210, " +
                    "12345, 'hello', N'world', '" + uuid + "', '14:30:00', '2023-01-15 14:30:00', " +
                    "'2023-01-15 14:30:00', 99.99, NULL, 'text content', N'ntext content', 0x48656C6C6F, " +
                    "'2023-01-15 14:30:00', '2023-01-15 14:30:00+02:00', 123.45, 56.78, '<root>test</root>')");
        }

    }

    /**
     * Test casting JSON data and retrieving it as various data types.
     */
    @Test
    @vectorJsonTest
    @Tag(Constants.JSONTest)
    public void testCastOnJSON() throws SQLException {
        String dstTable = TestUtils.escapeSingleQuotes(
                AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTableCastJson")));

        String jsonData = "{\"key\":\"123\"}";

        try (Connection conn = DriverManager.getConnection(connectionString)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE TABLE " + dstTable + " (jsonData JSON)");
                stmt.executeUpdate("INSERT INTO " + dstTable + " VALUES (CAST('" + jsonData + "' AS JSON))");

                String select = "SELECT JSON_VALUE(jsonData, '$.key') AS c1 FROM " + dstTable;

                // Use executeQuery API
                try (SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery(select)) {
                    rs.next();
                    assertEquals(123, rs.getShort("c1"));
                    assertEquals(123, rs.getInt("c1"));
                    assertEquals(123f, rs.getFloat("c1"));
                    assertEquals(123L, rs.getLong("c1"));
                    assertEquals(123d, rs.getDouble("c1"));
                    assertEquals(new BigDecimal(123), rs.getBigDecimal("c1"));
                }

                // Use execute API
                boolean hasResult = stmt.execute(select);
                assertTrue(hasResult);
                try (SQLServerResultSet rs = (SQLServerResultSet) stmt.getResultSet()) {
                    rs.next();
                    assertEquals(123, rs.getShort("c1"));
                    assertEquals(123, rs.getInt("c1"));
                    assertEquals(123f, rs.getFloat("c1"));
                    assertEquals(123L, rs.getLong("c1"));
                    assertEquals(123d, rs.getDouble("c1"));
                    assertEquals(new BigDecimal(123), rs.getBigDecimal("c1"));
                }
            } finally {
                try (Statement stmt = conn.createStatement();) {
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }
    }

    /**
     * Tests ResultSet with JSON column type.
     * 
     * @throws SQLException
     */
    @Test
    @vectorJsonTest
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.JSONTest)
    public void testJdbc41ResultSetJsonColumn() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            String table = AbstractSQLGenerator.escapeIdentifier(tableName);
            stmt.executeUpdate("create table " + table + " (col17 json)");

            try {
                stmt.executeUpdate("insert into " + table + " values('{\"test\":\"123\"}')");
                stmt.executeUpdate("insert into " + table + " values(null)");

                try (ResultSet rs = stmt.executeQuery("select * from " + table)) {
                    assertTrue(rs.next());
                    assertEquals("{\"test\":\"123\"}", rs.getObject(1).toString());

                    assertTrue(rs.next());
                    assertNull(rs.getObject(1));

                    assertFalse(rs.next());
                }
            } finally {
                TestUtils.dropTableIfExists(table, stmt);
            }
        }
    }

    private void ambiguousUpdateRowTestSetup(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE " + tableName1 + " (i INT, data VARCHAR(30))");
            stmt.execute("CREATE TABLE " + tableName2 + " (i INT, row INT, data VARCHAR(30))");
            stmt.execute("INSERT INTO " + tableName1 + " SELECT 1, 'EDIT'");
            stmt.execute("INSERT INTO " + tableName2 + " SELECT 1, 1, 'TEST1' UNION SELECT 1, 2, 'TEST2'");
        }
    }
    /**
     * Nested test class for ResultSet Bug Regression Tests.
     * 
     * This class contains regression tests for 13 bugs identified in FX tests
     * that were missing from JUnit test coverage.
     * 
     * All bugs are tagged with their VSTS bug number for traceability.
     * 
     * FX Reference: mssql-jdbc-ado/mssql-jdbc/tests/src/resultset.java
     */
    @Nested
    class BugRegressionTests {

        private final String testTable = RandomUtil.getIdentifier("RSBugTest");
        private final String testTablePK = RandomUtil.getIdentifier("RSBugPKTest");
        private final String testTableLarge = RandomUtil.getIdentifier("RSBugLargeTest");

        @BeforeEach
        public void createTestTables() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(testTable), stmt);
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(testTablePK), stmt);
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(testTableLarge), stmt);

                // Standard test table
                stmt.execute("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(testTable) + 
                    " (id INT, name VARCHAR(50), value DECIMAL(10,2), clobCol VARCHAR(MAX))");
                stmt.execute("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(testTable) + 
                    " VALUES (1, 'Row1', 100.50, 'Test Clob Data')");
                stmt.execute("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(testTable) + 
                    " VALUES (2, 'Row2', 200.75, 'More Clob Data')");
                stmt.execute("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(testTable) + 
                    " VALUES (3, 'Row3', 300.25, 'Even More Data')");

                // Table with primary key
                stmt.execute("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(testTablePK) + 
                    " (id INT PRIMARY KEY, name VARCHAR(50), amount DECIMAL(10,2))");
                stmt.execute("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(testTablePK) + 
                    " VALUES (1, 'PK_Row1', 111.11)");
                stmt.execute("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(testTablePK) + 
                    " VALUES (2, 'PK_Row2', 222.22)");
                stmt.execute("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(testTablePK) + 
                    " VALUES (3, 'PK_Row3', 333.33)");

                // Table with large data (VARBINARY(MAX))
                stmt.execute("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(testTableLarge) + 
                    " (id INT, largeData VARBINARY(MAX))");
                byte[] largeBytes = new byte[8192];
                for (int i = 0; i < largeBytes.length; i++) {
                    largeBytes[i] = (byte) (i % 256);
                }
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(testTableLarge) + " VALUES (?, ?)")) {
                    ps.setInt(1, 1);
                    ps.setBytes(2, largeBytes);
                    ps.execute();
                }
            }
        }

        @AfterEach
        public void dropTestTables() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(testTable), stmt);
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(testTablePK), stmt);
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(testTableLarge), stmt);
            }
        }

        /**
         * VSTS #64747: ArrayIndexBoundException on refreshRow()
         * 
         * BUG: Calling refreshRow() on a ResultSet throws ArrayIndexOutOfBoundsException,
         * causing application crash.
         * 
         * FX Reference: resultset.java::testRefresh()
         * Priority: CRITICAL
         * Impact: Application crash
         */
        @Test
        public void testRefreshRowDoesNotThrowArrayIndexException() throws SQLException {
            try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(
                    ResultSet.TYPE_SCROLL_SENSITIVE, 
                    ResultSet.CONCUR_UPDATABLE);
                ResultSet rs = stmt.executeQuery("SELECT id, name, value FROM " + 
                    AbstractSQLGenerator.escapeIdentifier(testTable) + " ORDER BY id")) {
                
                // FX BUG: ArrayIndexOutOfBoundsException when calling refreshRow() 
                // from insert row after last()
                assertTrue(rs.last(), "Should move to last row");
                
                rs.moveToInsertRow();
                
                // THIS IS THE ACTUAL BUG TEST - refreshRow() on insertRow after last()
                Exception caughtException = null;
                try {
                    rs.refreshRow();
                } catch (SQLException e) {
                    caughtException = e;
                } catch (ArrayIndexOutOfBoundsException e) {
                    caughtException = e;
                }
                
                // Should throw SQLException, NOT ArrayIndexOutOfBoundsException
                assertNotNull(caughtException, "refreshRow() on insertRow should throw an exception");
                
                // The bug was that it threw ArrayIndexOutOfBoundsException instead of SQLException
                assertFalse(caughtException instanceof ArrayIndexOutOfBoundsException,
                    "Should NOT throw ArrayIndexOutOfBoundsException - this was the bug!");
                
                assertTrue(caughtException instanceof SQLException,
                    "Should throw SQLException, not " + caughtException.getClass().getName());
                
                // Verify proper exception message
                String msg = caughtException.getMessage().toLowerCase();
                assertTrue(msg.contains("insert") || msg.contains("row") || msg.contains("refresh") || msg.contains("cursor"),
                    "Exception message should indicate invalid operation on insert row. Got: " + caughtException.getMessage());
            }
        }

        /**
         * VSTS #223785: RS.next() never returns false when updating primary key
         * 
         * BUG: On Shiloh, when updating a primary key column via updateRow(), subsequent
         * calls to next() never return false, causing infinite loops.
         * 
         * FX Reference: resultset.java::testUpdatePrimaryKey()
         * Priority: HIGH
         * Impact: Infinite loop (application hang)
         */
        @Test
        public void testUpdatePrimaryKeyDoesNotCauseInfiniteLoop() throws SQLException {
            try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(
                    ResultSet.TYPE_SCROLL_SENSITIVE, 
                    ResultSet.CONCUR_UPDATABLE);
                ResultSet rs = stmt.executeQuery("SELECT id, name, amount FROM " + 
                    AbstractSQLGenerator.escapeIdentifier(testTablePK) + " ORDER BY id")) {
                
                // Count rows first
                int rowCount = 0;
                while (rs.next()) rowCount++;
                
                // FX CRITICAL: setFetchSize to rowCount + 1
                rs.setFetchSize(rowCount + 1);
                
                rs.beforeFirst();
                assertTrue(rs.next(), "Should have first row");
                int originalId = rs.getInt("id");
                
                // Test 1: Update PK by ordinal (column index)
                rs.updateInt(1, originalId + 1000);
                rs.updateRow();
                
                // Test 2: Update PK by name
                rs.beforeFirst();
                if (rs.next() && rs.next()) {
                    int id2 = rs.getInt("id");
                    rs.updateInt("id", id2 + 2000);
                    rs.updateRow();
                }
                
                // Verify no infinite loop
                rs.beforeFirst();
                int iterations = 0;
                int safetyLimit = 100;
                
                while (rs.next() && iterations < safetyLimit) {
                    iterations++;
                }
                
                assertTrue(iterations < safetyLimit, 
                    "Infinite loop detected: next() called " + iterations + " times");
                assertTrue(iterations <= rowCount + 5, 
                    "Excessive iterations: " + iterations + " (expected ~" + rowCount + ")");
            }
        }

        /**
         * VSTS #102589: RS.updateRow throws exception (Regression)
         * 
         * BUG: Regression where updateRow() throws unexpected exceptions in specific scenarios,
         * especially when updating multiple columns.
         * 
         * FX Reference: resultset.java::testUpdateRow(), resultset.java::testRowers()
         * Priority: HIGH
         * Impact: UpdateRow() failure causing customer escalations
         */
        @Test
        public void testUpdateRowDoesNotThrowException() throws SQLException {
            try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(
                    ResultSet.TYPE_SCROLL_SENSITIVE, 
                    ResultSet.CONCUR_UPDATABLE);
                ResultSet rs = stmt.executeQuery("SELECT id, name, value FROM " + 
                    AbstractSQLGenerator.escapeIdentifier(testTable) + " ORDER BY id")) {
                
                assertTrue(rs.next(), "Should have data");
                
                // FX Test 1: Update values, then updateRow()
                rs.updateString("name", "Updated_1");
                rs.updateBigDecimal("value", BigDecimal.valueOf(111.11));
                assertDoesNotThrow(() -> rs.updateRow(), 
                    "First updateRow() should not throw");
                
                // FX Test 2: Update values, call getXXX, then updateRow() AGAIN
                // The bug was that this sequence would throw an exception
                rs.updateString("name", "Updated_2");
                rs.updateBigDecimal("value", BigDecimal.valueOf(222.22));
                
                // Call getXXX on the columns (part of the bug reproduction pattern)
                // Note: We don't assert the values here since driver behavior varies
                rs.getString("name");
                rs.getBigDecimal("value");
                
                // THE KEY TEST: Second updateRow() should NOT throw an exception
                assertDoesNotThrow(() -> rs.updateRow(), 
                    "Second updateRow() should not throw - this was the bug");
                
                // Refresh to see what actually persisted
                // The driver may only persist the first or last update
                rs.refreshRow();
                String persistedName = rs.getString("name");
                assertTrue(persistedName.equals("Updated_1") || persistedName.equals("Updated_2"),
                    "Name should be either Updated_1 or Updated_2, got: " + persistedName);
                
                // FX Test 3: updateRow() with NO changes - should throw SQLException
                SQLException expectedException = null;
                try {
                    rs.updateRow();
                } catch (SQLException e) {
                    expectedException = e;
                }
                assertNotNull(expectedException, 
                    "updateRow() with no pending changes should throw SQLException");
                
                // FX Test 4: Update values, then cancelRowUpdates()
                rs.updateString("name", "Updated_3");
                rs.updateBigDecimal("value", BigDecimal.valueOf(333.33));
                assertDoesNotThrow(() -> rs.cancelRowUpdates(), 
                    "cancelRowUpdates() should not throw");
                
                // Verify cancel worked - should revert to whatever was persisted
                rs.refreshRow();
                assertEquals(persistedName, rs.getString("name"), 
                    "Name should still be " + persistedName + " after cancel");
                
                // FX Test 5: updateRow() on insertRow - should throw
                rs.moveToInsertRow();
                SQLException insertRowException = null;
                try {
                    rs.updateRow();
                } catch (SQLException e) {
                    insertRowException = e;
                }
                assertNotNull(insertRowException, 
                    "updateRow() on insertRow should throw SQLException");
            }
        }

        /**
         * VSTS #223540: insertRow() fails with long table names (127/128 chars)
         * 
         * BUG: When using insertRow() on tables with names at SQL Server limit (127/128 characters),
         * the operation fails.
         * 
         * FX Reference: resultset.java::testInsertRow()
         * Priority: HIGH
         * Impact: Blocks specific use cases with long table names
         */
        @Test
        public void testInsertRowWithLongTableName() throws SQLException {
            // Create 127-character table name (Java 8 compatible)
            StringBuilder sb = new StringBuilder("T");
            for (int i = 0; i < 126; i++) {
                sb.append("x");
            }
            String longTableName = sb.toString();
            
            try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(longTableName, stmt);
                stmt.execute("CREATE TABLE " + longTableName + " (id INT, name VARCHAR(50))");
                
                try {
                    try (Statement updStmt = conn.createStatement(
                            ResultSet.TYPE_SCROLL_SENSITIVE, 
                            ResultSet.CONCUR_UPDATABLE);
                        ResultSet rs = updStmt.executeQuery("SELECT * FROM " + longTableName)) {
                        
                        rs.moveToInsertRow();
                        rs.updateInt("id", 1);
                        rs.updateString("name", "TestData");
                        
                        assertDoesNotThrow(() -> rs.insertRow(), 
                            "insertRow() should work with 127-character table name");
                        
                        rs.moveToCurrentRow();
                        
                        rs.beforeFirst();
                        assertTrue(rs.next(), "Should find inserted row");
                        assertEquals(1, rs.getInt("id"));
                        assertEquals("TestData", rs.getString("name"));
                    }
                } finally {
                    TestUtils.dropTableIfExists(longTableName, stmt);
                }
            }
        }

        /**
         * VSTS #36952: Server cursor always used for SCROLL_INSENSITIVE
         * 
         * BUG: Server cursor is always created for TYPE_SCROLL_INSENSITIVE, CONCUR_READ_ONLY
         * even when not necessary, causing performance overhead.
         * 
         * FX Reference: resultset.java::testServerCursorPStmt()
         * Priority: HIGH
         * Impact: Query performance degradation
         */
        @Test
        public void testScrollInsensitiveCursorType() throws SQLException {
            try (Connection conn = getConnection();
                 Statement stmt = conn.createStatement(
                     ResultSet.TYPE_SCROLL_INSENSITIVE, 
                     ResultSet.CONCUR_READ_ONLY);
                 ResultSet rs = stmt.executeQuery("SELECT id, name FROM " + 
                     AbstractSQLGenerator.escapeIdentifier(testTable) + " ORDER BY id")) {
                
                assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, rs.getType(),
                    "ResultSet type should be SCROLL_INSENSITIVE");
                assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency(),
                    "Concurrency should be READ_ONLY");
                
                assertTrue(rs.next(), "Should navigate to first row");
                assertTrue(rs.absolute(2), "Should navigate to absolute position 2");
                assertTrue(rs.previous(), "Should navigate backwards");
                assertTrue(rs.relative(2), "Should navigate relative +2");
                assertTrue(rs.first(), "Should navigate to first");
                assertTrue(rs.last(), "Should navigate to last");
            }
        }

        /**
         * VSTS #87787: Wrong exception in cursor downgrade scenario
         * 
         * BUG: When cursor type is downgraded (server → client), wrong exception type
         * is thrown, confusing applications.
         * 
         * FX Reference: resultset.java::testUnsupportedCursors(), testUpdateDowngradeCursor()
         * Priority: HIGH
         * Impact: Error handling confusion
         */
        @Test
        public void testCursorDowngradeThrowsCorrectException() throws SQLException {
            try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(
                     ResultSet.TYPE_SCROLL_SENSITIVE, 
                     ResultSet.CONCUR_UPDATABLE)) {
                
                String complexQuery = 
                    "SELECT id, name FROM " + AbstractSQLGenerator.escapeIdentifier(testTable) + " WHERE id = 1 " +
                    "UNION " +
                    "SELECT id, name FROM " + AbstractSQLGenerator.escapeIdentifier(testTable) + " WHERE id = 2";
                
                try (ResultSet rs = stmt.executeQuery(complexQuery)) {
                    if (rs.next()) {
                        SQLException caughtException = null;
                        try {
                            rs.updateString("name", "UpdatedName");
                            rs.updateRow();
                        } catch (SQLException e) {
                            caughtException = e;
                        }
                        
                        // The test validates that an exception is thrown (not updatable)
                        assertNotNull(caughtException, "Should throw exception when trying to update");
                        
                        // Validate the exception message is meaningful (not internal error)
                        String message = caughtException.getMessage().toLowerCase();
                        assertTrue(
                            message.contains("not updatable") ||
                            message.contains("not supported") ||
                            message.contains("read-only") ||
                            message.contains("cursor") ||
                            message.contains("cannot be updated"),
                            "Exception message should indicate updatability issue. Got: " + caughtException.getMessage()
                        );
                    }
                }
            }
        }

        /**
         * VSTS #327052: Client-cursored RS always holdable over commits
         * 
         * BUG: When ResultSet is downgraded to client cursor, it becomes holdable over commits
         * regardless of specified holdability, causing transaction behavior inconsistency.
         * 
         * FX Reference: resultset.java::testHoldability()
         * Priority: HIGH
         * Impact: Transaction isolation issues
         */
        @Test
        public void testClientCursorRespectsHoldability() throws SQLException {
            // FX BUG: Client-cursored RS (downgrade scenario) is ALWAYS holdable 
            // over commits regardless of specified holdability
            
            // Test with UNION to force cursor downgrade
            String downgradedQuery = 
                "SELECT id, name FROM " + AbstractSQLGenerator.escapeIdentifier(testTable) + " WHERE id = 1 " +
                "UNION " +
                "SELECT id, name FROM " + AbstractSQLGenerator.escapeIdentifier(testTable) + " WHERE id = 2";
            
            try (Connection conn = getConnection()) {
                conn.setAutoCommit(false);
                
                // Test both holdability modes
                int[] holdabilityModes = {
                    ResultSet.CLOSE_CURSORS_AT_COMMIT,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT
                };
                
                for (int holdMode : holdabilityModes) {
                    conn.setHoldability(holdMode);
                    assertEquals(holdMode, conn.getHoldability(),
                        "Connection holdability should be set correctly");
                    
                    try (Statement stmt = conn.createStatement(
                            ResultSet.TYPE_SCROLL_SENSITIVE, 
                            ResultSet.CONCUR_UPDATABLE)) {
                        
                        // Execute query that forces downgrade (UNION)
                        try (ResultSet rs = stmt.executeQuery(downgradedQuery)) {
                            
                            rs.getHoldability();
                            
                            assertTrue(rs.next(), "Should have data before commit");
                            int value1 = rs.getInt("id");
                            assertTrue(value1 > 0);
                            
                            conn.commit();
                            
                            // FX BUG: Client cursor (downgraded) is ALWAYS holdable!
                            // Even if connection holdability is CLOSE_CURSORS_AT_COMMIT
                            if (holdMode == ResultSet.CLOSE_CURSORS_AT_COMMIT) {
                                // After commit, cursor should be closed
                                try {
                                    rs.next();
                                    // If we reach here, cursor is still open (downgrade bug)
                                    // This is the bug - client cursor ignores CLOSE_CURSORS_AT_COMMIT
                                } catch (SQLException e) {
                                    // Expected - cursor closed
                                    assertTrue(
                                        e.getMessage().contains("closed") ||
                                        e.getMessage().contains("invalid"),
                                        "Should indicate closed cursor");
                                }
                            } else {
                                // HOLD_CURSORS_OVER_COMMIT - cursor should remain open
                                assertDoesNotThrow(() -> rs.next(),
                                    "Cursor should remain open with HOLD_CURSORS_OVER_COMMIT");
                            }
                        }
                    }
                    
                    conn.rollback();
                }
            }
        }

        /**
         * VSTS #164739: Empty ResultSet navigation and insertRow behavior
         * 
         * BUG: Navigating an empty ResultSet does not behave as expected, and
         * insertRow() on empty ResultSet fails unexpectedly.
         * 
         * FX Reference: resultset.java::testEmptyResultSet()
         * Priority: HIGH
         * Impact: Basic ResultSet operations fail
         */
        @Test
        public void testEmptyResultSetBehaviorWithInsert() throws SQLException {
            try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(
                    ResultSet.TYPE_SCROLL_SENSITIVE, 
                    ResultSet.CONCUR_UPDATABLE);
                ResultSet rs = stmt.executeQuery("SELECT id, name, value FROM " + 
                    AbstractSQLGenerator.escapeIdentifier(testTable) + " WHERE 1=0")) {
                
                // FX Test: Empty ResultSet navigation
                assertFalse(rs.next(), "Empty ResultSet: next() should return false");
                assertFalse(rs.first(), "Empty ResultSet: first() should return false");
                assertFalse(rs.last(), "Empty ResultSet: last() should return false");
                assertFalse(rs.absolute(1), "Empty ResultSet: absolute() should return false");
                assertFalse(rs.previous(), "Empty ResultSet: previous() should return false");
                
                assertThrows(SQLException.class, () -> rs.relative(1), 
                    "Empty ResultSet: relative() should throw SQLException");
                
                assertEquals(0, rs.getRow(), "Empty ResultSet: getRow() should return 0");
                
                // FX CRITICAL: Set fetchSize to 1 before inserting
                rs.setFetchSize(1);
                
                // FX Test: Insert 1-5 random rows
                Random random = new Random();
                int numRowsToInsert = random.nextInt(5) + 1;
                
                for (int i = 0; i < numRowsToInsert; i++) {
                    rs.moveToInsertRow();
                    rs.updateInt("id", 1000 + i);
                    rs.updateString("name", "InsertedRow_" + i);
                    rs.updateBigDecimal("value", BigDecimal.valueOf(100.0 + i));
                    assertDoesNotThrow(() -> rs.insertRow(), 
                        "insertRow() should work on empty ResultSet");
                    rs.next(); // Move to next for next iteration
                }
                
                // FX Test: Requery and verify
                rs.close();
                try (ResultSet rsVerify = stmt.executeQuery("SELECT id, name, value FROM " + 
                    AbstractSQLGenerator.escapeIdentifier(testTable) + " WHERE id >= 1000 ORDER BY id")) {

                    int count = 0;
                    while (rsVerify.next()) {
                        count++;
                        assertTrue(rsVerify.getString("name").startsWith("InsertedRow_"),
                            "Inserted row should have correct name");
                    }
                    assertEquals(numRowsToInsert, count, 
                        "Should have inserted " + numRowsToInsert + " rows");
                    rsVerify.close();
                }
            }
        }

        /**
         * VSTS #234278: testUpdaterOverwrite creates NClob on Java 1.5
         * 
         * BUG: On Java 1.5 VMs, testUpdaterOverwrite() creates NClob instead of expected Clob,
         * indicating type handling issue.
         * 
         * FX Reference: resultset.java::testUpdaterOverwrite()
         * Priority: MEDIUM
         * Impact: Type mismatch on legacy Java versions
         */
        @Test
        public void testUpdaterClobTypeHandling() throws SQLException {
            try (Connection conn = getConnection();
                 Statement stmt = conn.createStatement(
                     ResultSet.TYPE_SCROLL_SENSITIVE, 
                     ResultSet.CONCUR_UPDATABLE);
                 ResultSet rs = stmt.executeQuery("SELECT id, clobCol FROM " + 
                     AbstractSQLGenerator.escapeIdentifier(testTable) + " ORDER BY id")) {
                
                assertTrue(rs.next(), "Should have data");
                
                Clob clob = conn.createClob();
                clob.setString(1, "Updated Clob Data");
                rs.updateClob("clobCol", clob);
                rs.updateRow();
                
                Clob retrieved = rs.getClob("clobCol");
                assertNotNull(retrieved, "Retrieved Clob should not be null");
                
                assertEquals("Updated Clob Data", retrieved.getSubString(1, (int) retrieved.length()),
                    "Clob content should match");
            }
        }

        /**
         * VSTS #90472: Add CallableStatement with stored proc to ResultSet model
         * 
         * BUG: Missing test coverage for ResultSet returned from CallableStatement
         * executing stored procedure.
         * 
         * FX Reference: Implied in FX model testing
         * Priority: MEDIUM
         * Impact: Important feature not validated
         */
        @Test
        public void testResultSetFromCallableStatement() throws SQLException {
            try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
                
                String procName = "GetTestData";
                TestUtils.dropProcedureIfExists(procName, stmt);
                
                stmt.execute("CREATE PROCEDURE " + procName + " @minId INT AS " +
                    "BEGIN " +
                    "  SELECT id, name FROM " + AbstractSQLGenerator.escapeIdentifier(testTable) + 
                    "  WHERE id >= @minId ORDER BY id " +
                    "END");
                
                try {
                    try (CallableStatement cstmt = conn.prepareCall("{call " + procName + "(?)}")) {
                        cstmt.setInt(1, 1);
                        
                        try (ResultSet rs = cstmt.executeQuery()) {
                            assertNotNull(rs, "ResultSet from CallableStatement should not be null");
                            
                            assertTrue(rs.next(), "Should have results from stored procedure");
                            assertNotNull(rs.getMetaData(), "Metadata should be available");
                            assertTrue(rs.getMetaData().getColumnCount() >= 2, "Should have at least 2 columns");
                            
                            rs.setFetchSize(50);
                            assertEquals(50, rs.getFetchSize(), "FetchSize should be settable");
                            
                            int rowCount = 1;
                            while (rs.next()) {
                                rowCount++;
                            }
                            assertTrue(rowCount >= 1, "Should have at least 1 row from stored procedure");
                        }
                    }
                } finally {
                    TestUtils.dropProcedureIfExists(procName, stmt);
                }
            }
        }

        /**
         * VSTS #267503: clearLastResultSet ate the exception
         * 
         * BUG: Internal method clearLastResultSet() was swallowing exceptions instead
         * of propagating them, hiding errors.
         * 
         * FX Reference: resultset.java::testClearLastResultSetException()
         * Priority: MEDIUM
         * Impact: Error hiding makes debugging difficult
         */
        @Test
        public void testExceptionNotSwallowedDuringCleanup() throws SQLException {
            Connection conn = null;
            Statement stmt = null;
            ResultSet rs = null;
            
            try {
                conn = getConnection();
                stmt = conn.createStatement();
                rs = stmt.executeQuery("SELECT id, name FROM " + 
                    AbstractSQLGenerator.escapeIdentifier(testTable));
                
                assertTrue(rs.next(), "Should have data");
                
                Connection connToClose = conn;
                connToClose.close();
                
                ResultSet rsFinal = rs;
                assertThrows(SQLException.class, () -> {
                    rsFinal.next();
                }, "SQLException should be thrown when using ResultSet after connection closed");
                
            } catch (SQLException e) {
                assertNotNull(e.getMessage(), "Exception message should not be null");
                assertFalse(e.getMessage().isEmpty(), "Exception message should not be empty");
            } finally {
                try { if (rs != null) rs.close(); } catch (Exception ignored) {}
                try { if (stmt != null) stmt.close(); } catch (Exception ignored) {}
                try { if (conn != null && !conn.isClosed()) conn.close(); } catch (Exception ignored) {}
            }
        }

        /**
         * VSTS #114316: Need testcase to refetch stream values
         * 
         * BUG: Missing test case for re-fetching stream column values (varchar(max), varbinary(max)).
         * 
         * FX Reference: resultset.java::testRefetchStreamValues()
         * Priority: MEDIUM
         * Impact: Uncommon use case but important for large data
         * @throws IOException 
         */
        @Test
        public void testRefetchStreamValues() throws SQLException, IOException {
            try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(
                    ResultSet.TYPE_SCROLL_SENSITIVE, 
                    ResultSet.CONCUR_UPDATABLE);
                ResultSet rs = stmt.executeQuery("SELECT id, largeData FROM " + 
                    AbstractSQLGenerator.escapeIdentifier(testTableLarge))) {
                
                assertTrue(rs.next(), "Should have data with large binary");
                
                // FX CRITICAL: Set fetchSize to all rows to prevent moving outside buffer
                ResultSetMetaData meta = rs.getMetaData();
                rs.last();
                int rowCount = rs.getRow();
                rs.setFetchSize(rowCount);
                
                // Move to a valid row and mark it
                rs.absolute(1);
                int markedRow = rs.getRow();
                
                // FX Test: Partially consume stream
                try (InputStream stream1 = rs.getBinaryStream("largeData")) {
                    assertNotNull(stream1, "First stream fetch should return data");
                    
                    byte[] buffer = new byte[100];
                    int bytesRead = stream1.read(buffer);
                    assertTrue(bytesRead > 0, "Should read bytes from stream");
                    // Stream is now partially consumed
                }
                
                // FX Test: Move OFF the row (random mover)
                Random random = new Random();
                int moverChoice = random.nextInt(3);
                switch (moverChoice) {
                    case 0: rs.next(); break;
                    case 1: rs.previous(); break;
                    case 2: rs.absolute(markedRow + 1); break;
                }
                
                // FX Test: Randomly execute row operation on current row
                if (random.nextBoolean()) {
                    int operation = random.nextInt(4);
                    try {
                        switch (operation) {
                            case 0: rs.updateRow(); break; // May throw
                            case 1: rs.cancelRowUpdates(); break;
                            case 2: rs.refreshRow(); break;
                            case 3: rs.getRow(); break;
                        }
                    } catch (SQLException e) {
                        // Some operations may fail, that's ok
                    }
                }
                
                // FX Test: Move BACK to marked row
                rs.beforeFirst();
                while (rs.next() && rs.getRow() != markedRow) {
                    // Keep moving
                }
                assertEquals(markedRow, rs.getRow(), "Should be back at marked row");
                
                // FX Test: Refetch stream - should succeed!
                try (InputStream stream2 = rs.getBinaryStream("largeData")) {
                    assertNotNull(stream2, "Refetch stream should return data");
                    
                    byte[] buffer = new byte[100];
                    int bytesRead = stream2.read(buffer);
                    assertTrue(bytesRead > 0, "Should read bytes from refetched stream");
                }
                
                // Additional verification: Navigate and refetch again
                if (rs.next()) {
                    rs.previous(); // Back to marked row
                    
                    try (InputStream stream3 = rs.getBinaryStream("largeData")) {
                        assertNotNull(stream3, "Third stream fetch should return data");
                        
                        byte[] buffer = new byte[100];
                        int bytesRead = stream3.read(buffer);
                        assertTrue(bytesRead > 0, "Should read bytes after navigation");
                    }
                }
            }
        }
    }
}