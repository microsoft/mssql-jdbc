/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.resultset;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.TimeZone;
import java.util.UUID;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ISQLServerResultSet;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;


@RunWith(JUnitPlatform.class)
public class ResultSetTest extends AbstractTest {
    private static final String tableName = RandomUtil.getIdentifier("StatementParam");

    static final String uuid = UUID.randomUUID().toString();

    /**
     * Tests proper exception for unsupported operation
     * 
     * @throws SQLException
     */
    @Test
    public void testJdbc41ResultSetMethods() throws SQLException {
        try (Connection con = DriverManager.getConnection(connectionString); Statement stmt = con.createStatement()) {
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
                stmt.executeUpdate("drop table " + AbstractSQLGenerator.escapeIdentifier(tableName));
            }
        }
    }

    /**
     * Tests getObject(n, java.time.LocalDateTime.class).
     * 
     * @throws SQLException
     */
    @Test
    @Tag("AzureDWTest")
    public void testGetObjectAsLocalDateTime() throws SQLException {
        try (Connection con = DriverManager.getConnection(connectionString); Statement stmt = con.createStatement()) {
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
    public void testGetObjectAsOffsetDateTime() throws SQLException {
        try (Connection con = DriverManager.getConnection(connectionString); Statement stmt = con.createStatement()) {
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
    @Tag("AzureDWTest")
    public void testResultSetWrapper() throws SQLException {
        try (Connection con = DriverManager.getConnection(connectionString); Statement stmt = con.createStatement()) {

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
    @Tag("AzureDWTest")
    public void testGetterOnNull() throws SQLException {
        try (Connection con = DriverManager.getConnection(connectionString); Statement stmt = con.createStatement();
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
    public void testGetSetHoldability() throws SQLException {
        int[] holdabilityOptions = {ResultSet.HOLD_CURSORS_OVER_COMMIT, ResultSet.CLOSE_CURSORS_AT_COMMIT};

        try (Connection con = DriverManager.getConnection(connectionString); Statement stmt = con.createStatement();
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
    public void testResultSetMethods() throws SQLException {
        try (Connection con = DriverManager.getConnection(connectionString);
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
                fail(e.toString());
            } finally {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            }
        }
    }
}
