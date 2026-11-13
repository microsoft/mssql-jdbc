/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.preparedStatement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.OffsetTime;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;

import microsoft.sql.DateTimeOffset;


@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDW)
public class SetObjectTest extends AbstractTest {
    private static final String tableName = RandomUtil.getIdentifier("SetObjectTestTable");

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @Test
    public void testDateTimeOffsetMetadataRoundTripAndUpperBound() throws SQLException {
        OffsetDateTime upperBound = OffsetDateTime.parse("9999-12-31T23:59:59.999999999Z");
        OffsetDateTime expected = OffsetDateTime.parse("9999-12-31T23:59:59.999999900Z");
        try (Connection con = getConnection(); Statement stmt = con.createStatement();
                ResultSet source = stmt.executeQuery("SELECT CAST('2024-01-02T03:04:05+05:30' AS datetimeoffset)")) {
            assertTrue(source.next());
            Object value = source.getObject(1);
            assertEquals(DateTimeOffset.class.getName(), source.getMetaData().getColumnClassName(1));
            assertTrue(value instanceof DateTimeOffset);
            try (PreparedStatement copy = con.prepareStatement("SELECT CAST(? AS datetimeoffset)")) {
                copy.setObject(1, value, source.getMetaData().getColumnType(1));
                try (ResultSet result = copy.executeQuery()) {
                    assertTrue(result.next());
                    assertEquals(value, result.getObject(1));
                }
                copy.setObject(1, value, java.sql.Types.TIMESTAMP_WITH_TIMEZONE, 7);
                try (ResultSet result = copy.executeQuery()) {
                    assertTrue(result.next());
                    assertEquals(value, result.getObject(1));
                }
            }
            try (SQLServerPreparedStatement p = (SQLServerPreparedStatement) con
                    .prepareStatement("SELECT CAST(? AS datetimeoffset)")) {
                p.setOffsetDateTime(1, upperBound);
                try (SQLServerResultSet result = (SQLServerResultSet) p.executeQuery()) {
                    assertTrue(result.next());
                    assertEquals(expected, result.getOffsetDateTime(1));
                }
            }
        }
    }

    @Test
    public void testDateTimeOffsetBulkCopyAndDiscoveryMetadata() throws SQLException {
        String table = AbstractSQLGenerator.escapeIdentifier(tableName);
        OffsetDateTime expected = OffsetDateTime.parse("2024-01-02T03:04:05.0000001+05:30");
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE " + table + " (id int identity primary key, dto datetimeoffset(7))");
            try {
                try (ResultSet metadata = con.getMetaData().getColumns(null, null, tableName, "dto")) {
                    assertTrue(metadata.next());
                    assertEquals(microsoft.sql.Types.DATETIMEOFFSET, metadata.getInt("DATA_TYPE"));
                }
                try (Statement sourceStatement = con.createStatement();
                        ResultSet source = sourceStatement.executeQuery(
                                "SELECT CAST('2024-01-02T03:04:05.0000001+05:30' AS datetimeoffset(7)) AS dto");
                        SQLServerBulkCopy copy = new SQLServerBulkCopy(con)) {
                    copy.setDestinationTableName(table);
                    copy.addColumnMapping("dto", "dto");
                    copy.writeToServer(source);
                }
                try (ResultSet result = stmt.executeQuery("SELECT dto FROM " + table)) {
                    assertTrue(result.next());
                    assertEquals(expected, result.getObject(1, OffsetDateTime.class));
                }
                OffsetDateTime updated = OffsetDateTime.parse("2024-06-07T08:09:10.0000002-04:00");
                try (Statement updateStatement = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                        ResultSet.CONCUR_UPDATABLE);
                        SQLServerResultSet result = (SQLServerResultSet) updateStatement
                                .executeQuery("SELECT id, dto FROM " + table)) {
                    assertTrue(result.next());
                    result.updateOffsetDateTime("dto", updated);
                    result.updateRow();
                }
                try (ResultSet result = stmt.executeQuery("SELECT dto FROM " + table)) {
                    assertTrue(result.next());
                    assertEquals(updated, result.getObject(1, OffsetDateTime.class));
                }
            } finally {
                TestUtils.dropTableIfExists(table, stmt);
            }
        }
    }

    /**
     * Tests setObject(n, java.time.OffsetDateTime.class).
     * 
     * @throws SQLException
     */
    @Test
    public void testSetObjectWithOffsetDateTime() throws SQLException {
        try (Connection con = getConnection()) {
            final String testValue = "2018-01-02T11:22:33.123456700+12:34";
            try (Statement stmt = con.createStatement()) {
                stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                        + " (id INT PRIMARY KEY, dto DATETIMEOFFSET)");
                try {
                    try (PreparedStatement pstmt = con.prepareStatement("INSERT INTO "
                            + AbstractSQLGenerator.escapeIdentifier(tableName) + " (id, dto) VALUES (?, ?)")) {
                        pstmt.setInt(1, 1);
                        pstmt.setObject(2, OffsetDateTime.parse(testValue));
                        pstmt.executeUpdate();
                    }

                    try (ResultSet rs = stmt
                            .executeQuery("SELECT COUNT(*) FROM " + AbstractSQLGenerator.escapeIdentifier(tableName)
                                    + " WHERE id = 1 AND dto = '" + testValue + "'")) {
                        rs.next();
                        assertEquals(1, rs.getInt(1));
                    }
                } finally {
                    TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
                }
            }
        }
    }

    /**
     * Tests setObject(n, java.time.OffsetTime.class).
     * 
     * @throws SQLException
     */
    @Test
    public void testSetObjectWithOffsetTime() throws SQLException {
        try (Connection con = getConnection()) {
            final String testValue = "11:22:33.123456700+12:34";
            final String expectedDto = "1970-01-01T" + testValue;
            try (Statement stmt = con.createStatement()) {
                stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                        + " (id INT PRIMARY KEY, dto DATETIMEOFFSET)");
                try {
                    try (PreparedStatement pstmt = con.prepareStatement("INSERT INTO "
                            + AbstractSQLGenerator.escapeIdentifier(tableName) + " (id, dto) VALUES (?, ?)")) {
                        pstmt.setInt(1, 1);
                        pstmt.setObject(2, OffsetTime.parse(testValue));
                        pstmt.executeUpdate();
                    }

                    try (ResultSet rs = stmt
                            .executeQuery("SELECT COUNT(*) FROM " + AbstractSQLGenerator.escapeIdentifier(tableName)
                                    + " WHERE id = 1 AND dto = '" + expectedDto + "'")) {
                        rs.next();
                        assertEquals(1, rs.getInt(1));
                    }
                } finally {
                    TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
                }
            }
        }
    }

    /**
     * Tests setObject(n, java.time.OffsetTime.class) when 'setSendTimeAsDatetime' connection property is false.
     * 
     * @throws SQLException
     */
    @Test
    public void testSetObjectWithOffsetTime_sendTimeAsDatetimeDisabled() throws SQLException {
        try (Connection con = getConnection()) {
            ((SQLServerConnection) con).setSendTimeAsDatetime(false);
            final String testValue = "11:22:33.123456700+12:34";
            final String expectedDto = "1900-01-01T" + testValue;
            try (Statement stmt = con.createStatement()) {
                stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                        + " (id INT PRIMARY KEY, dto DATETIMEOFFSET)");
                try {
                    try (PreparedStatement pstmt = con.prepareStatement("INSERT INTO "
                            + AbstractSQLGenerator.escapeIdentifier(tableName) + " (id, dto) VALUES (?, ?)")) {
                        pstmt.setInt(1, 1);
                        pstmt.setObject(2, OffsetTime.parse(testValue));
                        pstmt.executeUpdate();
                    }

                    try (ResultSet rs = stmt
                            .executeQuery("SELECT COUNT(*) FROM " + AbstractSQLGenerator.escapeIdentifier(tableName)
                                    + " WHERE id = 1 AND dto = '" + expectedDto + "'")) {
                        rs.next();
                        assertEquals(1, rs.getInt(1));
                    }
                } finally {
                    TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
                }
            }
        }
    }

    /**
     * Tests DateTimeOffset Conversions when 'setSendTimeAsDatetime' connection property is false.
     * 
     * @throws SQLException
     */
    @Test
    public void testDateTimeOffsetConversions() throws SQLException {
        testDTOConversionsInternal(true);
        testDTOConversionsInternal(false);
    }

    /**
     * Test sending and retrieving of data as OffsetTime, OffsetDateTime, DateTimeOffset or as String interchangeably.
     * 
     * @param sendTimeAsDateTime
     *        Toggles SendTimeAsDatetime connection property on connection
     * @throws SQLException
     */
    private void testDTOConversionsInternal(boolean sendTimeAsDateTime) throws SQLException {
        try (Connection con = getConnection()) {
            ((SQLServerConnection) con).setSendTimeAsDatetime(sendTimeAsDateTime);

            final String date = sendTimeAsDateTime ? "1970-01-01" : "1900-01-01";
            final String time = "11:22:33.123456700";
            final String offset = "+01:00"; // Europe/Paris TimeZone
            final int minutesOffset = 60; // Europe/Paris TimeZone

            final String timestampString = date + " " + time;

            // Fix nanoseconds from SQL DTO
            final String expectedDTOString = timestampString.substring(0, timestampString.length() - 2) + " " + offset;
            final String sqlDto = date + "T" + time + offset; // SQL Server's interpretation of DateTimeOffset
            final OffsetTime offsetTime = OffsetTime.parse(time + offset);
            final OffsetDateTime offsetDateTime = OffsetDateTime.parse(sqlDto);
            final DateTimeOffset dateTimeOffset = DateTimeOffset.valueOf(Timestamp.from(offsetDateTime.toInstant()),
                    minutesOffset);

            try (Statement stmt = con.createStatement()) {
                stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                        + " (id INT PRIMARY KEY, dto DATETIMEOFFSET)");
                try {
                    try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con
                            .prepareStatement("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                                    + " (id, dto) VALUES (?, ?)")) {

                        // Set Object - OffsetTime
                        pstmt.setInt(1, 1);
                        pstmt.setObject(2, offsetTime);
                        pstmt.executeUpdate();

                        // Set Object - OffsetDateTime
                        pstmt.setInt(1, 2);
                        pstmt.setObject(2, offsetDateTime);
                        pstmt.executeUpdate();

                        // Set Object - DateTimeOffset
                        pstmt.setInt(1, 3);
                        pstmt.setObject(2, dateTimeOffset);
                        pstmt.executeUpdate();

                        // Set OffsetDateTime (new API)
                        pstmt.setInt(1, 4);
                        pstmt.setOffsetDateTime(2, offsetDateTime);
                        pstmt.executeUpdate();

                        // Set DateTimeOffset (legacy API)
                        pstmt.setInt(1, 5);
                        pstmt.setDateTimeOffset(2, dateTimeOffset);
                        pstmt.executeUpdate();

                        // Set String
                        pstmt.setInt(1, 6);
                        pstmt.setString(2, expectedDTOString);
                        pstmt.executeUpdate();
                    }

                    String query = "SELECT dto FROM " + AbstractSQLGenerator.escapeIdentifier(tableName);
                    verifyDateTimeOffsetValues(stmt, query, dateTimeOffset, offsetDateTime, expectedDTOString, sqlDto);

                    for (int i = 1; i <= 6; i++) {
                        query = "SELECT dto FROM " + AbstractSQLGenerator.escapeIdentifier(tableName) + " WHERE id = "
                                + i + " AND dto = '" + sqlDto + "'";
                        verifyDateTimeOffsetValues(stmt, query, dateTimeOffset, offsetDateTime, expectedDTOString,
                                sqlDto);
                    }
                } finally {
                    TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
                }
            }
        }
    }

    private void verifyDateTimeOffsetValues(Statement stmt, String query, DateTimeOffset dateTimeOffset,
            OffsetDateTime offsetDateTime, String expectedDTOString, String sqlDto) throws SQLException {
        try (SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery(query)) {
            while (rs.next()) {
                assertEquals(dateTimeOffset, rs.getObject(1));
                assertEquals(expectedDTOString, rs.getObject(1).toString());

                assertEquals(dateTimeOffset, rs.getDateTimeOffset(1));
                assertEquals(expectedDTOString, rs.getDateTimeOffset(1).toString());

                assertEquals(dateTimeOffset, rs.getObject(1, DateTimeOffset.class));
                assertEquals(expectedDTOString, rs.getObject(1, DateTimeOffset.class).toString());

                assertEquals(offsetDateTime, rs.getOffsetDateTime(1));
                assertEquals(sqlDto, rs.getOffsetDateTime(1).toString());

                assertEquals(offsetDateTime, rs.getObject(1, OffsetDateTime.class));
                assertEquals(sqlDto, rs.getObject(1, OffsetDateTime.class).toString());
            }
        }
    }
}
