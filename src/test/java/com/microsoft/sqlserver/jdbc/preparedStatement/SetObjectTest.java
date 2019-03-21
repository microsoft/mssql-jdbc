/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.preparedStatement;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.OffsetTime;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;

import microsoft.sql.DateTimeOffset;


@RunWith(JUnitPlatform.class)
public class SetObjectTest extends AbstractTest {
    private static final String tableName = RandomUtil.getIdentifier("SetObjectTestTable");

    /**
     * Tests setObject(n, java.time.OffsetDateTime.class).
     * 
     * @throws SQLException
     */
    @Test
    public void testSetObjectWithOffsetDateTime() throws SQLException {
        try (Connection con = DriverManager.getConnection(connectionString)) {
            final String testValue = "2018-01-02T11:22:33.123456700-12:34";
            final String testValue2 = "2018-01-02T11:22:33Z";
            Instant ist = Instant.parse(testValue2);
            ist = ist.plusNanos(123456700);
            DateTimeOffset dto = DateTimeOffset.valueOf(Timestamp.from(ist), -754);
            try (Statement stmt = con.createStatement()) {
                stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                        + " (id INT PRIMARY KEY, dto DATETIMEOFFSET)");
                try {
                    try (PreparedStatement pstmt = con.prepareStatement("INSERT INTO "
                            + AbstractSQLGenerator.escapeIdentifier(tableName) + " (id, dto) VALUES (?, ?)")) {
                        pstmt.setInt(1, 1);
                        pstmt.setObject(2, OffsetDateTime.parse(testValue));
                        pstmt.executeUpdate();
                        
                        pstmt.setInt(1, 2);
                        pstmt.setObject(2, dto);
                        pstmt.executeUpdate();
                    }

                    try (ResultSet rs = stmt
                            .executeQuery("SELECT COUNT(*) FROM " + AbstractSQLGenerator.escapeIdentifier(tableName)
                                    + " WHERE dto = '" + testValue + "'")) {
                        rs.next();
                        assertEquals(2, rs.getInt(1));
                    }
                    
                    try (SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM "
                            + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                        rs.next();
                        assertEquals(dto, rs.getDateTimeOffset(2));
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
            final String testValue2 = "1970-01-01T11:22:33Z";
            Instant ist = Instant.parse(testValue2);
            ist = ist.plusNanos(123456700);
            DateTimeOffset dto = DateTimeOffset.valueOf(Timestamp.from(ist), 754);
            try (Statement stmt = con.createStatement()) {
                stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                        + " (id INT PRIMARY KEY, dto DATETIMEOFFSET)");
                try {
                    try (PreparedStatement pstmt = con.prepareStatement("INSERT INTO "
                            + AbstractSQLGenerator.escapeIdentifier(tableName) + " (id, dto) VALUES (?, ?)")) {
                        pstmt.setInt(1, 1);
                        pstmt.setObject(2, OffsetTime.parse(testValue));
                        pstmt.executeUpdate();
                        
                        pstmt.setInt(1, 2);
                        pstmt.setObject(2, DateTimeOffset.valueOf(Timestamp.from(ist), 754));
                        pstmt.executeUpdate();
                    }

                    try (ResultSet rs = stmt
                            .executeQuery("SELECT COUNT(*) FROM " + AbstractSQLGenerator.escapeIdentifier(tableName)
                                    + " WHERE dto = '" + expectedDto + "'")) {
                        rs.next();
                        assertEquals(2, rs.getInt(1));
                    }
                    
                    try (SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM "
                            + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                        rs.next();
                        assertEquals(dto, rs.getDateTimeOffset(2));
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
            final String testValue2 = "1900-01-01T11:22:33Z";
            Instant ist = Instant.parse(testValue2);
            ist = ist.plusNanos(123456700);
            DateTimeOffset dto = DateTimeOffset.valueOf(Timestamp.from(ist), 754);
            try (Statement stmt = con.createStatement()) {
                stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                        + " (id INT PRIMARY KEY, dto DATETIMEOFFSET)");
                try {
                    try (PreparedStatement pstmt = con.prepareStatement("INSERT INTO "
                            + AbstractSQLGenerator.escapeIdentifier(tableName) + " (id, dto) VALUES (?, ?)")) {
                        pstmt.setInt(1, 1);
                        pstmt.setObject(2, OffsetTime.parse(testValue));
                        pstmt.executeUpdate();
                        
                        pstmt.setInt(1, 2);
                        pstmt.setObject(2, DateTimeOffset.valueOf(Timestamp.from(ist), 754));
                        pstmt.executeUpdate();
                    }

                    try (ResultSet rs = stmt
                            .executeQuery("SELECT COUNT(*) FROM " + AbstractSQLGenerator.escapeIdentifier(tableName)
                                    + " WHERE dto = '" + expectedDto + "'")) {
                        rs.next();
                        assertEquals(2, rs.getInt(1));
                    }
                    
                    try (SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM "
                            + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                        rs.next();
                        assertEquals(dto, rs.getDateTimeOffset(2));
                    }
                } finally {
                    TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
                }
            }
        }
    }
}
