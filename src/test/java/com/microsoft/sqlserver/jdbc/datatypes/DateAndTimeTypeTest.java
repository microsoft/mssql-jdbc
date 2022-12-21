/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.datatypes;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.opentest4j.TestAbortedException;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerDataTable;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDW)
public class DateAndTimeTypeTest extends AbstractTest {

    private static final Date DATE_TO_TEST = new java.sql.Date(61494793200000L);
    private static final Time TIME_TO_TEST = new java.sql.Time(74096000L);
    private static final Timestamp TIMESTAMP_TO_TEST = new java.sql.Timestamp(61494838496000L);

    private static final String dateTVP = RandomUtil.getIdentifier("dateTVP");
    private static final String timeTVP = RandomUtil.getIdentifier("timeTVP");
    private static final String timestampTVP = RandomUtil.getIdentifier("timestampTVP");
    private static final String tableName = RandomUtil.getIdentifier("DataTypesTable");
    private static final String primaryKeyConstraintName = "pk_" + tableName;

    /**
     * Test query with date
     */
    @Test
    public void testQueryDate() throws SQLException {
        try (Connection connection = PrepUtil.getConnection(connectionString + ";sendTimeAsDatetime=false")) {

            String sPrepStmt = "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " where my_date = ?";
            try (PreparedStatement pstmt = connection.prepareStatement(sPrepStmt)) {
                pstmt.setDate(1, DATE_TO_TEST);

                try (ResultSet rs = pstmt.executeQuery()) {
                    rs.next();
                    assertTrue(rs.getInt(1) == 42, "did not find correct timestamp");
                }
            }
        }
    }

    /**
     * Test query with timestamp
     */
    @Test
    public void testQueryTimestamp() throws SQLException {
        try (Connection connection = PrepUtil.getConnection(connectionString + ";sendTimeAsDatetime=false")) {

            String sPrepStmt = "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " where my_timestamp = ?";
            try (PreparedStatement pstmt = connection.prepareStatement(sPrepStmt)) {
                pstmt.setTimestamp(1, TIMESTAMP_TO_TEST);

                try (ResultSet rs = pstmt.executeQuery()) {
                    rs.next();
                    assertTrue(rs.getInt(1) == 42, "did not find correct timestamp");
                }
            }
        }
    }

    /**
     * Test query with time
     */
    @Test
    public void testQueryTime() throws SQLException {
        try (Connection connection = PrepUtil.getConnection(connectionString + ";sendTimeAsDatetime=false")) {

            String sPrepStmt = "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " where my_time = ?";
            try (PreparedStatement pstmt = connection.prepareStatement(sPrepStmt)) {
                pstmt.setTime(1, TIME_TO_TEST);

                try (ResultSet rs = pstmt.executeQuery()) {
                    rs.next();
                    assertTrue(rs.getInt(1) == 42, "did not find correct timestamp");
                }
            }
        }
    }

    /**
     * Test query with date TVP
     */
    @Test
    public void testQueryDateTVP() throws SQLException {
        try (Connection connection = PrepUtil.getConnection(connectionString + ";sendTimeAsDatetime=false")) {

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c1", java.sql.Types.DATE);
            tvp.addRow(DATE_TO_TEST);
            String sPrepStmt = "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " where my_date IN (select * from ?)";
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                    .prepareStatement(sPrepStmt)) {
                pstmt.setStructured(1, dateTVP, tvp);

                try (ResultSet rs = pstmt.executeQuery()) {
                    rs.next();
                    assertTrue(rs.getInt(1) == 42, "did not find correct timestamp");
                }
            }
        }
    }

    /**
     * Test query with date TVP
     */
    @Test
    public void testQueryTimestampTVP() throws SQLException {
        try (Connection connection = PrepUtil.getConnection(connectionString + ";sendTimeAsDatetime=false")) {

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c1", java.sql.Types.TIMESTAMP);
            tvp.addRow(TIMESTAMP_TO_TEST);
            String sPrepStmt = "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " where my_timestamp IN (select * from ?)";
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                    .prepareStatement(sPrepStmt)) {
                pstmt.setStructured(1, timestampTVP, tvp);

                try (ResultSet rs = pstmt.executeQuery()) {
                    rs.next();
                    assertTrue(rs.getInt(1) == 42, "did not find correct timestamp");
                }
            }
        }
    }

    /**
     * Test query with date TVP
     */
    @Test
    public void testQueryTimeTVP() throws SQLException {
        try (Connection connection = PrepUtil.getConnection(connectionString + ";sendTimeAsDatetime=false")) {

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c1", java.sql.Types.TIME);
            tvp.addRow(TIME_TO_TEST);
            String sPrepStmt = "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " where my_time IN (select * from ?)";
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                    .prepareStatement(sPrepStmt)) {
                ((SQLServerPreparedStatement) pstmt).setStructured(1, timeTVP, tvp);

                try (ResultSet rs = pstmt.executeQuery()) {
                    rs.next();
                    assertTrue(rs.getInt(1) == 42, "did not find correct timestamp");
                }
            }
        }
    }

    private void createTVPs(String tvpName, String tvpType) throws SQLException {
        try (Connection connection = PrepUtil.getConnection(connectionString + ";sendTimeAsDatetime=false");
                Statement stmt = (SQLServerStatement) connection.createStatement()) {

            stmt.executeUpdate("IF EXISTS (SELECT * FROM sys.types WHERE is_table_type = 1 AND name = '"
                    + TestUtils.escapeSingleQuotes(tvpName) + "') " + " drop type "
                    + AbstractSQLGenerator.escapeIdentifier(tvpName));
            String TVPCreateCmd = "CREATE TYPE " + AbstractSQLGenerator.escapeIdentifier(tvpName) + " as table (c1 "
                    + tvpType + " null)";
            stmt.executeUpdate(TVPCreateCmd);
        }
    }

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    /*
     * Test to make sure that a Timestamp is treated as a datetime object.
     */
    @Test
    public void testSendTimestampAsDatetime() throws Exception { 
        String expected = "2010-02-01T23:59:59.997";
        String actual = null;
        String query = "SELECT CONVERT(VARCHAR(40), ?, 126) as [value]";

        try (SQLServerConnection conn = PrepUtil.getConnection(connectionString + ";datetimeParameterType=datetime"); 
            PreparedStatement stmt = conn.prepareStatement(query)) {

            Timestamp ts = Timestamp.valueOf("2010-02-01 23:59:59.996"); // if cast to a datetime, 996ms is rounded up to 997ms

            /*
            * send the timestamp to the server using the TIME SQL type rather than TIMESTAMP. The driver will
            * strip the date portion and, because sendTimeAsDatetime=true, round the resulting time value to
            * midnight because it should be sending a DATETIME which has only 1/300s accuracy
            */
            stmt.setObject(1, ts, java.sql.Types.TIMESTAMP);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                actual = rs.getString("value");
            }

        }

        assertEquals(expected, actual.toString());
    }

    /*
     * Test to make sure that a Timestamp is treated as a datetime2 object.
     */
    @Test
    public void testSendTimestampAsDatetime2() throws Exception { 
        String expected = "2010-02-02T23:59:59.1234567";
        String actual = null;
        String query = "SELECT CONVERT(VARCHAR(40), ?, 126) as [value]";

        try (SQLServerConnection conn = PrepUtil.getConnection(connectionString + ";datetimeParameterType=datetime2"); 
            PreparedStatement stmt = conn.prepareStatement(query)) {

            Timestamp ts = Timestamp.valueOf("2010-02-02 23:59:59.1234567");

            /*
            * send the timestamp to the server using the TIME SQL type rather than TIMESTAMP. The driver will
            * strip the date portion and, because sendTimeAsDatetime=true, round the resulting time value to
            * midnight because it should be sending a DATETIME which has only 1/300s accuracy
            */
            stmt.setObject(1, ts, java.sql.Types.TIMESTAMP);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                actual = rs.getString("value");
            }

        }

        assertEquals(expected, actual);
    }

    /*
     * Test to make sure that a Timestamp is treated as a datetime2 object.
     */
    @Test
    public void testSendTimestampAsDatetimeoffset() throws Exception { 
        String expected = "2010-02-03T23:59:59.7654321Z";
        String actual = null;
        String query = "SELECT CONVERT(VARCHAR(40), ?, 127) as [value]";


        try (SQLServerConnection conn = PrepUtil.getConnection(connectionString + ";datetimeParameterType=datetimeoffset"); 
            PreparedStatement stmt = conn.prepareStatement(query)) {

            Timestamp ts = Timestamp.valueOf("2010-02-03 23:59:59.7654321");

            /*
            * send the timestamp to the server using the TIME SQL type rather than TIMESTAMP. The driver will
            * strip the date portion and, because sendTimeAsDatetime=true, round the resulting time value to
            * midnight because it should be sending a DATETIME which has only 1/300s accuracy
            */
            stmt.setObject(1, ts, java.sql.Types.TIMESTAMP);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                actual = rs.getString("value");
            }

        }

        assertEquals(expected, actual);
    }

    @BeforeEach
    public void testSetup() throws TestAbortedException, Exception {
        // To get TIME & setTime working on Servers >= 2008, we must add 'sendTimeAsDatetime=false'
        // by default to the connection. See issue https://github.com/Microsoft/mssql-jdbc/issues/559
        try (Connection connection = PrepUtil.getConnection(connectionString + ";sendTimeAsDatetime=false");
                Statement stmt = (SQLServerStatement) connection.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            String sql1 = "create table " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (id integer not null, my_date date, my_time time, my_timestamp datetime2 constraint "
                    + AbstractSQLGenerator.escapeIdentifier(primaryKeyConstraintName) + " primary key (id))";
            stmt.execute(sql1);

            // add one sample data
            String sPrepStmt = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (id, my_date, my_time, my_timestamp) values (?, ?, ?, ?)";
            try (PreparedStatement pstmt = connection.prepareStatement(sPrepStmt)) {
                pstmt.setInt(1, 42);
                pstmt.setDate(2, DATE_TO_TEST);
                pstmt.setTime(3, TIME_TO_TEST);
                pstmt.setTimestamp(4, TIMESTAMP_TO_TEST);
                pstmt.execute();
                pstmt.close();
                createTVPs(dateTVP, "date");
                createTVPs(timeTVP, "time");
                createTVPs(timestampTVP, "datetime2");
            }
        }
    }

    @AfterAll
    public static void terminateVariation() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
        }
    }
}
