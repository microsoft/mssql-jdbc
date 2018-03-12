/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.datatypes;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.opentest4j.TestAbortedException;

import com.microsoft.sqlserver.jdbc.SQLServerDataTable;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.Utils;

@RunWith(JUnitPlatform.class)
public class DateAndTimeTypeTest extends AbstractTest {

    private static final Date DATE_TO_TEST = new java.sql.Date(61494793200000L);
    private static final Time TIME_TO_TEST = new java.sql.Time(74096000L);
    private static final Timestamp TIMESTAMP_TO_TEST = new java.sql.Timestamp(61494838496000L);

    static Statement stmt = null;
    static Connection connection = null;
    static PreparedStatement pstmt = null;
    static ResultSet rs = null;

    /**
     * Test query with date
     */
    @Test
    public void testQueryDate() throws SQLException {
        String sPrepStmt = "select * from dateandtime where my_date = ?";
        pstmt = connection.prepareStatement(sPrepStmt);
        pstmt.setDate(1, DATE_TO_TEST);

        rs = pstmt.executeQuery();
        rs.next();
        assertTrue(rs.getInt(1) == 42, "did not find correct timestamp");
        rs.close();
        pstmt.close();

    }

    /**
     * Test query with timestamp
     */
    @Test
    public void testQueryTimestamp() throws SQLException {
        String sPrepStmt = "select * from dateandtime where my_timestamp = ?";
        pstmt = connection.prepareStatement(sPrepStmt);
        pstmt.setTimestamp(1, TIMESTAMP_TO_TEST);

        rs = pstmt.executeQuery();
        rs.next();
        assertTrue(rs.getInt(1) == 42, "did not find correct timestamp");
        rs.close();
        pstmt.close();
    }

    /**
     * Test query with time
     */
    @Test
    public void testQueryTime() throws SQLException {
        String sPrepStmt = "select * from dateandtime where my_time = ?";
        pstmt = connection.prepareStatement(sPrepStmt);
        pstmt.setTime(1, TIME_TO_TEST);

        rs = pstmt.executeQuery();
        rs.next();
        assertTrue(rs.getInt(1) == 42, "did not find correct timestamp");
        rs.close();
        pstmt.close();
    }

    /**
     * Test query with date TVP
     */
    @Test
    public void testQueryDateTVP() throws SQLException {
        SQLServerDataTable tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", java.sql.Types.DATE);
        tvp.addRow(DATE_TO_TEST);
        String sPrepStmt = "select * from dateandtime where my_date IN (select * from ?)";
        pstmt = connection.prepareStatement(sPrepStmt);
        ((SQLServerPreparedStatement)pstmt).setStructured(1, "dateTVP", tvp);

        rs = pstmt.executeQuery();
        rs.next();
        assertTrue(rs.getInt(1) == 42, "did not find correct timestamp");
        rs.close();
        pstmt.close();
    }

    /**
     * Test query with date TVP
     */
    @Test
    public void testQueryTimestampTVP() throws SQLException {
        SQLServerDataTable tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", java.sql.Types.TIMESTAMP);
        tvp.addRow(TIMESTAMP_TO_TEST);
        String sPrepStmt = "select * from dateandtime where my_timestamp IN (select * from ?)";
        pstmt = connection.prepareStatement(sPrepStmt);
        ((SQLServerPreparedStatement)pstmt).setStructured(1, "timestampTVP", tvp);

        rs = pstmt.executeQuery();
        rs.next();
        assertTrue(rs.getInt(1) == 42, "did not find correct timestamp");
        rs.close();
        pstmt.close();
    }

    /**
     * Test query with date TVP
     */
    @Test
    public void testQueryTimeTVP() throws SQLException {
        SQLServerDataTable tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", java.sql.Types.TIME);
        tvp.addRow(TIME_TO_TEST);
        String sPrepStmt = "select * from dateandtime where my_time IN (select * from ?)";
        pstmt = connection.prepareStatement(sPrepStmt);
        ((SQLServerPreparedStatement)pstmt).setStructured(1, "timeTVP", tvp);

        rs = pstmt.executeQuery();
        rs.next();
        assertTrue(rs.getInt(1) == 42, "did not find correct timestamp");
        rs.close();
        pstmt.close();
    }
    private void createTVPs(String tvpName, String tvpType) throws SQLException {
      stmt.executeUpdate("IF EXISTS (SELECT * FROM sys.types WHERE is_table_type = 1 AND name = '" + tvpName + "') " + " drop type " + tvpName);
      String TVPCreateCmd = "CREATE TYPE " + tvpName + " as table (c1 " + tvpType + " null)";
      stmt.executeUpdate(TVPCreateCmd);
    }

    @BeforeEach
    public void testSetup() throws TestAbortedException, Exception {
        try (DBConnection dbc = new DBConnection(connectionString)) {
            assumeTrue(9 <= dbc.getServerVersion(),
                    "Aborting test case as SQL Server version does not support TIME");
        }
        // To get TIME & setTime working on Servers >= 2008, we must add 'sendTimeAsDatetime=false'
        // by default to the connection. See issue https://github.com/Microsoft/mssql-jdbc/issues/559
        connection = DriverManager.getConnection(connectionString + ";sendTimeAsDatetime=false");
        stmt = (SQLServerStatement) connection.createStatement();
        Utils.dropTableIfExists("dateandtime", stmt);
        String sql1 = "create table dateandtime (id integer not null, my_date date, my_time time, my_timestamp datetime2 constraint pk_esimple primary key (id))";
        stmt.execute(sql1);

        // add one sample data
        String sPrepStmt = "insert into dateandtime (id, my_date, my_time, my_timestamp) values (?, ?, ?, ?)";
        pstmt = connection.prepareStatement(sPrepStmt);
        pstmt.setInt(1, 42);
        pstmt.setDate(2, DATE_TO_TEST); 
        pstmt.setTime(3, TIME_TO_TEST);
        pstmt.setTimestamp(4, TIMESTAMP_TO_TEST); 
        pstmt.execute();
        pstmt.close();
        createTVPs("dateTVP", "date");
        createTVPs("timeTVP", "time");
        createTVPs("timestampTVP", "datetime2");
    }

    @AfterAll
    public static void terminateVariation() throws SQLException {

        Utils.dropTableIfExists("dateandtime", stmt);

        if (null != connection) {
            connection.close();
        }
        if (null != pstmt) {
            pstmt.close();
        }
        if (null != stmt) {
            stmt.close();
        }
        if (null != rs) {
            rs.close();
        }
    }
}