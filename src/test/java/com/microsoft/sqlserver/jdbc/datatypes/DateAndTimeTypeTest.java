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

import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.Utils;

@RunWith(JUnitPlatform.class)
public class DateAndTimeTypeTest extends AbstractTest {

    private static final Date DATE_TO_TEST = new java.sql.Date(2017, 20, 11);
    private static final Time TIME_TO_TEST = new java.sql.Time(12, 34, 56);
    private static final Timestamp TIMESTAMP_TO_TEST = new java.sql.Timestamp(2017, 20, 11, 12, 34, 56, 0);

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

    @BeforeEach
    public void testSetup() throws TestAbortedException, Exception {
        assumeTrue(13 <= new DBConnection(connectionString).getServerVersion(),
                "Aborting test case as SQL Server version is not compatible with Always encrypted ");

        connection = DriverManager.getConnection(connectionString);
        SQLServerStatement stmt = (SQLServerStatement) connection.createStatement();
        Utils.dropTableIfExists("dateandtime", stmt);
        String sql1 = "create table dateandtime (id integer not null, my_date date, my_time time, my_timestamp datetime2 constraint pk_esimple primary key (id))";
        stmt.execute(sql1);
        stmt.close();

        // add one sample data
        String sPrepStmt = "insert into dateandtime (id, my_date, my_time, my_timestamp) values (?, ?, ?, ?)";
        pstmt = connection.prepareStatement(sPrepStmt);
        pstmt.setInt(1, 42);
        pstmt.setDate(2, DATE_TO_TEST); 
        pstmt.setTime(3, TIME_TO_TEST);
        pstmt.setTimestamp(4, TIMESTAMP_TO_TEST); 
        pstmt.execute();
        pstmt.close();
    }

    @AfterAll
    public static void terminateVariation() throws SQLException {

        SQLServerStatement stmt = (SQLServerStatement) connection.createStatement();
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