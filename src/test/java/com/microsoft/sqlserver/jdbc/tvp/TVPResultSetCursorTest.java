/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.tvp;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Properties;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.testframework.AbstractTest;

@RunWith(JUnitPlatform.class)
public class TVPResultSetCursorTest extends AbstractTest {

    private static Connection conn = null;
    static Statement stmt = null;

    static BigDecimal[] expectedBigDecimals = {new BigDecimal("12345.12345"), new BigDecimal("125.123"), new BigDecimal("45.12345")};
    static String[] expectedBigDecimalStrings = {"12345.12345", "125.12300", "45.12345"};

    static String[] expectedStrings = {"hello", "world", "!!!"};

    static Timestamp[] expectedTimestamps = {new Timestamp(1433338533461L), new Timestamp(14917485583999L), new Timestamp(1491123533000L)};
    static String[] expectedTimestampStrings = {"2015-06-03 06:35:33.4610000", "2442-09-18 18:59:43.9990000", "2017-04-02 01:58:53.0000000"};

    private static String tvpName = "TVPResultSetCursorTest_TVP";
    private static String srcTable = "TVPResultSetCursorTest_SourceTable";
    private static String desTable = "TVPResultSetCursorTest_DestinationTable";

    /**
     * Test a previous failure when using server cursor and using the same connection to create TVP and result set.
     * 
     * @throws SQLException
     */
    @Test
    public void testServerCursors() throws SQLException {
        conn = DriverManager.getConnection(connectionString);
        stmt = conn.createStatement();

        dropTVPS();
        dropTables();

        createTVPS();
        createTables();

        populateSourceTable();

        ResultSet rs = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE).executeQuery("select * from " + srcTable);

        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn.prepareStatement("INSERT INTO " + desTable + " select * from ? ;");
        pstmt.setStructured(1, tvpName, rs);
        pstmt.execute();

        verifyDestinationTableData();

        if (null != pstmt) {
            pstmt.close();
        }
        if (null != rs) {
            rs.close();
        }
    }

    /**
     * Test a previous failure when setting SelectMethod to cursor and using the same connection to create TVP and result set.
     * 
     * @throws SQLException
     */
    @Test
    public void testSelectMethodSetToCursor() throws SQLException {
        Properties info = new Properties();
        info.setProperty("SelectMethod", "cursor");
        conn = DriverManager.getConnection(connectionString, info);

        stmt = conn.createStatement();

        dropTVPS();
        dropTables();

        createTVPS();
        createTables();

        populateSourceTable();

        ResultSet rs = conn.createStatement().executeQuery("select * from " + srcTable);

        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn.prepareStatement("INSERT INTO " + desTable + " select * from ? ;");
        pstmt.setStructured(1, tvpName, rs);
        pstmt.execute();

        verifyDestinationTableData();

        if (null != pstmt) {
            pstmt.close();
        }
        if (null != rs) {
            rs.close();
        }
    }

    private static void verifyDestinationTableData() throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("select * from " + desTable);

        int i = 0;
        while (rs.next()) {
            assertTrue(rs.getString(1).equals(expectedBigDecimalStrings[i]));
            assertTrue(rs.getString(2).trim().equals(expectedStrings[i]));
            assertTrue(rs.getString(3).equals(expectedTimestampStrings[i]));
            i++;
        }

        assertTrue(i == expectedBigDecimals.length);
    }

    private static void populateSourceTable() throws SQLException {
        String sql = "insert into " + srcTable + " values (?,?,?)";

        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn.prepareStatement(sql);

        for (int i = 0; i < expectedBigDecimals.length; i++) {
            pstmt.setBigDecimal(1, expectedBigDecimals[i]);
            pstmt.setString(2, expectedStrings[i]);
            pstmt.setTimestamp(3, expectedTimestamps[i]);
            pstmt.execute();
        }
    }

    private static void dropTables() throws SQLException {
        stmt.executeUpdate("if object_id('" + srcTable + "','U') is not null" + " drop table " + srcTable);
        stmt.executeUpdate("if object_id('" + desTable + "','U') is not null" + " drop table " + desTable);
    }

    private static void createTables() throws SQLException {
        String sql = "create table " + srcTable + " (c1 decimal(10,5) null, c2 nchar(50) null, c3 datetime2(7) null);";
        stmt.execute(sql);

        sql = "create table " + desTable + " (c1 decimal(10,5) null, c2 nchar(50) null, c3 datetime2(7) null);";
        stmt.execute(sql);
    }

    private static void createTVPS() throws SQLException {
        String TVPCreateCmd = "CREATE TYPE " + tvpName + " as table (c1 decimal(10,5) null, c2 nchar(50) null, c3 datetime2(7) null)";
        stmt.executeUpdate(TVPCreateCmd);
    }

    private static void dropTVPS() throws SQLException {
        stmt.executeUpdate("IF EXISTS (SELECT * FROM sys.types WHERE is_table_type = 1 AND name = '" + tvpName + "') " + " drop type " + tvpName);
    }

    @AfterEach
    private void terminateVariation() throws SQLException {
        if (null != conn) {
            conn.close();
        }
        if (null != stmt) {
            stmt.close();
        }
    }

}