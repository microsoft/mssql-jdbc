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
import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Utils;

@RunWith(JUnitPlatform.class)
public class TVPResultSetCursorTest extends AbstractTest {

    private static Connection conn = null;
    static Statement stmt = null;

    static BigDecimal[] expectedBigDecimals = {new BigDecimal("12345.12345"), new BigDecimal("125.123"), new BigDecimal("45.12345")};
    static String[] expectedBigDecimalStrings = {"12345.12345", "125.12300", "45.12345"};

    static String[] expectedStrings = {"hello", "world", "!!!"};

    static Timestamp[] expectedTimestamps = {new Timestamp(1433338533461L), new Timestamp(14917485583999L), new Timestamp(1491123533000L)};
    static String[] expectedTimestampStrings = {"2015-06-03 13:35:33.4610000", "2442-09-19 01:59:43.9990000", "2017-04-02 08:58:53.0000000"};

    private static String tvpName = "TVPResultSetCursorTest_TVP";
    private static String procedureName = "TVPResultSetCursorTest_SP";
    private static String srcTable = "TVPResultSetCursorTest_SourceTable";
    private static String desTable = "TVPResultSetCursorTest_DestinationTable";

    /**
     * Test a previous failure when using server cursor and using the same connection to create TVP and result set.
     * 
     * @throws SQLException
     */
    @Test
    public void testServerCursors() throws SQLException {
        serverCursorsTest(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        serverCursorsTest(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        serverCursorsTest(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        serverCursorsTest(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
    }

    private void serverCursorsTest(int resultSetType,
            int resultSetConcurrency) throws SQLException {
        conn = DriverManager.getConnection(connectionString);
        stmt = conn.createStatement();

        dropTVPS();
        dropTables();

        createTVPS();
        createTables();

        populateSourceTable();

        ResultSet rs = conn.createStatement(resultSetType, resultSetConcurrency).executeQuery("select * from " + srcTable);

        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn.prepareStatement("INSERT INTO " + desTable + " select * from ? ;");
        pstmt.setStructured(1, tvpName, rs);
        pstmt.execute();

        verifyDestinationTableData(expectedBigDecimals.length);

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

        verifyDestinationTableData(expectedBigDecimals.length);

        if (null != pstmt) {
            pstmt.close();
        }
        if (null != rs) {
            rs.close();
        }
    }

    /**
     * Test a previous failure when setting SelectMethod to cursor and using the same connection to create TVP, SP and result set.
     * 
     * @throws SQLException
     */
    @Test
    public void testSelectMethodSetToCursorWithSP() throws SQLException {
        Properties info = new Properties();
        info.setProperty("SelectMethod", "cursor");
        conn = DriverManager.getConnection(connectionString, info);

        stmt = conn.createStatement();

        dropProcedure();
        dropTVPS();
        dropTables();

        createTVPS();
        createTables();
        createPreocedure();

        populateSourceTable();

        ResultSet rs = conn.createStatement().executeQuery("select * from " + srcTable);

        final String sql = "{call " + procedureName + "(?)}";
        SQLServerCallableStatement pstmt = (SQLServerCallableStatement) conn.prepareCall(sql);
        pstmt.setStructured(1, tvpName, rs);

        try {
            pstmt.execute();

            verifyDestinationTableData(expectedBigDecimals.length);
        }
        finally {
            if (null != pstmt) {
                pstmt.close();
            }
            if (null != rs) {
                rs.close();
            }

            dropProcedure();
        }
    }

    /**
     * Test exception when giving invalid TVP name
     * 
     * @throws SQLException
     */
    @Test
    public void testInvalidTVPName() throws SQLException {
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
        pstmt.setStructured(1, "invalid" + tvpName, rs);

        try {
            pstmt.execute();
        }
        catch (SQLException e) {
            if (!e.getMessage().contains("Cannot find data type")) {
                throw e;
            }
        }
        finally {
            if (null != pstmt) {
                pstmt.close();
            }
            if (null != rs) {
                rs.close();
            }
        }
    }

    /**
     * Test exception when giving invalid stored procedure name
     * 
     * @throws SQLException
     */
    @Test
    public void testInvalidStoredProcedureName() throws SQLException {
        Properties info = new Properties();
        info.setProperty("SelectMethod", "cursor");
        conn = DriverManager.getConnection(connectionString, info);

        stmt = conn.createStatement();

        dropProcedure();
        dropTVPS();
        dropTables();

        createTVPS();
        createTables();
        createPreocedure();

        populateSourceTable();

        ResultSet rs = conn.createStatement().executeQuery("select * from " + srcTable);

        final String sql = "{call invalid" + procedureName + "(?)}";
        SQLServerCallableStatement pstmt = (SQLServerCallableStatement) conn.prepareCall(sql);
        pstmt.setStructured(1, tvpName, rs);

        try {
            pstmt.execute();
        }
        catch (SQLException e) {
            if (!e.getMessage().contains("Could not find stored procedure")) {
                throw e;
            }
        }
        finally {

            if (null != pstmt) {
                pstmt.close();
            }
            if (null != rs) {
                rs.close();
            }

            dropProcedure();
        }
    }

    /**
     * test with multiple prepared statements and result sets
     * 
     * @throws SQLException
     */
    @Test
    public void testMultiplePreparedStatementAndResultSet() throws SQLException {
        conn = DriverManager.getConnection(connectionString);

        stmt = conn.createStatement();

        dropTVPS();
        dropTables();

        createTVPS();
        createTables();

        populateSourceTable();

        ResultSet rs = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE).executeQuery("select * from " + srcTable);

        SQLServerPreparedStatement pstmt1 = (SQLServerPreparedStatement) conn.prepareStatement("INSERT INTO " + desTable + " select * from ? ;");
        pstmt1.setStructured(1, tvpName, rs);
        pstmt1.execute();
        verifyDestinationTableData(expectedBigDecimals.length);

        rs.beforeFirst();
        pstmt1 = (SQLServerPreparedStatement) conn.prepareStatement("INSERT INTO " + desTable + " select * from ? ;");
        pstmt1.setStructured(1, tvpName, rs);
        pstmt1.execute();
        verifyDestinationTableData(expectedBigDecimals.length * 2);

        rs.beforeFirst();
        SQLServerPreparedStatement pstmt2 = (SQLServerPreparedStatement) conn.prepareStatement("INSERT INTO " + desTable + " select * from ? ;");
        pstmt2.setStructured(1, tvpName, rs);
        pstmt2.execute();
        verifyDestinationTableData(expectedBigDecimals.length * 3);

        String sql = "insert into " + desTable + " values (?,?,?,?)";
        Calendar calGMT = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        pstmt1 = (SQLServerPreparedStatement) conn.prepareStatement(sql);
        for (int i = 0; i < expectedBigDecimals.length; i++) {
            pstmt1.setBigDecimal(1, expectedBigDecimals[i]);
            pstmt1.setString(2, expectedStrings[i]);
            pstmt1.setTimestamp(3, expectedTimestamps[i], calGMT);
            pstmt1.setString(4, expectedStrings[i]);
            pstmt1.execute();
        }
        verifyDestinationTableData(expectedBigDecimals.length * 4);

        ResultSet rs2 = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE).executeQuery("select * from " + srcTable);

        pstmt1 = (SQLServerPreparedStatement) conn.prepareStatement("INSERT INTO " + desTable + " select * from ? ;");
        pstmt1.setStructured(1, tvpName, rs2);
        pstmt1.execute();
        verifyDestinationTableData(expectedBigDecimals.length * 5);

        if (null != pstmt1) {
            pstmt1.close();
        }
        if (null != pstmt2) {
            pstmt2.close();
        }
        if (null != rs) {
            rs.close();
        }
        if (null != rs2) {
            rs2.close();
        }
    }

    private static void verifyDestinationTableData(int expectedNumberOfRows) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("select * from " + desTable);

        int expectedArrayLength = expectedBigDecimals.length;

        int i = 0;
        while (rs.next()) {
            assertTrue(rs.getString(1).equals(expectedBigDecimalStrings[i % expectedArrayLength]),
                    "Expected Value:" + expectedBigDecimalStrings[i % expectedArrayLength] + ", Actual Value: " + rs.getString(1));
            assertTrue(rs.getString(2).trim().equals(expectedStrings[i % expectedArrayLength]),
                    "Expected Value:" + expectedStrings[i % expectedArrayLength] + ", Actual Value: " + rs.getString(2));
            assertTrue(rs.getString(3).equals(expectedTimestampStrings[i % expectedArrayLength]),
                    "Expected Value:" + expectedTimestampStrings[i % expectedArrayLength] + ", Actual Value: " + rs.getString(3));
            assertTrue(rs.getString(4).trim().equals(expectedStrings[i % expectedArrayLength]),
                    "Expected Value:" + expectedStrings[i % expectedArrayLength] + ", Actual Value: " + rs.getString(4));
            i++;
        }

        assertTrue(i == expectedNumberOfRows);
    }

    private static void populateSourceTable() throws SQLException {
        String sql = "insert into " + srcTable + " values (?,?,?,?)";

        Calendar calGMT = Calendar.getInstance(TimeZone.getTimeZone("GMT"));

        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn.prepareStatement(sql);

        for (int i = 0; i < expectedBigDecimals.length; i++) {
            pstmt.setBigDecimal(1, expectedBigDecimals[i]);
            pstmt.setString(2, expectedStrings[i]);
            pstmt.setTimestamp(3, expectedTimestamps[i], calGMT);
            pstmt.setString(4, expectedStrings[i]);
            pstmt.execute();
        }
    }

    private static void dropTables() throws SQLException {
        Utils.dropTableIfExists(srcTable, stmt);
        Utils.dropTableIfExists(desTable, stmt);
    }

    private static void createTables() throws SQLException {
        String sql = "create table " + srcTable + " (c1 decimal(10,5) null, c2 nchar(50) null, c3 datetime2(7) null, c4 char(7000));";
        stmt.execute(sql);

        sql = "create table " + desTable + " (c1 decimal(10,5) null, c2 nchar(50) null, c3 datetime2(7) null, c4 char(7000));";
        stmt.execute(sql);
    }

    private static void createTVPS() throws SQLException {
        String TVPCreateCmd = "CREATE TYPE " + tvpName
                + " as table (c1 decimal(10,5) null, c2 nchar(50) null, c3 datetime2(7) null, c4 char(7000) null)";
        stmt.execute(TVPCreateCmd);
    }

    private static void dropTVPS() throws SQLException {
        stmt.execute("IF EXISTS (SELECT * FROM sys.types WHERE is_table_type = 1 AND name = '" + tvpName + "') " + " drop type " + tvpName);
    }

    private static void dropProcedure() throws SQLException {
        Utils.dropProcedureIfExists(procedureName, stmt);
    }

    private static void createPreocedure() throws SQLException {
        String sql = "CREATE PROCEDURE " + procedureName + " @InputData " + tvpName + " READONLY " + " AS " + " BEGIN " + " INSERT INTO " + desTable
                + " SELECT * FROM @InputData" + " END";

        stmt.execute(sql);
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