/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.tvp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Utils;

@RunWith(JUnitPlatform.class)
public class TVPIssuesTest extends AbstractTest {

    static Connection connection = null;
    static Statement stmt = null;
    private static String tvp_varcharMax = "TVPIssuesTest_varcharMax_TVP";
    private static String spName_varcharMax = "TVPIssuesTest_varcharMax_SP";
    private static String srcTable_varcharMax = "TVPIssuesTest_varcharMax_srcTable";
    private static String desTable_varcharMax = "TVPIssuesTest_varcharMax_destTable";

    private static String tvp_time_6 = "TVPIssuesTest_time_6_TVP";
    private static String srcTable_time_6 = "TVPIssuesTest_time_6_srcTable";
    private static String desTable_time_6 = "TVPIssuesTest_time_6_destTable";

    private static String expectedTime6value = "15:39:27.616667";

    @Test
    public void tryTVPRSvarcharMax4000Issue() throws Exception {

        setup();

        SQLServerStatement st = (SQLServerStatement) connection.createStatement();
        ResultSet rs = st.executeQuery("select * from " + srcTable_varcharMax);

        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                .prepareStatement("INSERT INTO " + desTable_varcharMax + " select * from ? ;");

        pstmt.setStructured(1, tvp_varcharMax, rs);
        pstmt.execute();

        testCharDestTable();
    }

    /**
     * Test exception when invalid stored procedure name is used.
     * 
     * @throws Exception
     */
    @Test
    public void testExceptionWithInvalidStoredProcedureName() throws Exception {
        SQLServerStatement st = (SQLServerStatement) connection.createStatement();
        ResultSet rs = st.executeQuery("select * from " + srcTable_varcharMax);

        dropProcedure();

        final String sql = "{call " + spName_varcharMax + "(?)}";
        SQLServerCallableStatement Cstmt = (SQLServerCallableStatement) connection.prepareCall(sql);
        try {
            Cstmt.setObject(1, rs);
            throw new Exception("Expected Exception for invalied stored procedure name is not thrown.");
        }
        catch (Exception e) {
            if (e instanceof SQLException) {
                assertTrue(e.getMessage().contains("Could not find stored procedure"), "Invalid Error Message.");
            }
            else {
                throw e;
            }
        }
    }

    /**
     * Fix an issue: If column is time(x) and TVP is used (with either ResultSet, Stored Procedure or SQLServerDataTable). The milliseconds or
     * nanoseconds are not copied into the destination table.
     * 
     * @throws Exception
     */
    @Test
    public void tryTVPPrecisionmissedissue315() throws Exception {

        setup();

        ResultSet rs = stmt.executeQuery("select * from " + srcTable_time_6);

        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                .prepareStatement("INSERT INTO " + desTable_time_6 + " select * from ? ;");
        pstmt.setStructured(1, tvp_time_6, rs);
        pstmt.execute();

        testTime6DestTable();
    }

    private void testCharDestTable() throws SQLException, IOException {
        ResultSet rs = connection.createStatement().executeQuery("select * from " + desTable_varcharMax);
        while (rs.next()) {
            assertEquals(rs.getString(1).length(), 4001, " The inserted length is truncated or not correct!");
        }
        if (null != rs) {
            rs.close();
        }
    }

    private void testTime6DestTable() throws SQLException, IOException {
        ResultSet rs = connection.createStatement().executeQuery("select * from " + desTable_time_6);
        while (rs.next()) {
            assertEquals(rs.getString(1), expectedTime6value, " The time value is truncated or not correct!");
        }
        if (null != rs) {
            rs.close();
        }
    }

    @BeforeAll
    public static void beforeAll() throws SQLException {

        connection = DriverManager.getConnection(connectionString);
        stmt = connection.createStatement();

        dropProcedure();

        stmt.executeUpdate(
                "IF EXISTS (SELECT * FROM sys.types WHERE is_table_type = 1 AND name = '" + tvp_varcharMax + "') " + " drop type " + tvp_varcharMax);
        Utils.dropTableIfExists(srcTable_varcharMax, stmt);
        Utils.dropTableIfExists(desTable_varcharMax, stmt);

        stmt.executeUpdate(
                "IF EXISTS (SELECT * FROM sys.types WHERE is_table_type = 1 AND name = '" + tvp_time_6 + "') " + " drop type " + tvp_time_6);
        Utils.dropTableIfExists(srcTable_time_6, stmt);
        Utils.dropTableIfExists(desTable_time_6, stmt);

        String sql = "create table " + srcTable_varcharMax + " (c1 varchar(max) null);";
        stmt.execute(sql);
        sql = "create table " + desTable_varcharMax + " (c1 varchar(max) null);";
        stmt.execute(sql);

        sql = "create table " + srcTable_time_6 + " (c1 time(6) null);";
        stmt.execute(sql);
        sql = "create table " + desTable_time_6 + " (c1 time(6) null);";
        stmt.execute(sql);

        String TVPCreateCmd = "CREATE TYPE " + tvp_varcharMax + " as table (c1 varchar(max) null)";
        stmt.executeUpdate(TVPCreateCmd);

        TVPCreateCmd = "CREATE TYPE " + tvp_time_6 + " as table (c1 time(6) null)";
        stmt.executeUpdate(TVPCreateCmd);

        createPreocedure();

        populateCharSrcTable();
        populateTime6SrcTable();
    }

    private static void populateCharSrcTable() throws SQLException {
        String sql = "insert into " + srcTable_varcharMax + " values (?)";

        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < 4001; i++) {
            sb.append("a");
        }
        String value = sb.toString();

        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(sql);

        pstmt.setString(1, value);
        pstmt.execute();
    }

    private static void populateTime6SrcTable() throws SQLException {
        String sql = "insert into " + srcTable_time_6 + " values ('2017-05-12 " + expectedTime6value + "')";
        connection.createStatement().execute(sql);
    }

    private static void dropProcedure() throws SQLException {
        Utils.dropProcedureIfExists(spName_varcharMax, stmt);
    }

    private static void createPreocedure() throws SQLException {
        String sql = "CREATE PROCEDURE " + spName_varcharMax + " @InputData " + tvp_varcharMax + " READONLY " + " AS " + " BEGIN " + " INSERT INTO "
                + desTable_varcharMax + " SELECT * FROM @InputData" + " END";

        stmt.execute(sql);
    }

    @AfterAll
    public static void terminateVariation() throws SQLException {
        dropProcedure();
        stmt.executeUpdate(
                "IF EXISTS (SELECT * FROM sys.types WHERE is_table_type = 1 AND name = '" + tvp_varcharMax + "') " + " drop type " + tvp_varcharMax);
        Utils.dropTableIfExists(srcTable_varcharMax, stmt);
        Utils.dropTableIfExists(desTable_varcharMax, stmt);

        stmt.executeUpdate(
                "IF EXISTS (SELECT * FROM sys.types WHERE is_table_type = 1 AND name = '" + tvp_time_6 + "') " + " drop type " + tvp_time_6);
        Utils.dropTableIfExists(srcTable_time_6, stmt);
        Utils.dropTableIfExists(desTable_time_6, stmt);

        if (null != connection) {
            connection.close();
        }
        if (null != stmt) {
            stmt.close();
        }
    }
}
