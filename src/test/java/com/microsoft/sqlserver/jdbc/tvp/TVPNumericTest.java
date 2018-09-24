/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.tvp;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.DriverManager;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerDataTable;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.testframework.AbstractTest;


@RunWith(JUnitPlatform.class)
public class TVPNumericTest extends AbstractTest {

    static SQLServerDataTable tvp = null;
    static String expectecValue1 = "hello";
    static String expectecValue2 = "world";
    static String expectecValue3 = "again";
    private static String tvpName;
    private static String charTable;
    private static String procedureName;

    /**
     * Test a previous failure regarding to numeric precision. Issue #211
     * 
     * @throws SQLException
     * @throws SQLTimeoutException
     */
    @Test
    public void testNumericPresicionIssue211() throws SQLException {
        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", java.sql.Types.NUMERIC);

        tvp.addRow(12.12);
        tvp.addRow(1.123);

        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                .prepareStatement("INSERT INTO " + charTable + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, tvp);

            pstmt.execute();
        }
    }

    @BeforeEach
    public void testSetup() throws SQLException {
        tvpName = "[" + RandomUtil.getIdentifier("numericTVP") + "]";
        procedureName = "procedureThatCallsTVP";
        charTable = "[" + RandomUtil.getIdentifier("tvpNumericTable") + "]";

        dropProcedure();
        dropTables();
        dropTVPS();

        createTVPS();
        createTables();
        createPreocedure();
    }

    private void dropProcedure() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            String sql = " IF EXISTS (select * from sysobjects where id = object_id(N'" + procedureName
                    + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)" + " DROP PROCEDURE " + procedureName;
            stmt.execute(sql);
        }
    }

    private static void dropTables() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("if object_id('" + charTable + "','U') is not null" + " drop table " + charTable);
        }
    }

    private static void dropTVPS() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("IF EXISTS (SELECT * FROM sys.types WHERE is_table_type = 1 AND name = '" + tvpName.replaceAll("\\[|\\]", "")
                    + "') " + " drop type " + tvpName);
        }
    }

    private static void createPreocedure() throws SQLException {
        String sql = "CREATE PROCEDURE " + procedureName + " @InputData " + tvpName + " READONLY " + " AS " + " BEGIN "
                + " INSERT INTO " + charTable + " SELECT * FROM @InputData" + " END";
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    private void createTables() throws SQLException {
        String sql = "create table " + charTable + " (c1 numeric(6,3) null);";
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    private void createTVPS() throws SQLException {
        String TVPCreateCmd = "CREATE TYPE " + tvpName + " as table (c1 numeric(6,3) null)";
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(TVPCreateCmd);
        }
    }

    @AfterEach
    public void terminateVariation() throws SQLException {
        if (null != tvp) {
            tvp.clear();
        }
    }

}
