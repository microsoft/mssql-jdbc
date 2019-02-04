/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.tvp;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerDataTable;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
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

        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(charTable) + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, tvp);

            pstmt.execute();
        }
    }

    @BeforeEach
    public void testSetup() throws SQLException {
        tvpName = RandomUtil.getIdentifier("numericTVP");
        procedureName = RandomUtil.getIdentifier("procedureThatCallsTVP");
        charTable = RandomUtil.getIdentifier("tvpNumericTable");

        dropProcedure();
        dropTables();
        dropTVPS();

        createTVPS();
        createTables();
        createPreocedure();
    }

    private void dropProcedure() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            String sql = " IF EXISTS (select * from sysobjects where id = object_id(N'"
                    + TestUtils.escapeSingleQuotes(procedureName) + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)"
                    + " DROP PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(procedureName);
            stmt.execute(sql);
        }
    }

    private static void dropTables() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("if object_id('" + TestUtils.escapeSingleQuotes(charTable) + "','U') is not null"
                    + " drop table " + AbstractSQLGenerator.escapeIdentifier(charTable));
        }
    }

    private static void dropTVPS() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("IF EXISTS (SELECT * FROM sys.types WHERE is_table_type = 1 AND name = '"
                    + TestUtils.escapeSingleQuotes(tvpName) + "') " + " drop type "
                    + AbstractSQLGenerator.escapeIdentifier(tvpName));
        }
    }

    private static void createPreocedure() throws SQLException {
        String sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(procedureName) + " @InputData "
                + AbstractSQLGenerator.escapeIdentifier(tvpName) + " READONLY " + " AS " + " BEGIN " + " INSERT INTO "
                + AbstractSQLGenerator.escapeIdentifier(charTable) + " SELECT * FROM @InputData" + " END";
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    private void createTables() throws SQLException {
        String sql = "create table " + AbstractSQLGenerator.escapeIdentifier(charTable) + " (c1 numeric(6,3) null);";
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    private void createTVPS() throws SQLException {
        String TVPCreateCmd = "CREATE TYPE " + AbstractSQLGenerator.escapeIdentifier(tvpName)
                + " as table (c1 numeric(6,3) null)";
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(TVPCreateCmd);
        }
    }

    @AfterEach
    public void terminateVariation() throws SQLException {
        dropProcedure();
        dropTables();
        dropTVPS();

        if (null != tvp) {
            tvp.clear();
        }
    }

}
