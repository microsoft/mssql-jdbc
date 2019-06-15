/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.tvp;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerDataTable;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDW)
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

    @BeforeAll
    public static void testSetup() throws SQLException {
        tvpName = RandomUtil.getIdentifier("numericTVP");
        procedureName = RandomUtil.getIdentifier("procedureThatCallsTVP");
        charTable = RandomUtil.getIdentifier("tvpNumericTable");

        createTVPS();
        createTables();
        createProcedure();
    }

    private static void createProcedure() throws SQLException {
        String sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(procedureName) + " @InputData "
                + AbstractSQLGenerator.escapeIdentifier(tvpName) + " READONLY " + " AS " + " BEGIN " + " INSERT INTO "
                + AbstractSQLGenerator.escapeIdentifier(charTable) + " SELECT * FROM @InputData" + " END";
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    private static void createTables() throws SQLException {
        String sql = "create table " + AbstractSQLGenerator.escapeIdentifier(charTable) + " (c1 numeric(6,3) null);";
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    private static void createTVPS() throws SQLException {
        String TVPCreateCmd = "CREATE TYPE " + AbstractSQLGenerator.escapeIdentifier(tvpName)
                + " as table (c1 numeric(6,3) null)";
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(TVPCreateCmd);
        }
    }

    @AfterAll
    public static void terminateVariation() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropProcedureIfExists(procedureName, stmt);
            TestUtils.dropTableIfExists(charTable, stmt);
            TestUtils.dropTypeIfExists(tvpName, stmt);
        }

        if (null != tvp) {
            tvp.clear();
        }
    }
}
