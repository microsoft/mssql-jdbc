/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.tvp;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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

    static String expectecValue1 = "hello";
    static String expectecValue2 = "world";
    static String expectecValue3 = "again";

    private static String tvpName;
    private static String charTableName;
    private static String escapedCharTableName;
    private static String procedureName;

    /**
     * Test a previous failure regarding to numeric precision reported on Issue #211. Test TVP name in
     * SQLServerDataTable with setObject()
     * 
     * @throws SQLException
     * @throws SQLTimeoutException
     */
    @Test
    public void testTVPNameWithDataTable() throws SQLException {
        String selectSQL = "SELECT * FROM " + escapedCharTableName + " ORDER BY c1 ASC";
        String insertSQL = "INSERT INTO " + escapedCharTableName + " SELECT * FROM ? ;";
        float[] testValues = new float[] {0.0F, 1.123F, 12.12F};

        SQLServerDataTable tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", java.sql.Types.NUMERIC);
        tvp.setTvpName(tvpName);

        for (int i = 0; i < testValues.length; i++) {
            tvp.addRow(testValues[i]);
        }

        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(insertSQL)) {
            pstmt.setStructured(1, tvpName, tvp);
            pstmt.execute();
        }
        validateTVPInsert(selectSQL, testValues);

        TestUtils.clearTable(connection, escapedCharTableName);
        try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) {
            pstmt.setObject(1, tvp, microsoft.sql.Types.STRUCTURED);
            pstmt.execute();
        }
        validateTVPInsert(selectSQL, testValues);

        TestUtils.clearTable(connection, escapedCharTableName);
        try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) {
            pstmt.setObject(1, tvp);
            pstmt.execute();
        }
        validateTVPInsert(selectSQL, testValues);

        TestUtils.clearTable(connection, escapedCharTableName);
        try (CallableStatement cstmt = connection
                .prepareCall("{call " + AbstractSQLGenerator.escapeIdentifier(procedureName) + "(?)}");) {
            cstmt.setObject("@InputData", tvp, microsoft.sql.Types.STRUCTURED);
            cstmt.executeUpdate();
        }
        validateTVPInsert(selectSQL, testValues);
        tvp.clear();
    }

    @BeforeAll
    public static void testSetup() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"trustServerCertificate", "true");
        setConnection();

        tvpName = RandomUtil.getIdentifier("numericTVP");
        procedureName = RandomUtil.getIdentifier("procedureThatCallsTVP");
        charTableName = RandomUtil.getIdentifier("tvpNumericTable");
        escapedCharTableName = AbstractSQLGenerator.escapeIdentifier(charTableName);

        createTVPS();
        createTables();
        createProcedure();
    }

    private static void createProcedure() throws SQLException {
        String sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(procedureName) + " @InputData "
                + AbstractSQLGenerator.escapeIdentifier(tvpName) + " READONLY " + " AS " + " BEGIN " + " INSERT INTO "
                + escapedCharTableName + " SELECT * FROM @InputData" + " END";
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    private static void createTables() throws SQLException {
        String sql = "create table " + escapedCharTableName + " (c1 numeric(6,3) null);";
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

    private static void validateTVPInsert(String sql, float[] expectedValues) throws SQLException {
        try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            int i = 1;
            while (rs.next()) {
                assertEquals(expectedValues[i - 1], rs.getFloat(1));
                i++;
            }
        }
    }

    @AfterAll
    public static void terminateVariation() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropProcedureIfExists(procedureName, stmt);
            TestUtils.dropTableIfExists(charTableName, stmt);
            TestUtils.dropTypeIfExists(tvpName, stmt);
        }
    }
}
