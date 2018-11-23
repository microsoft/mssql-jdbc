/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;


@RunWith(JUnitPlatform.class)
public class TVPResultSetCursorTest extends AbstractTest {

    static BigDecimal[] expectedBigDecimals = {new BigDecimal("12345.12345"), new BigDecimal("125.123"),
            new BigDecimal("45.12345")};
    static String[] expectedBigDecimalStrings = {"12345.12345", "125.12300", "45.12345"};

    static String[] expectedStrings = {"hello", "world", "!!!"};

    static Timestamp[] expectedTimestamps = {new Timestamp(1433338533461L), new Timestamp(14917485583999L),
            new Timestamp(1491123533000L)};
    static String[] expectedTimestampStrings = {"2015-06-03 13:35:33.4610000", "2442-09-19 01:59:43.9990000",
            "2017-04-02 08:58:53.0000000"};

    private static String tvpName = RandomUtil.getIdentifier("TVPResultSetCursorTest_TVP");
    private static String procedureName = RandomUtil.getIdentifier("TVPResultSetCursorTest_SP");
    private static String srcTable = RandomUtil.getIdentifier("TVPResultSetCursorTest_SourceTable");
    private static String desTable = RandomUtil.getIdentifier("TVPResultSetCursorTest_DestinationTable");

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

    private void serverCursorsTest(int resultSetType, int resultSetConcurrency) throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {

            dropTVPS();
            dropTables();

            createTVPS();
            createTables();

            populateSourceTable();

            try (ResultSet rs = conn.createStatement(resultSetType, resultSetConcurrency)
                    .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(srcTable));
                    SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn.prepareStatement(
                            "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(desTable) + " select * from ? ;")) {
                pstmt.setStructured(1, AbstractSQLGenerator.escapeIdentifier(tvpName), rs);
                pstmt.execute();

                verifyDestinationTableData(expectedBigDecimals.length);
            }
        }
    }

    /**
     * Test a previous failure when setting SelectMethod to cursor and using the same connection to create TVP and
     * result set.
     * 
     * @throws SQLException
     */
    @Test
    public void testSelectMethodSetToCursor() throws SQLException {
        Properties info = new Properties();
        info.setProperty("SelectMethod", "cursor");
        try (Connection conn = DriverManager.getConnection(connectionString, info);
                Statement stmt = conn.createStatement()) {

            dropTVPS();
            dropTables();

            createTVPS();
            createTables();

            populateSourceTable();

            try (ResultSet rs = conn.createStatement()
                    .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(srcTable));
                    SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn.prepareStatement(
                            "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(desTable) + " select * from ? ;")) {
                pstmt.setStructured(1, AbstractSQLGenerator.escapeIdentifier(tvpName), rs);
                pstmt.execute();

                verifyDestinationTableData(expectedBigDecimals.length);
            }
        }
    }

    /**
     * Test a previous failure when setting SelectMethod to cursor and using the same connection to create TVP, SP and
     * result set.
     * 
     * @throws SQLException
     */
    @Test
    public void testSelectMethodSetToCursorWithSP() throws SQLException {
        Properties info = new Properties();
        info.setProperty("SelectMethod", "cursor");
        try (Connection conn = DriverManager.getConnection(connectionString, info);
                Statement stmt = conn.createStatement()) {

            dropProcedure();
            dropTVPS();
            dropTables();

            createTVPS();
            createTables();
            createProcedure();

            populateSourceTable();

            try (ResultSet rs = conn.createStatement()
                    .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(srcTable));
                    SQLServerCallableStatement pstmt = (SQLServerCallableStatement) conn
                            .prepareCall("{call " + AbstractSQLGenerator.escapeIdentifier(procedureName) + "(?)}")) {
                pstmt.setStructured(1, AbstractSQLGenerator.escapeIdentifier(tvpName), rs);

                pstmt.execute();

                verifyDestinationTableData(expectedBigDecimals.length);
            } finally {
                dropProcedure();
            }
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
        try (Connection conn = DriverManager.getConnection(connectionString, info);
                Statement stmt = conn.createStatement()) {

            dropTVPS();
            dropTables();

            createTVPS();
            createTables();

            populateSourceTable();

            try (ResultSet rs = conn.createStatement()
                    .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(srcTable));
                    SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn.prepareStatement(
                            "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(desTable) + " select * from ? ;")) {

                pstmt.setStructured(1, "invalid" + tvpName, rs);

                pstmt.execute();
            } catch (SQLException e) {
                if (!e.getMessage().contains(TestResource.getResource("R_dataTypeNotFound"))) {
                    throw e;
                }
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
        try (Connection conn = DriverManager.getConnection(connectionString, info);
                Statement stmt = conn.createStatement()) {

            dropProcedure();
            dropTVPS();
            dropTables();

            createTVPS();
            createTables();
            createProcedure();

            populateSourceTable();

            try (ResultSet rs = conn.createStatement()
                    .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(srcTable));
                    SQLServerCallableStatement pstmt = (SQLServerCallableStatement) conn.prepareCall(
                            "{call invalid" + AbstractSQLGenerator.escapeIdentifier(procedureName) + "(?)}")) {
                pstmt.setStructured(1, tvpName, rs);

                pstmt.execute();
            } catch (SQLException e) {
                if (!e.getMessage().contains(TestResource.getResource("R_StoredProcedureNotFound"))) {
                    throw e;
                }
            } finally {

                dropProcedure();
            }
        }
    }

    /**
     * test with multiple prepared statements and result sets
     * 
     * @throws SQLException
     */
    @Test
    public void testMultiplePreparedStatementAndResultSet() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString)) {

            dropTVPS();
            dropTables();

            createTVPS();
            createTables();

            populateSourceTable();

            try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
                try (ResultSet rs = stmt
                        .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(srcTable))) {

                    try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn.prepareStatement(
                            "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(desTable) + " select * from ? ;")) {
                        pstmt.setStructured(1, AbstractSQLGenerator.escapeIdentifier(tvpName), rs);
                        pstmt.execute();
                        verifyDestinationTableData(expectedBigDecimals.length);

                        rs.beforeFirst();
                    }

                    try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn.prepareStatement(
                            "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(desTable) + " select * from ? ;")) {
                        pstmt.setStructured(1, AbstractSQLGenerator.escapeIdentifier(tvpName), rs);
                        pstmt.execute();
                        verifyDestinationTableData(expectedBigDecimals.length * 2);

                        rs.beforeFirst();
                    }

                    try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn.prepareStatement(
                            "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(desTable) + " select * from ? ;")) {
                        pstmt.setStructured(1, AbstractSQLGenerator.escapeIdentifier(tvpName), rs);
                        pstmt.execute();
                        verifyDestinationTableData(expectedBigDecimals.length * 3);
                    }

                    String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(desTable) + " values (?,?,?,?)";
                    Calendar calGMT = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
                    try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn.prepareStatement(sql)) {
                        for (int i = 0; i < expectedBigDecimals.length; i++) {
                            pstmt.setBigDecimal(1, expectedBigDecimals[i]);
                            pstmt.setString(2, expectedStrings[i]);
                            pstmt.setTimestamp(3, expectedTimestamps[i], calGMT);
                            pstmt.setString(4, expectedStrings[i]);
                            pstmt.execute();
                        }
                        verifyDestinationTableData(expectedBigDecimals.length * 4);
                    }
                }
                try (ResultSet rs = stmt
                        .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(srcTable));
                        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn
                                .prepareStatement("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(desTable)
                                        + " select * from ? ;")) {
                    pstmt.setStructured(1, AbstractSQLGenerator.escapeIdentifier(tvpName), rs);
                    pstmt.execute();
                    verifyDestinationTableData(expectedBigDecimals.length * 5);
                }
            }
        }
    }

    private static void verifyDestinationTableData(int expectedNumberOfRows) throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement();
                ResultSet rs = conn.createStatement()
                        .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(desTable))) {

            int expectedArrayLength = expectedBigDecimals.length;

            int i = 0;
            while (rs.next()) {
                assertTrue(rs.getString(1).equals(expectedBigDecimalStrings[i % expectedArrayLength]), "Expected Value:"
                        + expectedBigDecimalStrings[i % expectedArrayLength] + ", Actual Value: " + rs.getString(1));
                assertTrue(rs.getString(2).trim().equals(expectedStrings[i % expectedArrayLength]), "Expected Value:"
                        + expectedStrings[i % expectedArrayLength] + ", Actual Value: " + rs.getString(2));
                assertTrue(rs.getString(3).equals(expectedTimestampStrings[i % expectedArrayLength]), "Expected Value:"
                        + expectedTimestampStrings[i % expectedArrayLength] + ", Actual Value: " + rs.getString(3));
                assertTrue(rs.getString(4).trim().equals(expectedStrings[i % expectedArrayLength]), "Expected Value:"
                        + expectedStrings[i % expectedArrayLength] + ", Actual Value: " + rs.getString(4));
                i++;
            }

            assertTrue(i == expectedNumberOfRows);
        }
    }

    private static void populateSourceTable() throws SQLException {
        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(srcTable) + " values (?,?,?,?)";

        Calendar calGMT = Calendar.getInstance(TimeZone.getTimeZone("GMT"));

        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement();
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn.prepareStatement(sql)) {

            for (int i = 0; i < expectedBigDecimals.length; i++) {
                pstmt.setBigDecimal(1, expectedBigDecimals[i]);
                pstmt.setString(2, expectedStrings[i]);
                pstmt.setTimestamp(3, expectedTimestamps[i], calGMT);
                pstmt.setString(4, expectedStrings[i]);
                pstmt.execute();
            }
        }
    }

    private static void dropTables() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(srcTable), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(desTable), stmt);
        }
    }

    private static void createTables() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            String sql = "create table " + AbstractSQLGenerator.escapeIdentifier(srcTable)
                    + " (c1 decimal(10,5) null, c2 nchar(50) null, c3 datetime2(7) null, c4 char(7000));";
            stmt.execute(sql);

            sql = "create table " + AbstractSQLGenerator.escapeIdentifier(desTable)
                    + " (c1 decimal(10,5) null, c2 nchar(50) null, c3 datetime2(7) null, c4 char(7000));";
            stmt.execute(sql);
        }
    }

    private static void createTVPS() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            String TVPCreateCmd = "CREATE TYPE " + AbstractSQLGenerator.escapeIdentifier(tvpName)
                    + " as table (c1 decimal(10,5) null, c2 nchar(50) null, c3 datetime2(7) null, c4 char(7000) null)";
            stmt.execute(TVPCreateCmd);
        }
    }

    private static void dropTVPS() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            stmt.execute("IF EXISTS (SELECT * FROM sys.types WHERE is_table_type = 1 AND name = '"
                    + TestUtils.escapeSingleQuotes(tvpName) + "') " + " drop type "
                    + AbstractSQLGenerator.escapeIdentifier(tvpName));
        }
    }

    private static void dropProcedure() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(procedureName), stmt);
        }
    }

    private static void createProcedure() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            String sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(procedureName) + " @InputData "
                    + AbstractSQLGenerator.escapeIdentifier(tvpName) + " READONLY " + " AS " + " BEGIN "
                    + " INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(desTable) + " SELECT * FROM @InputData"
                    + " END";

            stmt.execute(sql);
        }
    }

    @AfterAll
    public static void terminate() throws SQLException {
        dropProcedure();
        dropTVPS();
        dropTables();
    }
}
