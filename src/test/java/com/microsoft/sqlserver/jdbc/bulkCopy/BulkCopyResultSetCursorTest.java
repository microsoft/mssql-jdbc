/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

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
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;


@RunWith(JUnitPlatform.class)
public class BulkCopyResultSetCursorTest extends AbstractTest {

    static BigDecimal[] expectedBigDecimals = {new BigDecimal("12345.12345"), new BigDecimal("125.123"),
            new BigDecimal("45.12345")};
    static String[] expectedBigDecimalStrings = {"12345.12345", "125.12300", "45.12345"};

    static String[] expectedStrings = {"hello", "world", "!!!"};

    static Timestamp[] expectedTimestamps = {new Timestamp(1433338533461L), new Timestamp(14917485583999L),
            new Timestamp(1491123533000L)};
    static String[] expectedTimestampStrings = {"2015-06-03 13:35:33.4610000", "2442-09-19 01:59:43.9990000",
            "2017-04-02 08:58:53.0000000"};

    private static String srcTable = RandomUtil.getIdentifier("BulkCopyResultSetCursorTest_SourceTable");
    private static String desTable = RandomUtil.getIdentifier("BulkCopyResultSetCursorTest_DestinationTable");

    /**
     * Test a previous failure when using server cursor and using the same connection to create Bulk Copy and result
     * set.
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

            dropTables(stmt);
            createTables(stmt);
            populateSourceTable();

            try (Statement stmt2 = conn.createStatement(resultSetType, resultSetConcurrency);
                    ResultSet rs = stmt2.executeQuery(
                            "select * from " + AbstractSQLGenerator.escapeIdentifier(srcTable) + " ORDER BY id ASC");
                    SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn)) {
                bulkCopy.setDestinationTableName(AbstractSQLGenerator.escapeIdentifier(desTable));
                bulkCopy.writeToServer(rs);

                verifyDestinationTableData(expectedBigDecimals.length);
            }
        }
    }

    /**
     * Test a previous failure when setting SelectMethod to cursor and using the same connection to create Bulk Copy and
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
            dropTables(stmt);
            createTables(stmt);
            populateSourceTable();

            try (ResultSet rs = stmt.executeQuery(
                    "select * from " + AbstractSQLGenerator.escapeIdentifier(srcTable) + " ORDER BY id ASC");
                    SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn)) {

                bulkCopy.setDestinationTableName(AbstractSQLGenerator.escapeIdentifier(desTable));
                bulkCopy.writeToServer(rs);

                verifyDestinationTableData(expectedBigDecimals.length);
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
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {

            dropTables(stmt);
            createTables(stmt);
            populateSourceTable();

            try (Statement stmt1 = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs = stmt1.executeQuery(
                            "select * from " + AbstractSQLGenerator.escapeIdentifier(srcTable) + " ORDER BY id ASC")) {
                try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn)) {
                    bulkCopy.setDestinationTableName(AbstractSQLGenerator.escapeIdentifier(desTable));
                    bulkCopy.writeToServer(rs);
                    verifyDestinationTableData(expectedBigDecimals.length);
                }

                rs.beforeFirst();
                try (SQLServerBulkCopy bulkCopy1 = new SQLServerBulkCopy(conn)) {
                    bulkCopy1.setDestinationTableName(AbstractSQLGenerator.escapeIdentifier(desTable));
                    bulkCopy1.writeToServer(rs);
                    verifyDestinationTableData(expectedBigDecimals.length * 2);
                }

                rs.beforeFirst();
                try (SQLServerBulkCopy bulkCopy2 = new SQLServerBulkCopy(conn)) {
                    bulkCopy2.setDestinationTableName(AbstractSQLGenerator.escapeIdentifier(desTable));
                    bulkCopy2.writeToServer(rs);
                    verifyDestinationTableData(expectedBigDecimals.length * 3);
                }

                String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(desTable) + " values (?,?,?,?)";
                Calendar calGMT = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
                try (SQLServerPreparedStatement pstmt1 = (SQLServerPreparedStatement) conn.prepareStatement(sql)) {
                    for (int i = 0; i < expectedBigDecimals.length; i++) {
                        pstmt1.setBigDecimal(1, expectedBigDecimals[i]);
                        pstmt1.setString(2, expectedStrings[i]);
                        pstmt1.setTimestamp(3, expectedTimestamps[i], calGMT);
                        pstmt1.setString(4, expectedStrings[i]);
                        pstmt1.execute();
                    }
                    verifyDestinationTableData(expectedBigDecimals.length * 4);
                }
                try (Statement stmt2 = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                        ResultSet.CONCUR_UPDATABLE);
                        ResultSet rs2 = stmt2.executeQuery("select * from "
                                + AbstractSQLGenerator.escapeIdentifier(srcTable) + " ORDER BY id ASC");
                        SQLServerBulkCopy bulkCopy3 = new SQLServerBulkCopy(conn)) {
                    bulkCopy3.setDestinationTableName(AbstractSQLGenerator.escapeIdentifier(desTable));
                    bulkCopy3.writeToServer(rs2);
                    verifyDestinationTableData(expectedBigDecimals.length * 5);
                }
            }
        }
    }

    private static void verifyDestinationTableData(int expectedNumberOfRows) throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString);
                ResultSet rs = conn.createStatement().executeQuery(
                        "select * from " + AbstractSQLGenerator.escapeIdentifier(desTable) + " ORDER BY id ASC")) {

            int expectedArrayLength = expectedBigDecimals.length;

            int i = 0;
            while (rs.next()) {
                assertTrue(rs.getString(2).equals(expectedBigDecimalStrings[i % expectedArrayLength]), "Expected Value:"
                        + expectedBigDecimalStrings[i % expectedArrayLength] + ", Actual Value: " + rs.getString(2));
                assertTrue(rs.getString(3).trim().equals(expectedStrings[i % expectedArrayLength]), "Expected Value:"
                        + expectedStrings[i % expectedArrayLength] + ", Actual Value: " + rs.getString(3));
                assertTrue(rs.getString(4).equals(expectedTimestampStrings[i % expectedArrayLength]), "Expected Value:"
                        + expectedTimestampStrings[i % expectedArrayLength] + ", Actual Value: " + rs.getString(4));
                assertTrue(rs.getString(5).trim().equals(expectedStrings[i % expectedArrayLength]), "Expected Value:"
                        + expectedStrings[i % expectedArrayLength] + ", Actual Value: " + rs.getString(5));
                i++;
            }

            assertTrue(i == expectedNumberOfRows);
        }
    }

    private static void populateSourceTable() throws SQLException {
        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(srcTable) + " values (?,?,?,?)";
        Calendar calGMT = Calendar.getInstance(TimeZone.getTimeZone("GMT"));

        try (Connection conn = DriverManager.getConnection(connectionString);
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

    private static void dropTables(Statement stmt) throws SQLException {
        if (null != srcTable) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(srcTable), stmt);
        }
        if (null != desTable) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(desTable), stmt);
        }
    }

    private static void createTables(Statement stmt) throws SQLException {
        String sql = "create table " + AbstractSQLGenerator.escapeIdentifier(srcTable)
                + " (id INT NOT NULL IDENTITY PRIMARY KEY, c1 decimal(10,5) null, c2 nchar(50) null, c3 datetime2(7) null, c4 char(7000));";
        stmt.execute(sql);
        sql = "create table " + AbstractSQLGenerator.escapeIdentifier(desTable)
                + " (id INT NOT NULL IDENTITY PRIMARY KEY, c1 decimal(10,5) null, c2 nchar(50) null, c3 datetime2(7) null, c4 char(7000));";
        stmt.execute(sql);
    }

    /**
     * drops tables
     * 
     * @throws SQLException
     */
    @AfterAll
    public static void terminate() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            dropTables(stmt);
        }
    }
}
