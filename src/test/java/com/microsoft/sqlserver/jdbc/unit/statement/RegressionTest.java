/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;


@RunWith(JUnitPlatform.class)
public class RegressionTest extends AbstractTest {
    private static String tableName;
    private static String procName = RandomUtil.getIdentifier("ServerCursorProc");

    /**
     * Tests select into stored proc
     * 
     * @throws SQLException
     */
    @Test
    public void testServerCursorPStmt() throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(connectionString);
                Statement stmt = con.createStatement()) {

            // expected values
            int numRowsInResult = 1;
            String col3Value = "India";
            String col3Lookup = "IN";

            tableName = RandomUtil.getIdentifier("ServerCursorPStmt");

            stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (col1 int primary key, col2 varchar(3), col3 varchar(128))");
            stmt.executeUpdate(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " VALUES (1, 'CAN', 'Canada')");
            stmt.executeUpdate("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " VALUES (2, 'USA', 'United States of America')");
            stmt.executeUpdate(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " VALUES (3, 'JPN', 'Japan')");
            stmt.executeUpdate("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " VALUES (4, '"
                    + col3Lookup + "', '" + col3Value + "')");

            // create stored proc
            String storedProcString;

            if (isSqlAzure()) {
                // On SQL Azure, 'SELECT INTO' is not supported. So do not use it.
                storedProcString = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(procName)
                        + " @param varchar(3) AS SELECT col3 FROM " + AbstractSQLGenerator.escapeIdentifier(tableName)
                        + " WHERE col2 = @param";
            } else {
                // On SQL Server
                storedProcString = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(procName)
                        + " @param varchar(3) AS SELECT col3 INTO #TMPTABLE FROM "
                        + AbstractSQLGenerator.escapeIdentifier(tableName)
                        + " WHERE col2 = @param SELECT col3 FROM #TMPTABLE";
            }

            stmt.executeUpdate(storedProcString);

            // execute stored proc via pstmt
            try (PreparedStatement pstmt = con.prepareStatement(
                    "EXEC " + AbstractSQLGenerator.escapeIdentifier(procName) + " ?", ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY)) {
                pstmt.setString(1, col3Lookup);

                // should return 1 row
                try (ResultSet rs = pstmt.executeQuery()) {
                    rs.last();
                    assertEquals(rs.getRow(), numRowsInResult,
                            TestResource.getResource("R_valueNotMatch") + rs.getRow() + ", " + numRowsInResult);
                    rs.beforeFirst();
                    while (rs.next()) {
                        assertEquals(rs.getString(1), col3Value,
                                TestResource.getResource("R_valueNotMatch") + rs.getString(1) + ", " + col3Value);
                    }
                }
            } finally {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            }
        }
    }

    /**
     * Tests update count returned by SELECT INTO
     * 
     * @throws SQLException
     */
    @Test
    public void testSelectIntoUpdateCount() throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(connectionString)) {

            // Azure does not do SELECT INTO
            if (!isSqlAzure()) {
                tableName = RandomUtil.getIdentifier("[#SourceTableForSelectInto]]");

                try (Statement stmt = con.createStatement()) {
                    stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                            + " (col1 int primary key, col2 varchar(3), col3 varchar(128))");
                    stmt.executeUpdate("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                            + " VALUES (1, 'CAN', 'Canada')");
                    stmt.executeUpdate("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                            + " VALUES (2, 'USA', 'United States of America')");
                    stmt.executeUpdate("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                            + " VALUES (3, 'JPN', 'Japan')");

                    // expected values
                    int numRowsToCopy = 2;

                    try (PreparedStatement ps = con.prepareStatement("SELECT * INTO #TMPTABLE FROM "
                            + AbstractSQLGenerator.escapeIdentifier(tableName) + " WHERE col1 <= ?")) {
                        ps.setInt(1, numRowsToCopy);
                        int updateCount = ps.executeUpdate();
                        assertEquals(numRowsToCopy, updateCount, TestResource.getResource("R_incorrectUpdateCount"));
                    } finally {
                        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
                    }
                }
            }
        }
    }

    /**
     * Tests update query
     * 
     * @throws SQLException
     */
    @Test
    public void testUpdateQuery() throws SQLException {
        assumeTrue("JDBC41".equals(TestUtils.getConfiguredProperty("JDBC_Version")),
                TestResource.getResource("R_incompatJDBC"));

        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(connectionString);
                Statement stmt = con.createStatement()) {
            String sql;
            JDBCType[] targets = {JDBCType.INTEGER, JDBCType.SMALLINT};
            int rows = 3;
            tableName = RandomUtil.getIdentifier("[updateQuery]");

            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName) + " ("
                    + "c1 int null," + "PK int NOT NULL PRIMARY KEY" + ")");

            /*
             * populate table
             */
            sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values(" + "?,?" + ")";
            try (PreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(sql,
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, con.getHoldability())) {

                for (int i = 1; i <= rows; i++) {
                    pstmt.setObject(1, i, JDBCType.INTEGER);
                    pstmt.setObject(2, i, JDBCType.INTEGER);
                    pstmt.executeUpdate();
                }
            }
            /*
             * Update table
             */
            sql = "update " + AbstractSQLGenerator.escapeIdentifier(tableName) + " SET c1= ? where PK =1";
            for (int i = 1; i <= rows; i++) {
                try (PreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(sql,
                        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                    for (JDBCType target : targets) {
                        pstmt.setObject(1, 5 + i, target);
                        pstmt.executeUpdate();
                    }
                }
            }

            /*
             * Verify
             */
            try (ResultSet rs = stmt
                    .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertEquals(rs.getInt(1), 8, "Value mismatch");
            } finally {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            }
        }
    }

    /**
     * Tests XML query
     * 
     * @throws SQLException
     */
    @Test
    public void testXmlQuery() throws SQLException {
        assumeTrue("JDBC41".equals(TestUtils.getConfiguredProperty("JDBC_Version")),
                TestResource.getResource("R_incompatJDBC"));

        try (Connection connection = DriverManager.getConnection(connectionString);
                Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            createTable(stmt);

            tableName = RandomUtil.getIdentifier("try_SQLXML_Table");

            String sql = "UPDATE " + AbstractSQLGenerator.escapeIdentifier(tableName) + " SET [c2] = ?, [c3] = ?";
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(sql)) {
                pstmt.setObject(1, null);
                pstmt.setObject(2, null, Types.SQLXML);
                pstmt.executeUpdate();
            }

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(sql)) {
                pstmt.setObject(1, null, Types.SQLXML);
                pstmt.setObject(2, null);
                pstmt.executeUpdate();
            }

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(sql)) {
                pstmt.setObject(1, null);
                pstmt.setObject(2, null, Types.SQLXML);
                pstmt.executeUpdate();
            } finally {
                TestUtils.dropTableIfExists(tableName, stmt);
            }
        }
    }

    private void createTable(Statement stmt) throws SQLException {

        String sql = "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                + " ([c1] int, [c2] xml, [c3] xml)";

        stmt.execute(sql);
    }

    @AfterAll
    public static void terminate() throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(connectionString);
                Statement stmt = con.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(procName), stmt);
        }
    }
}
