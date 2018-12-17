/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bvt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.math.BigDecimal;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.DBPreparedStatement;
import com.microsoft.sqlserver.testframework.DBResultSet;
import com.microsoft.sqlserver.testframework.DBResultSetTypes;
import com.microsoft.sqlserver.testframework.DBStatement;
import com.microsoft.sqlserver.testframework.DBTable;


@RunWith(JUnitPlatform.class)
@DisplayName("BVT Test")
@Tag("AzureDWTest")
public class BvtTest extends AbstractTest {
    private static String driverNamePattern = "Microsoft JDBC Driver \\d.\\d for SQL Server";
    static DBTable table1;
    static DBTable table2;

    /**
     * Connect to specified server and close the connection
     * 
     * @throws SQLException
     */
    @Test
    @DisplayName("test connection")
    public void testConnection() throws SQLException {
        try (DBConnection conn = new DBConnection(connectionString)) {}
    }

    /**
     * Verify isClosed()
     * 
     * @throws SQLException
     */
    @Test
    public void testConnectionIsClosed() throws SQLException {
        try (DBConnection conn = new DBConnection(connectionString)) {
            assertTrue(!conn.isClosed(), TestResource.getResource("R_connShouldNotBeClosed"));
            conn.close();
            assertTrue(conn.isClosed(), TestResource.getResource("R_connShouldNotBeOpen"));
        }
    }

    /**
     * Verify Driver Name and Version from MetaData
     * 
     * @throws SQLException
     */
    @Test
    public void testDriverNameAndDriverVersion() throws SQLException {
        try (DBConnection conn = new DBConnection(connectionString)) {
            DatabaseMetaData metaData = conn.getMetaData();
            Pattern p = Pattern.compile(driverNamePattern);
            Matcher m = p.matcher(metaData.getDriverName());
            assertTrue(m.find(), TestResource.getResource("R_incorrectDriverNameFormat"));
            String[] parts = metaData.getDriverVersion().split("\\.");
            if (parts.length != 4)
                assertTrue(true, TestResource.getResource("R_incorrectDriverVewrsionFormat"));
        }
    }

    /**
     * Create a statement, call close
     * 
     * @throws SQLException
     */
    @Test
    public void testCreateStatement() throws SQLException {
        try (DBConnection conn = new DBConnection(connectionString); DBStatement stmt = conn.createStatement();
                DBResultSet rs = stmt.selectAll(table1)) {
            rs.verify(table1);
        }
    }

    /**
     * Create a statement with a query timeout
     * 
     * @throws SQLException
     */
    @Test
    public void testCreateStatementWithQueryTimeout() throws SQLException {

        try (DBConnection conn = new DBConnection(connectionString + ";querytimeout=10");
                DBStatement stmt = conn.createStatement()) {
            assertEquals(10, stmt.getQueryTimeout());
        }
    }

    /**
     * Create a statement ResultSet.Type_forward_only, ResultSet.CONCUR_READ_ONLY, executeQuery verify cursor by using
     * next and previous and verify data
     * 
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    @Test
    public void testStmtForwardOnlyReadOnly() throws SQLException, ClassNotFoundException {
        try (DBConnection conn = new DBConnection(connectionString);
                DBStatement stmt = conn.createStatement(DBResultSetTypes.TYPE_FORWARD_ONLY_CONCUR_READ_ONLY);
                DBResultSet rs = stmt.selectAll(table1)) {

            rs.next();
            rs.verifyCurrentRow(table1);
            rs.next();
            rs.verifyCurrentRow(table1);

            try {
                rs.previous();
                assertTrue(false, "Previous should have thrown an exception");
            } catch (SQLException ex) {
                // expected exception
            }
            rs.verify(table1);
        }
    }

    /**
     * Create a statement, ResultSet.SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY, executeQuery verify cursor by using
     * next, afterlast and previous and verify data
     * 
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    @Test
    public void testStmtScrollInsensitiveReadOnly() throws SQLException, ClassNotFoundException {
        assumeFalse(isSqlAzureDW(), TestResource.getResource("R_cursorAzureDW"));
        try (DBConnection conn = new DBConnection(connectionString);
                DBStatement stmt = conn.createStatement(DBResultSetTypes.TYPE_SCROLL_INSENSITIVE_CONCUR_READ_ONLY);
                DBResultSet rs = stmt.selectAll(table1)) {
            rs.next();
            rs.verifyCurrentRow(table1);
            rs.afterLast();
            rs.previous();
            rs.verifyCurrentRow(table1);
            rs.verify(table1);
        }
    }

    /**
     * Create a statement ResultSet.SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY, executeQuery verify cursor by using
     * next and absolute and verify data
     * 
     * @throws SQLException
     */
    @Test
    public void testStmtScrollSensitiveReadOnly() throws SQLException {
        assumeFalse(isSqlAzureDW(), TestResource.getResource("R_cursorAzureDW"));
        try (DBConnection conn = new DBConnection(connectionString);
                DBStatement stmt = conn.createStatement(DBResultSetTypes.TYPE_SCROLL_SENSITIVE_CONCUR_READ_ONLY);
                DBResultSet rs = stmt.selectAll(table1)) {
            rs.next();
            rs.next();
            rs.verifyCurrentRow(table1);
            rs.absolute(3);
            rs.verifyCurrentRow(table1);
            rs.absolute(1);
            rs.verify(table1);

        }
    }

    /**
     * Create a statement ResultSet.Type_forward_only, ResultSet.CONCUR_UPDATABLE, executeQuery verify cursor by using
     * next and previous and verify data
     * 
     * @throws SQLException
     */
    @Test
    public void testStmtForwardOnlyUpdateable() throws SQLException {
        assumeFalse(isSqlAzureDW(), TestResource.getResource("R_cursorAzureDW"));
        try (DBConnection conn = new DBConnection(connectionString);
                DBStatement stmt = conn.createStatement(DBResultSetTypes.TYPE_FORWARD_ONLY_CONCUR_UPDATABLE);
                DBResultSet rs = stmt.selectAll(table1)) {
            rs.next();
            // Verify resultset behavior
            rs.next();
            rs.verifyCurrentRow(table1);
            rs.next();
            rs.verifyCurrentRow(table1);
            try {
                rs.previous();
                assertTrue(false, TestResource.getResource("R_previousShouldThrow"));
            } catch (SQLException ex) {
                // expected exception
            }
            rs.verify(table1);
        }
    }

    /**
     * Create a statement ResultSet.SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE, executeQuery verify cursor by using
     * next and previous and verify data
     * 
     * @throws SQLException
     */
    @Test
    public void testStmtScrollSensitiveUpdatable() throws SQLException {
        assumeFalse(isSqlAzureDW(), TestResource.getResource("R_cursorAzureDW"));
        try (DBConnection conn = new DBConnection(connectionString);
                DBStatement stmt = conn.createStatement(DBResultSetTypes.TYPE_SCROLL_SENSITIVE_CONCUR_UPDATABLE);
                DBResultSet rs = stmt.selectAll(table1)) {

            // Verify resultset behavior
            rs.next();
            rs.next();
            rs.verifyCurrentRow(table1);
            rs.absolute(3);
            rs.verifyCurrentRow(table1);
            rs.absolute(1);
            rs.verify(table1);
        }
    }

    /**
     * Create a statement TYPE_SS_SCROLL_DYNAMIC, CONCUR_SS_OPTIMISTIC_CC, executeQuery verify cursor by using next and
     * previous and verify data
     * 
     * @throws SQLException
     */
    @Test
    public void testStmtSSScrollDynamicOptimisticCC() throws SQLException {
        assumeFalse(isSqlAzureDW(), TestResource.getResource("R_cursorAzureDW"));
        try (DBConnection conn = new DBConnection(connectionString);
                DBStatement stmt = conn.createStatement(DBResultSetTypes.TYPE_DYNAMIC_CONCUR_OPTIMISTIC);
                DBResultSet rs = stmt.selectAll(table1)) {

            // Verify resultset behavior
            rs.next();
            rs.afterLast();
            rs.previous();
            rs.verify(table1);
        }
    }

    /**
     * Create a statement TYPE_SS_SEVER_CURSOR_FORWARD_ONLY, CONCUR_READ_ONLY, executeQuery verify cursor by using next
     * and verify data
     * 
     * @throws SQLException
     */
    @Test
    public void testStmtSserverCursorForwardOnly() throws SQLException {

        DBResultSetTypes rsType = DBResultSetTypes.TYPE_FORWARD_ONLY_CONCUR_READ_ONLY;
        try (DBConnection conn = new DBConnection(connectionString);
                DBStatement stmt = conn.createStatement(rsType.resultsetCursor, rsType.resultSetConcurrency);
                DBResultSet rs = stmt.selectAll(table1)) {
            // Verify resultset behavior
            rs.next();
            rs.verify(table1);
        }
    }

    /**
     * Create a preparedStatement, call close
     * 
     * @throws SQLException
     */
    @Test
    public void testCreatepreparedStatement() throws SQLException {

        String colName = table1.getEscapedColumnName(7);
        String value = table1.getRowData(7, 0).toString();
        String query = "SELECT * from " + table1.getEscapedTableName() + " where " + colName + " = ? ";

        try (DBConnection conn = new DBConnection(connectionString);
                DBPreparedStatement pstmt = conn.prepareStatement(query)) {

            pstmt.setObject(1, new BigDecimal(value));
            try (DBResultSet rs = pstmt.executeQuery()) {
                rs.verify(table1);
            }
        }
    }

    /**
     * Verify resultset using ResultSetMetaData
     * 
     * @throws SQLException
     */
    @Test
    public void testResultSet() throws SQLException {
        try (DBConnection conn = new DBConnection(connectionString); DBStatement stmt = conn.createStatement();
                DBResultSet rs = stmt.selectAll(table1)) {
            // verify resultSet
            rs.verify(table1);
        }
    }

    /**
     * Verify resultset and close resultSet
     * 
     * @throws SQLException
     */
    @Test
    public void testResultSetAndClose() throws SQLException {
        try (DBConnection conn = new DBConnection(connectionString); DBStatement stmt = conn.createStatement();
                DBResultSet rs = stmt.selectAll(table1)) {

            try {
                if (null != rs)
                    rs.close();
            } catch (SQLException e) {
                fail(e.toString());
            }
        }
    }

    /**
     * Verify two concurrent resultsets from same connection, separate statements
     * 
     * @throws SQLException
     */
    @Test
    public void testTwoResultsetsDifferentStmt() throws SQLException {
        try (DBConnection conn = new DBConnection(connectionString); DBStatement stmt1 = conn.createStatement();
                DBStatement stmt2 = conn.createStatement()) {

            try (DBResultSet rs1 = stmt1.selectAll(table1); DBResultSet rs2 = stmt2.selectAll(table2)) {

                // Interleave resultset calls
                rs1.next();
                rs1.verifyCurrentRow(table1);
                rs2.next();
                rs2.verifyCurrentRow(table2);
                rs1.next();
                rs1.verifyCurrentRow(table1);
                rs1.verify(table1);
                rs1.close();
                rs2.next();
                rs2.verify(table2);
                rs2.close();
            }
        }
    }

    /**
     * Verify two concurrent resultsets from same connection, same statement
     * 
     * @throws SQLException
     */
    @Test
    public void testTwoResultsetsSameStmt() throws SQLException {
        try (DBConnection conn = new DBConnection(connectionString); DBStatement stmt = conn.createStatement()) {

            try (DBResultSet rs1 = stmt.selectAll(table1); DBResultSet rs2 = stmt.selectAll(table2)) {
                // Interleave resultset calls. rs is expected to be closed
                try {
                    rs1.next();
                } catch (SQLException e) {
                    assertEquals(e.getMessage(), TestResource.getResource("R_resultsetClosed"));
                }
                rs2.next();
                rs2.verifyCurrentRow(table2);
                try {
                    rs1.next();
                } catch (SQLException e) {
                    assertEquals(e.getMessage(), TestResource.getResource("R_resultsetClosed"));
                }
                rs1.close();
                rs2.next();
                rs2.verify(table2);
                rs2.close();
            }
        }
    }

    /**
     * Verify resultset closed after statement is closed
     * 
     * @throws SQLException
     */
    @Test
    public void testResultSetAndCloseStmt() throws SQLException {
        try (DBConnection conn = new DBConnection(connectionString); DBStatement stmt = conn.createStatement();
                DBResultSet rs = stmt.selectAll(table1)) {

            stmt.close(); // this should close the resultSet
            try {
                rs.next();
            } catch (SQLException e) {
                assertEquals(e.getMessage(), TestResource.getResource("R_resultsetClosed"));
            }
            assertTrue(true, "Previous one should have thrown exception!");
        }
    }

    /**
     * Verify resultset using SelectMethod
     * 
     * @throws SQLException
     */
    @Test
    public void testResultSetSelectMethod() throws SQLException {
        assumeFalse(isSqlAzureDW(), TestResource.getResource("R_cursorAzureDW"));
        try (DBConnection conn = new DBConnection(connectionString + ";selectMethod=cursor;");
                DBStatement stmt = conn.createStatement(); DBResultSet rs = stmt.selectAll(table1)) {
            rs.verify(table1);
        }
    }

    /**
     * drops tables
     * 
     * @throws SQLException
     */
    @AfterAll
    public static void terminate() throws SQLException {

        try (DBConnection conn = new DBConnection(connectionString); DBStatement stmt = conn.createStatement()) {
            stmt.execute("if object_id('" + table2.getEscapedQuotesTableName() + "','U') is not null" + " drop table "
                    + table1.getEscapedTableName());
            stmt.execute("if object_id('" + table2.getEscapedQuotesTableName() + "','U') is not null" + " drop table "
                    + table2.getEscapedTableName());
        }
    }

    @BeforeAll
    public static void init() throws SQLException {
        try (DBConnection conn = new DBConnection(connectionString); DBStatement stmt = conn.createStatement()) {
            // create tables
            table1 = new DBTable(true);
            stmt.createTable(table1);
            stmt.populateTable(table1);
            table2 = new DBTable(true);
            stmt.createTable(table2);
            stmt.populateTable(table2);
        }
    }
}
