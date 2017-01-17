/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) 2016 Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bvt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.AfterClass;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.DBPreparedStatement;
import com.microsoft.sqlserver.testframework.DBResultSet;
import com.microsoft.sqlserver.testframework.DBStatement;

@RunWith(JUnitPlatform.class)
@DisplayName("BVT Test")
public class bvtTest extends bvtTestSetup {
    private static String driverNamePattern = "Microsoft JDBC Driver \\d.\\d for SQL Server";
    private static DBResultSet rs = null;
    private static DBPreparedStatement pstmt = null;
    private static DBConnection conn = null;
    private static DBStatement stmt = null;

    ///////////////////////////////////////////////////////////////////
    //// Connect to specified server and close the connection
    /////////////////////////////////////////////////////////////////////
    @Test
    @DisplayName("test connection")
    public void testConnection() throws SQLException {
        try {
            conn = new DBConnection(connectionString);
            conn.close();
        }
        finally {
            terminateVariation();
        }
    }

    /////////////////////////////////////////////////////////////////////
    //// Verify isClosed()
    /////////////////////////////////////////////////////////////////////
    @Test
    public void testConnectionIsClosed() throws SQLException {
        try {
            conn = new DBConnection(connectionString);
            assertTrue("BVT connection should not be closed", !conn.isClosed());
            conn.close();
            assertTrue("BVT connection should not be open", conn.isClosed());
        }
        finally {
            terminateVariation();
        }
    }

    /////////////////////////////////////////////////////////////////////
    //// Verify Driver Name and Version from MetaData
    /////////////////////////////////////////////////////////////////////
    @Test
    public void testDriverNameAndDriverVersion() throws SQLException {
        try {
            conn = new DBConnection(connectionString);
            DatabaseMetaData metaData = conn.getMetaData();
            Pattern p = Pattern.compile(driverNamePattern);
            Matcher m = p.matcher(metaData.getDriverName());
            assertTrue("Driver name is not a correct format! ", m.find());
            String[] parts = metaData.getDriverVersion().split("\\.");
            if (parts.length != 4)
                assertTrue("Driver version number should be four parts! ", true);
        }
        finally {
            terminateVariation();
        }
    }

    ///////////////////////////////////////////////////////////////////
    // Create a statement, call close
    ///////////////////////////////////////////////////////////////////
    @Test
    public void testCreateStatement() throws SQLException {

        try {
            conn = new DBConnection(connectionString);
            stmt = conn.createStatement();
            String query = "SELECT * FROM " + table1.getEscapedTableName() + ";";
            rs = stmt.executeQuery(query);
            rs.verify(table1);
        }
        finally {

            terminateVariation();
        }
    }

    ///////////////////////////////////////////////////////////////////
    // Create a statement with a query timeout
    // ResultSet.Type_forward_only,
    // ResultSet.CONCUR_READ_ONLY, executeQuery
    // verify cursor by using next and previous and verify data
    ///////////////////////////////////////////////////////////////////
    @Test
    public void testCreateStatementWithQueryTimeout() throws SQLException {

        try {
            conn = new DBConnection(connectionString + ";querytimeout=10");
            stmt = conn.createStatement();
            assertEquals(10, stmt.getQueryTimeout());
        }
        finally {
            terminateVariation();
        }
    }

    ///////////////////////////////////////////////////////////////////
    // Create a statement
    // ResultSet.Type_forward_only,
    // ResultSet.CONCUR_READ_ONLY, executeQuery
    // verify cursor by using next and previous and verify data
    ///////////////////////////////////////////////////////////////////
    @Test
    public void testStmtForwardOnlyReadOnly() throws SQLException, ClassNotFoundException {

        try {
            conn = new DBConnection(connectionString);
            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

            String query = "SELECT * FROM " + table1.getEscapedTableName();
            rs = stmt.executeQuery(query);

            rs.next();
            rs.verifyCurrentRow(table1);
            rs.next();
            rs.verifyCurrentRow(table1);

            try {
                rs.previous();
                assertTrue("Previous should have thrown an exception", false);
            }
            catch (SQLException ex) {
                // expected exception
            }
            rs.verify(table1);
        }
        finally {
            terminateVariation();
        }
    }

    ///////////////////////////////////////////////////////////////////
    // Create a statement
    // ResultSet.SCROLL_INSENSITIVE,
    // ResultSet.CONCUR_READ_ONLY, executeQuery
    // verify cursor by using next, afterlast and previous and verify data
    ///////////////////////////////////////////////////////////////////
    @Test
    public void testStmtScrollInsensitiveReadOnly() throws SQLException, ClassNotFoundException {
        try {
            conn = new DBConnection(connectionString);
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

            String query = "SELECT * FROM" + table1.getEscapedTableName();
            rs = stmt.executeQuery(query);
            rs.next();
            rs.verifyCurrentRow(table1);
            rs.afterLast();
            rs.previous();
            rs.verifyCurrentRow(table1);
            rs.verify(table1);
        }
        finally {
            terminateVariation();
        }
    }

    // /////////////////////////////////////////////////////////////////
    // // Create a statement
    // // ResultSet.SCROLL_SENSITIVE,
    // // ResultSet.CONCUR_READ_ONLY, executeQuery
    // // verify cursor by using next and absolute and verify data
    // ///////////////////////////////////////////////////////////////////
    @Test
    public void testStmtScrollSensitiveReadOnly() throws SQLException {

        try {
            conn = new DBConnection(connectionString);
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);

            String query = "SELECT * FROM " + table1.getEscapedTableName();
            rs = stmt.executeQuery(query);
            rs.next();
            rs.next();
            rs.verifyCurrentRow(table1);
            rs.absolute(3);
            rs.verifyCurrentRow(table1);
            rs.absolute(1);
            rs.verify(table1);

        }
        finally {
            terminateVariation();
        }
    }

    ///////////////////////////////////////////////////////////////////
    // Create a statement
    // ResultSet.Type_forward_only,
    // ResultSet.CONCUR_UPDATABLE, executeQuery
    // verify cursor by using next and previous and verify data
    ///////////////////////////////////////////////////////////////////
    @Test
    public void testStmtForwardOnlyUpdateable() throws SQLException {

        try {
            conn = new DBConnection(connectionString);
            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);

            String query = "SELECT * FROM " + table1.getEscapedTableName();
            rs = stmt.executeQuery(query);
            rs.next();

            // Verify resultset behavior
            rs.next();
            rs.verifyCurrentRow(table1);
            rs.next();
            rs.verifyCurrentRow(table1);
            try {
                rs.previous();
                assertTrue("Previous should have thrown an exception", false);
            }
            catch (SQLException ex) {
                // expected exception
            }
            rs.verify(table1);
        }
        finally {
            terminateVariation();
        }
    }

    ///////////////////////////////////////////////////////////////////
    // Create a statement
    // ResultSet.SCROLL_SENSITIVE,
    // ResultSet.CONCUR_UPDATABLE, executeQuery
    // verify cursor by using next and previous and verify data
    ///////////////////////////////////////////////////////////////////
    @Test
    public void testStmtScrollSensitiveUpdatable() throws SQLException {

        try {
            conn = new DBConnection(connectionString);
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

            // DBResultSet.currentTable = table1;
            String query = "SELECT * FROM " + table1.getEscapedTableName();
            rs = stmt.executeQuery(query);

            // Verify resultset behavior
            rs.next();
            rs.next();
            rs.verifyCurrentRow(table1);
            rs.absolute(3);
            rs.verifyCurrentRow(table1);
            rs.absolute(1);
            rs.verify(table1);
        }
        finally {
            terminateVariation();
        }
    }

    ///////////////////////////////////////////////////////////////////
    // Create a statement
    // TYPE_SS_SCROLL_DYNAMIC,
    // CONCUR_SS_OPTIMISTIC_CC, executeQuery
    // verify cursor by using next and previous and verify data
    ///////////////////////////////////////////////////////////////////
    @Test
    public void testStmtSS_ScrollDynamicOptimistic_CC() throws SQLException {

        try {
            int TYPE_SS_SCROLL_DYNAMIC = 1006;
            int CONCUR_SS_OPTIMISTIC_CC = 1008;
            conn = new DBConnection(connectionString);
            stmt = conn.createStatement(TYPE_SS_SCROLL_DYNAMIC, CONCUR_SS_OPTIMISTIC_CC);

            String query = "SELECT * FROM " + table1.getEscapedTableName();
            rs = stmt.executeQuery(query);

            // Verify resultset behavior
            rs.next();
            rs.afterLast();
            rs.previous();
            rs.verify(table1);
        }
        finally {
            terminateVariation();
        }
    }

    ///////////////////////////////////////////////////////////////////
    // Create a statement
    // TYPE_SS_SEVER_CURSOR_FORWARD_ONLY,
    // CONCUR_READ_ONLY, executeQuery
    // verify cursor by using next and verify data
    ///////////////////////////////////////////////////////////////////
    @Test
    public void testStmtSS_SEVER_CURSOR_FORWARD_ONLY() throws SQLException {

        try {
            int TYPE_SS_SEVER_CURSOR_FORWARD_ONLY = 2004;
            int CONCUR_READ_ONLY = 1008;
            conn = new DBConnection(connectionString);
            stmt = conn.createStatement(TYPE_SS_SEVER_CURSOR_FORWARD_ONLY, CONCUR_READ_ONLY);

            String query = "SELECT * FROM " + table1.getEscapedTableName();

            rs = stmt.executeQuery(query);

            // Verify resultset behavior
            rs.next();
            rs.verify(table1);
        }
        finally {
            terminateVariation();
        }

    }

    ///////////////////////////////////////////////////////////////////
    // Create a preparedstatement, call close
    ///////////////////////////////////////////////////////////////////
    @Test
    public void testCreatepreparedStatement() throws SQLException {

        try {
            conn = new DBConnection(connectionString);
            String colName = table1.getColumnName(7);
            String value = DBResultSet.getRow(table1, 0).get(7).toString();

            String query = "SELECT * from " + table1.getEscapedTableName() + " where [" + colName + "] = ? ";

            pstmt = conn.prepareStatement(query);
            pstmt.setObject(1, new BigDecimal(value));
           
            rs = pstmt.executeQuery();
            rs.verify(table1);
        }
        finally {
            terminateVariation();
        }
    }

    ///////////////////////////////////////////////////////////////////
    // Verify resultset using ResultSetMetaData
    ///////////////////////////////////////////////////////////////////
    @Test
    public void testResultSet() throws SQLException {

        try {
            conn = new DBConnection(connectionString);
            stmt = conn.createStatement();

            String query = "SELECT * FROM " + table1.getEscapedTableName();
            rs = stmt.executeQuery(query);

            // verify resultSet
            rs.verify(table1);
        }
        finally {
            terminateVariation();
        }
    }

    ///////////////////////////////////////////////////////////////////
    // Verify resultset and close resultSet
    ///////////////////////////////////////////////////////////////////
    @Test
    public void testResultSetAndClose() throws SQLException {

        try {
            conn = new DBConnection(connectionString);
            stmt = conn.createStatement();

            String query = "SELECT * FROM " + table1.getEscapedTableName();
            rs = stmt.executeQuery(query);
            rs.currentTable = table1;

            rs.verify(table1);
            stmt.close();
        }
        finally {
            terminateVariation();
        }
    }

    ///////////////////////////////////////////////////////////////////
    // Verify two concurrent resultsets from same connection,
    // separate statements
    ///////////////////////////////////////////////////////////////////
    @Test
    public void testTwoResultsetsDifferentStmt() throws SQLException {

        DBStatement stmt1 = null;
        DBStatement stmt2 = null;
        DBResultSet rs1 = null;
        DBResultSet rs2 = null;
        try {
            conn = new DBConnection(connectionString);
            stmt1 = conn.createStatement();
            stmt2 = conn.createStatement();

            String query = "SELECT * FROM " + table1.getEscapedTableName();
            rs1 = stmt1.executeQuery(query);
            rs1.currentTable = table1;

            String query2 = "SELECT * FROM " + table2.getEscapedTableName();
            rs2 = stmt2.executeQuery(query2);
            rs2.currentTable = table2;

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
        }
        finally {
            if (null != rs1) {
                rs1.close();
            }
            if (null != rs2) {
                rs2.close();
            }
            if (null != stmt1) {
                stmt1.close();
            }
            if (null != stmt2) {
                stmt2.close();
            }
            terminateVariation();
        }
    }

    ///////////////////////////////////////////////////////////////////
    // Verify two concurrent resultsets from same connection,
    // same statement
    ///////////////////////////////////////////////////////////////////
    @Test
    public void testTwoResultsetsSameStmt() throws SQLException {

        DBResultSet rs1 = null;
        DBResultSet rs2 = null;
        try {
            conn = new DBConnection(connectionString);
            stmt = conn.createStatement();

            String query = "SELECT * FROM " + table1.getEscapedTableName();
            rs1 = stmt.executeQuery(query);

            String query2 = "SELECT * FROM " + table2.getEscapedTableName();
            rs2 = stmt.executeQuery(query2);

            // Interleave resultset calls. rs is expected to be closed
            try {
                rs1.next();
            }
            catch (SQLException e) {
                assertEquals(e.toString(), "com.microsoft.sqlserver.jdbc.SQLServerException: The result set is closed.");
            }
            rs2.next();
            rs2.verifyCurrentRow(table2);
            try {
                rs1.next();
            }
            catch (SQLException e) {
                assertEquals(e.toString(), "com.microsoft.sqlserver.jdbc.SQLServerException: The result set is closed.");
            }
            rs1.close();
            rs2.next();
            rs2.verify(table2);
        }
        finally {
            if (null != rs1) {
                rs1.close();
            }
            if (null != rs2) {
                rs2.close();
            }
            terminateVariation();
        }
    }

    ///////////////////////////////////////////////////////////////////
    // Verify resultset closed after statement is closed
    ///////////////////////////////////////////////////////////////////
    @Test
    public void testResultSetAndCloseStmt() throws SQLException {
        try {
            conn = new DBConnection(connectionString);
            stmt = conn.createStatement();

            // SELECT * FROM <table2> ORDER BY <key>
            String query = "SELECT * FROM " + table1.getEscapedTableName();
            rs = stmt.executeQuery(query);

            // close statement and verify resultSet
            stmt.close(); // this should close the resultSet
            try {
                rs.next();
            }
            catch (SQLException e) {
                assertEquals(e.toString(), "com.microsoft.sqlserver.jdbc.SQLServerException: The result set is closed.");
            }
        }
        finally {
            terminateVariation();
        }
    }

    ///////////////////////////////////////////////////////////////////
    // Verify resultset using SelectMethod
    ///////////////////////////////////////////////////////////////////
    @Test
    public void testResultSetSelectMethod() throws SQLException {

        try {
            conn = new DBConnection(connectionString + ";selectMethod=cursor;");
            stmt = conn.createStatement();

            // SELECT * FROM <table2> ORDER BY <key>
            String query = "SELECT * FROM " + table1.getEscapedTableName();
            rs = stmt.executeQuery(query);

            // verify resultSet
            rs.verify(table1);
        }
        finally {
            terminateVariation();
        }
    }

    @AfterClass
    public static void terminate() throws SQLException {

        try {
            stmt = conn.createStatement();
        }
        finally {
            terminateVariation();
        }
    }

    public static void terminateVariation() throws SQLException {
        if (conn != null && !conn.isClosed()) {
            try {
                conn.close();
            }
            finally {
                if (null != rs)
                    rs.close();
                if (null != stmt)
                    stmt.close();
            }
        }
    }

}
