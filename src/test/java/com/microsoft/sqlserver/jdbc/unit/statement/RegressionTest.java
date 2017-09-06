/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.Utils;

@RunWith(JUnitPlatform.class)
public class RegressionTest extends AbstractTest {
    private static String tableName = "[ServerCursorPStmt]";
    private static String procName = "[ServerCursorProc]";

    /**
     * Tests select into stored proc
     * 
     * @throws SQLException
     */
    @Test
    public void testServerCursorPStmt() throws SQLException {

        SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(connectionString);
        Statement stmt = con.createStatement();

        PreparedStatement pstmt = null;
        ResultSet rs = null;

        // expected values
        int numRowsInResult = 1;
        String col3Value = "India";
        String col3Lookup = "IN";

        stmt.executeUpdate("CREATE TABLE " + tableName + " (col1 int primary key, col2 varchar(3), col3 varchar(128))");
        stmt.executeUpdate("INSERT INTO " + tableName + " VALUES (1, 'CAN', 'Canada')");
        stmt.executeUpdate("INSERT INTO " + tableName + " VALUES (2, 'USA', 'United States of America')");
        stmt.executeUpdate("INSERT INTO " + tableName + " VALUES (3, 'JPN', 'Japan')");
        stmt.executeUpdate("INSERT INTO " + tableName + " VALUES (4, '" + col3Lookup + "', '" + col3Value + "')");

        // create stored proc
        String storedProcString;

        if (DBConnection.isSqlAzure(con)) {
            // On SQL Azure, 'SELECT INTO' is not supported. So do not use it.
            storedProcString = "CREATE PROCEDURE " + procName + " @param varchar(3) AS SELECT col3 FROM " + tableName + " WHERE col2 = @param";
        }
        else {
            // On SQL Server
            storedProcString = "CREATE PROCEDURE " + procName + " @param varchar(3) AS SELECT col3 INTO #TMPTABLE FROM " + tableName
                    + " WHERE col2 = @param SELECT col3 FROM #TMPTABLE";
        }

        stmt.executeUpdate(storedProcString);

        // execute stored proc via pstmt
        pstmt = con.prepareStatement("EXEC " + procName + " ?", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        pstmt.setString(1, col3Lookup);

        // should return 1 row
        rs = pstmt.executeQuery();
        rs.last();
        assertEquals(rs.getRow(), numRowsInResult, "getRow mismatch");
        rs.beforeFirst();
        while (rs.next()) {
            assertEquals(rs.getString(1), col3Value, "Value mismatch");
        }

        if (null != stmt)
            stmt.close();
        if (null != con)
            con.close();
    }

    /**
     * Tests update count returned by SELECT INTO
     * 
     * @throws SQLException
     */
    @Test
    public void testSelectIntoUpdateCount() throws SQLException {
        SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(connectionString);
        
        // Azure does not do SELECT INTO
        if (!DBConnection.isSqlAzure(con)) {
            final String tableName = "[#SourceTableForSelectInto]";
            
            Statement stmt = con.createStatement();
            stmt.executeUpdate("CREATE TABLE " + tableName + " (col1 int primary key, col2 varchar(3), col3 varchar(128))");
            stmt.executeUpdate("INSERT INTO " + tableName + " VALUES (1, 'CAN', 'Canada')");
            stmt.executeUpdate("INSERT INTO " + tableName + " VALUES (2, 'USA', 'United States of America')");
            stmt.executeUpdate("INSERT INTO " + tableName + " VALUES (3, 'JPN', 'Japan')");

            // expected values
            int numRowsToCopy = 2;
    
            PreparedStatement ps = con.prepareStatement("SELECT * INTO #TMPTABLE FROM " + tableName + " WHERE col1 <= ?");
            ps.setInt(1, numRowsToCopy);
            int updateCount = ps.executeUpdate();
            assertEquals(numRowsToCopy, updateCount, "Incorrect update count");
            
            if (null != stmt)
                stmt.close();
        }
        if (null != con)
            con.close();
    }
   
    /**
     * Tests update query
     * 
     * @throws SQLException
     */
    @Test
    public void testUpdateQuery() throws SQLException {
        assumeTrue("JDBC41".equals(Utils.getConfiguredProperty("JDBC_Version")), "Aborting test case as JDBC version is not compatible. ");

        SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(connectionString);
        String sql;
        SQLServerPreparedStatement pstmt = null;
        JDBCType[] targets = {JDBCType.INTEGER, JDBCType.SMALLINT};
        int rows = 3;       
        final String tableName = "[updateQuery]";

        Statement stmt = con.createStatement();
        Utils.dropTableIfExists(tableName, stmt);
        stmt.executeUpdate("CREATE TABLE " + tableName + " (" + "c1 int null," + "PK int NOT NULL PRIMARY KEY" + ")");

        /*
         * populate table
         */
        sql = "insert into " + tableName + " values(" + "?,?" + ")";
        pstmt =  (SQLServerPreparedStatement)con.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY, connection.getHoldability());
        
        for (int i = 1; i <= rows; i++) {
            pstmt.setObject(1, i, JDBCType.INTEGER);
            pstmt.setObject(2, i, JDBCType.INTEGER);
            pstmt.executeUpdate();
        }

        /*
         * Update table
         */
        sql = "update " + tableName + " SET c1= ? where PK =1";
        for (int i = 1; i <= rows; i++) {
            pstmt = (SQLServerPreparedStatement)con.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            for (JDBCType target : targets) {
                pstmt.setObject(1, 5 + i, target);
                pstmt.executeUpdate();
            }
        }
        
        /*
         * Verify
         */
        ResultSet rs =  stmt.executeQuery("select * from " + tableName);
        rs.next();
        assertEquals(rs.getInt(1), 8, "Value mismatch");
       

        if (null != stmt)
            stmt.close();
        if (null != con)
            con.close();
    }

    private String xmlTableName = "try_SQLXML_Table";

     /**
     * Tests XML query
     * 
     * @throws SQLException
     */
    @Test
    public void testXmlQuery() throws SQLException {
        assumeTrue("JDBC41".equals(Utils.getConfiguredProperty("JDBC_Version")), "Aborting test case as JDBC version is not compatible. ");

        Connection connection = DriverManager.getConnection(connectionString);

        Statement stmt = connection.createStatement();

        dropTables(stmt);
        createTable(stmt);

        String sql = "UPDATE " + xmlTableName + " SET [c2] = ?, [c3] = ?";
        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(sql);

        pstmt.setObject(1, null);
        pstmt.setObject(2, null, Types.SQLXML);
        pstmt.executeUpdate();

        pstmt = (SQLServerPreparedStatement) connection.prepareStatement(sql);
        pstmt.setObject(1, null, Types.SQLXML);
        pstmt.setObject(2, null);
        pstmt.executeUpdate();

        pstmt = (SQLServerPreparedStatement) connection.prepareStatement(sql);
        pstmt.setObject(1, null);
        pstmt.setObject(2, null, Types.SQLXML);
        pstmt.executeUpdate(); 
    }

    private void dropTables(Statement stmt) throws SQLException {
        stmt.executeUpdate("if object_id('" + xmlTableName + "','U') is not null" + " drop table " + xmlTableName);
    }

    private void createTable(Statement stmt) throws SQLException {

        String sql = "CREATE TABLE " + xmlTableName + " ([c1] int, [c2] xml, [c3] xml)";

        stmt.execute(sql);
    }

    @AfterAll
    public static void terminate() throws SQLException {
        SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(connectionString);
        Statement stmt = con.createStatement();
        Utils.dropTableIfExists(tableName, stmt);
        Utils.dropProcedureIfExists(procName, stmt);
    }

}
