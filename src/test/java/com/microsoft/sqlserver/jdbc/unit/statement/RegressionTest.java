/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
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

    @AfterAll
    public static void terminate() throws SQLException {
        SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(connectionString);
        Statement stmt = con.createStatement();
        Utils.dropTableIfExists(tableName, stmt);
        Utils.dropProcedureIfExists(procName, stmt);
    }

}
