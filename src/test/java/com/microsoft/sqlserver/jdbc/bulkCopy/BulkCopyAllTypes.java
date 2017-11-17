/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.DBStatement;
import com.microsoft.sqlserver.testframework.DBTable;
import com.microsoft.sqlserver.testframework.Utils;
import com.microsoft.sqlserver.testframework.util.ComparisonUtil;

@RunWith(JUnitPlatform.class)
public class BulkCopyAllTypes extends AbstractTest {
	
    private static DBTable tableSrc = null;
    private static DBTable tableDest = null;

    /**
     * Test TVP with result set
     * 
     * @throws SQLException
     */
    @Test
    public void testTVPResultSet() throws SQLException {
        testBulkCopyResultSet(false, null, null);
        testBulkCopyResultSet(true, null, null);
        testBulkCopyResultSet(false, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        testBulkCopyResultSet(false, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        testBulkCopyResultSet(false, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        testBulkCopyResultSet(false, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
    }

    private void testBulkCopyResultSet(boolean setSelectMethod,
            Integer resultSetType,
            Integer resultSetConcurrency) throws SQLException {
        setupVariation();

        try(Connection connnection = DriverManager.getConnection(connectionString + (setSelectMethod ? ";selectMethod=cursor;" : ""));
        	Statement statement = (null != resultSetType || null != resultSetConcurrency) ? 
        			connnection.createStatement(resultSetType, resultSetConcurrency) : connnection.createStatement()){

	        ResultSet rs = statement.executeQuery("select * from " + tableSrc.getEscapedTableName());
	
	        SQLServerBulkCopy bcOperation = new SQLServerBulkCopy(connection);
	        bcOperation.setDestinationTableName(tableDest.getEscapedTableName());
	        bcOperation.writeToServer(rs);
	        bcOperation.close();
	
	        ComparisonUtil.compareSrcTableAndDestTableIgnoreRowOrder(new DBConnection(connectionString), tableSrc, tableDest);
        }

        terminateVariation();
    }

    private void setupVariation() throws SQLException {
        try(DBConnection dbConnection = new DBConnection(connectionString);
    	    DBStatement dbStmt = dbConnection.createStatement()) {

	        tableSrc = new DBTable(true);
	        tableDest = tableSrc.cloneSchema();
	
	        dbStmt.createTable(tableSrc);
	        dbStmt.createTable(tableDest);
	
	        dbStmt.populateTable(tableSrc);
        }
    }

    private void terminateVariation() throws SQLException {
        try(Connection conn = DriverManager.getConnection(connectionString);
        	Statement stmt = conn.createStatement()) {

	        Utils.dropTableIfExists(tableSrc.getEscapedTableName(), stmt);
	        Utils.dropTableIfExists(tableDest.getEscapedTableName(), stmt);
        }
    }
}