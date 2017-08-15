/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.tvp;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.jdbc.SQLServerDataTable;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.DBStatement;
import com.microsoft.sqlserver.testframework.DBTable;
import com.microsoft.sqlserver.testframework.Utils;
import com.microsoft.sqlserver.testframework.sqlType.SqlType;
import com.microsoft.sqlserver.testframework.util.ComparisonUtil;

@RunWith(JUnitPlatform.class)
public class TVPAllTypes extends AbstractTest {
    private static Connection conn = null;
    static Statement stmt = null;

    private static String tvpName = "TVPAllTypesTable_char_TVP";
    private static String procedureName = "TVPAllTypesTable_char_SP";

    private static DBTable tableSrc = null;
    private static DBTable tableDest = null;

    /**
     * Test TVP with result set
     * 
     * @throws SQLException
     */
    @Test
    public void testTVPResultSet() throws SQLException {
        testTVPResultSet(false, null, null);
        testTVPResultSet(true, null, null);
        testTVPResultSet(false, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        testTVPResultSet(false, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        testTVPResultSet(false, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        testTVPResultSet(false, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
    }

    private void testTVPResultSet(boolean setSelectMethod,
            Integer resultSetType,
            Integer resultSetConcurrency) throws SQLException {
        setupVariation();

        Connection connnection = null;
        if (setSelectMethod) {
            connnection = DriverManager.getConnection(connectionString + ";selectMethod=cursor;");
        }
        else {
            connnection = DriverManager.getConnection(connectionString);
        }

        Statement stmtement = null;
        if (null != resultSetType || null != resultSetConcurrency) {
            stmtement = connnection.createStatement(resultSetType, resultSetConcurrency);
        }
        else {
            stmtement = connnection.createStatement();
        }

        ResultSet rs = stmtement.executeQuery("select * from " + tableSrc.getEscapedTableName());

        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connnection
                .prepareStatement("INSERT INTO " + tableDest.getEscapedTableName() + " select * from ? ;");
        pstmt.setStructured(1, tvpName, rs);
        pstmt.execute();

        ComparisonUtil.compareSrcTableAndDestTableIgnoreRowOrder(new DBConnection(connectionString), tableSrc, tableDest);

        terminateVariation();
    }

    /**
     * Test TVP with stored procedure and result set
     * 
     * @throws SQLException
     */
    @Test
    public void testTVPStoredProcedureResultSet() throws SQLException {
        testTVPStoredProcedureResultSet(false, null, null);
        testTVPStoredProcedureResultSet(true, null, null);
        testTVPStoredProcedureResultSet(false, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        testTVPStoredProcedureResultSet(false, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        testTVPStoredProcedureResultSet(false, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        testTVPStoredProcedureResultSet(false, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
    }

    private void testTVPStoredProcedureResultSet(boolean setSelectMethod,
            Integer resultSetType,
            Integer resultSetConcurrency) throws SQLException {
        setupVariation();

        Connection connnection = null;
        if (setSelectMethod) {
            connnection = DriverManager.getConnection(connectionString + ";selectMethod=cursor;");
        }
        else {
            connnection = DriverManager.getConnection(connectionString);
        }

        Statement stmtement = null;
        if (null != resultSetType || null != resultSetConcurrency) {
            stmtement = connnection.createStatement(resultSetType, resultSetConcurrency);
        }
        else {
            stmtement = connnection.createStatement();
        }

        ResultSet rs = stmtement.executeQuery("select * from " + tableSrc.getEscapedTableName());

        String sql = "{call " + procedureName + "(?)}";
        SQLServerCallableStatement Cstmt = (SQLServerCallableStatement) connnection.prepareCall(sql);
        Cstmt.setStructured(1, tvpName, rs);
        Cstmt.execute();

        ComparisonUtil.compareSrcTableAndDestTableIgnoreRowOrder(new DBConnection(connectionString), tableSrc, tableDest);

        terminateVariation();
    }

    /**
     * Test TVP with DataTable
     * 
     * @throws SQLException
     */
    @Test
    public void testTVPDataTable() throws SQLException {
        setupVariation();

        SQLServerDataTable dt = new SQLServerDataTable();

        int numberOfColumns = tableDest.getColumns().size();
        Object[] values = new Object[numberOfColumns];
        for (int i = 0; i < numberOfColumns; i++) {
            SqlType sqlType = tableDest.getColumns().get(i).getSqlType();
            dt.addColumnMetadata(tableDest.getColumnName(i), sqlType.getJdbctype().getVendorTypeNumber());
            values[i] = sqlType.createdata();
        }

        int numberOfRows = 10;
        for (int i = 0; i < numberOfRows; i++) {
            dt.addRow(values);
        }

        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                .prepareStatement("INSERT INTO " + tableDest.getEscapedTableName() + " select * from ? ;");
        pstmt.setStructured(1, tvpName, dt);
        pstmt.execute();
    }

    private static void createPreocedure(String procedureName,
            String destTable) throws SQLException {
        String sql = "CREATE PROCEDURE " + procedureName + " @InputData " + tvpName + " READONLY " + " AS " + " BEGIN " + " INSERT INTO " + destTable
                + " SELECT * FROM @InputData" + " END";

        stmt.execute(sql);
    }

    private static void dropTVPS(String tvpName) throws SQLException {
        stmt.executeUpdate("IF EXISTS (SELECT * FROM sys.types WHERE is_table_type = 1 AND name = '" + tvpName + "') " + " drop type " + tvpName);
    }

    private static void createTVPS(String TVPName,
            String TVPDefinition) throws SQLException {
        String TVPCreateCmd = "CREATE TYPE " + TVPName + " as table (" + TVPDefinition + ");";
        stmt.executeUpdate(TVPCreateCmd);
    }

    private void setupVariation() throws SQLException {
        conn = DriverManager.getConnection(connectionString);
        stmt = conn.createStatement();

        Utils.dropProcedureIfExists(procedureName, stmt);
        dropTVPS(tvpName);

        DBConnection dbConnection = new DBConnection(connectionString);
        DBStatement dbStmt = dbConnection.createStatement();

        tableSrc = new DBTable(true);
        tableDest = tableSrc.cloneSchema();

        dbStmt.createTable(tableSrc);
        dbStmt.createTable(tableDest);

        createTVPS(tvpName, tableSrc.getDefinitionOfColumns());
        createPreocedure(procedureName, tableDest.getEscapedTableName());

        dbStmt.populateTable(tableSrc);
    }

    private void terminateVariation() throws SQLException {
        conn = DriverManager.getConnection(connectionString);
        stmt = conn.createStatement();

        Utils.dropProcedureIfExists(procedureName, stmt);
        Utils.dropTableIfExists(tableSrc.getEscapedTableName(), stmt);
        Utils.dropTableIfExists(tableDest.getEscapedTableName(), stmt);
        dropTVPS(tvpName);
    }
}