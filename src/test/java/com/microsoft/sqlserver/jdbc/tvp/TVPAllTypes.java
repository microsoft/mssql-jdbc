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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.DBStatement;
import com.microsoft.sqlserver.testframework.DBTable;
import com.microsoft.sqlserver.testframework.Utils;

@RunWith(JUnitPlatform.class)
public class TVPAllTypes extends AbstractTest {
    private static Connection conn = null;
    static Statement stmt = null;

    private static String tvpName = "TVPAllTypesTable_char_TVP";
    private static String tableNameSrc;
    private static String tableNameDest;

    /**
     * Test TVP with result set
     * 
     * @throws SQLException
     */
    @Test
    public void testTVP_RS() throws SQLException {
        Connection connnection = DriverManager.getConnection(connectionString);
        Statement stmtement = connnection.createStatement();

        ResultSet rs = stmtement.executeQuery("select * from " + tableNameSrc);

        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connnection
                .prepareStatement("INSERT INTO " + tableNameDest + " select * from ? ;");
        pstmt.setStructured(1, tvpName, rs);
        pstmt.execute();
    }

    /**
     * Test TVP with result set and cursors
     * 
     * @throws SQLException
     */
    @Test
    public void testTVP_RS_WithCursor() throws SQLException {
        Connection connnection = DriverManager.getConnection(connectionString);
        Statement stmtement = connnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);

        ResultSet rs = stmtement.executeQuery("select * from " + tableNameSrc);

        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connnection
                .prepareStatement("INSERT INTO " + tableNameDest + " select * from ? ;");
        pstmt.setStructured(1, tvpName, rs);
        pstmt.execute();
    }

    private static void dropTVPS(String tvpName) throws SQLException {
        stmt.executeUpdate("IF EXISTS (SELECT * FROM sys.types WHERE is_table_type = 1 AND name = '" + tvpName + "') " + " drop type " + tvpName);
    }

    private static void createTVPS(String TVPName,
            String TVPDefinition) throws SQLException {
        String TVPCreateCmd = "CREATE TYPE " + TVPName + " as table (" + TVPDefinition + ");";
        stmt.executeUpdate(TVPCreateCmd);
    }

    @BeforeEach
    private void testSetup() throws SQLException {
        conn = DriverManager.getConnection(connectionString);
        stmt = conn.createStatement();

        dropTVPS(tvpName);

        DBConnection dbConnection = new DBConnection(connectionString);
        DBStatement dbStmt = dbConnection.createStatement();

        DBTable tableSrc = new DBTable(true);
        DBTable tableDest = tableSrc.cloneSchema();
        dbStmt.createTable(tableSrc);
        dbStmt.createTable(tableDest);

        createTVPS(tvpName, tableSrc.getTableDefinition());

        dbStmt.populateTable(tableSrc);

        tableNameSrc = tableSrc.getEscapedTableName();
        tableNameDest = tableDest.getEscapedTableName();
    }

    @AfterEach
    private void terminateVariation() throws SQLException {
        conn = DriverManager.getConnection(connectionString);
        stmt = conn.createStatement();

        Utils.dropTableIfExists(tableNameSrc, stmt);
        Utils.dropTableIfExists(tableNameDest, stmt);
        dropTVPS(tvpName);
    }
}