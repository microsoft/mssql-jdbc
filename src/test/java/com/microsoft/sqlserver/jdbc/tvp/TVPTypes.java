/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.tvp;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.jdbc.SQLServerDataTable;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.DBResultSet;
import com.microsoft.sqlserver.testframework.DBStatement;

@RunWith(JUnitPlatform.class)
public class TVPTypes extends AbstractTest {

    private static DBConnection conn = null;
    static DBStatement stmt = null;
    static DBResultSet rs = null;
    static SQLServerDataTable tvp = null;
    static String expectecValue1 = "hello";
    static String expectecValue2 = "world";
    static String expectecValue3 = "again";
    private static String tvpName = "numericTVP";
    private static String charTable = "tvpNumericTable";
    private static String procedureName = "procedureThatCallsTVP";

    /**
     * Test a longvarchar support
     * 
     * @throws SQLException
     */
    @Test
    public void testLongVarchar() throws SQLException {
        createTables("varchar(max)");
        createTVPS("varchar(max)");

        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < 9000; i++)
            buffer.append("a");

        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", java.sql.Types.LONGVARCHAR);
        tvp.addRow(buffer.toString());

        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                .prepareStatement("INSERT INTO " + charTable + " select * from ? ;");
        pstmt.setStructured(1, tvpName, tvp);

        pstmt.execute();

        if (null != pstmt) {
            pstmt.close();
        }
    }

    /**
     * Test longnvarchar
     * 
     * @throws SQLException
     */
    @Test
    public void testLongNVarchar() throws SQLException {
        createTables("nvarchar(max)");
        createTVPS("nvarchar(max)");

        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < 8001; i++)
            buffer.append("سس");

        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", java.sql.Types.LONGNVARCHAR);
        tvp.addRow(buffer.toString());

        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                .prepareStatement("INSERT INTO " + charTable + " select * from ? ;");
        pstmt.setStructured(1, tvpName, tvp);

        pstmt.execute();

        if (null != pstmt) {
            pstmt.close();
        }
    }

    /**
     * Test xml support
     * 
     * @throws SQLException
     */
    @Test
    public void testXML() throws SQLException {
        createTables("xml");
        createTVPS("xml");
        String value = "<vx53_e>Variable E</vx53_e>" + "<vx53_f>Variable F</vx53_f>" + "<doc>API<!-- comments --></doc>"
                + "<doc>The following are Japanese chars.</doc>"
                + "<doc>    Some UTF-8 encoded characters: Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½</doc>";

        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", java.sql.Types.SQLXML);
        tvp.addRow(value);

        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                .prepareStatement("INSERT INTO " + charTable + " select * from ? ;");
        pstmt.setStructured(1, tvpName, tvp);

        pstmt.execute();

        Connection con = DriverManager.getConnection(connectionString);
        ResultSet rs = con.createStatement().executeQuery("select * from " + charTable);
        while (rs.next())
            assertEquals(rs.getString(1), value);

        if (null != pstmt) {
            pstmt.close();
        }
    }

    /**
     * LongVarchar with StoredProcedure
     * 
     * @throws SQLException
     */
    @Test
    public void testTVPLongVarchar_StoredProcedure() throws SQLException {
        createTables("varchar(max)");
        createTVPS("varchar(max)");
        createPreocedure();

        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < 8001; i++)
            buffer.append("a");

        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", java.sql.Types.LONGVARCHAR);
        tvp.addRow(buffer.toString());

        final String sql = "{call " + procedureName + "(?)}";

        SQLServerCallableStatement P_C_statement = (SQLServerCallableStatement) connection.prepareCall(sql);
        P_C_statement.setStructured(1, tvpName, tvp);
        P_C_statement.execute();

        rs = stmt.executeQuery("select * from " + charTable);
        while (rs.next())
            assertEquals(rs.getString(1), buffer.toString());

        if (null != P_C_statement) {
            P_C_statement.close();
        }
    }

    /**
     * LongNVarchar with StoredProcedure
     * 
     * @throws SQLException
     */
    @Test
    public void testTVPLongNVarchar_StoredProcedure() throws SQLException {
        createTables("nvarchar(max)");
        createTVPS("nvarchar(max)");
        createPreocedure();

        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < 8001; i++)
            buffer.append("سس");

        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", java.sql.Types.LONGNVARCHAR);
        tvp.addRow(buffer.toString());

        final String sql = "{call " + procedureName + "(?)}";

        SQLServerCallableStatement P_C_statement = (SQLServerCallableStatement) connection.prepareCall(sql);
        P_C_statement.setStructured(1, tvpName, tvp);
        P_C_statement.execute();

        rs = stmt.executeQuery("select * from " + charTable);
        while (rs.next())
            assertEquals(rs.getString(1), buffer.toString());

        if (null != P_C_statement) {
            P_C_statement.close();
        }
    }

    /**
     * XML with StoredProcedure
     * 
     * @throws SQLException
     */
    @Test
    public void testTVPXML_StoredProcedure() throws SQLException {
        createTables("xml");
        createTVPS("xml");
        createPreocedure();

        String value = "<vx53_e>Variable E</vx53_e>" + "<vx53_f>Variable F</vx53_f>" + "<doc>API<!-- comments --></doc>"
                + "<doc>The following are Japanese chars.</doc>"
                + "<doc>    Some UTF-8 encoded characters: Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½</doc>";

        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", java.sql.Types.SQLXML);
        tvp.addRow(value);

        final String sql = "{call " + procedureName + "(?)}";

        SQLServerCallableStatement P_C_statement = (SQLServerCallableStatement) connection.prepareCall(sql);
        P_C_statement.setStructured(1, tvpName, tvp);
        P_C_statement.execute();

        rs = stmt.executeQuery("select * from " + charTable);
        while (rs.next())
            assertEquals(rs.getString(1), value);
        if (null != P_C_statement) {
            P_C_statement.close();
        }
    }

    @BeforeEach
    private void testSetup() throws SQLException {
        conn = new DBConnection(connectionString);
        stmt = conn.createStatement();

        dropProcedure();
        dropTables();
        dropTVPS();
    }

    private void dropProcedure() throws SQLException {
        String sql = " IF EXISTS (select * from sysobjects where id = object_id(N'" + procedureName + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)"
                + " DROP PROCEDURE " + procedureName;
        stmt.execute(sql);
    }

    private static void dropTables() throws SQLException {
        stmt.executeUpdate("if object_id('" + charTable + "','U') is not null" + " drop table " + charTable);
    }

    private static void dropTVPS() throws SQLException {
        stmt.executeUpdate("IF EXISTS (SELECT * FROM sys.types WHERE is_table_type = 1 AND name = '" + tvpName + "') " + " drop type " + tvpName);
    }

    private static void createPreocedure() throws SQLException {
        String sql = "CREATE PROCEDURE " + procedureName + " @InputData " + tvpName + " READONLY " + " AS " + " BEGIN " + " INSERT INTO " + charTable
                + " SELECT * FROM @InputData" + " END";

        stmt.execute(sql);
    }

    private void createTables(String colType) throws SQLException {
        String sql = "create table " + charTable + " (c1 " + colType + " null);";
        stmt.execute(sql);
    }

    private void createTVPS(String colType) throws SQLException {
        String TVPCreateCmd = "CREATE TYPE " + tvpName + " as table (c1 " + colType + " null)";
        stmt.executeUpdate(TVPCreateCmd);
    }

    @AfterEach
    private void terminateVariation() throws SQLException {
        if (null != conn) {
            conn.close();
        }
        if (null != stmt) {
            stmt.close();
        }
        if (null != rs) {
            rs.close();
        }
        if (null != tvp) {
            tvp.clear();
        }
    }

}