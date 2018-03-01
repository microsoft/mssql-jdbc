/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.tvp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.jdbc.SQLServerDataTable;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.testframework.AbstractTest;

@RunWith(JUnitPlatform.class)
public class TVPTypesTest extends AbstractTest {

    private static Connection conn = null;
    static Statement stmt = null;
    static ResultSet rs = null;
    static SQLServerDataTable tvp = null;
    private static String tvpName = "TVP";
    private static String table = "TVPTable";
    private static String procedureName = "procedureThatCallsTVP";
    private String value = null;

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

        value = buffer.toString();
        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", java.sql.Types.LONGVARCHAR);
        tvp.addRow(value);

        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                .prepareStatement("INSERT INTO " + table + " select * from ? ;");
        pstmt.setStructured(1, tvpName, tvp);

        pstmt.execute();

        rs = conn.createStatement().executeQuery("select * from " + table);
        while (rs.next()) {
            assertEquals(rs.getString(1), value);
        }
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

        value = buffer.toString();
        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", java.sql.Types.LONGNVARCHAR);
        tvp.addRow(value);

        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                .prepareStatement("INSERT INTO " + table + " select * from ? ;");
        pstmt.setStructured(1, tvpName, tvp);

        pstmt.execute();

        rs = conn.createStatement().executeQuery("select * from " + table);
        while (rs.next()) {
            assertEquals(rs.getString(1), value);
        }

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
        value = "<vx53_e>Variable E</vx53_e>" + "<vx53_f>Variable F</vx53_f>" + "<doc>API<!-- comments --></doc>"
                + "<doc>The following are Japanese chars.</doc>"
                + "<doc>    Some UTF-8 encoded characters: Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½</doc>";

        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", java.sql.Types.SQLXML);
        tvp.addRow(value);

        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                .prepareStatement("INSERT INTO " + table + " select * from ? ;");
        pstmt.setStructured(1, tvpName, tvp);

        pstmt.execute();

        Connection con = DriverManager.getConnection(connectionString);
        ResultSet rs = con.createStatement().executeQuery("select * from " + table);
        while (rs.next())
            assertEquals(rs.getString(1), value);

        if (null != pstmt) {
            pstmt.close();
        }
    }

    /**
     * Test ntext support
     * 
     * @throws SQLException
     */
    @Test
    public void testnText() throws SQLException {
        createTables("ntext");
        createTVPS("ntext");
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < 9000; i++)
            buffer.append("س");
        value = buffer.toString();
        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", java.sql.Types.LONGNVARCHAR);
        tvp.addRow(value);

        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                .prepareStatement("INSERT INTO " + table + " select * from ? ;");
        pstmt.setStructured(1, tvpName, tvp);

        pstmt.execute();

        Connection con = DriverManager.getConnection(connectionString);
        ResultSet rs = con.createStatement().executeQuery("select * from " + table);
        while (rs.next())
            assertEquals(rs.getString(1), value);

        if (null != pstmt) {
            pstmt.close();
        }
    }

    /**
     * Test text support
     * 
     * @throws SQLException
     */
    @Test
    public void testText() throws SQLException {
        createTables("text");
        createTVPS("text");
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < 9000; i++)
            buffer.append("a");
        value = buffer.toString();
        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", java.sql.Types.LONGVARCHAR);
        tvp.addRow(value);

        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                .prepareStatement("INSERT INTO " + table + " select * from ? ;");
        pstmt.setStructured(1, tvpName, tvp);

        pstmt.execute();

        Connection con = DriverManager.getConnection(connectionString);
        ResultSet rs = con.createStatement().executeQuery("select * from " + table);
        while (rs.next())
            assertEquals(rs.getString(1), value);

        if (null != pstmt) {
            pstmt.close();
        }
    }

    /**
     * Test text support
     * 
     * @throws SQLException
     */
    @Test
    public void testImage() throws SQLException {
        createTables("varbinary(max)");
        createTVPS("varbinary(max)");
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < 10000; i++)
            buffer.append("a");
        value = buffer.toString();
        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", java.sql.Types.LONGVARBINARY);
        tvp.addRow(value.getBytes());

        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                .prepareStatement("INSERT INTO " + table + " select * from ? ;");
        pstmt.setStructured(1, tvpName, tvp);

        pstmt.execute();

        Connection con = DriverManager.getConnection(connectionString);
        ResultSet rs = con.createStatement().executeQuery("select * from " + table);

        while (rs.next())
            assertTrue(parseByte(rs.getBytes(1), value.getBytes()));

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
    public void testTVPLongVarcharStoredProcedure() throws SQLException {
        createTables("varchar(max)");
        createTVPS("varchar(max)");
        createPreocedure();

        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < 8001; i++)
            buffer.append("a");

        value = buffer.toString();
        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", java.sql.Types.LONGVARCHAR);
        tvp.addRow(value);

        final String sql = "{call " + procedureName + "(?)}";

        SQLServerCallableStatement P_C_statement = (SQLServerCallableStatement) connection.prepareCall(sql);
        P_C_statement.setStructured(1, tvpName, tvp);
        P_C_statement.execute();

        rs = stmt.executeQuery("select * from " + table);
        while (rs.next())
            assertEquals(rs.getString(1), value);

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
    public void testTVPLongNVarcharStoredProcedure() throws SQLException {
        createTables("nvarchar(max)");
        createTVPS("nvarchar(max)");
        createPreocedure();

        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < 8001; i++)
            buffer.append("سس");
        value = buffer.toString();
        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", java.sql.Types.LONGNVARCHAR);
        tvp.addRow(buffer.toString());

        final String sql = "{call " + procedureName + "(?)}";

        SQLServerCallableStatement P_C_statement = (SQLServerCallableStatement) connection.prepareCall(sql);
        P_C_statement.setStructured(1, tvpName, tvp);
        P_C_statement.execute();

        rs = stmt.executeQuery("select * from " + table);
        while (rs.next())
            assertEquals(rs.getString(1), value);

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
    public void testTVPXMLStoredProcedure() throws SQLException {
        createTables("xml");
        createTVPS("xml");
        createPreocedure();

        value = "<vx53_e>Variable E</vx53_e>" + "<vx53_f>Variable F</vx53_f>" + "<doc>API<!-- comments --></doc>"
                + "<doc>The following are Japanese chars.</doc>"
                + "<doc>    Some UTF-8 encoded characters: Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½</doc>";

        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", java.sql.Types.SQLXML);
        tvp.addRow(value);

        final String sql = "{call " + procedureName + "(?)}";

        SQLServerCallableStatement P_C_statement = (SQLServerCallableStatement) connection.prepareCall(sql);
        P_C_statement.setStructured(1, tvpName, tvp);
        P_C_statement.execute();

        rs = stmt.executeQuery("select * from " + table);
        while (rs.next())
            assertEquals(rs.getString(1), value);
        if (null != P_C_statement) {
            P_C_statement.close();
        }
    }

    /**
     * Text with StoredProcedure
     * 
     * @throws SQLException
     */
    @Test
    public void testTVPTextStoredProcedure() throws SQLException {
        createTables("text");
        createTVPS("text");
        createPreocedure();

        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < 9000; i++)
            buffer.append("a");
        value = buffer.toString();

        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", java.sql.Types.LONGVARCHAR);
        tvp.addRow(value);

        final String sql = "{call " + procedureName + "(?)}";

        SQLServerCallableStatement P_C_statement = (SQLServerCallableStatement) connection.prepareCall(sql);
        P_C_statement.setStructured(1, tvpName, tvp);
        P_C_statement.execute();

        rs = stmt.executeQuery("select * from " + table);
        while (rs.next())
            assertEquals(rs.getString(1), value);
        if (null != P_C_statement) {
            P_C_statement.close();
        }
    }

    /**
     * Text with StoredProcedure
     * 
     * @throws SQLException
     */
    @Test
    public void testTVPNTextStoredProcedure() throws SQLException {
        createTables("ntext");
        createTVPS("ntext");
        createPreocedure();

        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < 9000; i++)
            buffer.append("س");
        value = buffer.toString();

        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", java.sql.Types.LONGNVARCHAR);
        tvp.addRow(value);

        final String sql = "{call " + procedureName + "(?)}";

        SQLServerCallableStatement P_C_statement = (SQLServerCallableStatement) connection.prepareCall(sql);
        P_C_statement.setStructured(1, tvpName, tvp);
        P_C_statement.execute();

        rs = stmt.executeQuery("select * from " + table);
        while (rs.next())
            assertEquals(rs.getString(1), value);
        if (null != P_C_statement) {
            P_C_statement.close();
        }
    }

    /**
     * Image with StoredProcedure acts the same as varbinary(max)
     * 
     * @throws SQLException
     */
    @Test
    public void testTVPImageStoredProcedure() throws SQLException {
        createTables("image");
        createTVPS("image");
        createPreocedure();

        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < 9000; i++)
            buffer.append("a");
        value = buffer.toString();

        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", java.sql.Types.LONGVARBINARY);
        tvp.addRow(value.getBytes());

        final String sql = "{call " + procedureName + "(?)}";

        SQLServerCallableStatement P_C_statement = (SQLServerCallableStatement) connection.prepareCall(sql);
        P_C_statement.setStructured(1, tvpName, tvp);
        P_C_statement.execute();

        rs = stmt.executeQuery("select * from " + table);
        while (rs.next())
            assertTrue(parseByte(rs.getBytes(1), value.getBytes()));
        if (null != P_C_statement) {
            P_C_statement.close();
        }
    }
    
    /**
     * Test a datetime support
     * 
     * @throws SQLException
     */
    @Test
    public void testDateTime() throws SQLException {
        createTables("datetime");
        createTVPS("datetime");

        java.sql.Timestamp value = java.sql.Timestamp.valueOf("2007-09-23 10:10:10.123");

        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", microsoft.sql.Types.DATETIME);
        tvp.addRow(value);

        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                .prepareStatement("INSERT INTO " + table + " select * from ? ;");
        pstmt.setStructured(1, tvpName, tvp);

        pstmt.execute();

        rs = conn.createStatement().executeQuery("select * from " + table);
        while (rs.next()) {
            assertEquals(((SQLServerResultSet) rs).getDateTime(1), value);
        }
        if (null != pstmt) {
            pstmt.close();
        }
    }
    
    /**
     * Test a smalldatetime support
     * 
     * @throws SQLException
     */
    @Test
    public void testSmallDateTime() throws SQLException {
        createTables("smalldatetime");
        createTVPS("smalldatetime");

        java.sql.Timestamp value = java.sql.Timestamp.valueOf("2007-09-23 10:10:10.123");
        java.sql.Timestamp returnValue = java.sql.Timestamp.valueOf("2007-09-23 10:10:00.0");

        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", microsoft.sql.Types.SMALLDATETIME);
        tvp.addRow(value);

        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                .prepareStatement("INSERT INTO " + table + " select * from ? ;");
        pstmt.setStructured(1, tvpName, tvp);

        pstmt.execute();

        rs = conn.createStatement().executeQuery("select * from " + table);
        while (rs.next()) {
            assertEquals(((SQLServerResultSet) rs).getSmallDateTime(1), returnValue);
        }
        if (null != pstmt) {
            pstmt.close();
        }
    }
    
    @BeforeEach
    private void testSetup() throws SQLException {
        conn = DriverManager.getConnection(connectionString);
        stmt = conn.createStatement();

        dropProcedure();
        dropTables();
        dropTVPS();
    }

    @AfterAll
    public static void terminate() throws SQLException {
        conn = DriverManager.getConnection(connectionString);
        stmt = conn.createStatement();
        dropProcedure();
        dropTables();
        dropTVPS();
    }

    private static void dropProcedure() throws SQLException {
        String sql = " IF EXISTS (select * from sysobjects where id = object_id(N'" + procedureName + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)"
                + " DROP PROCEDURE " + procedureName;
        stmt.execute(sql);
    }

    private static void dropTables() throws SQLException {
        stmt.executeUpdate("if object_id('" + table + "','U') is not null" + " drop table " + table);
    }

    private static void dropTVPS() throws SQLException {
        stmt.executeUpdate("IF EXISTS (SELECT * FROM sys.types WHERE is_table_type = 1 AND name = '" + tvpName + "') " + " drop type " + tvpName);
    }

    private static void createPreocedure() throws SQLException {
        String sql = "CREATE PROCEDURE " + procedureName + " @InputData " + tvpName + " READONLY " + " AS " + " BEGIN " + " INSERT INTO " + table
                + " SELECT * FROM @InputData" + " END";

        stmt.execute(sql);
    }

    private void createTables(String colType) throws SQLException {
        String sql = "create table " + table + " (c1 " + colType + " null);";
        stmt.execute(sql);
    }

    private void createTVPS(String colType) throws SQLException {
        String TVPCreateCmd = "CREATE TYPE " + tvpName + " as table (c1 " + colType + " null)";
        stmt.executeUpdate(TVPCreateCmd);
    }

    private boolean parseByte(byte[] expectedData,
            byte[] retrieved) {
        assertTrue(Arrays.equals(expectedData, Arrays.copyOf(retrieved, expectedData.length)), " unexpected BINARY value, expected");
        for (int i = expectedData.length; i < retrieved.length; i++) {
            assertTrue(0 == retrieved[i], "unexpected data BINARY");
        }
        return true;
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