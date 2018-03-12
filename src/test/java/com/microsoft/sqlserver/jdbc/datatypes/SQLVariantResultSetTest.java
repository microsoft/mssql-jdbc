/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.datatypes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Utils;
import com.microsoft.sqlserver.testframework.util.RandomData;

/**
 * Tests for supporting sqlVariant
 *
 */
@RunWith(JUnitPlatform.class)
public class SQLVariantResultSetTest extends AbstractTest {

    static SQLServerConnection con = null;
    static Statement stmt = null;
    static String tableName = "sqlVariantTestSrcTable";
    static String inputProc = "sqlVariantProc";
    static SQLServerResultSet rs = null;
    static SQLServerPreparedStatement pstmt = null;

    /**
     * Read int value
     * 
     * @throws SQLException
     * @throws SecurityException
     * @throws IOException
     */
    @Test
    public void readInt() throws SQLException, SecurityException, IOException {
        int value = 2;
        createAndPopulateTable("int", value);
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertEquals(rs.getString(1), "" + value);
    }

    /**
     * Read money type stored in SqlVariant
     * 
     * @throws SQLException
     * 
     */
    @Test
    public void readMoney() throws SQLException {
        Double value = 123.12;
        createAndPopulateTable("Money", value);
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertEquals(rs.getObject(1), new BigDecimal("123.1200"));
    }

    /**
     * Reading smallmoney from SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    public void readSmallMoney() throws SQLException {
        Double value = 123.12;
        createAndPopulateTable("smallmoney", value);
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertEquals(rs.getObject(1), new BigDecimal("123.1200"));
    }

    /**
     * Read GUID stored in SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    public void readGUID() throws SQLException {
        String value = "1AE740A2-2272-4B0F-8086-3DDAC595BC11";
        createAndPopulateTable("uniqueidentifier", "'" + value + "'");
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertEquals(rs.getUniqueIdentifier(1), value);
    }

    /**
     * Reading date stored in SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    public void readDate() throws SQLException {
        String value = "'2015-05-08'";
        createAndPopulateTable("date", value);
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertEquals("" + rs.getObject(1), "2015-05-08");
    }

    /**
     * Read time from SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    public void readTime() throws SQLException {
        String value = "'12:26:27.123345'";
        createAndPopulateTable("time(3)", value);
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertEquals("" + rs.getObject(1).toString(), "12:26:27");
    }

    /**
     * Read datetime from SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    public void readDateTime() throws SQLException {
        String value = "'2015-05-08 12:26:24'";
        createAndPopulateTable("datetime", value);
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertEquals("" + rs.getObject(1), "2015-05-08 12:26:24.0");
    }

    /**
     * Read smalldatetime from SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    public void readSmallDateTime() throws SQLException {
        String value = "'2015-05-08 12:26:24'";
        createAndPopulateTable("smalldatetime", value);
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertEquals("" + rs.getObject(1), "2015-05-08 12:26:00.0");
    }

    /**
     * Read VarChar8000 from SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    public void readVarChar8000() throws SQLException {
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < 8000; i++) {
            buffer.append("a");
        }
        String value = "'" + buffer.toString() + "'";
        createAndPopulateTable("VARCHAR(8000)", value);
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertEquals(rs.getObject(1), buffer.toString());
    }

    /**
     * Read float from SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    public void readFloat() throws SQLException {
        float value = 5;
        createAndPopulateTable("float", value);
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertEquals(rs.getObject(1), Double.valueOf("5.0"));
    }

    /**
     * Read bigint from SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    public void readBigInt() throws SQLException {
        long value = 5;
        createAndPopulateTable("bigint", value);
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertEquals(rs.getObject(1), value);
    }

    /**
     * read smallint from SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    public void readSmallInt() throws SQLException {
        short value = 5;
        createAndPopulateTable("smallint", value);
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertEquals(rs.getObject(1), value);
    }

    /**
     * Read tinyint from SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    public void readTinyInt() throws SQLException {
        short value = 5;
        createAndPopulateTable("tinyint", value);
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertEquals(rs.getObject(1), value);
    }

    /**
     * read bit from SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    public void readBit() throws SQLException {
        int value = 50000;
        createAndPopulateTable("bit", value);
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertEquals(rs.getObject(1), true);
    }

    /**
     * Read float from SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    public void readReal() throws SQLException {
        float value = 5;
        createAndPopulateTable("Real", value);
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertEquals(rs.getObject(1), Float.valueOf("5.0"));
    }

    /**
     * Read nchar from SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    public void readNChar() throws SQLException, SecurityException, IOException {
        String value = "a";
        createAndPopulateTable("nchar(5)", "'" + value + "'");
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertEquals(rs.getNString(1).trim(), value);
    }

    /**
     * Read nVarChar
     * 
     * @throws SQLException
     * @throws SecurityException
     * @throws IOException
     */
    @Test
    public void readNVarChar() throws SQLException, SecurityException, IOException {
        String value = "nvarchar";
        createAndPopulateTable("nvarchar(10)", "'" + value + "'");
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertEquals(rs.getObject(1), value);
    }

    /**
     * readBinary
     * 
     * @throws SQLException
     * @throws SecurityException
     * @throws IOException
     */
    @Test
    public void readBinary20() throws SQLException, SecurityException, IOException {
        String value = "hi";
        createAndPopulateTable("binary(20)", "'" + value + "'");
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertTrue(parseByte((byte[]) rs.getObject(1), (byte[]) value.getBytes()));
    }

    /**
     * read varBinary
     * 
     * @throws SQLException
     * @throws SecurityException
     * @throws IOException
     */
    @Test
    public void readVarBinary20() throws SQLException, SecurityException, IOException {
        String value = "hi";
        createAndPopulateTable("varbinary(20)", "'" + value + "'");
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertTrue(parseByte((byte[]) rs.getObject(1), (byte[]) value.getBytes()));
    }

    /**
     * read Binary512
     * 
     * @throws SQLException
     * @throws SecurityException
     * @throws IOException
     */
    @Test
    public void readBinary512() throws SQLException, SecurityException, IOException {
        String value = "hi";
        createAndPopulateTable("binary(512)", "'" + value + "'");
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertTrue(parseByte((byte[]) rs.getObject(1), (byte[]) value.getBytes()));
    }

    /**
     * read Binary(8000)
     * 
     * @throws SQLException
     * @throws SecurityException
     * @throws IOException
     */
    @Test
    public void readBinary8000() throws SQLException, SecurityException, IOException {
        String value = "hi";
        createAndPopulateTable("binary(8000)", "'" + value + "'");
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertTrue(parseByte((byte[]) rs.getObject(1), (byte[]) value.getBytes()));
    }

    /**
     * read varBinary(8000)
     * 
     * @throws SQLException
     * @throws SecurityException
     * @throws IOException
     */
    @Test
    public void readvarBinary8000() throws SQLException, SecurityException, IOException {
        String value = "hi";
        createAndPopulateTable("varbinary(8000)", "'" + value + "'");
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertTrue(parseByte((byte[]) rs.getObject(1), (byte[]) value.getBytes()));
    }

    /**
     * Read SqlVariantProperty
     * 
     * @throws SQLException
     * @throws SecurityException
     * @throws IOException
     */
    @Test
    public void readSQLVariantProperty() throws SQLException, SecurityException, IOException {
        String value = "hi";
        createAndPopulateTable("binary(8000)", "'" + value + "'");
        rs = (SQLServerResultSet) stmt.executeQuery(
                "SELECT SQL_VARIANT_PROPERTY(col1,'BaseType') AS 'Base Type', SQL_VARIANT_PROPERTY(col1,'Precision') AS 'Precision' from "
                        + tableName);
        rs.next();
        assertTrue(rs.getString(1).equalsIgnoreCase("binary"), "unexpected baseType, expected: binary, retrieved:" + rs.getString(1));
    }

    /**
     * Testing that inserting value more than 8000 on varchar(8000) should throw failure operand type clash
     *
     * @throws SQLException
     */
    @Test
    public void insertVarChar8001() throws SQLException {
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < 8001; i++) {
            buffer.append("a");
        }
        Utils.dropTableIfExists(tableName, stmt);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement("insert into " + tableName + " values (?)");
        pstmt.setObject(1, buffer.toString());
        try {
            pstmt.execute();
        }
        catch (SQLServerException e) {
            assertTrue(e.toString().contains("com.microsoft.sqlserver.jdbc.SQLServerException: Operand type clash"));
        }
    }

    /**
     * Testing nvarchar4000
     *
     * @throws SQLException
     */
    @Test
    public void readNvarChar4000() throws SQLException {
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < 4000; i++) {
            buffer.append("a");
        }
        String value = "'" + buffer.toString() + "'";
        createAndPopulateTable("NVARCHAR(4000)", value);
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertEquals(rs.getObject(1), buffer.toString());
    }

    /**
     * Update int value
     * 
     * @throws SQLException
     * @throws SecurityException
     * @throws IOException
     */
    @Test
    public void UpdateInt() throws SQLException, SecurityException, IOException {
        int value = 2;
        int updatedValue = 3;
        createAndPopulateTable("int", value);
        stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertEquals(rs.getString(1), "" + value);
        rs.updateInt(1, updatedValue);
        rs.updateRow();
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertEquals(rs.getString(1), "" + updatedValue);
        if (null != rs) {
            rs.close();
        }
    }

    /**
     * Update nChar value
     * 
     * @throws SQLException
     * @throws SecurityException
     * @throws IOException
     */
    @Test
    public void UpdateNChar() throws SQLException, SecurityException, IOException {
        String value = "a";
        String updatedValue = "b";

        createAndPopulateTable("nchar", "'" + value + "'");
        stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertEquals(rs.getString(1).trim(), "" + value);
        rs.updateNString(1, updatedValue);
        rs.updateRow();
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertEquals(rs.getString(1), "" + updatedValue);
        if (null != rs) {
            rs.close();
        }
    }

    /**
     * update Binary
     * 
     * @throws SQLException
     * @throws SecurityException
     * @throws IOException
     */
    @Test
    public void updateBinary20() throws SQLException, SecurityException, IOException {
        String value = "hi";
        String updatedValue = "bye";
        createAndPopulateTable("binary(20)", "'" + value + "'");
        stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertTrue(parseByte((byte[]) rs.getObject(1), (byte[]) value.getBytes()));
        rs.updateBytes(1, updatedValue.getBytes());
        rs.updateRow();
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertTrue(parseByte((byte[]) rs.getBytes(1), updatedValue.getBytes()));
        if (null != rs) {
            rs.close();
        }
    }

    /**
     * Testing inserting and reading from SqlVariant and int column
     *
     * @throws SQLException
     */
    @Test
    public void insertTest() throws SQLException {
        Utils.dropTableIfExists(tableName, stmt);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant, col2 int)");
        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement("insert into " + tableName + " values (?, ?)");

        String[] col1Value = {"Hello", null};
        int[] col2Value = {1, 2};
        pstmt.setObject(1, "Hello");
        pstmt.setInt(2, 1);
        pstmt.execute();
        pstmt.setObject(1, null);
        pstmt.setInt(2, 2);
        pstmt.execute();

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        int i = 0;
        rs.next();
        do {
            assertEquals(rs.getObject(1), col1Value[i]);
            assertEquals(rs.getObject(2), col2Value[i]);
            i++;
        }
        while (rs.next());
    }

    /**
     * test inserting null value
     *
     * @throws SQLException
     */
    @Test
    public void insertTestNull() throws SQLException {
        Utils.dropTableIfExists(tableName, stmt);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        pstmt = (SQLServerPreparedStatement) con.prepareStatement("insert into " + tableName + " values ( ?)");

        pstmt.setObject(1, null);
        pstmt.execute();

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertEquals(rs.getBoolean(1), false);
    }

    /**
     * Test inserting using setObject
     *
     * @throws SQLException
     * @throws ParseException
     */
    @Test
    public void insertSetObject() throws SQLException {
        Utils.dropTableIfExists(tableName, stmt);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        pstmt = (SQLServerPreparedStatement) con.prepareStatement("insert into " + tableName + " values (?)");

        pstmt.setObject(1, 2);
        pstmt.execute();

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertEquals(rs.getObject(1), 2);
    }

    /**
     * Test callableStatement with SqlVariant
     *
     * @throws SQLException
     */
    @Test
    public void callableStatementOutputIntTest() throws SQLException {
        int value = 5;
        Utils.dropTableIfExists(tableName, stmt);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + " values (CAST (" + value + " AS " + "int" + "))");

        Utils.dropProcedureIfExists(inputProc, stmt);
        String sql = "CREATE PROCEDURE " + inputProc + " @p0 sql_variant OUTPUT AS SELECT TOP 1 @p0=col1 FROM " + tableName;
        stmt.execute(sql);

        CallableStatement cs = con.prepareCall(" {call " + inputProc + " (?) }");
        cs.registerOutParameter(1, microsoft.sql.Types.SQL_VARIANT);
        cs.execute();
        assertEquals(cs.getString(1), String.valueOf(value));
        if (null != cs) {
            cs.close();
        }
    }

    /**
     * Test callableStatement with SqlVariant
     *
     * @throws SQLException
     */
    @Test
    public void callableStatementOutputDateTest() throws SQLException {
        String value = "2015-05-08";

        Utils.dropTableIfExists(tableName, stmt);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + " values (CAST ('" + value + "' AS " + "date" + "))");

        Utils.dropProcedureIfExists(inputProc, stmt);
        String sql = "CREATE PROCEDURE " + inputProc + " @p0 sql_variant OUTPUT AS SELECT TOP 1 @p0=col1 FROM " + tableName;
        stmt.execute(sql);

        CallableStatement cs = con.prepareCall(" {call " + inputProc + " (?) }");
        cs.registerOutParameter(1, microsoft.sql.Types.SQL_VARIANT);
        cs.execute();
        assertEquals(cs.getString(1), String.valueOf(value));
        if (null != cs) {
            cs.close();
        }
    }

    /**
     * Test callableStatement with SqlVariant
     *
     * @throws SQLException
     */
    @Test
    public void callableStatementOutputTimeTest() throws SQLException {
        String value = "12:26:27.123345";
        String returnValue = "12:26:27";
        Utils.dropTableIfExists(tableName, stmt);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + " values (CAST ('" + value + "' AS " + "time(3)" + "))");

        Utils.dropProcedureIfExists(inputProc, stmt);
        String sql = "CREATE PROCEDURE " + inputProc + " @p0 sql_variant OUTPUT AS SELECT TOP 1 @p0=col1 FROM " + tableName;
        stmt.execute(sql);

        CallableStatement cs = con.prepareCall(" {call " + inputProc + " (?) }");
        cs.registerOutParameter(1, microsoft.sql.Types.SQL_VARIANT, 3);
        cs.execute();
        assertEquals(String.valueOf(returnValue), "" + cs.getObject(1));
        if (null != cs) {
            cs.close();
        }
    }

    /**
     * Test callableStatement with SqlVariant Binary value
     *
     * @throws SQLException
     */
    @Test
    public void callableStatementOutputBinaryTest() throws SQLException {
        byte[] binary20 = RandomData.generateBinaryTypes("20", false, false);
        byte[] secondBinary20 = RandomData.generateBinaryTypes("20", false, false);
        Utils.dropTableIfExists(tableName, stmt);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant, col2 sql_variant)");
        pstmt = (SQLServerPreparedStatement) con.prepareStatement("insert into " + tableName + " values (?,?)");
        pstmt.setObject(1, binary20);
        pstmt.setObject(2, secondBinary20);
        pstmt.execute();
        Utils.dropProcedureIfExists(inputProc, stmt);
        String sql = "CREATE PROCEDURE " + inputProc + " @p0 sql_variant OUTPUT, @p1 sql_variant" + " AS" + " SELECT top 1 @p0=col1 FROM " + tableName
                + " where col2=@p1 ";
        stmt.execute(sql);

        CallableStatement cs = con.prepareCall(" {call " + inputProc + " (?,?) }");
        cs.registerOutParameter(1, microsoft.sql.Types.SQL_VARIANT);
        cs.setObject(2, secondBinary20, microsoft.sql.Types.SQL_VARIANT);

        cs.execute();
        assertTrue(parseByte((byte[]) cs.getBytes(1), binary20));
        if (null != cs) {
            cs.close();
        }
    }

    /**
     * Test stored procedure with input and output params
     *
     * @throws SQLException
     */
    @Test
    public void callableStatementInputOutputIntTest() throws SQLException {
        int col1Value = 5;
        int col2Value = 2;
        Utils.dropTableIfExists(tableName, stmt);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant, col2 int)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1, col2) values (CAST (" + col1Value + " AS " + "int" + "), " + col2Value + ")");
        Utils.dropProcedureIfExists(inputProc, stmt);
        String sql = "CREATE PROCEDURE " + inputProc + " @p0 sql_variant OUTPUT, @p1 sql_variant" + " AS" + " SELECT top 1 @p0=col1 FROM " + tableName
                + " where col2=@p1";
        stmt.execute(sql);
        CallableStatement cs = con.prepareCall(" {call " + inputProc + " (?,?) }");

        cs.registerOutParameter(1, microsoft.sql.Types.SQL_VARIANT);
        cs.setObject(2, col2Value, microsoft.sql.Types.SQL_VARIANT);
        cs.execute();
        assertEquals(cs.getObject(1), col1Value);
        if (null != cs) {
            cs.close();
        }
    }

    /**
     * Test stored procedure with input and output and return value
     *
     * @throws SQLException
     */
    @Test
    public void callableStatementInputOutputReturnIntTest() throws SQLException {
        int col1Value = 5;
        int col2Value = 2;
        int returnValue = 12;
        Utils.dropTableIfExists(tableName, stmt);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant, col2 int)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1, col2) values (CAST (" + col1Value + " AS " + "int" + "), " + col2Value + ")");
        Utils.dropProcedureIfExists(inputProc, stmt);
        String sql = "CREATE PROCEDURE " + inputProc + " @p0 sql_variant OUTPUT, @p1 sql_variant" + " AS" + " SELECT top 1 @p0=col1 FROM " + tableName
                + " where col2=@p1" + " return " + returnValue;
        stmt.execute(sql);
        CallableStatement cs = con.prepareCall(" {? = call " + inputProc + " (?,?) }");

        cs.registerOutParameter(1, microsoft.sql.Types.SQL_VARIANT);
        cs.registerOutParameter(2, microsoft.sql.Types.SQL_VARIANT);
        cs.setObject(3, col2Value, microsoft.sql.Types.SQL_VARIANT);
        cs.execute();
        assertEquals(cs.getString(1), String.valueOf(returnValue));
        assertEquals(cs.getString(2), String.valueOf(col1Value));
        if (null != cs) {
            cs.close();
        }
    }

    /**
     * test input output procedure
     *
     * @throws SQLException
     */
    @Test
    public void callableStatementInputOutputReturnStringTest() throws SQLException {
        String col1Value = "aa";
        String col2Value = "bb";
        int returnValue = 12;

        Utils.dropTableIfExists(tableName, stmt);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant, col2 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1,col2) values" + " (CAST ('" + col1Value + "' AS " + "varchar(5)" + ")" + " ,CAST ('"
                + col2Value + "' AS " + "varchar(5)" + ")" + ")");
        Utils.dropProcedureIfExists(inputProc, stmt);
        String sql = "CREATE PROCEDURE " + inputProc + " @p0 sql_variant OUTPUT, @p1 sql_variant" + " AS" + " SELECT top 1 @p0=col1 FROM " + tableName
                + " where col2=@p1 " + " return " + returnValue;
        stmt.execute(sql);
        CallableStatement cs = con.prepareCall(" {? = call " + inputProc + " (?,?) }");
        cs.registerOutParameter(1, java.sql.Types.INTEGER);
        cs.registerOutParameter(2, microsoft.sql.Types.SQL_VARIANT);
        cs.setObject(3, col2Value, microsoft.sql.Types.SQL_VARIANT);

        cs.execute();
        assertEquals(returnValue, cs.getObject(1));
        assertEquals(cs.getObject(2), col1Value);
        if (null != cs) {
            cs.close();
        }
    }

    /**
     * Read several rows from SqlVariant
     *
     * @throws SQLException
     */
    @Test
    public void readSeveralRows() throws SQLException {
        short value1 = 5;
        int value2 = 10;
        String value3 = "hi";
        Utils.dropTableIfExists(tableName, stmt);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant, col2 sql_variant, col3 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + " values (CAST (" + value1 + " AS " + "tinyint" + ")" + ",CAST (" + value2 + " AS " + "int"
                + ")" + ",CAST ('" + value3 + "' AS " + "char(2)" + ")" + ")");

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        rs.next();
        assertEquals(rs.getObject(1), value1);
        assertEquals(rs.getObject(2), value2);
        assertEquals(rs.getObject(3), value3);
        if (null != rs) {
            rs.close();
        }

    }
    
    /**
     * Test retrieving values with varchar and integer as basetype
     * @throws SQLException
     */
    @Test
    public void readVarcharInteger() throws SQLException {
        Object expected[] = {"abc", 42};
        int index = 0;
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT cast('abc' as sql_variant) UNION ALL SELECT cast(42 as sql_variant)");
        while (rs.next()) {
            assertEquals(rs.getObject(1), expected[index++]);
        }
    }

    /**
     * Tests unsupported type
     * 
     * @throws SQLException
     */
    @Test
    public void testUnsupportedDatatype() throws SQLException {
        rs = (SQLServerResultSet) stmt.executeQuery("select cast(cast('2017-08-16 17:31:09.995 +07:00' as datetimeoffset) as sql_variant)");
        rs.next();
        try {
            rs.getObject(1);
            fail("Should have thrown unssuported tds type exception");
        }
        catch (Exception e) {
            assertTrue(e.getMessage().equalsIgnoreCase("Unexpected TDS type  DATETIMEOFFSETN  in SQL_VARIANT."));
        }
        if (null != rs) {
            rs.close();
        }
    }

    /**
     * Tests that the returning class of base type time in sql_variant is correct.
     * 
     * @throws SQLException
     * 
     */
    @Test
    public void testTimeClassAsSqlVariant() throws SQLException {
        rs = (SQLServerResultSet) stmt.executeQuery("select cast(cast('17:31:09.995' as time(3)) as sql_variant)");
        rs.next();
        Object object = rs.getObject(1);
        assertEquals(object.getClass(), java.sql.Time.class);
        ;
    }

    private boolean parseByte(byte[] expectedData,
            byte[] retrieved) {
        assertTrue(Arrays.equals(expectedData, Arrays.copyOf(retrieved, expectedData.length)), " unexpected BINARY value, expected");
        for (int i = expectedData.length; i < retrieved.length; i++) {
            assertTrue(0 == retrieved[i], "unexpected data BINARY");
        }
        return true;
    }

    /**
     * Create and populate table
     * 
     * @param columnType
     * @param value
     * @throws SQLException
     */
    private void createAndPopulateTable(String columnType,
            Object value) throws SQLException {
        Utils.dropTableIfExists(tableName, stmt);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + " values (CAST (" + value + " AS " + columnType + "))");
    }

    /**
     * Prepare test
     * 
     * @throws SQLException
     * @throws SecurityException
     * @throws IOException
     */
    @BeforeAll
    public static void setupHere() throws SQLException, SecurityException, IOException {
        con = (SQLServerConnection) DriverManager.getConnection(connectionString);
        stmt = con.createStatement();
    }

    /**
     * drop the tables
     * 
     * @throws SQLException
     */
    @AfterAll
    public static void afterAll() throws SQLException {
        Utils.dropProcedureIfExists(inputProc, stmt);
        Utils.dropTableIfExists(tableName, stmt);

        if (null != stmt) {
            stmt.close();
        }

        if (null != pstmt) {
            pstmt.close();
        }

        if (null != rs) {
            rs.close();
        }

        if (null != con) {
            con.close();
        }
    }

}
