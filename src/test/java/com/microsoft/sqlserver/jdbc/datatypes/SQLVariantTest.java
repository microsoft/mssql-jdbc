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

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.nimbusds.oauth2.sdk.ParseException;

/**
 * Tests for supporting sqlVariant
 *
 */
@RunWith(JUnitPlatform.class)
public class SQLVariantTest extends AbstractTest {

    static SQLServerConnection con = null;
    static Statement stmt = null;
    static String tableName = "SqlVariant_Test";
    static String inputProc = "sqlVariant_Proc";
    static String procedureName = "TVP_SQLVariant_Proc";

    @Test
    public void readInt() throws SQLException, SecurityException, IOException {
        int value = 2;
        createAndPopulateTable("int", value);
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertEquals(rs.getString(1), "" + value);
        }

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
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertEquals(rs.getObject(1), new BigDecimal("123.1200"));
        }
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
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertEquals(rs.getObject(1), new BigDecimal("123.1200"));
        }
    }

    /**
     * Read GUID stored in SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    public void readGUID() throws SQLException {
        String value = "1AE740A2-2272-4B0F-8086-3DDAC595BC11";// NEWID()";
        createAndPopulateTable("uniqueidentifier", "'" + value + "'");
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertEquals(rs.getObject(1), value);
        }
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
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertEquals("" + rs.getObject(1), "2015-05-08");
        }
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
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertEquals("" + rs.getObject(1).toString(), "12:26:27.123"); // TODO
        }
    }

    @Test
    public void bulkCopyTest_time() throws SQLException {
        String col1Value = "'12:26:27.1452367'";
        String destTableName = "dest_sqlVariant";
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + destTableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + destTableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1) values (CAST (" + col1Value + " AS " + "time(2)" + ") )");
        stmt.executeUpdate("create table " + destTableName + " (col1 sql_variant)");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals("" + rs.getObject(1).toString(), "12:26:27.15"); // TODO
        }
    }

    @Test
    public void readTime2() throws SQLException {
        String col1Value = "'12:26:27.123345'";
        String destTableName = "dest_sqlVariant";
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + destTableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + destTableName);
        stmt.executeUpdate("create table " + tableName + " (col1 time)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1) values (CAST (" + col1Value + " AS " + "time" + ") )");
        stmt.executeUpdate("create table " + destTableName + " (col1 time)");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals("" + rs.getString(1).toString(), "12:26:27.1233450"); // TODO
        }
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
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertEquals("" + rs.getObject(1), "2015-05-08 12:26:24.0");
        }
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
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertEquals("" + rs.getObject(1), "2015-05-08 12:26:00.0");
        }
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
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertEquals(rs.getObject(1), buffer.toString());
        }
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
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertEquals(rs.getObject(1), Double.valueOf("5.0"));
        }
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
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertEquals(rs.getObject(1), value);
        }
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
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertEquals(rs.getObject(1), value);
        }
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
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertEquals(rs.getObject(1), value);
        }
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
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertEquals(rs.getObject(1), true);
        }
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
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertEquals(rs.getObject(1), Float.valueOf("5.0"));
        }
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
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertEquals(rs.getNString(1).trim(), value);
        }
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
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertEquals(rs.getObject(1), value);
        }
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
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertTrue(parseByte((byte[]) rs.getObject(1), (byte[]) value.getBytes()));
        }
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
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertTrue(parseByte((byte[]) rs.getObject(1), (byte[]) value.getBytes()));
        }
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
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertTrue(parseByte((byte[]) rs.getObject(1), (byte[]) value.getBytes()));
        }
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
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertTrue(parseByte((byte[]) rs.getObject(1), (byte[]) value.getBytes()));
        }
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
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertTrue(parseByte((byte[]) rs.getObject(1), (byte[]) value.getBytes()));
        }
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
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT SQL_VARIANT_PROPERTY(col1,'BaseType') AS 'Base Type',"
                + " SQL_VARIANT_PROPERTY(col1,'Precision') AS 'Precision' from " + tableName);
        while (rs.next()) {
            assertTrue(rs.getString(1).equalsIgnoreCase("binary"), "unexpected baseType, expected: binary, retrieved:" + rs.getString(1));
        }
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
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
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
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertEquals(rs.getObject(1), buffer.toString());
        }
    }

    /**
     * Testing inserting and reading from SqlVariant and int column
     *
     * @throws SQLException
     */
    @Test
    public void insertTest() throws SQLException {
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
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

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        int i = 0;
        while (rs.next()) {
            assertEquals(rs.getObject(1), col1Value[i]);
            assertEquals(rs.getObject(2), col2Value[i]);
            i++;
        }
    }

    @Test
    public void insertTestNull() throws SQLException {
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement("insert into " + tableName + " values ( ?)");

        pstmt.setObject(1, null);
        pstmt.execute();

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertEquals(rs.getBoolean(1), false);
            // assertEquals(rs.getObject(2), col2Value[i]);
        }
    }

    /**
     * Test inserting using setObject
     *
     * @throws SQLException
     * @throws ParseException
     */
    @Test
    public void insertSetObject() throws SQLException {
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement("insert into " + tableName + " values (?)");

        pstmt.setObject(1, 2);
        pstmt.execute();

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertEquals(rs.getObject(1), 2);
        }
    }

    /**
     * Test callableStatement with SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    public void callableStatementOutputTest() throws SQLException {
        int value = 5;
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + " values (CAST (" + value + " AS " + "int" + "))");

        String outPutProc = "sqlVariant_Proc";
        String sql = " IF EXISTS (select * from sysobjects where id = object_id(N'" + outPutProc + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)"
                + " DROP PROCEDURE " + outPutProc;
        stmt.execute(sql);
        sql = "CREATE PROCEDURE " + outPutProc + " @p0 sql_variant OUTPUT  AS SELECT TOP 1 @p0=col1 FROM  " + tableName;
        stmt.execute(sql);

        CallableStatement cs = con.prepareCall(" {call " + outPutProc + " (?)  }");
        cs.registerOutParameter(1, microsoft.sql.Types.SQL_VARIANT);
        cs.execute();
        assertEquals(cs.getString(1), String.valueOf(value));
    }

    /**
     * Test stored procedure with input and output params
     * 
     * @throws SQLException
     */
    @Test
    public void callableStatementInOutTest() throws SQLException {
        int col1Value = 5;
        int col2Value = 2;
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant, col2 int)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1, col2) values (CAST (" + col1Value + " AS " + "int" + "), " + col2Value + ")");
        String sql = " IF EXISTS (select * from sysobjects where id = object_id(N'" + inputProc + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)"
                + " DROP PROCEDURE " + inputProc;
        stmt.execute(sql);
        sql = "CREATE PROCEDURE " + inputProc + " @p0 sql_variant OUTPUT, @p1 sql_variant" + " AS" + " SELECT top 1 @p0=col1 FROM " + tableName
                + " where col2=@p1";
        stmt.execute(sql);
        CallableStatement cs = con.prepareCall(" {call " + inputProc + " (?,?)  }");

        cs.registerOutParameter(1, microsoft.sql.Types.SQL_VARIANT);
        cs.setObject(2, col2Value, microsoft.sql.Types.SQL_VARIANT);
        cs.execute();
        assertEquals(cs.getObject(1), col1Value);
    }

    /**
     * Test stored procedure with input and output and return value
     * 
     * @throws SQLException
     */
    @Test
    public void callableStatementInOutRetTest() throws SQLException {
        int col1Value = 5;
        int col2Value = 2;
        int returnValue = 12;
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant, col2 int)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1, col2) values (CAST (" + col1Value + " AS " + "int" + "), " + col2Value + ")");
        String sql = " IF EXISTS (select * from sysobjects where id = object_id(N'" + inputProc + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)"
                + " DROP PROCEDURE " + inputProc;
        stmt.execute(sql);
        sql = "CREATE PROCEDURE " + inputProc + " @p0 sql_variant OUTPUT, @p1 sql_variant" + " AS" + " SELECT top 1 @p0=col1 FROM " + tableName
                + " where col2=@p1" + " return " + returnValue;
        stmt.execute(sql);
        CallableStatement cs = con.prepareCall(" {? = call " + inputProc + " (?,?)  }");

        cs.registerOutParameter(1, microsoft.sql.Types.SQL_VARIANT);
        cs.registerOutParameter(2, microsoft.sql.Types.SQL_VARIANT);
        cs.setObject(3, col2Value, microsoft.sql.Types.SQL_VARIANT);
        cs.execute();
        assertEquals(cs.getString(1), String.valueOf(returnValue));
        assertEquals(cs.getString(2), String.valueOf(col1Value));
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
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant, col2 sql_variant, col3 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + " values (CAST (" + value1 + " AS " + "tinyint" + ")" + ",CAST (" + value2 + " AS " + "int"
                + ")" + ",CAST ('" + value3 + "' AS " + "char(2)" + ")" + ")");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertEquals(rs.getObject(1), value1);
            assertEquals(rs.getObject(2), value2);
            assertEquals(rs.getObject(3), value3);
        }
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
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
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

        stmt.executeUpdate(" IF EXISTS (select * from sysobjects where id = object_id(N'" + inputProc
                + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)" + " DROP PROCEDURE " + inputProc);
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') and OBJECTPROPERTY(id, N'IsTable') = 1)"
                + " DROP TABLE " + tableName);

        if (null != stmt) {
            stmt.close();
        }
        if (null != con) {
            con.close();
        }
    }

}
