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
import java.sql.DriverManager;
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

    /**
     * Read from a SqlVariant table int value
     * 
     * @throws SQLException
     * @throws IOException
     * @throws SecurityException
     */
    @Test
    public void readInt() throws SQLException, SecurityException, IOException {
        int value = 2;
        createAndPopulateTable("int", value);
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertEquals(rs.getObject(1), String.valueOf(value));
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
            assertEquals(rs.getObject(1), "123.1200");
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
            assertEquals(rs.getObject(1), "123.1200");
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
            assertEquals(rs.getObject(1), "2015-05-08");
        }
    }

    /**
     * Read time from SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    public void readTime() throws SQLException {
        String value = "'12:26:27.123'";
        createAndPopulateTable("time(3)", value);
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertEquals(rs.getObject(1), "12:26:27.123");
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
            assertEquals(rs.getObject(1), "2015-05-08 12:26:24.0");
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
            assertEquals(rs.getObject(1), "2015-05-08 12:26:00.0");
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
            assertEquals(rs.getObject(1), "5.0");
        }
    }

    /**
     * Read bigint from SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    public void readBigInt() throws SQLException {
        int value = 5;
        createAndPopulateTable("bigint", value);
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertEquals(rs.getObject(1), "5");
        }
    }

    /**
     * read smallint from SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    public void readSmallInt() throws SQLException {
        int value = 5;
        createAndPopulateTable("smallint", value);
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertEquals(rs.getObject(1), "5");
        }
    }

    /**
     * Read tinyint from SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    public void readTinyInt() throws SQLException {
        int value = 5;
        createAndPopulateTable("tinyint", value);
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertEquals(rs.getString(1), "5");
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
            assertEquals(rs.getObject(1), "1");
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
            assertEquals(rs.getObject(1), "5.0");
        }
    }

    /**
     * Read nchar from SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    public void readNChar() throws SQLException, SecurityException, IOException {
        String value = "nchar";
        createAndPopulateTable("nchar(5)", "'" + value + "'");
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertEquals(rs.getObject(1), value);
        }
    }

    @Test
    public void readNVarChar() throws SQLException, SecurityException, IOException {
        String value = "nvarchar";
        createAndPopulateTable("nvarchar(10)", "'" + value + "'");
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertEquals(rs.getObject(1), value);
        }
    }

    @Test
    public void readBinary20() throws SQLException, SecurityException, IOException {
        String value = "hi";
        createAndPopulateTable("binary(20)", "'" + value + "'");
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertTrue(parseByte((byte[]) rs.getObject(1), (byte[]) value.getBytes()));
        }
    }

    @Test
    public void readVarBinary20() throws SQLException, SecurityException, IOException {
        String value = "hi";
        createAndPopulateTable("varbinary(20)", "'" + value + "'");
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertTrue(parseByte((byte[]) rs.getObject(1), (byte[]) value.getBytes()));
        }
    }

    @Test
    public void readBinary512() throws SQLException, SecurityException, IOException {
        String value = "hi";
        createAndPopulateTable("binary(512)", "'" + value + "'");
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertTrue(parseByte((byte[]) rs.getObject(1), (byte[]) value.getBytes()));
        }
    }

    @Test
    public void readVarBinary8000() throws SQLException, SecurityException, IOException {
        String value = "hi";
        createAndPopulateTable("binary(8000)", "'" + value + "'");
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertTrue(parseByte((byte[]) rs.getObject(1), (byte[]) value.getBytes()));
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
    public void insert() throws SQLException {
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

    /**
     * Test inserting using setObject
     *
     * @throws SQLException
     * @throws ParseException
     */
    @Test
    public void test() throws SQLException {
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement("insert into " + tableName + " values (?)");

        pstmt.setObject(1, 2);
        pstmt.execute();

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {
            assertEquals(rs.getObject(1), String.valueOf(2));
        }
    }

    /**
     * Create and populate table
     * 
     * @param columnType
     * @param value
     * @throws SQLException
     */
    public void createAndPopulateTable(String columnType,
            Object value) throws SQLException {
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + " values (CAST (" + value + " AS " + columnType + "))");
    }
    

    /**
     * Prepare test
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
