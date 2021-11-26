/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.datatypes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomData;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


/**
 * Tests for supporting sqlVariant
 *
 */
@RunWith(JUnitPlatform.class)
public class SQLVariantResultSetTest extends AbstractTest {

    static String tableName;
    static String inputProc;

    /**
     * Read int value
     * 
     * @throws SQLException
     * @throws SecurityException
     * @throws IOException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void readInt() throws SQLException, SecurityException, IOException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {

            int value = 2;
            createAndPopulateTable("int", value);
            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertEquals(rs.getString(1), "" + value);
            }
        }
    }

    /**
     * Read money type stored in SqlVariant
     * 
     * @throws SQLException
     * 
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void readMoney() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            Double value = 123.12;
            createAndPopulateTable("Money", value);
            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertEquals(rs.getObject(1), new BigDecimal("123.1200"));
            }
        }
    }

    /**
     * Reading smallmoney from SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void readSmallMoney() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            Double value = 123.12;
            createAndPopulateTable("smallmoney", value);
            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertEquals(rs.getObject(1), new BigDecimal("123.1200"));
            }
        }
    }

    /**
     * Read GUID stored in SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void readGUID() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            String value = "1AE740A2-2272-4B0F-8086-3DDAC595BC11";
            createAndPopulateTable("uniqueidentifier", "'" + value + "'");
            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertEquals(rs.getUniqueIdentifier(1), value);
            }
        }
    }

    /**
     * Reading date stored in SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void readDate() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            String value = "'2015-05-08'";
            createAndPopulateTable("date", value);
            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertEquals("" + rs.getObject(1), "2015-05-08");
            }
        }
    }

    /**
     * Read time from SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void readTime() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            String value = "'12:26:27.123345'";
            createAndPopulateTable("time(3)", value);
            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertEquals("" + rs.getObject(1).toString(), "12:26:27");
            }
        }
    }

    /**
     * Read datetime from SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void readDateTime() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            String value = "'2015-05-08 12:26:24'";
            createAndPopulateTable("datetime", value);
            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertEquals("" + rs.getObject(1), "2015-05-08 12:26:24.0");
            }
        }
    }

    /**
     * Read smalldatetime from SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void readSmallDateTime() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            String value = "'2015-05-08 12:26:24'";
            createAndPopulateTable("smalldatetime", value);
            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertEquals("" + rs.getObject(1), "2015-05-08 12:26:00.0");
            }
        }
    }

    /**
     * Read VarChar8000 from SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void readVarChar8000() throws SQLException {
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < 8000; i++) {
            buffer.append("a");
        }
        String value = "'" + buffer.toString() + "'";
        createAndPopulateTable("VARCHAR(8000)", value);
        try (Connection con = getConnection(); Statement stmt = con.createStatement();
                SQLServerResultSet rs = (SQLServerResultSet) stmt
                        .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
            rs.next();
            assertEquals(rs.getObject(1), buffer.toString());
        }
    }

    /**
     * Read float from SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void readFloat() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            float value = 5;
            createAndPopulateTable("float", value);
            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertEquals(rs.getObject(1), Double.valueOf("5.0"));
            }
        }
    }

    /**
     * Read bigint from SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void readBigInt() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            long value = 5;
            createAndPopulateTable("bigint", value);
            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertEquals(rs.getObject(1), value);
            }
        }
    }

    /**
     * read smallint from SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void readSmallInt() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            short value = 5;
            createAndPopulateTable("smallint", value);
            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertEquals(rs.getObject(1), value);
            }
        }
    }

    /**
     * Read tinyint from SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void readTinyInt() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            short value = 5;
            createAndPopulateTable("tinyint", value);
            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertEquals(rs.getObject(1), value);
            }
        }
    }

    /**
     * read bit from SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void readBit() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            int value = 50000;
            createAndPopulateTable("bit", value);
            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertEquals(rs.getObject(1), true);
            }
        }
    }

    /**
     * Read float from SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void readReal() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            float value = 5;
            createAndPopulateTable("Real", value);
            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertEquals(rs.getObject(1), Float.valueOf("5.0"));
            }
        }
    }

    /**
     * Read nchar from SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void readNChar() throws SQLException, SecurityException, IOException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            String value = "a";
            createAndPopulateTable("nchar(5)", "'" + value + "'");
            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertEquals(rs.getNString(1).trim(), value);
            }
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
    @Tag(Constants.xAzureSQLDW)
    public void readNVarChar() throws SQLException, SecurityException, IOException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            String value = "nvarchar";
            createAndPopulateTable("nvarchar(10)", "'" + value + "'");
            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertEquals(rs.getObject(1), value);
            }
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
    @Tag(Constants.xAzureSQLDW)
    public void readBinary20() throws SQLException, SecurityException, IOException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            String value = "hi";
            createAndPopulateTable("binary(20)", "'" + value + "'");
            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertTrue(parseByte((byte[]) rs.getObject(1), (byte[]) value.getBytes()));
            }
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
    @Tag(Constants.xAzureSQLDW)
    public void readVarBinary20() throws SQLException, SecurityException, IOException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            String value = "hi";
            createAndPopulateTable("varbinary(20)", "'" + value + "'");
            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertTrue(parseByte((byte[]) rs.getObject(1), (byte[]) value.getBytes()));
            }
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
    @Tag(Constants.xAzureSQLDW)
    public void readBinary512() throws SQLException, SecurityException, IOException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            String value = "hi";
            createAndPopulateTable("binary(512)", "'" + value + "'");
            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertTrue(parseByte((byte[]) rs.getObject(1), (byte[]) value.getBytes()));
            }
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
    @Tag(Constants.xAzureSQLDW)
    public void readBinary8000() throws SQLException, SecurityException, IOException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            String value = "hi";
            createAndPopulateTable("binary(8000)", "'" + value + "'");
            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertTrue(parseByte((byte[]) rs.getObject(1), (byte[]) value.getBytes()));
            }
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
    @Tag(Constants.xAzureSQLDW)
    public void readvarBinary8000() throws SQLException, SecurityException, IOException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            String value = "hi";
            createAndPopulateTable("varbinary(8000)", "'" + value + "'");
            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertTrue(parseByte((byte[]) rs.getObject(1), (byte[]) value.getBytes()));
            }
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
    @Tag(Constants.xAzureSQLDW)
    public void readSQLVariantProperty() throws SQLException, SecurityException, IOException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            String value = "hi";
            createAndPopulateTable("binary(8000)", "'" + value + "'");
            try (SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery(
                    "SELECT SQL_VARIANT_PROPERTY(col1,'BaseType') AS 'Base Type', SQL_VARIANT_PROPERTY(col1,'Precision') AS 'Precision' from "
                            + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertTrue(rs.getString(1).equalsIgnoreCase("binary"),
                        "unexpected baseType, expected: binary, retrieved:" + rs.getString(1));
            }
        }
    }

    /**
     * Testing that inserting value more than 8000 on varchar(8000) should throw failure operand type clash
     *
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void insertVarChar8001() throws SQLException {
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < 8001; i++) {
            buffer.append("a");
        }
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            stmt.executeUpdate(
                    "create table " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (col1 sql_variant)");
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(
                    "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values (?)")) {
                pstmt.setObject(1, buffer.toString());
                try {
                    pstmt.execute();
                } catch (SQLServerException e) {
                    assertTrue(e.getMessage().contains(TestResource.getResource("R_OperandTypeClash")));
                }
            }
        }
    }

    /**
     * Testing nvarchar4000
     *
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void readNvarChar4000() throws SQLException {
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < 4000; i++) {
            buffer.append("a");
        }
        String value = "'" + buffer.toString() + "'";
        createAndPopulateTable("NVARCHAR(4000)", value);
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertEquals(rs.getObject(1), buffer.toString());
            }
        }
    }

    /**
     * Update int value
     * 
     * @throws SQLException
     * @throws SecurityException
     * @throws IOException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void UpdateInt() throws SQLException, SecurityException, IOException {
        try (Connection con = getConnection();
                Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
            int value = 2;
            int updatedValue = 3;
            createAndPopulateTable("int", value);
            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertEquals(rs.getString(1), "" + value);
                rs.updateInt(1, updatedValue);
                rs.updateRow();
            }
            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertEquals(rs.getString(1), "" + updatedValue);
            }
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
    @Tag(Constants.xAzureSQLDW)
    public void UpdateNChar() throws SQLException, SecurityException, IOException {
        try (Connection con = getConnection();
                Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
            String value = "a";
            String updatedValue = "b";

            createAndPopulateTable("nchar", "'" + value + "'");

            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertEquals(rs.getString(1).trim(), "" + value);
                rs.updateNString(1, updatedValue);
                rs.updateRow();
            }
            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertEquals(rs.getString(1), "" + updatedValue);
            }
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
    @Tag(Constants.xAzureSQLDW)
    public void updateBinary20() throws SQLException, SecurityException, IOException {
        try (Connection con = getConnection();
                Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
            String value = "hi";
            String updatedValue = "bye";
            createAndPopulateTable("binary(20)", "'" + value + "'");

            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertTrue(parseByte((byte[]) rs.getObject(1), (byte[]) value.getBytes()));
                rs.updateBytes(1, updatedValue.getBytes());
                rs.updateRow();
            }
            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertTrue(parseByte((byte[]) rs.getBytes(1), updatedValue.getBytes()));
            }
        }
    }

    /**
     * Testing inserting and reading from SqlVariant and int column
     *
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void insertTest() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            stmt.executeUpdate("create table " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (col1 sql_variant, col2 int)");
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(
                    "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values (?, ?)")) {

                String[] col1Value = {"Hello", null};
                int[] col2Value = {1, 2};
                pstmt.setObject(1, "Hello");
                pstmt.setInt(2, 1);
                pstmt.execute();
                pstmt.setObject(1, null);
                pstmt.setInt(2, 2);
                pstmt.execute();
                try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                        .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                    int i = 0;
                    rs.next();
                    do {
                        assertEquals(rs.getObject(1), col1Value[i]);
                        assertEquals(rs.getObject(2), col2Value[i]);
                        i++;
                    } while (rs.next());
                }
            }
        }
    }

    /**
     * test inserting null value
     *
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void insertTestNull() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            stmt.executeUpdate(
                    "create table " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (col1 sql_variant)");
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(
                    "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values ( ?)")) {

                pstmt.setObject(1, null);
                pstmt.execute();
            }
            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertEquals(rs.getBoolean(1), false);
            }
        }
    }

    /**
     * Test inserting using setObject
     *
     * @throws SQLException
     * @throws ParseException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void insertSetObject() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            stmt.executeUpdate(
                    "create table " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (col1 sql_variant)");
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(
                    "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values (?)")) {

                pstmt.setObject(1, 2);
                pstmt.execute();
            }

            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertEquals(rs.getObject(1), 2);
            }
        }
    }

    /**
     * Test callableStatement with SqlVariant
     *
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void callableStatementOutputIntTest() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            int value = 5;
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            stmt.executeUpdate(
                    "create table " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (col1 sql_variant)");
            stmt.executeUpdate("INSERT into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values (CAST ("
                    + value + " AS " + "int" + "))");

            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(inputProc), stmt);
            String sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(inputProc)
                    + " @p0 sql_variant OUTPUT AS SELECT TOP 1 @p0=col1 FROM "
                    + AbstractSQLGenerator.escapeIdentifier(tableName);
            stmt.execute(sql);

            try (CallableStatement cs = con
                    .prepareCall(" {call " + AbstractSQLGenerator.escapeIdentifier(inputProc) + " (?) }")) {
                cs.registerOutParameter(1, microsoft.sql.Types.SQL_VARIANT);
                cs.execute();
                assertEquals(cs.getString(1), String.valueOf(value));
            }
        }
    }

    /**
     * Test callableStatement with SqlVariant
     *
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void callableStatementOutputDateTest() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            String value = "2015-05-08";

            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            stmt.executeUpdate(
                    "create table " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (col1 sql_variant)");
            stmt.executeUpdate("INSERT into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values (CAST ('"
                    + value + "' AS " + "date" + "))");

            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(inputProc), stmt);
            String sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(inputProc)
                    + " @p0 sql_variant OUTPUT AS SELECT TOP 1 @p0=col1 FROM "
                    + AbstractSQLGenerator.escapeIdentifier(tableName);
            stmt.execute(sql);

            try (CallableStatement cs = con
                    .prepareCall(" {call " + AbstractSQLGenerator.escapeIdentifier(inputProc) + " (?) }")) {
                cs.registerOutParameter(1, microsoft.sql.Types.SQL_VARIANT);
                cs.execute();
                assertEquals(cs.getString(1), String.valueOf(value));
            }
        }
    }

    /**
     * Test callableStatement with SqlVariant
     *
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void callableStatementOutputTimeTest() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            String value = "12:26:27.123345";
            String returnValue = "12:26:27";
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            stmt.executeUpdate(
                    "create table " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (col1 sql_variant)");
            stmt.executeUpdate("INSERT into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values (CAST ('"
                    + value + "' AS " + "time(3)" + "))");

            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(inputProc), stmt);
            String sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(inputProc)
                    + " @p0 sql_variant OUTPUT AS SELECT TOP 1 @p0=col1 FROM "
                    + AbstractSQLGenerator.escapeIdentifier(tableName);
            stmt.execute(sql);

            try (CallableStatement cs = con
                    .prepareCall(" {call " + AbstractSQLGenerator.escapeIdentifier(inputProc) + " (?) }")) {
                cs.registerOutParameter(1, microsoft.sql.Types.SQL_VARIANT, 3);
                cs.execute();
                assertEquals(String.valueOf(returnValue), "" + cs.getObject(1));
            }
        }
    }

    /**
     * Test callableStatement with SqlVariant Binary value
     *
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void callableStatementOutputBinaryTest() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            byte[] binary20 = RandomData.generateBinaryTypes("20", false, false);
            byte[] secondBinary20 = RandomData.generateBinaryTypes("20", false, false);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            stmt.executeUpdate("create table " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (col1 sql_variant, col2 sql_variant)");
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(
                    "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values (?,?)")) {
                pstmt.setObject(1, binary20);
                pstmt.setObject(2, secondBinary20);
                pstmt.execute();
            }

            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(inputProc), stmt);
            String sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(inputProc)
                    + " @p0 sql_variant OUTPUT, @p1 sql_variant" + " AS" + " SELECT top 1 @p0=col1 FROM "
                    + AbstractSQLGenerator.escapeIdentifier(tableName) + " where col2=@p1 ";
            stmt.execute(sql);

            try (CallableStatement cs = con
                    .prepareCall(" {call " + AbstractSQLGenerator.escapeIdentifier(inputProc) + " (?,?) }")) {
                cs.registerOutParameter(1, microsoft.sql.Types.SQL_VARIANT);
                cs.setObject(2, secondBinary20, microsoft.sql.Types.SQL_VARIANT);
                cs.execute();
                assertTrue(parseByte((byte[]) cs.getBytes(1), binary20));
            }
        }
    }

    /**
     * Test stored procedure with input and output params
     *
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void callableStatementInputOutputIntTest() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            int col1Value = 5;
            int col2Value = 2;
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            stmt.executeUpdate("create table " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (col1 sql_variant, col2 int)");
            stmt.executeUpdate("INSERT into " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + "(col1, col2) values (CAST (" + col1Value + " AS " + "int" + "), " + col2Value + ")");
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(inputProc), stmt);
            String sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(inputProc)
                    + " @p0 sql_variant OUTPUT, @p1 sql_variant" + " AS" + " SELECT top 1 @p0=col1 FROM "
                    + AbstractSQLGenerator.escapeIdentifier(tableName) + " where col2=@p1";
            stmt.execute(sql);
            try (CallableStatement cs = con
                    .prepareCall(" {call " + AbstractSQLGenerator.escapeIdentifier(inputProc) + " (?,?) }")) {

                cs.registerOutParameter(1, microsoft.sql.Types.SQL_VARIANT);
                cs.setObject(2, col2Value, microsoft.sql.Types.SQL_VARIANT);
                cs.execute();
                assertEquals(cs.getObject(1), col1Value);
            }
        }
    }

    /**
     * Test stored procedure with input and output and return value
     *
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void callableStatementInputOutputReturnIntTest() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            int col1Value = 5;
            int col2Value = 2;
            int returnValue = 12;
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            stmt.executeUpdate("create table " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (col1 sql_variant, col2 int)");
            stmt.executeUpdate("INSERT into " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + "(col1, col2) values (CAST (" + col1Value + " AS " + "int" + "), " + col2Value + ")");
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(inputProc), stmt);
            String sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(inputProc)
                    + " @p0 sql_variant OUTPUT, @p1 sql_variant" + " AS" + " SELECT top 1 @p0=col1 FROM "
                    + AbstractSQLGenerator.escapeIdentifier(tableName) + " where col2=@p1" + " return " + returnValue;
            stmt.execute(sql);
            try (CallableStatement cs = con
                    .prepareCall(" {? = call " + AbstractSQLGenerator.escapeIdentifier(inputProc) + " (?,?) }")) {

                cs.registerOutParameter(1, microsoft.sql.Types.SQL_VARIANT);
                cs.registerOutParameter(2, microsoft.sql.Types.SQL_VARIANT);
                cs.setObject(3, col2Value, microsoft.sql.Types.SQL_VARIANT);
                cs.execute();
                assertEquals(cs.getString(1), String.valueOf(returnValue));
                assertEquals(cs.getString(2), String.valueOf(col1Value));
            }
        }
    }

    /**
     * test input output procedure
     *
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void callableStatementInputOutputReturnStringTest() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            String col1Value = "aa";
            String col2Value = "bb";
            int returnValue = 12;

            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            stmt.executeUpdate("create table " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (col1 sql_variant, col2 sql_variant)");
            stmt.executeUpdate("INSERT into " + AbstractSQLGenerator.escapeIdentifier(tableName) + "(col1,col2) values"
                    + " (CAST ('" + col1Value + "' AS " + "varchar(5)" + ")" + " ,CAST ('" + col2Value + "' AS "
                    + "varchar(5)" + ")" + ")");
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(inputProc), stmt);
            String sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(inputProc)
                    + " @p0 sql_variant OUTPUT, @p1 sql_variant" + " AS" + " SELECT top 1 @p0=col1 FROM "
                    + AbstractSQLGenerator.escapeIdentifier(tableName) + " where col2=@p1 " + " return " + returnValue;
            stmt.execute(sql);
            try (CallableStatement cs = con
                    .prepareCall(" {? = call " + AbstractSQLGenerator.escapeIdentifier(inputProc) + " (?,?) }")) {
                cs.registerOutParameter(1, java.sql.Types.INTEGER);
                cs.registerOutParameter(2, microsoft.sql.Types.SQL_VARIANT);
                cs.setObject(3, col2Value, microsoft.sql.Types.SQL_VARIANT);

                cs.execute();
                assertEquals(returnValue, cs.getObject(1));
                assertEquals(cs.getObject(2), col1Value);
            }
        }
    }

    /**
     * Read several rows from SqlVariant
     *
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void readSeveralRows() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            short value1 = 5;
            int value2 = 10;
            String value3 = "hi";
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            stmt.executeUpdate("create table " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (col1 sql_variant, col2 sql_variant, col3 sql_variant)");
            stmt.executeUpdate("INSERT into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values (CAST ("
                    + value1 + " AS " + "tinyint" + ")" + ",CAST (" + value2 + " AS " + "int" + ")" + ",CAST ('"
                    + value3 + "' AS " + "char(2)" + ")" + ")");

            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertEquals(rs.getObject(1), value1);
                assertEquals(rs.getObject(2), value2);
                assertEquals(rs.getObject(3), value3);
            }
        }
    }

    /**
     * Test retrieving values with varchar and integer as basetype
     * 
     * @throws SQLException
     */
    @Test
    public void readVarcharInteger() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            Object expected[] = {"abc", 42};
            int index = 0;
            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("SELECT cast('abc' as sql_variant) UNION ALL SELECT cast(42 as sql_variant)")) {
                while (rs.next()) {
                    assertEquals(rs.getObject(1), expected[index++]);
                }
            }
        }
    }

    /**
     * Tests unsupported type
     * 
     * @throws SQLException
     */
    @Test
    public void testUnsupportedDatatype() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement();
                SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery(
                        "select cast(cast('2017-08-16 17:31:09.995 +07:00' as datetimeoffset) as sql_variant)")) {
            rs.next();
            try {
                rs.getObject(1);
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            } catch (Exception e) {
                assertTrue(e.getMessage().equalsIgnoreCase("Unexpected TDS type  DATETIMEOFFSETN  in SQL_VARIANT."));
            }
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
        try (Connection con = getConnection(); Statement stmt = con.createStatement();
                SQLServerResultSet rs = (SQLServerResultSet) stmt
                        .executeQuery("select cast(cast('17:31:09.995' as time(3)) as sql_variant)")) {
            rs.next();
            Object object = rs.getObject(1);
            assertEquals(object.getClass(), java.sql.Time.class);;
        }
    }
    
    @Test
    public void testCastThenGetNumeric() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement();
                SQLServerResultSet rs = (SQLServerResultSet) stmt
                        .executeQuery("select cast(123 as sql_variant) as c1");) {

            rs.next();
            assertEquals(true, rs.getBoolean("c1")); // select int as boolean inside sql_variant
            assertEquals(123, rs.getShort("c1")); // select int as short inside sql_variant
            assertEquals(123L, rs.getInt("c1")); // select int as int inside sql_variant
            assertEquals(123f, rs.getFloat("c1")); // select int as float inside sql_variant
            assertEquals(123L, rs.getLong("c1")); // select int as long inside sql_variant
            assertEquals(123d, rs.getDouble("c1")); // select int as double inside sql_variant
            assertEquals(new BigDecimal(123), rs.getBigDecimal("c1")); // select int as bigdecimal (money) inside sql_variant
        }
    }

    private boolean parseByte(byte[] expectedData, byte[] retrieved) {
        assertTrue(Arrays.equals(expectedData, Arrays.copyOf(retrieved, expectedData.length)),
                " unexpected BINARY value, expected");
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
    private void createAndPopulateTable(String columnType, Object value) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            stmt.executeUpdate(
                    "create table " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (col1 sql_variant)");
            stmt.executeUpdate("INSERT into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values (CAST ("
                    + value + " AS " + columnType + "))");
        }
    }

    /**
     * Prepare test
     * 
     * @throws SQLException
     * @throws SecurityException
     * @throws IOException
     */
    @BeforeAll
    public static void setupHere() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"trustServerCertificate", "true");
        setConnection();

        tableName = RandomUtil.getIdentifier("sqlVariantTestSrcTable");
        inputProc = RandomUtil.getIdentifier("sqlVariantProc");
    }

    /**
     * drop the tables
     * 
     * @throws SQLException
     */
    @AfterAll
    public static void afterAll() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(inputProc), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
        }
    }
}
