/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerParameterMetaData;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


/**
 * Tests different kinds of queries
 *
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDW)
public class PQImpsTest extends AbstractTest {
    private static final int SQL_SERVER_2012_VERSION = 11;

    private static Connection connection = null;
    private static Statement stmt = null;
    private static PreparedStatement pstmt = null;
    private static ResultSet rs = null;
    private static ResultSet versionRS = null;
    private static int version = -1;

    private static String nameTable = RandomUtil.getIdentifier("names_DB");
    private static String phoneNumberTable = RandomUtil.getIdentifier("phoneNumbers_DB");
    private static String mergeNameDesTable = RandomUtil.getIdentifier("mergeNameDesTable_DB");
    private static String numericTable = RandomUtil.getIdentifier("numericTable_DB");
    private static String charTable = RandomUtil.getIdentifier("charTable_DB");
    private static String charTable2 = RandomUtil.getIdentifier("charTable2_DB");
    private static String binaryTable = RandomUtil.getIdentifier("binaryTable_DB");
    private static String dateAndTimeTable = RandomUtil.getIdentifier("dateAndTimeTable_DB");
    private static String multipleTypesTable = RandomUtil.getIdentifier("multipleTypesTable_DB");
    private static String spaceTable = RandomUtil.getIdentifier("spaceTable_DB");

    /**
     * Setup
     * 
     * @throws SQLException
     */
    @BeforeAll
    public static void BeforeTests() throws Exception {
        setConnection();

        connection = getConnection();
        stmt = connection.createStatement();
        version = getSQLServerVersion();
        createMultipleTypesTable();
        createNumericTable();
        createCharTable();
        createChar2Table();
        createBinaryTable();
        createDateAndTimeTable();
        createTablesForCompexQueries();
        createSpaceTable();
        populateTablesForCompexQueries();
    }

    /**
     * Numeric types test
     * 
     * @throws SQLException
     */
    @Test
    @DisplayName("Numeric types")
    public void numericTest() throws SQLException {
        try {
            populateNumericTable();
            testBeforeExcute();
            selectNumeric();
            checkNumericMetaData();
            // old PQ implementation doesn't work with "insert"
            if (version >= SQL_SERVER_2012_VERSION) {
                insertNumeric();
                checkNumericMetaData();
                updateNumeric();
                checkNumericMetaData();
            }
            deleteNumeric();
            checkNumericMetaData();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Char types test
     * 
     * @throws SQLException
     */
    @Test
    @DisplayName("Char Types")
    public void charTests() throws SQLException {
        try {
            populateCharTable();
            selectChar();
            checkCharMetaData(4);

            if (version >= SQL_SERVER_2012_VERSION) {
                insertChar();
                checkCharMetaData(6);
                updateChar();
                checkCharMetaData(6);
            }
            deleteChar();
            checkCharMetaData(4);
        } catch (Exception e) {
            fail(e.getMessage());
        }

    }

    /**
     * Binary types test
     * 
     * @throws SQLException
     */
    @Test
    @DisplayName("Binary Types")
    public void binaryTests() throws SQLException {
        try {

            populateBinaryTable();
            selectBinary();
            checkBinaryMetaData();

            if (version >= SQL_SERVER_2012_VERSION) {
                insertBinary();
                checkBinaryMetaData();
                updateBinary();
                checkBinaryMetaData();
            }
            deleteBinary();
            checkBinaryMetaData();
        } catch (Exception e) {
            fail(e.getMessage());
        }

    }

    /**
     * Temporal types test
     * 
     * @throws SQLException
     */
    @Test
    @DisplayName("Temporal Types")
    public void temporalTests() throws SQLException {

        try {
            populateDateAndTimeTable();
            selectDateAndTime();
            checkDateAndTimeMetaData();

            if (version >= SQL_SERVER_2012_VERSION) {
                insertDateAndTime();
                checkDateAndTimeMetaData();
                updateDateAndTime();
                checkDateAndTimeMetaData();
            }
            deleteDateAndTime();
            checkDateAndTimeMetaData();
        } catch (Exception e) {
            fail(e.getMessage());
        }

    }

    /**
     * Multiple Types table
     * 
     * @throws Exception
     */
    @Test
    @DisplayName("Multiple Types Table")
    public void MultipleTypesTableTest() throws Exception {

        try {
            if (version >= SQL_SERVER_2012_VERSION) {
                testInsertMultipleTypes();
                testMixedWithHardcodedValues();
            }
        } catch (SQLException e) {
            fail(e.getMessage());
        }

    }

    private static int getSQLServerVersion() throws SQLException {
        versionRS = stmt.executeQuery("SELECT CONVERT(varchar(100), SERVERPROPERTY('ProductVersion'))");
        versionRS.next();
        String versionString = versionRS.getString(1);
        int dotIndex = versionString.indexOf(".");

        return Integer.parseInt(versionString.substring(0, dotIndex));
    }

    private static void checkNumericMetaData() throws SQLException {

        ParameterMetaData pmd = pstmt.getParameterMetaData();

        assertEquals(pmd.getParameterCount(), 15, TestResource.getResource("R_paramNotRecognized"));

        compareParameterMetaData(pmd, 1, "java.math.BigDecimal", 3, "decimal", 18, 0);
        compareParameterMetaData(pmd, 2, "java.math.BigDecimal", 3, "decimal", 10, 5);
        compareParameterMetaData(pmd, 3, "java.math.BigDecimal", 2, "numeric", 18, 0);
        compareParameterMetaData(pmd, 4, "java.math.BigDecimal", 2, "numeric", 8, 4);
        compareParameterMetaData(pmd, 5, "java.lang.Double", 8, "float", 15, 0);
        compareParameterMetaData(pmd, 6, "java.lang.Float", 7, "real", 7, 0);
        compareParameterMetaData(pmd, 7, "java.lang.Float", 7, "real", 7, 0);
        compareParameterMetaData(pmd, 8, "java.lang.Integer", 4, "int", 10, 0);
        compareParameterMetaData(pmd, 9, "java.lang.Long", -5, "bigint", 19, 0);
        compareParameterMetaData(pmd, 10, "java.lang.Short", 5, "smallint", 5, 0);
        compareParameterMetaData(pmd, 11, "java.lang.Short", -6, "tinyint", 3, 0);
        compareParameterMetaData(pmd, 12, "java.math.BigDecimal", 3, "money", 19, 4);
        compareParameterMetaData(pmd, 13, "java.math.BigDecimal", 3, "smallmoney", 10, 4);
        compareParameterMetaData(pmd, 14, "java.math.BigDecimal", 3, "decimal", 10, 9);
        compareParameterMetaData(pmd, 15, "java.math.BigDecimal", 3, "decimal", 38, 37);
    }

    private static void checkCharMetaData(int expectedParameterCount) throws SQLException {

        ParameterMetaData pmd = pstmt.getParameterMetaData();

        assertEquals(pmd.getParameterCount(), expectedParameterCount, TestResource.getResource("R_paramNotRecognized"));

        compareParameterMetaData(pmd, 1, "java.lang.String", 1, "char", 50, 0);
        compareParameterMetaData(pmd, 2, "java.lang.String", 12, "varchar", 20, 0);
        compareParameterMetaData(pmd, 3, "java.lang.String", -15, "nchar", 30, 0);
        compareParameterMetaData(pmd, 4, "java.lang.String", -9, "nvarchar", 60, 0);

        if (expectedParameterCount > 4) {
            compareParameterMetaData(pmd, 5, "java.lang.String", -1, "text", 2147483647, 0);
            compareParameterMetaData(pmd, 6, "java.lang.String", -16, "ntext", 1073741823, 0);
        }
    }

    private static void checkBinaryMetaData() throws SQLException {

        ParameterMetaData pmd = pstmt.getParameterMetaData();

        assertEquals(pmd.getParameterCount(), 2, TestResource.getResource("R_paramNotRecognized"));

        compareParameterMetaData(pmd, 1, "[B", -2, "binary", 100, 0);
        compareParameterMetaData(pmd, 2, "[B", -3, "varbinary", 200, 0);
    }

    private static void checkDateAndTimeMetaData() throws SQLException {

        ParameterMetaData pmd = pstmt.getParameterMetaData();
        assertEquals(pmd.getParameterCount(), 9, TestResource.getResource("R_paramNotRecognized"));

        compareParameterMetaData(pmd, 1, "java.sql.Date", 91, "date", 10, 0);
        compareParameterMetaData(pmd, 2, "java.sql.Timestamp", 93, "datetime", 23, 3);
        compareParameterMetaData(pmd, 3, "java.sql.Timestamp", 93, "datetime2", 27, 7);
        compareParameterMetaData(pmd, 4, "java.sql.Timestamp", 93, "datetime2", 25, 5);
        compareParameterMetaData(pmd, 5, "microsoft.sql.DateTimeOffset", -155, "datetimeoffset", 34, 7);
        compareParameterMetaData(pmd, 6, "microsoft.sql.DateTimeOffset", -155, "datetimeoffset", 32, 5);
        compareParameterMetaData(pmd, 7, "java.sql.Timestamp", 93, "smalldatetime", 16, 0);
        compareParameterMetaData(pmd, 8, "java.sql.Time", 92, "time", 16, 7);
        compareParameterMetaData(pmd, 9, "java.sql.Time", 92, "time", 14, 5);
    }

    private static void compareParameterMetaData(ParameterMetaData pmd, int index, String expectedClassName,
            int expectedType, String expectedTypeName, int expectedPrecision, int expectedScale) {

        try {
            assertTrue(pmd.getParameterClassName(index).equalsIgnoreCase(expectedClassName),
                    "Parameter class Name error:\n" + "expected: " + expectedClassName + "\n" + "actual: "
                            + pmd.getParameterClassName(index));
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        try {
            assertTrue(pmd.getParameterType(index) == expectedType, "getParameterType: "
                    + TestResource.getResource("R_valueNotMatch") + expectedType + ", " + pmd.getParameterType(index));
        } catch (SQLException e) {
            fail(e.getMessage());
        }

        try {
            assertTrue(pmd.getParameterTypeName(index).equalsIgnoreCase(expectedTypeName),
                    "getParameterTypeName: " + TestResource.getResource("R_valueNotMatch") + expectedTypeName + ", "
                            + pmd.getParameterTypeName(index));
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        try {
            assertTrue(pmd.getPrecision(index) == expectedPrecision, "getPrecision: "
                    + TestResource.getResource("R_valueNotMatch") + expectedPrecision + ", " + pmd.getPrecision(index));
        } catch (SQLException e) {
            fail(e.getMessage());
        }

        try {
            assertTrue(pmd.getScale(index) == expectedScale, "getScale: " + TestResource.getResource("R_valueNotMatch")
                    + expectedScale + ", " + pmd.getScale(index));
        } catch (SQLException e) {
            fail(e.getMessage());
        }

    }

    private static void populateNumericTable() throws SQLException {
        stmt.execute("insert into " + AbstractSQLGenerator.escapeIdentifier(numericTable) + " values (" + "1.123,"
                + "1.123," + "1.2345," + "1.2345," + "1.543," + "1.543," + "5.1234," + "104935," + "34323," + "123,"
                + "5," + "1.45," + "1.3," + "0.123456789," + "0.1234567890123456789012345678901234567" + ")");
    }

    private static void testBeforeExcute() throws SQLException {
        if (null != pstmt) {
            pstmt.close();
        }

        String sql = "select * from " + AbstractSQLGenerator.escapeIdentifier(numericTable) + " where " + "c1 = ? and "
                + "c2 = ? and " + "c3 = ? and " + "c4 = ? and " + "c5 = ? and " + "c6 = ? and " + "c7 = ? and "
                + "c8 = ? and " + "c9 = ? and " + "c10 = ? and " + "c11 = ? and " + "c12 = ? and " + "c13 = ? and "
                + "c14 = ? and " + "c15 = ? ";

        pstmt = connection.prepareStatement(sql);

        checkNumericMetaData();

        if (null != pstmt) {
            pstmt.close();
        }
    }

    private static void selectNumeric() throws SQLException {
        String sql = "select * from " + AbstractSQLGenerator.escapeIdentifier(numericTable) + " where " + "c1 = ? and "
                + "c2 = ? and " + "c3 = ? and " + "c4 = ? and " + "c5 = ? and " + "c6 = ? and " + "c7 = ? and "
                + "c8 = ? and " + "c9 = ? and " + "c10 = ? and " + "c11 = ? and " + "c12 = ? and " + "c13 = ?  and "
                + "c14 = ? and " + "c15 = ? ";

        pstmt = connection.prepareStatement(sql);

        for (int i = 1; i <= 15; i++) {
            pstmt.setString(i, "1");
        }

        rs = pstmt.executeQuery();
    }

    private static void insertNumeric() throws SQLException {

        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(numericTable) + " values( " + "?," + "?,"
                + "?," + "?," + "?," + "?," + "?," + "?," + "?," + "?," + "?," + "?," + "?," + "?," + "?" + ")";

        pstmt = connection.prepareStatement(sql);

        for (int i = 1; i <= 15; i++) {
            pstmt.setString(i, "1");
        }

        pstmt.execute();
    }

    private static void updateNumeric() throws SQLException {

        String sql = "update " + AbstractSQLGenerator.escapeIdentifier(numericTable) + " set " + "c1 = ?," + "c2 = ?,"
                + "c3 = ?," + "c4 = ?," + "c5 = ?," + "c6 = ?," + "c7 = ?," + "c8 = ?," + "c9 = ?," + "c10 = ?,"
                + "c11 = ?," + "c12 = ?," + "c13 = ?," + "c14 = ?," + "c15 = ?" + Constants.SEMI_COLON;

        pstmt = connection.prepareStatement(sql);

        for (int i = 1; i <= 15; i++) {
            pstmt.setString(i, "1");
        }

        pstmt.execute();
    }

    private static void deleteNumeric() throws SQLException {

        String sql = "delete from " + AbstractSQLGenerator.escapeIdentifier(numericTable) + " where " + "c1 = ? and "
                + "c2 = ? and " + "c3 = ? and " + "c4 = ? and " + "c5 = ? and " + "c6 = ? and " + "c7 = ? and "
                + "c8 = ? and " + "c9 = ? and " + "c10 = ? and " + "c11 = ? and " + "c12 = ? and " + "c13 = ? and "
                + "c14 = ? and " + "c15 = ?" + Constants.SEMI_COLON;

        pstmt = connection.prepareStatement(sql);

        for (int i = 1; i <= 15; i++) {
            pstmt.setString(i, "1");
        }

        pstmt.execute();
    }

    private static void createNumericTable() throws SQLException {

        stmt.execute("Create table " + AbstractSQLGenerator.escapeIdentifier(numericTable) + " ("
                + "c1 decimal not null," + "c2 decimal(10,5) not null," + "c3 numeric not null,"
                + "c4 numeric(8,4) not null," + "c5 float not null," + "c6 float(10) not null," + "c7 real not null,"
                + "c8 int not null," + "c9 bigint not null," + "c10 smallint not null," + "c11 tinyint not null,"
                + "c12 money not null," + "c13 smallmoney not null," + "c14 decimal(10,9) not null,"
                + "c15 decimal(38,37) not null" + ")");
    }

    private static void createCharTable() throws SQLException {

        stmt.execute("Create table " + AbstractSQLGenerator.escapeIdentifier(charTable) + " (" + "c1 char(50) not null,"
                + "c2 varchar(20) not null," + "c3 nchar(30) not null," + "c4 nvarchar(60) not null,"
                + "c5 text not null," + "c6 ntext not null" + ")");
    }

    private static void createSpaceTable() throws SQLException {
        stmt.execute("Create table " + AbstractSQLGenerator.escapeIdentifier(spaceTable) + " ("
                + "[c1*/someString withspace] char(50) not null," + "c2 varchar(20) not null,"
                + "c3 nchar(30) not null," + "c4 nvarchar(60) not null," + "c5 text not null," + "c6 ntext not null"
                + ")");
    }

    private static void createChar2Table() throws SQLException {
        stmt.execute("Create table " + AbstractSQLGenerator.escapeIdentifier(charTable2) + " ("
                + "table2c1 char(50) not null)");
    }

    private static void populateCharTable() throws SQLException {
        stmt.execute("insert into " + AbstractSQLGenerator.escapeIdentifier(charTable) + " values (" + "'Hello',"
                + "'Hello'," + "N'Hello'," + "N'Hello'," + "'Hello'," + "N'Hello'" + ")");
    }

    private static void selectChar() throws SQLException {
        String sql = "select * from " + AbstractSQLGenerator.escapeIdentifier(charTable) + " where " + "c1 = ? and "
                + "c2 = ? and " + "c3 = ? and " + "c4 = ? ";

        pstmt = connection.prepareStatement(sql);

        for (int i = 1; i <= 4; i++) {
            pstmt.setString(i, "Hello");
        }

        rs = pstmt.executeQuery();
    }

    private static void insertChar() throws SQLException {

        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(charTable) + " values( " + "?," + "?,"
                + "?," + "?," + "?," + "?" + ")";

        pstmt = connection.prepareStatement(sql);

        for (int i = 1; i <= 6; i++) {
            pstmt.setString(i, "simba tech");
        }

        pstmt.execute();
    }

    private static void updateChar() throws SQLException {

        String sql = "update " + AbstractSQLGenerator.escapeIdentifier(charTable) + " set " + "c1 = ?," + "c2 = ?,"
                + "c3 = ?," + "c4 = ?," + "c5 = ?," + "c6 = ?" + Constants.SEMI_COLON;

        pstmt = connection.prepareStatement(sql);

        for (int i = 1; i <= 6; i++) {
            pstmt.setString(i, "Simba!!!");
        }

        pstmt.execute();
    }

    private static void deleteChar() throws SQLException {

        String sql = "delete from " + AbstractSQLGenerator.escapeIdentifier(charTable) + " where " + "c1 = ? and "
                + "c2 = ? and " + "c3 = ? and " + "c4 = ? ";

        pstmt = connection.prepareStatement(sql);

        for (int i = 1; i <= 4; i++) {
            pstmt.setString(i, "Simba!!!");
        }

        pstmt.execute();
    }

    private static void createBinaryTable() throws SQLException {

        stmt.execute("Create table " + AbstractSQLGenerator.escapeIdentifier(binaryTable) + " ("
                + "c1 binary(100) not null," + "c2 varbinary(200) not null" + ")");
    }

    private static void populateBinaryTable() throws SQLException {

        stmt.execute("insert into " + AbstractSQLGenerator.escapeIdentifier(binaryTable) + " values ("
                + "convert(binary(50), 'Simba tech', 0), " + "convert(varbinary(50), 'Simba tech', 0)" + ")");
    }

    private static void selectBinary() throws SQLException {
        String sql = "select * from " + AbstractSQLGenerator.escapeIdentifier(binaryTable) + " where " + "c1 = ? and "
                + "c2 = ? ";

        pstmt = connection.prepareStatement(sql);

        for (int i = 1; i <= 2; i++) {
            pstmt.setString(i, "1");
        }

        rs = pstmt.executeQuery();
    }

    private static void insertBinary() throws SQLException {

        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(binaryTable) + " values( " + "?," + "?"
                + ")";

        pstmt = connection.prepareStatement(sql);

        String str = "simba tech";
        for (int i = 1; i <= 2; i++) {
            pstmt.setBytes(i, str.getBytes());
        }

        pstmt.execute();
    }

    private static void updateBinary() throws SQLException {

        String sql = "update " + AbstractSQLGenerator.escapeIdentifier(binaryTable) + " set " + "c1 = ?," + "c2 = ?"
                + Constants.SEMI_COLON;

        pstmt = connection.prepareStatement(sql);

        String str = "simbaaaaaaaa";
        for (int i = 1; i <= 2; i++) {
            pstmt.setBytes(i, str.getBytes());
        }

        pstmt.execute();
    }

    private static void deleteBinary() throws SQLException {

        String sql = "delete from " + AbstractSQLGenerator.escapeIdentifier(binaryTable) + " where " + "c1 = ? and "
                + "c2 = ?" + Constants.SEMI_COLON;

        pstmt = connection.prepareStatement(sql);

        for (int i = 1; i <= 2; i++) {
            pstmt.setString(i, "1");
        }

        pstmt.execute();
    }

    private static void createDateAndTimeTable() throws SQLException {

        stmt.execute("Create table " + AbstractSQLGenerator.escapeIdentifier(dateAndTimeTable) + " ("
                + "c1 date not null," + "c2 datetime not null," + "c3 datetime2 not null," + "c4 datetime2(5) not null,"
                + "c5 datetimeoffset not null," + "c6 datetimeoffset(5) not null," + "c7 smalldatetime not null,"
                + "c8 time not null," + "c9 time(5) not null" + ")");
    }

    private static void populateDateAndTimeTable() throws SQLException {
        stmt.execute("insert into " + AbstractSQLGenerator.escapeIdentifier(dateAndTimeTable) + " values ("
                + "'1991-10-23'," + "'1991-10-23 06:20:50'," + "'1991-10-23 07:20:50.123',"
                + "'1991-10-23 07:20:50.123'," + "'1991-10-23 08:20:50.123'," + "'1991-10-23 08:20:50.123',"
                + "'1991-10-23 09:20:50'," + "'10:20:50'," + "'10:20:50'" + ")");
    }

    private static void insertDateAndTime() throws SQLException {

        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(dateAndTimeTable) + " values( " + "?,"
                + "?," + "?," + "?," + "?," + "?," + "?," + "?," + "?" + ")";

        pstmt = connection.prepareStatement(sql);

        for (int i = 1; i <= 9; i++) {
            pstmt.setString(i, "1991-10-23");
        }

        pstmt.execute();
    }

    private static void updateDateAndTime() throws SQLException {

        String sql = "update " + AbstractSQLGenerator.escapeIdentifier(dateAndTimeTable) + " set " + "c1 = ?,"
                + "c2 = ?," + "c3 = ?," + "c4 = ?," + "c5 = ?," + "c6 = ?," + "c7 = ?," + "c8 = ?," + "c9 = ?"
                + Constants.SEMI_COLON;

        pstmt = connection.prepareStatement(sql);

        for (int i = 1; i <= 9; i++) {
            pstmt.setString(i, "1991-10-23");
        }

        pstmt.execute();
    }

    private static void deleteDateAndTime() throws SQLException {

        String sql = "delete from " + AbstractSQLGenerator.escapeIdentifier(dateAndTimeTable) + " where "
                + "c1 = ? and " + "c2 = ? and " + "c3 = ? and " + "c4 = ? and " + "c5 = ? and " + "c6 = ? and "
                + "c7 = ? and " + "c8 = ? and " + "c9 = ?" + Constants.SEMI_COLON;

        pstmt = connection.prepareStatement(sql);

        for (int i = 1; i <= 9; i++) {
            pstmt.setString(i, "1991-10-23");
        }

        pstmt.execute();
    }

    private static void selectDateAndTime() throws SQLException {
        String sql = "select * from " + AbstractSQLGenerator.escapeIdentifier(dateAndTimeTable) + " where "
                + "c1 = ? and " + "c2 = ? and " + "c3 = ? and " + "c4 = ? and " + "c5 = ? and " + "c6 = ? and "
                + "c7 = ? and " + "c8 = ? and " + "c9 = ? ";

        pstmt = connection.prepareStatement(sql);

        for (int i = 1; i <= 9; i++) {
            pstmt.setString(i, "1");
        }

        rs = pstmt.executeQuery();
    }

    private static void createTablesForCompexQueries() throws SQLException {
        stmt.executeUpdate("if object_id('" + TestUtils.escapeSingleQuotes(nameTable) + "','U') is not null"
                + " drop table " + AbstractSQLGenerator.escapeIdentifier(nameTable));

        stmt.executeUpdate("if object_id('" + TestUtils.escapeSingleQuotes(phoneNumberTable) + "','U') is not null"
                + " drop table " + AbstractSQLGenerator.escapeIdentifier(phoneNumberTable));

        stmt.executeUpdate("if object_id('" + TestUtils.escapeSingleQuotes(mergeNameDesTable) + "','U') is not null"
                + " drop table " + AbstractSQLGenerator.escapeIdentifier(mergeNameDesTable));

        String sql = "create table " + AbstractSQLGenerator.escapeIdentifier(nameTable) + " ("
        // + "ID int NOT NULL,"
                + "PlainID int not null," + "ID smallint NOT NULL," + "FirstName varchar(50) NOT NULL,"
                + "LastName nchar(60) NOT NULL" + ");";

        try {
            stmt.execute(sql);
        } catch (SQLException e) {
            fail(e.getMessage());
        }

        sql = "create table " + AbstractSQLGenerator.escapeIdentifier(phoneNumberTable) + " ("
                + "PlainID smallint not null," + "ID int NOT NULL," + "PhoneNumber bigint NOT NULL" + ");";

        try {
            stmt.execute(sql);
        } catch (SQLException e) {
            fail(e.getMessage());
        }

        sql = "create table " + AbstractSQLGenerator.escapeIdentifier(mergeNameDesTable) + " ("
        // + "ID int NOT NULL,"
                + "PlainID smallint not null," + "ID int NULL," + "FirstName char(30) NULL,"
                + "LastName varchar(50) NULL" + ");";

        try {
            stmt.execute(sql);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    private static void populateTablesForCompexQueries() throws SQLException {
        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(nameTable) + " values " + "(?,?,?,?),"
                + "(?,?,?,?)," + "(?,?,?,?)" + "";
        pstmt = connection.prepareStatement(sql);
        int id = 1;
        for (int i = 0; i < 5; i++) {
            pstmt.setInt(1, id);
            pstmt.setInt(2, id);
            pstmt.setString(3, "QWERTYUIOP");
            pstmt.setString(4, "ASDFGHJKL");
            id++;

            pstmt.setInt(5, id);
            pstmt.setInt(6, id);
            pstmt.setString(7, "QWE");
            pstmt.setString(8, "ASD");
            id++;

            pstmt.setInt(9, id);
            pstmt.setInt(10, id);
            pstmt.setString(11, "QAZ");
            pstmt.setString(12, "WSX");
            pstmt.execute();
            id++;
        }
        pstmt.close();

        sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(phoneNumberTable) + " values " + "(?,?,?),"
                + "(?,?,?)," + "(?,?,?)" + "";
        pstmt = connection.prepareStatement(sql);
        id = 1;
        for (int i = 0; i < 5; i++) {
            pstmt.setInt(1, id);
            pstmt.setInt(2, id);
            pstmt.setLong(3, 1234567L);
            id++;

            pstmt.setInt(4, id);
            pstmt.setInt(5, id);
            pstmt.setLong(6, 7654321L);
            id++;

            pstmt.setInt(7, id);
            pstmt.setInt(8, id);
            pstmt.setLong(9, 1231231L);
            pstmt.execute();
            id++;
        }
        pstmt.close();

        sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(mergeNameDesTable) + " (PlainID) values " + "(?),"
                + "(?)," + "(?)" + "";
        pstmt = connection.prepareStatement(sql);
        id = 1;
        for (int i = 0; i < 5; i++) {
            pstmt.setInt(1, id);
            id++;

            pstmt.setInt(2, id);
            id++;

            pstmt.setInt(3, id);
            pstmt.execute();
            id++;
        }
        pstmt.close();
    }

    /**
     * Test subquery
     * 
     * @throws SQLException
     */
    @Test
    @DisplayName("SubQuery")
    public void testSubquery() throws SQLException {
        if (version >= SQL_SERVER_2012_VERSION) {
            String sql = "SELECT FirstName,LastName" + " FROM " + AbstractSQLGenerator.escapeIdentifier(nameTable)
                    + " a WHERE a.ID IN " + " (SELECT ID" + " FROM "
                    + AbstractSQLGenerator.escapeIdentifier(phoneNumberTable)
                    + " WHERE PhoneNumber = ? and ID = ? and PlainID = ?" + ")";

            pstmt = connection.prepareStatement(sql);

            ParameterMetaData pmd = null;

            try {
                pmd = pstmt.getParameterMetaData();
                assertEquals(pmd.getParameterCount(), 3, TestResource.getResource("R_paramNotRecognized"));
            } catch (Exception e) {
                fail(e.getMessage());
            }

            compareParameterMetaData(pmd, 1, "java.lang.Long", -5, "BIGINT", 19, 0);
            compareParameterMetaData(pmd, 2, "java.lang.Integer", 4, "int", 10, 0);
            compareParameterMetaData(pmd, 3, "java.lang.Short", 5, "smallint", 5, 0);
        }
    }

    /**
     * Test join
     * 
     * @throws SQLException
     */
    @Test
    @DisplayName("Join Queries")
    public void testJoin() throws SQLException {
        if (version >= SQL_SERVER_2012_VERSION) {
            String sql = String.format(
                    "select %s.FirstName, %s.LastName, %s.PhoneNumber" + " from %s join %s on %s.PlainID = %s.PlainID"
                            + " where %s.ID = ? and %s.PlainID = ?",
                    AbstractSQLGenerator.escapeIdentifier(nameTable), AbstractSQLGenerator.escapeIdentifier(nameTable),
                    AbstractSQLGenerator.escapeIdentifier(phoneNumberTable),
                    AbstractSQLGenerator.escapeIdentifier(nameTable),
                    AbstractSQLGenerator.escapeIdentifier(phoneNumberTable),
                    AbstractSQLGenerator.escapeIdentifier(nameTable),
                    AbstractSQLGenerator.escapeIdentifier(phoneNumberTable),
                    AbstractSQLGenerator.escapeIdentifier(phoneNumberTable),
                    AbstractSQLGenerator.escapeIdentifier(phoneNumberTable));

            pstmt = connection.prepareStatement(sql);

            ParameterMetaData pmd = null;

            try {
                pmd = pstmt.getParameterMetaData();
                assertEquals(pmd.getParameterCount(), 2, TestResource.getResource("R_paramNotRecognized"));
            } catch (Exception e) {
                fail(e.getMessage());
            }

            compareParameterMetaData(pmd, 1, "java.lang.Integer", 4, "int", 10, 0);
            compareParameterMetaData(pmd, 2, "java.lang.Short", 5, "smallint", 5, 0);
        }
    }

    /**
     * Test merge
     * 
     * @throws SQLException
     */
    @Test
    @DisplayName("Merge Queries")
    public void testMerge() throws SQLException {
        // FMTOnly currently only supports SELECT/INSERT/DELETE/UPDATE
        if (version >= SQL_SERVER_2012_VERSION && !((SQLServerConnection)connection).getUseFmtOnly()) {
            String sql = "merge " + AbstractSQLGenerator.escapeIdentifier(mergeNameDesTable) + " as T" + " using "
                    + AbstractSQLGenerator.escapeIdentifier(nameTable) + " as S" + " on T.PlainID=S.PlainID"
                    + " when matched" + " then update set T.firstName = ?, T.lastName = ?;";

            pstmt = connection.prepareStatement(sql);

            pstmt.setString(1, "hello");
            pstmt.setString(2, "world");
            pstmt.execute();

            ParameterMetaData pmd = null;

            try {
                pmd = pstmt.getParameterMetaData();
                assertEquals(pmd.getParameterCount(), 2, TestResource.getResource("R_paramNotRecognized"));
            } catch (Exception e) {
                fail(e.getMessage());
            }

            compareParameterMetaData(pmd, 1, "java.lang.String", 1, "CHAR", 30, 0);
            compareParameterMetaData(pmd, 2, "java.lang.String", 12, "VARCHAR", 50, 0);
        }
    }

    private static void createMultipleTypesTable() throws SQLException {

        stmt.execute("Create table " + AbstractSQLGenerator.escapeIdentifier(multipleTypesTable) + " ("
                + "c1n decimal not null," + "c2n decimal(10,5) not null," + "c3n numeric not null,"
                + "c4n numeric(8,4) not null," + "c5n float not null," + "c6n float(10) not null,"
                + "c7n real not null," + "c8n int not null," + "c9n bigint not null," + "c10n smallint not null,"
                + "c11n tinyint not null," + "c12n money not null," + "c13n smallmoney not null,"

                + "c1c char(50) not null," + "c2c varchar(20) not null," + "c3c nchar(30) not null,"
                + "c4c nvarchar(60) not null," + "c5c text not null," + "c6c ntext not null,"

                + "c1 binary(100) not null," + "c2 varbinary(200) not null,"

                + "c1d date not null," + "c2d datetime not null," + "c3d datetime2 not null,"
                + "c4d datetime2(5) not null," + "c5d datetimeoffset not null," + "c6d datetimeoffset(5) not null,"
                + "c7d smalldatetime not null," + "c8d time not null," + "c9d time(5) not null" + ")");
    }

    private static void testInsertMultipleTypes() throws SQLException {

        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(multipleTypesTable) + " values( "
                + "?,?,?,?,?,?,?,?,?,?,?,?,?," + "?,?,?,?,?,?," + "?,?," + "?,?,?,?,?,?,?,?,?" + ")";

        pstmt = connection.prepareStatement(sql);

        for (int i = 1; i <= 13; i++) {
            pstmt.setString(i, "1");
        }
        for (int i = 14; i <= 19; i++) {
            pstmt.setString(i, "simba tech");
        }
        String str = "simba tech";
        for (int i = 20; i <= 21; i++) {
            pstmt.setBytes(i, str.getBytes());
        }
        for (int i = 22; i <= 30; i++) {
            pstmt.setString(i, "1991-10-23");
        }

        pstmt.execute();

        ParameterMetaData pmd = null;
        try {
            pmd = pstmt.getParameterMetaData();
            assertEquals(pmd.getParameterCount(), 30, TestResource.getResource("R_paramNotRecognized"));
        } catch (Exception e) {
            fail(e.getMessage());
        }

        compareParameterMetaData(pmd, 1, "java.math.BigDecimal", 3, "decimal", 18, 0);
        compareParameterMetaData(pmd, 2, "java.math.BigDecimal", 3, "decimal", 10, 5);
        compareParameterMetaData(pmd, 3, "java.math.BigDecimal", 2, "numeric", 18, 0);
        compareParameterMetaData(pmd, 4, "java.math.BigDecimal", 2, "numeric", 8, 4);
        compareParameterMetaData(pmd, 5, "java.lang.Double", 8, "float", 15, 0);
        compareParameterMetaData(pmd, 6, "java.lang.Float", 7, "real", 7, 0);
        compareParameterMetaData(pmd, 7, "java.lang.Float", 7, "real", 7, 0);
        if (version >= SQL_SERVER_2012_VERSION) {
            compareParameterMetaData(pmd, 8, "java.lang.Integer", 4, "int", 10, 0);
        } else {
            compareParameterMetaData(pmd, 8, "java.lang.Integer", 4, "int", 10, 0);
        }
        compareParameterMetaData(pmd, 9, "java.lang.Long", -5, "bigint", 19, 0);
        compareParameterMetaData(pmd, 10, "java.lang.Short", 5, "smallint", 5, 0);
        compareParameterMetaData(pmd, 11, "java.lang.Short", -6, "tinyint", 3, 0);
        compareParameterMetaData(pmd, 12, "java.math.BigDecimal", 3, "money", 19, 4);
        compareParameterMetaData(pmd, 13, "java.math.BigDecimal", 3, "smallmoney", 10, 4);

        compareParameterMetaData(pmd, 14, "java.lang.String", 1, "char", 50, 0);
        compareParameterMetaData(pmd, 15, "java.lang.String", 12, "varchar", 20, 0);
        compareParameterMetaData(pmd, 16, "java.lang.String", -15, "nchar", 30, 0);
        compareParameterMetaData(pmd, 17, "java.lang.String", -9, "nvarchar", 60, 0);
        compareParameterMetaData(pmd, 18, "java.lang.String", -1, "text", 2147483647, 0);
        compareParameterMetaData(pmd, 19, "java.lang.String", -16, "ntext", 1073741823, 0);

        compareParameterMetaData(pmd, 20, "[B", -2, "binary", 100, 0);
        compareParameterMetaData(pmd, 21, "[B", -3, "varbinary", 200, 0);

        compareParameterMetaData(pmd, 22, "java.sql.Date", 91, "date", 10, 0);
        compareParameterMetaData(pmd, 23, "java.sql.Timestamp", 93, "datetime", 23, 3);
        compareParameterMetaData(pmd, 24, "java.sql.Timestamp", 93, "datetime2", 27, 7);
        compareParameterMetaData(pmd, 25, "java.sql.Timestamp", 93, "datetime2", 25, 5);
        compareParameterMetaData(pmd, 26, "microsoft.sql.DateTimeOffset", -155, "datetimeoffset", 34, 7);
        compareParameterMetaData(pmd, 27, "microsoft.sql.DateTimeOffset", -155, "datetimeoffset", 32, 5);
        compareParameterMetaData(pmd, 28, "java.sql.Timestamp", 93, "smalldatetime", 16, 0);
        compareParameterMetaData(pmd, 29, "java.sql.Time", 92, "time", 16, 7);
        compareParameterMetaData(pmd, 30, "java.sql.Time", 92, "time", 14, 5);
    }

    @Test
    @DisplayName("testNoParameter")
    public void testNoParameter() throws SQLException {
        String sql = "select * from " + AbstractSQLGenerator.escapeIdentifier(multipleTypesTable);

        pstmt = connection.prepareStatement(sql);

        rs = pstmt.executeQuery();

        ParameterMetaData pmd = null;
        try {
            pmd = pstmt.getParameterMetaData();
            assertEquals(pmd.getParameterCount(), 0, TestResource.getResource("R_paramNotRecognized"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private static void testMixedWithHardcodedValues() throws SQLException {

        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(multipleTypesTable) + " values( "
                + "1,?,?,1,?,?,?,1,?,?,?,1,1," + "?,'simba tech','simba tech',?,?,?," + "?,?,"
                + "?,'1991-10-23',?,?,?,'1991-10-23',?,?,?" + ")";

        pstmt = connection.prepareStatement(sql);

        for (int i = 1; i <= 8; i++) {
            pstmt.setString(i, "1");
        }
        for (int i = 9; i <= 12; i++) {
            pstmt.setString(i, "simba tech");
        }
        String str = "simba tech";
        for (int i = 13; i <= 14; i++) {
            pstmt.setBytes(i, str.getBytes());
        }
        for (int i = 15; i <= 21; i++) {
            pstmt.setString(i, "1991-10-23");
        }

        pstmt.execute();

        ParameterMetaData pmd = null;
        try {
            pmd = pstmt.getParameterMetaData();

            assertEquals(pmd.getParameterCount(), 21, TestResource.getResource("R_paramNotRecognized"));
        } catch (Exception e) {
            fail(e.getMessage());
        }

        compareParameterMetaData(pmd, 1, "java.math.BigDecimal", 3, "decimal", 10, 5);
        compareParameterMetaData(pmd, 2, "java.math.BigDecimal", 2, "numeric", 18, 0);
        compareParameterMetaData(pmd, 3, "java.lang.Double", 8, "float", 15, 0);
        compareParameterMetaData(pmd, 4, "java.lang.Float", 7, "real", 7, 0);
        compareParameterMetaData(pmd, 5, "java.lang.Float", 7, "real", 7, 0);
        compareParameterMetaData(pmd, 6, "java.lang.Long", -5, "bigint", 19, 0);
        compareParameterMetaData(pmd, 7, "java.lang.Short", 5, "smallint", 5, 0);
        compareParameterMetaData(pmd, 8, "java.lang.Short", -6, "tinyint", 3, 0);

        compareParameterMetaData(pmd, 9, "java.lang.String", 1, "char", 50, 0);
        compareParameterMetaData(pmd, 10, "java.lang.String", -9, "nvarchar", 60, 0);
        compareParameterMetaData(pmd, 11, "java.lang.String", -1, "text", 2147483647, 0);
        compareParameterMetaData(pmd, 12, "java.lang.String", -16, "ntext", 1073741823, 0);

        compareParameterMetaData(pmd, 13, "[B", -2, "binary", 100, 0);
        compareParameterMetaData(pmd, 14, "[B", -3, "varbinary", 200, 0);

        compareParameterMetaData(pmd, 15, "java.sql.Date", 91, "date", 10, 0);
        compareParameterMetaData(pmd, 16, "java.sql.Timestamp", 93, "datetime2", 27, 7);
        compareParameterMetaData(pmd, 17, "java.sql.Timestamp", 93, "datetime2", 25, 5);
        compareParameterMetaData(pmd, 18, "microsoft.sql.DateTimeOffset", -155, "datetimeoffset", 34, 7);
        compareParameterMetaData(pmd, 19, "java.sql.Timestamp", 93, "smalldatetime", 16, 0);
        compareParameterMetaData(pmd, 20, "java.sql.Time", 92, "time", 16, 7);
        compareParameterMetaData(pmd, 21, "java.sql.Time", 92, "time", 14, 5);
    }

    /**
     * Test Orderby
     * 
     * @throws SQLException
     */
    @Test
    @DisplayName("Test OrderBy")
    public void testOrderBy() throws SQLException {
        String sql = "SELECT FirstName,LastName" + " FROM " + AbstractSQLGenerator.escapeIdentifier(nameTable)
                + " WHERE FirstName = ? and LastName = ? and PlainID = ? and ID = ? " + " ORDER BY ID ASC";

        pstmt = connection.prepareStatement(sql);

        ParameterMetaData pmd = null;

        try {
            pmd = pstmt.getParameterMetaData();
            assertEquals(pmd.getParameterCount(), 4, TestResource.getResource("R_paramNotRecognized"));
        } catch (Exception e) {
            fail(e.getMessage());
        }

        compareParameterMetaData(pmd, 1, "java.lang.String", 12, "varchar", 50, 0);
        compareParameterMetaData(pmd, 2, "java.lang.String", -15, "nchar", 60, 0);
        compareParameterMetaData(pmd, 3, "java.lang.Integer", 4, "int", 10, 0);
        compareParameterMetaData(pmd, 4, "java.lang.Short", 5, "smallint", 5, 0);

    }

    /**
     * Test Groupby
     * 
     * @throws SQLException
     */
    @Test
    @DisplayName("Test GroupBy")
    private void testGroupBy() throws SQLException {
        String sql = "SELECT FirstName,COUNT(LastName)" + " FROM " + AbstractSQLGenerator.escapeIdentifier(nameTable)
                + " WHERE FirstName = ? and LastName = ? and PlainID = ? and ID = ? " + " group by Firstname";

        pstmt = connection.prepareStatement(sql);

        ParameterMetaData pmd = null;

        try {
            pmd = pstmt.getParameterMetaData();
            assertEquals(pmd.getParameterCount(), 4, TestResource.getResource("R_paramNotRecognized"));
        } catch (Exception e) {
            fail(e.getMessage());
        }

        compareParameterMetaData(pmd, 1, "java.lang.String", 12, "varchar", 50, 0);
        compareParameterMetaData(pmd, 2, "java.lang.String", -15, "nchar", 60, 0);
        compareParameterMetaData(pmd, 3, "java.lang.Integer", 4, "int", 10, 0);
        compareParameterMetaData(pmd, 4, "java.lang.Short", 5, "smallint", 5, 0);

    }

    /**
     * Test Lower
     * 
     * @throws SQLException
     */
    @Test
    public void testLower() throws SQLException {
        String sql = "SELECT FirstName,LOWER(LastName)" + " FROM " + AbstractSQLGenerator.escapeIdentifier(nameTable)
                + " WHERE FirstName = ? and LastName = ? and PlainID = ? and ID = ? ";

        pstmt = connection.prepareStatement(sql);

        ParameterMetaData pmd = null;

        try {
            pmd = pstmt.getParameterMetaData();

            assertEquals(pmd.getParameterCount(), 4, TestResource.getResource("R_paramNotRecognized"));
        } catch (Exception e) {
            fail(e.getMessage());
        }

        compareParameterMetaData(pmd, 1, "java.lang.String", 12, "varchar", 50, 0);
        compareParameterMetaData(pmd, 2, "java.lang.String", -15, "nchar", 60, 0);
        compareParameterMetaData(pmd, 3, "java.lang.Integer", 4, "int", 10, 0);
        compareParameterMetaData(pmd, 4, "java.lang.Short", 5, "smallint", 5, 0);

    }

    /**
     * Test Power
     * 
     * @throws SQLException
     */
    @Test
    public void testPower() throws SQLException {
        String sql = "SELECT POWER(ID,2)" + " FROM " + AbstractSQLGenerator.escapeIdentifier(nameTable)
                + " WHERE FirstName = ? and LastName = ? and PlainID = ? and ID = ? ";

        pstmt = connection.prepareStatement(sql);

        ParameterMetaData pmd = null;

        try {
            pmd = pstmt.getParameterMetaData();
            assertEquals(pmd.getParameterCount(), 4, TestResource.getResource("R_paramNotRecognized"));
        } catch (Exception e) {
            fail(e.getMessage());
        }

        compareParameterMetaData(pmd, 1, "java.lang.String", 12, "varchar", 50, 0);
        compareParameterMetaData(pmd, 2, "java.lang.String", -15, "nchar", 60, 0);
        compareParameterMetaData(pmd, 3, "java.lang.Integer", 4, "int", 10, 0);
        compareParameterMetaData(pmd, 4, "java.lang.Short", 5, "smallint", 5, 0);

    }

    /**
     * All in one queries
     * 
     * @throws SQLException
     */
    @Test
    @DisplayName("All In One Queries")
    public void testAllInOneQuery() throws SQLException {
        if (version >= SQL_SERVER_2012_VERSION) {

            String sql = "select lower(FirstName), count(lastName) from "
                    + AbstractSQLGenerator.escapeIdentifier(nameTable) + " a where a.ID = ? and FirstName in" + "("
                    + " select " + AbstractSQLGenerator.escapeIdentifier(nameTable) + ".FirstName from "
                    + AbstractSQLGenerator.escapeIdentifier(nameTable) + " join "
                    + AbstractSQLGenerator.escapeIdentifier(phoneNumberTable) + " on "
                    + AbstractSQLGenerator.escapeIdentifier(nameTable) + ".ID = "
                    + AbstractSQLGenerator.escapeIdentifier(phoneNumberTable) + ".ID" + " where "
                    + AbstractSQLGenerator.escapeIdentifier(nameTable) + ".ID = ? and "
                    + AbstractSQLGenerator.escapeIdentifier(phoneNumberTable) + ".ID = ?" + ")" + " group by FirstName "
                    + " order by FirstName ASC";

            pstmt = connection.prepareStatement(sql);

            ParameterMetaData pmd = null;

            try {
                pmd = pstmt.getParameterMetaData();

                assertEquals(pmd.getParameterCount(), 3, TestResource.getResource("R_paramNotRecognized"));
            } catch (Exception e) {
                fail(e.getMessage());
            }

            compareParameterMetaData(pmd, 1, "java.lang.Short", 5, "smallint", 5, 0);
            compareParameterMetaData(pmd, 2, "java.lang.Short", 5, "smallint", 5, 0);
            compareParameterMetaData(pmd, 3, "java.lang.Integer", 4, "int", 10, 0);
        }
    }

    /**
     * test query with simple multiple line comments
     * 
     * @throws SQLException
     */
    @Test
    public void testQueryWithMultipleLineComments1() throws SQLException {
        pstmt = connection.prepareStatement("/*te\nst*//*test*/select top 100 c1 from "
                + AbstractSQLGenerator.escapeIdentifier(charTable) + " where c1 = ?");
        pstmt.setString(1, "abc");

        try {
            pstmt.getParameterMetaData();
            pstmt.executeQuery();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * test query with complex multiple line comments
     * 
     * @throws SQLException
     */
    @Test
    public void testQueryWithMultipleLineComments2() throws SQLException {
        pstmt = connection
                .prepareStatement("/*/*te\nst*/ te/*test*/st /*te\nst*/*//*te/*test*/st*/select top 100 c1 from "
                        + AbstractSQLGenerator.escapeIdentifier(charTable) + " where c1 = ?");
        pstmt.setString(1, "abc");

        try {
            pstmt.getParameterMetaData();
            pstmt.executeQuery();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * test insertion query with multiple line comments
     * 
     * @throws SQLException
     */
    @Test
    public void testQueryWithMultipleLineCommentsInsert() throws SQLException {
        pstmt = connection.prepareStatement("/*te\nst*//*test*/insert /*test*/into "
                + AbstractSQLGenerator.escapeIdentifier(charTable) + " (c1) VALUES(?)");

        try {
            pstmt.getParameterMetaData();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * test update query with multiple line comments
     * 
     * @throws SQLException
     */
    @Test
    public void testQueryWithMultipleLineCommentsUpdate() throws SQLException {
        pstmt = connection.prepareStatement("/*te\nst*//*test*/update /*test*/"
                + AbstractSQLGenerator.escapeIdentifier(charTable) + " set c1=123 where c1=?");

        try {
            pstmt.getParameterMetaData();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * test deletion query with multiple line comments
     * 
     * @throws SQLException
     */
    @Test
    public void testQueryWithMultipleLineCommentsDeletion() throws SQLException {
        pstmt = connection.prepareStatement("/*te\nst*//*test*/delete /*test*/from "
                + AbstractSQLGenerator.escapeIdentifier(charTable) + " where c1=?");

        try {
            pstmt.getParameterMetaData();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * test query with single line comments
     * 
     * @throws SQLException
     */
    @Test
    public void testQueryWithSingleLineComments1() throws SQLException {
        pstmt = connection.prepareStatement("-- #test \n select top 100 c1 from "
                + AbstractSQLGenerator.escapeIdentifier(charTable) + " where c1 = ?");
        pstmt.setString(1, "abc");

        try {
            pstmt.getParameterMetaData();
            pstmt.executeQuery();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * test query with single line comments
     * 
     * @throws SQLException
     */
    @Test
    public void testQueryWithSingleLineComments2() throws SQLException {
        pstmt = connection.prepareStatement("--#test\nselect top 100 c1 from "
                + AbstractSQLGenerator.escapeIdentifier(charTable) + " where c1 = ?");
        pstmt.setString(1, "abc");

        try {
            pstmt.getParameterMetaData();
            pstmt.executeQuery();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * test query with single line comment
     * 
     * @throws SQLException
     */
    @Test
    public void testQueryWithSingleLineComments3() throws SQLException {
        pstmt = connection.prepareStatement(
                "select top 100 c1\nfrom " + AbstractSQLGenerator.escapeIdentifier(charTable) + " where c1 = ?");
        pstmt.setString(1, "abc");

        try {
            pstmt.getParameterMetaData();
            pstmt.executeQuery();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * test insertion query with single line comments
     * 
     * @throws SQLException
     */
    @Test
    public void testQueryWithSingleLineCommentsInsert() throws SQLException {
        pstmt = connection.prepareStatement(
                "--#test\ninsert /*test*/into " + AbstractSQLGenerator.escapeIdentifier(charTable) + " (c1) VALUES(?)");

        try {
            pstmt.getParameterMetaData();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * test update query with single line comments
     * 
     * @throws SQLException
     */
    @Test
    public void testQueryWithSingleLineCommentsUpdate() throws SQLException {
        pstmt = connection.prepareStatement("--#test\nupdate /*test*/"
                + AbstractSQLGenerator.escapeIdentifier(charTable) + " set c1=123 where c1=?");

        try {
            pstmt.getParameterMetaData();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * test deletion query with single line comments
     * 
     * @throws SQLException
     */
    @Test
    public void testQueryWithSingleLineCommentsDeletion() throws SQLException {
        pstmt = connection.prepareStatement(
                "--#test\ndelete /*test*/from " + AbstractSQLGenerator.escapeIdentifier(charTable) + " where c1=?");

        try {
            pstmt.getParameterMetaData();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * test column name with end comment mark and space
     * 
     * @throws SQLException
     */
    @Test
    public void testQueryWithSpaceAndEndCommentMarkInColumnName() throws SQLException {
        pstmt = connection.prepareStatement(
                "SELECT [c1*/someString withspace] from " + AbstractSQLGenerator.escapeIdentifier(spaceTable));

        try {
            pstmt.getParameterMetaData();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * test getting parameter count with a complex query with multiple table
     * 
     * @throws SQLException
     */
    @Test
    public void testComplexQueryWithMultipleTables() throws SQLException {
        pstmt = connection.prepareStatement("insert into " + AbstractSQLGenerator.escapeIdentifier(charTable)
                + " (c1) select ? where not exists (select * from " + AbstractSQLGenerator.escapeIdentifier(charTable2)
                + " where table2c1 = ?)");

        try {
            SQLServerParameterMetaData pMD = (SQLServerParameterMetaData) pstmt.getParameterMetaData();
            int parameterCount = pMD.getParameterCount();

            assertTrue(2 == parameterCount, "Parameter Count should be 2.");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Cleanup
     * 
     * @throws SQLException
     */
    @AfterAll
    public static void dropTables() throws SQLException {
        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(nameTable), stmt);
        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(phoneNumberTable), stmt);
        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(mergeNameDesTable), stmt);
        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(numericTable), stmt);
        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(charTable), stmt);
        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(charTable2), stmt);
        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(binaryTable), stmt);
        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(dateAndTimeTable), stmt);
        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(multipleTypesTable), stmt);
        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(spaceTable), stmt);

        if (null != rs) {
            rs.close();
        }
        if (null != stmt) {
            stmt.close();
        }
        if (null != pstmt) {
            pstmt.close();
        }
        if (null != connection) {
            connection.close();
        }
    }
}
