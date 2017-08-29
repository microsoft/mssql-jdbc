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
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Utils;

/**
 * Test Bulkcopy with sql_variant datatype, testing all underlying supported datatypes
 *
 */
@RunWith(JUnitPlatform.class)
public class BulkCopyWithSqlVariantTest extends AbstractTest {

    static SQLServerConnection con = null;
    static Statement stmt = null;
    static String tableName = "sqlVariantTestSrcTable";
    static String destTableName = "sqlVariantDestTable";
    static SQLServerResultSet rs = null;

    /**
     * Test integer value
     * 
     * @throws SQLException
     */
    @Test
    public void bulkCopyTestInt() throws SQLException {
        int col1Value = 5;
        beforeEachSetup("int", col1Value);
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);
        bulkCopy.close();

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals(rs.getInt(1), 5);
        }
    }

    /**
     * Test smallInt value
     * 
     * @throws SQLException
     */
    @Test
    public void bulkCopyTestSmallInt() throws SQLException {
        int col1Value = 5;
        beforeEachSetup("smallint", col1Value);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);
        bulkCopy.close();

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals(rs.getShort(1), 5);
        }
        bulkCopy.close();
    }

    /**
     * Test tinyInt value
     * 
     * @throws SQLException
     */
    @Test
    public void bulkCopyTestTinyint() throws SQLException {
        int col1Value = 5;
        beforeEachSetup("tinyint", col1Value);
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);
        bulkCopy.close();

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals(rs.getByte(1), 5);
        }
        bulkCopy.close();
    }

    /**
     * test Bigint value
     * 
     * @throws SQLException
     */
    @Test
    public void bulkCopyTestBigint() throws SQLException {
        int col1Value = 5;
        beforeEachSetup("bigint", col1Value);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);
        bulkCopy.close();

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals(rs.getLong(1), col1Value);
        }
    }

    /**
     * test float value
     * 
     * @throws SQLException
     */
    @Test
    public void bulkCopyTestFloat() throws SQLException {
        int col1Value = 5;
        beforeEachSetup("float", col1Value);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);
        bulkCopy.close();

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals(rs.getDouble(1), col1Value);
        }
    }

    /**
     * test real value
     * 
     * @throws SQLException
     */
    @Test
    public void bulkCopyTestReal() throws SQLException {
        int col1Value = 5;
        beforeEachSetup("real", col1Value);
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);
        bulkCopy.close();

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals(rs.getFloat(1), col1Value);
        }

    }

    /**
     * test money value
     * 
     * @throws SQLException
     */
    @Test
    public void bulkCopyTestMoney() throws SQLException {
        String col1Value = "126.1230";
        beforeEachSetup("money", col1Value);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);
        bulkCopy.close();

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals(rs.getMoney(1), new BigDecimal(col1Value));
        }

    }

    /**
     * test smallmoney
     * 
     * @throws SQLException
     */
    @Test
    public void bulkCopyTestSmallmoney() throws SQLException {
        String col1Value = "126.1230";
        String destTableName = "dest_sqlVariant";
        Utils.dropTableIfExists(tableName, stmt);
        Utils.dropTableIfExists(destTableName, stmt);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1) values (CAST (" + col1Value + " AS " + "smallmoney" + ") )");
        stmt.executeUpdate("create table " + destTableName + " (col1 sql_variant)");

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);
        bulkCopy.close();

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals(rs.getSmallMoney(1), new BigDecimal(col1Value));
        }

    }

    /**
     * test date value
     * 
     * @throws SQLException
     */
    @Test
    public void bulkCopyTestDate() throws SQLException {
        String col1Value = "2015-05-05";
        beforeEachSetup("date", "'" + col1Value + "'");

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);
        bulkCopy.close();

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals("" + rs.getDate(1), col1Value);
        }

    }

    /**
     * Test bulkcoping two column with sql_variant datatype
     * 
     * @throws SQLException
     */
    @Test
    public void bulkCopyTestTwoCols() throws SQLException {
        String col1Value = "2015-05-05";
        String col2Value = "126.1230";
        String destTableName = "dest_sqlVariant";
        Utils.dropTableIfExists(tableName, stmt);
        Utils.dropTableIfExists(destTableName, stmt);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant, col2 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1, col2) values (CAST ('" + col1Value + "' AS " + "date" + ")" + ",CAST (" + col2Value
                + " AS " + "smallmoney" + ")   )");
        stmt.executeUpdate("create table " + destTableName + " (col1 sql_variant, col2 sql_variant)");

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);
        bulkCopy.close();

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals("" + rs.getDate(1), col1Value);
            assertEquals(rs.getSmallMoney(2), new BigDecimal(col2Value));
        }

    }

    /**
     * test time with scale value
     * 
     * @throws SQLException
     */
    @Test
    public void bulkCopyTestTimeWithScale() throws SQLException {
        String col1Value = "'12:26:27.1452367'";
        beforeEachSetup("time(2)", col1Value);
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);
        bulkCopy.close();

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals("" + rs.getString(1), "12:26:27.15");  // getTime does not work
        }

    }

    /**
     * test char value
     * 
     * @throws SQLException
     */
    @Test
    public void bulkCopyTestChar() throws SQLException {
        String col1Value = "'sample'";

        beforeEachSetup("char", col1Value);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);
        bulkCopy.close();

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals("'" + rs.getString(1).trim() + "'", col1Value); // adds space between
        }

    }

    /**
     * test nchar value
     * 
     * @throws SQLException
     */
    @Test
    public void bulkCopyTestNchar() throws SQLException {
        String col1Value = "'a'";

        beforeEachSetup("nchar", col1Value);
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);
        bulkCopy.close();

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals("'" + rs.getNString(1).trim() + "'", col1Value);
        }

    }

    /**
     * test varchar value
     * 
     * @throws SQLException
     */
    @Test
    public void bulkCopyTestVarchar() throws SQLException {
        String col1Value = "'hello'";

        beforeEachSetup("varchar", col1Value);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);
        bulkCopy.close();

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals("'" + rs.getString(1).trim() + "'", col1Value);
        }

    }

    /**
     * test nvarchar value
     * 
     * @throws SQLException
     */
    @Test
    public void bulkCopyTestNvarchar() throws SQLException {
        String col1Value = "'hello'";
        beforeEachSetup("nvarchar", col1Value);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);
        bulkCopy.close();

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals("'" + rs.getString(1).trim() + "'", col1Value);
        }

    }

    /**
     * test Binary value
     * 
     * @throws SQLException
     */
    @Test
    public void bulkCopyTestBinary20() throws SQLException {
        String col1Value = "hello";
        beforeEachSetup("binary(20)", "'" + col1Value + "'");

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);
        bulkCopy.close();

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertTrue(Utils.parseByte(rs.getBytes(1), col1Value.getBytes()));
        }
    }

    /**
     * test varbinary value
     * 
     * @throws SQLException
     */
    @Test
    public void bulkCopyTestVarbinary20() throws SQLException {
        String col1Value = "hello";

        beforeEachSetup("varbinary(20)", "'" + col1Value + "'");
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);
        bulkCopy.close();

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertTrue(Utils.parseByte(rs.getBytes(1), col1Value.getBytes()));
        }
    }

    /**
     * test varbinary8000
     * 
     * @throws SQLException
     */
    @Test
    public void bulkCopyTestVarbinary8000() throws SQLException {
        String col1Value = "hello";
        beforeEachSetup("binary(8000)", "'" + col1Value + "'");
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);
        bulkCopy.close();

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertTrue(Utils.parseByte(rs.getBytes(1), col1Value.getBytes()));
        }
    }

    /**
     * test null value for underlying bit data type
     * 
     * @throws SQLException
     */
    @Test // TODO: check bitnull
    public void bulkCopyTestBitNull() throws SQLException {
        beforeEachSetup("bit", null);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);
        bulkCopy.close();

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals(rs.getBoolean(1), false);
        }
    }

    /**
     * test bit value
     * 
     * @throws SQLException
     */
    @Test
    public void bulkCopyTestBit() throws SQLException {
        int col1Value = 5000;
        beforeEachSetup("bit", col1Value);
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);
        bulkCopy.close();

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals(rs.getBoolean(1), true);
        }
    }

    /**
     * test datetime value
     * 
     * @throws SQLException
     */
    @Test
    public void bulkCopyTestDatetime() throws SQLException {
        String col1Value = "2015-05-08 12:26:24.0";
        beforeEachSetup("datetime", "'" + col1Value + "'");

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);
        bulkCopy.close();

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals("" + rs.getDateTime(1), col1Value);

        }

    }

    /**
     * test smalldatetime
     * 
     * @throws SQLException
     */
    @Test
    public void bulkCopyTestSmalldatetime() throws SQLException {
        String col1Value = "2015-05-08 12:26:24";
        beforeEachSetup("smalldatetime", "'" + col1Value + "'");

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);
        bulkCopy.close();

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals("" + rs.getSmallDateTime(1), "2015-05-08 12:26:00.0");
        }

    }

    /**
     * test datetime2
     * 
     * @throws SQLException
     */
    @Test
    public void bulkCopyTestDatetime2() throws SQLException {
        String col1Value = "2015-05-08 12:26:24.12645";
        beforeEachSetup("datetime2(2)", "'" + col1Value + "'");

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);
        bulkCopy.close();

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals("" + rs.getTimestamp(1), "2015-05-08 12:26:24.13");
        }

    }

    /**
     * test time
     * 
     * @throws SQLException
     */
    @Test
    public void bulkCopyTestTime() throws SQLException {
        String col1Value = "'12:26:27.1452367'";
        String destTableName = "dest_sqlVariant";
        Utils.dropTableIfExists(tableName, stmt);
        Utils.dropTableIfExists(destTableName, stmt);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1) values (CAST (" + col1Value + " AS " + "time(2)" + ") )");
        stmt.executeUpdate("create table " + destTableName + " (col1 sql_variant)");

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        rs.next();
        assertEquals("" + rs.getObject(1).toString(), "12:26:27");
    }

    /**
     * Read GUID stored in SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    public void bulkCopyTestReadGUID() throws SQLException {
        String col1Value = "1AE740A2-2272-4B0F-8086-3DDAC595BC11";
        beforeEachSetup("uniqueidentifier", "'" + col1Value + "'");
        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);
        bulkCopy.close();

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals("" + rs.getUniqueIdentifier(1), col1Value);

        }
    }

    /**
     * Read VarChar8000 from SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    public void bulkCopyTestVarChar8000() throws SQLException {
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < 8000; i++) {
            buffer.append("a");
        }
        String col1Value = buffer.toString();
        beforeEachSetup("varchar(8000)", "'" + col1Value + "'");

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);
        bulkCopy.close();

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals(rs.getString(1), col1Value);
        }
    }

    private void beforeEachSetup(String colType,
            Object colValue) throws SQLException {
        Utils.dropTableIfExists(tableName, stmt);
        Utils.dropTableIfExists(destTableName, stmt);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1) values (CAST (" + colValue + " AS " + colType + ") )");
        stmt.executeUpdate("create table " + destTableName + " (col1 sql_variant)");
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
        Utils.dropTableIfExists(tableName, stmt);
        Utils.dropTableIfExists(destTableName, stmt);

        if (null != stmt) {
            stmt.close();
        }

        if (null != rs) {
            rs.close();
        }

        if (null != con) {
            con.close();
        }
    }
}
