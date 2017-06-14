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

/**
 * Test Bulkcopy with sql_variant datatype, testing all underlying supported datatypes
 *
 */
@RunWith(JUnitPlatform.class)
public class BulkCopyWithSqlVariant extends AbstractTest {

    static SQLServerConnection con = null;
    static Statement stmt = null;
    static String tableName = "SqlVariant_Test";
    static String destTableName = "dest_sqlVariant";

    /**
     * 
     * @throws SQLException
     */
    // @Test
    public void bulkCopyTest_int() throws SQLException {
        int col1Value = 5;
        beforeEachSetup("int", col1Value);
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals(rs.getInt(1), 5);
        }
    }

    @Test
    public void bulkCopyTest_SmallInt() throws SQLException {
        int col1Value = 5;
        beforeEachSetup("smallint", col1Value);

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals(rs.getShort(1), 5);
        }
    }

    @Test
    public void bulkCopyTest_tinyint() throws SQLException {
        int col1Value = 5;
        beforeEachSetup("tinyint", col1Value);
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals(rs.getByte(1), 5);
        }
    }

    @Test
    public void bulkCopyTest_bigint() throws SQLException {
        int col1Value = 5;
        beforeEachSetup("bigint", col1Value);

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals(rs.getLong(1), col1Value);
        }
    }

    @Test
    public void bulkCopyTest_float() throws SQLException {
        int col1Value = 5;
        beforeEachSetup("float", col1Value);

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals(rs.getDouble(1), col1Value);
        }
    }

    @Test
    public void bulkCopyTest_real() throws SQLException {
        int col1Value = 5;
        beforeEachSetup("real", col1Value);
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals(rs.getFloat(1), col1Value);
        }

    }

    @Test
    public void bulkCopyTest_money() throws SQLException {
        String col1Value = "126.1230";
        beforeEachSetup("money", col1Value);

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals(rs.getMoney(1), new BigDecimal(col1Value));
        }

    }

    @Test
    public void bulkCopyTest_smallmoney() throws SQLException {
        String col1Value = "126.1230";
        String destTableName = "dest_sqlVariant";
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + destTableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + destTableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1) values (CAST (" + col1Value + " AS " + "smallmoney" + ") )");
        stmt.executeUpdate("create table " + destTableName + " (col1 sql_variant)");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals(rs.getSmallMoney(1), new BigDecimal(col1Value));
        }

    }

    @Test
    public void bulkCopyTest_date() throws SQLException {
        String col1Value = "2015-05-05";
        beforeEachSetup("date", "'" + col1Value + "'");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals("" + rs.getDate(1), col1Value);
        }

    }

    @Test
    public void bulkCopyTest_TwoCols() throws SQLException {
        String col1Value = "2015-05-05";
        String col2Value = "126.1230";
        String destTableName = "dest_sqlVariant";
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + destTableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + destTableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant, col2 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1, col2) values (CAST ('" + col1Value + "' AS " + "date" + ")" + ",CAST (" + col2Value
                + " AS " + "smallmoney" + ")   )");
        stmt.executeUpdate("create table " + destTableName + " (col1 sql_variant, col2 sql_variant)");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals("" + rs.getDate(1), col1Value);
            assertEquals(rs.getSmallMoney(2), new BigDecimal(col2Value));
        }

    }

    @Test
    public void bulkCopyTest_time() throws SQLException {
        String col1Value = "'12:26:27.1452367'";
        beforeEachSetup("time(2)", col1Value);
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals("" + rs.getString(1), "12:26:27.15");  // getTime does not work
        }

    }

    @Test
    public void bulkCopyTest_char() throws SQLException {
        String col1Value = "'sample'";

        beforeEachSetup("char", col1Value);

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals("'" + rs.getString(1).trim() + "'", col1Value); // adds space between
        }

    }

    @Test
    public void bulkCopyTest_nchar() throws SQLException {
        String col1Value = "'a'";

        beforeEachSetup("nchar", col1Value);
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals("'" + rs.getNString(1).trim() + "'", col1Value);
        }

    }

    @Test
    public void bulkCopyTest_varchar() throws SQLException {
        String col1Value = "'hello'";

        beforeEachSetup("varchar", col1Value);

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals("'" + rs.getString(1).trim() + "'", col1Value);
        }

    }

    @Test
    public void bulkCopyTest_nvarchar() throws SQLException {
        String col1Value = "'hello'";
        beforeEachSetup("nvarchar", col1Value);

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals("'" + rs.getString(1).trim() + "'", col1Value);
        }

    }

    @Test
    public void bulkCopyTest_binary20() throws SQLException {
        String col1Value = "hello";
        beforeEachSetup("binary(20)", "'" + col1Value + "'");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertTrue(Utils.parseByte(rs.getBytes(1), col1Value.getBytes()));
        }
    }

    @Test
    public void bulkCopyTest_varbinary20() throws SQLException {
        String col1Value = "hello";

        String destTableName = "dest_sqlVariant";
        beforeEachSetup("varbinary(20)", "'" + col1Value + "'");
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertTrue(Utils.parseByte(rs.getBytes(1), col1Value.getBytes()));
        }
    }

    @Test
    public void bulkCopyTest_varbinary8000() throws SQLException {
        String col1Value = "hello";
        beforeEachSetup("binary(8000)", "'" + col1Value + "'");
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertTrue(Utils.parseByte(rs.getBytes(1), col1Value.getBytes()));
        }
    }

    @Test // TODO: check bitnull
    public void bulkCopyTest_bitNull() throws SQLException {
        int col1Value = 5000;
        beforeEachSetup("bit", null);
  
         SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals(rs.getBoolean(1), false);
        }
    }

    @Test
    public void bulkCopyTest_bit() throws SQLException {
        int col1Value = 5000;
        beforeEachSetup("bit", col1Value);
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals(rs.getBoolean(1), true);
        }
    }

    @Test
    public void bulkCopyTest_datetime() throws SQLException {
        String col1Value = "2015-05-08 12:26:24.0";
        beforeEachSetup("datetime", "'" + col1Value + "'");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals("" + rs.getDateTime(1), col1Value);

        }

    }

    @Test
    public void bulkCopyTest_smalldatetime() throws SQLException {
        String col1Value = "2015-05-08 12:26:24";
        beforeEachSetup("smalldatetime", "'" + col1Value + "'");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals("" + rs.getSmallDateTime(1), "2015-05-08 12:26:00.0");
        }

    }

    @Test
    public void bulkCopyTest_datetime2() throws SQLException {
        String col1Value = "2015-05-08 12:26:24.12645";
        beforeEachSetup("datetime2(2)", "'" + col1Value + "'");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals("" + rs.getTimestamp(1), "2015-05-08 12:26:24.13");
        }

    }

    /**
     * Read GUID stored in SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    public void bulkCopyTest_readGUID() throws SQLException {
        String col1Value = "1AE740A2-2272-4B0F-8086-3DDAC595BC11";
        beforeEachSetup("uniqueidentifier", "'" + col1Value + "'");
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

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
    public void bulkCopyTest_Varchar8000() throws SQLException {
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < 8000; i++) {
            buffer.append("a");
        }
        String col1Value = buffer.toString();
        beforeEachSetup("varchar(8000)", "'" + col1Value + "'");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            assertEquals(rs.getString(1), col1Value);
        }
    }

    private void beforeEachSetup(String colType,
            Object colValue) throws SQLException {
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + destTableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + destTableName);
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
