/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.datatypes;

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
public class BulkCopyWithSqlVariant extends AbstractTest{

    static SQLServerConnection con = null;
    static Statement stmt = null;
    static String tableName = "SqlVariant_Test";

    /**
     * 
     * @throws SQLException
     */
    @Test
    public void bulkCopyTest_int() throws SQLException {
        int col1Value = 5;
        String destTableName = "dest_sqlVariant";
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + destTableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + destTableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1) values (CAST (" + col1Value + " AS " + "int" + ") )");
        stmt.executeUpdate("create table " + destTableName + " (col1 sql_variant)");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }

    @Test
    public void bulkCopyTest_SmallInt() throws SQLException {
        int col1Value = 5;
        String destTableName = "dest_sqlVariant";
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + destTableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + destTableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1) values (CAST (" + col1Value + " AS " + "smallint" + ") )");
        stmt.executeUpdate("create table " + destTableName + " (col1 sql_variant)");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }

    @Test
    public void bulkCopyTest_tinyint() throws SQLException {
        int col1Value = 5;
        String destTableName = "dest_sqlVariant";
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + destTableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + destTableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1) values (CAST (" + col1Value + " AS " + "tinyint" + ") )");
        stmt.executeUpdate("create table " + destTableName + " (col1 sql_variant)");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }

    @Test
    public void bulkCopyTest_bigint() throws SQLException {
        int col1Value = 5;
        String destTableName = "dest_sqlVariant";
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + destTableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + destTableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1) values (CAST (" + col1Value + " AS " + "bigint" + ") )");
        stmt.executeUpdate("create table " + destTableName + " (col1 sql_variant)");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }

    @Test
    public void bulkCopyTest_float() throws SQLException {
        int col1Value = 5;
        String destTableName = "dest_sqlVariant";
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + destTableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + destTableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1) values (CAST (" + col1Value + " AS " + "float" + ") )");
        stmt.executeUpdate("create table " + destTableName + " (col1 sql_variant)");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }

    }

    @Test
    public void bulkCopyTest_real() throws SQLException {
        int col1Value = 5;
        String destTableName = "dest_sqlVariant";
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + destTableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + destTableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1) values (CAST (" + col1Value + " AS " + "real" + ") )");
        stmt.executeUpdate("create table " + destTableName + " (col1 sql_variant)");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }

    }

    @Test
    public void bulkCopyTest_money() throws SQLException {
        String col1Value = "126.123";
        String destTableName = "dest_sqlVariant";
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + destTableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + destTableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1) values (CAST (" + col1Value + " AS " + "money" + ") )");
        stmt.executeUpdate("create table " + destTableName + " (col1 sql_variant)");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }

    }

    @Test
    public void bulkCopyTest_smallmoney() throws SQLException {
        String col1Value = "126.123";
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
            System.out.println(rs.getString(1));
        }

    }

    @Test
    public void bulkCopyTest_money3() throws SQLException {
        int col1Value = 5;
        String col = "5.00";
        System.out.println(new BigDecimal(col));
        String destTableName = "dest_sqlVariant";
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + destTableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + destTableName);
        stmt.executeUpdate("create table " + tableName + " (col1 money)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1) values (CAST (" + col1Value + " AS " + "money" + ") )");
        stmt.executeUpdate("create table " + destTableName + " (col1 money)");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }

    }

    @Test
    public void bulkCopyTest_date() throws SQLException {
        String col1Value = "'2015-05-05'";
        String destTableName = "dest_sqlVariant";
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + destTableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + destTableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1) values (CAST (" + col1Value + " AS " + "date" + ") )");
        stmt.executeUpdate("create table " + destTableName + " (col1 sql_variant)");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }

    }

    // @Test
    // public void bulkCopyTest_time() throws SQLException {
    // String col1Value = "'12:26:27.1452367'";
    // String destTableName = "dest_sqlVariant";
    // stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
    // + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
    // stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + destTableName + "') "
    // + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + destTableName);
    // stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
    // stmt.executeUpdate("INSERT into " + tableName + "(col1) values (CAST (" + col1Value + " AS " + "time(2)" + ") )");
    // stmt.executeUpdate("create table " + destTableName + " (col1 sql_variant)");
    //
    // SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);
    //
    // SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
    // bulkCopy.setDestinationTableName(destTableName);
    // bulkCopy.writeToServer(rs);
    //
    // rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
    // while (rs.next()) {
    // System.out.println(rs.getString(1));
    // }
    //
    // }

    @Test
    public void bulkCopyTest_char() throws SQLException {
        String col1Value = "'helkjloooghghoooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooox'";
        byte[] temp = col1Value.getBytes();
        for (int i = 0; i < temp.length; i++)
            System.out.println(temp[i]);

        String destTableName = "dest_sqlVariant";
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + destTableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + destTableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1) values (CAST (" + col1Value + " AS " + "char" + ") )");
        stmt.executeUpdate("create table " + destTableName + " (col1 sql_variant)");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }

    }

    @Test
    public void bulkCopyTest_nchar() throws SQLException {
        String col1Value = "'hello'";
        byte[] temp = col1Value.getBytes();
        for (int i = 0; i < temp.length; i++)
            System.out.println(temp[i]);

        String destTableName = "dest_sqlVariant";
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + destTableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + destTableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1) values (CAST (" + col1Value + " AS " + "nchar" + ") )");
        stmt.executeUpdate("create table " + destTableName + " (col1 sql_variant)");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }

    }

    @Test
    public void bulkCopyTest_varchar() throws SQLException {
        String col1Value = "'hello'";
        byte[] temp = col1Value.getBytes();
        for (int i = 0; i < temp.length; i++)
            System.out.println(temp[i]);

        String destTableName = "dest_sqlVariant";
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + destTableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + destTableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1) values (CAST (" + col1Value + " AS " + "varchar" + ") )");
        stmt.executeUpdate("create table " + destTableName + " (col1 sql_variant)");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }

    }

    @Test
    public void bulkCopyTest_nvarchar() throws SQLException {
        String col1Value = "'hello'";
        byte[] temp = col1Value.getBytes();
        for (int i = 0; i < temp.length; i++)
            System.out.println(temp[i]);

        String destTableName = "dest_sqlVariant";
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + destTableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + destTableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1) values (CAST (" + col1Value + " AS " + "nvarchar" + ") )");
        stmt.executeUpdate("create table " + destTableName + " (col1 sql_variant)");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }

    }

    @Test
    public void bulkCopyTest_binary20() throws SQLException {
        String col1Value = "'hello'";
        byte[] temp = col1Value.getBytes();
        for (int i = 0; i < temp.length; i++)
            System.out.println(temp[i]);

        String destTableName = "dest_sqlVariant";
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + destTableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + destTableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1) values (CAST (" + col1Value + " AS " + "binary(20)" + ") )");
        stmt.executeUpdate("create table " + destTableName + " (col1 sql_variant)");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }

    @Test
    public void bulkCopyTest_varbinary20() throws SQLException {
        String col1Value = "'hello'";

        String destTableName = "dest_sqlVariant";
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + destTableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + destTableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1) values (CAST (" + col1Value + " AS " + "varbinary(20)" + ") )");
        stmt.executeUpdate("create table " + destTableName + " (col1 sql_variant)");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }

    @Test
    public void bulkCopyTest_varbinary8000() throws SQLException {
        String col1Value = "'hello'";

        String destTableName = "dest_sqlVariant";
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + destTableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + destTableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1) values (CAST (" + col1Value + " AS " + "binary(8000)" + ") )");
        stmt.executeUpdate("create table " + destTableName + " (col1 sql_variant)");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            System.out.println(rs.getObject(1));
        }
    }

    @Test
    public void bulkCopyTest_bitNull() throws SQLException {
        int col1Value = 5000;

        String destTableName = "dest_sqlVariant";
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + destTableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + destTableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1) values (CAST (" + null + " AS " + "bit" + ") )");
        stmt.executeUpdate("create table " + destTableName + " (col1 sql_variant)");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            System.out.println(rs.getObject(1));
        }
    }

    @Test
    public void bulkCopyTest_bit() throws SQLException {
        int col1Value = 5000;

        String destTableName = "dest_sqlVariant";
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + destTableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + destTableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1) values (CAST (" + col1Value + " AS " + "bit" + ") )");
        stmt.executeUpdate("create table " + destTableName + " (col1 sql_variant)");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            System.out.println(rs.getObject(1));
        }
    }

    @Test
    public void bulkCopyTest_datetime() throws SQLException {
        String col1Value = "'2015-05-08 12:26:24'";
        String destTableName = "dest_sqlVariant";
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + destTableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + destTableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1) values (CAST (" + col1Value + " AS " + "datetime" + ") )");
        stmt.executeUpdate("create table " + destTableName + " (col1 sql_variant)");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }

    }

    @Test
    public void bulkCopyTest_smalldatetime() throws SQLException {
        String col1Value = "'2015-05-08 12:26:24'";
        String destTableName = "dest_sqlVariant";
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + destTableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + destTableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1) values (CAST (" + col1Value + " AS " + "smalldatetime" + ") )");
        stmt.executeUpdate("create table " + destTableName + " (col1 sql_variant)");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }

    }

    @Test
    public void bulkCopyTest_datetime2() throws SQLException {
        String col1Value = "'2015-05-08 12:26:24.12645'";
        String destTableName = "dest_sqlVariant";
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + destTableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + destTableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1) values (CAST (" + col1Value + " AS " + "datetime2(2)" + ") )");
        stmt.executeUpdate("create table " + destTableName + " (col1 sql_variant)");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }

    }

    /**
     * Read GUID stored in SqlVariant
     * 
     * @throws SQLException
     */
    @Test
    public void bulkCopyTest_readGUID() throws SQLException {
        String col1Value = "'1AE740A2-2272-4B0F-8086-3DDAC595BC11'";
        System.out.println(col1Value.getBytes().toString());
        String destTableName = "dest_sqlVariant";
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + tableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + tableName);
        stmt.executeUpdate("IF EXISTS (select * from sysobjects where id = object_id(N'" + destTableName + "') "
                + "and OBJECTPROPERTY(id, N'IsTable') = 1)" + " DROP TABLE " + destTableName);
        stmt.executeUpdate("create table " + tableName + " (col1 sql_variant)");
        stmt.executeUpdate("INSERT into " + tableName + "(col1) values (CAST (" + col1Value + " AS " + "uniqueidentifier" + ") )");
        stmt.executeUpdate("create table " + destTableName + " (col1 sql_variant)");

        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + tableName);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
        bulkCopy.setDestinationTableName(destTableName);
        bulkCopy.writeToServer(rs);

        rs = (SQLServerResultSet) stmt.executeQuery("SELECT * FROM " + destTableName);
        while (rs.next()) {
            System.out.println(rs.getString(1));
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
