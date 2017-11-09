/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
/* TODO: Make possible to run automated (including certs, only works on Windows now etc.)*/
/*
package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.testframework.AbstractTest;

@RunWith(JUnitPlatform.class)
public class RegressionTestAlwaysEncrypted extends AbstractTest {
    String dateTable = "DateTable";
    String charTable = "CharTable";
    String numericTable = "NumericTable";
    Statement stmt = null;
    Connection connection = null;
    Date date;
    String cekName = "CEK_Auto1";  // you need to change this to your CEK
    long dateValue = 212921879801519L;

    @Test
    public void alwaysEncrypted1() throws Exception {

        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        connection = DriverManager.getConnection(connectionString + ";trustservercertificate=true;columnEncryptionSetting=enabled;database=Tobias;");
        assertTrue(null != connection);
        
        stmt = ((SQLServerConnection) connection).createStatement();

        date = new Date(dateValue);

        dropTable();
        createNumericTable();
        populateNumericTable();
        printNumericTable();

        dropTable();
        createDateTable();
        populateDateTable();
        printDateTable();

        dropTable();
        createNumericTable();
        populateNumericTableWithNull();
        printNumericTable();
    }
    
    @Test
    public void alwaysEncrypted2() throws Exception {

    	Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        connection = DriverManager.getConnection(connectionString + ";trustservercertificate=true;columnEncryptionSetting=enabled;database=Tobias;");
        assertTrue(null != connection);
        
        stmt = ((SQLServerConnection) connection).createStatement();

        date = new Date(dateValue);

        dropTable();
        createCharTable();
        populateCharTable();
        printCharTable();

        dropTable();
        createDateTable();
        populateDateTable();
        printDateTable();

        dropTable();
        createNumericTable();
        populateNumericTableSpecificSetter();
        printNumericTable();

    }

    private void populateDateTable() {

        try {
            String sql = "insert into " + dateTable + " values( " + "?" + ")";
            SQLServerPreparedStatement sqlPstmt = (SQLServerPreparedStatement) ((SQLServerConnection) connection).prepareStatement(sql,
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, connection.getHoldability());
            sqlPstmt.setObject(1, date);
            sqlPstmt.executeUpdate();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void populateCharTable() {

        try {
            String sql = "insert into " + charTable + " values( " + "?,?,?,?,?,?" + ")";
            SQLServerPreparedStatement sqlPstmt = (SQLServerPreparedStatement) ((SQLServerConnection) connection).prepareStatement(sql,
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, connection.getHoldability());
            sqlPstmt.setObject(1, "hi");
            sqlPstmt.setObject(2, "sample");
            sqlPstmt.setObject(3, "hey");
            sqlPstmt.setObject(4, "test");
            sqlPstmt.setObject(5, "hello");
            sqlPstmt.setObject(6, "caching");
            sqlPstmt.executeUpdate();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void populateNumericTable() throws Exception {
        String sql = "insert into " + numericTable + " values( " + "?,?,?,?,?,?,?,?,?" + ")";
        SQLServerPreparedStatement sqlPstmt = (SQLServerPreparedStatement) ((SQLServerConnection) connection).prepareStatement(sql,
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, connection.getHoldability());
        sqlPstmt.setObject(1, true);
        sqlPstmt.setObject(2, false);
        sqlPstmt.setObject(3, true);

        Integer value = 255;
        sqlPstmt.setObject(4, value.shortValue(), JDBCType.TINYINT);
        sqlPstmt.setObject(5, value.shortValue(), JDBCType.TINYINT);
        sqlPstmt.setObject(6, value.shortValue(), JDBCType.TINYINT);

        sqlPstmt.setObject(7, Short.valueOf("1"), JDBCType.SMALLINT);
        sqlPstmt.setObject(8, Short.valueOf("2"), JDBCType.SMALLINT);
        sqlPstmt.setObject(9, Short.valueOf("3"), JDBCType.SMALLINT);

        sqlPstmt.executeUpdate();
    }

    private void populateNumericTableSpecificSetter() {

        try {
            String sql = "insert into " + numericTable + " values( " + "?,?,?,?,?,?,?,?,?" + ")";
            SQLServerPreparedStatement sqlPstmt = (SQLServerPreparedStatement) ((SQLServerConnection) connection).prepareStatement(sql,
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, connection.getHoldability());
            sqlPstmt.setBoolean(1, true);
            sqlPstmt.setBoolean(2, false);
            sqlPstmt.setBoolean(3, true);

            Integer value = 255;
            sqlPstmt.setShort(4, value.shortValue());
            sqlPstmt.setShort(5, value.shortValue());
            sqlPstmt.setShort(6, value.shortValue());

            sqlPstmt.setByte(7, Byte.valueOf("127"));
            sqlPstmt.setByte(8, Byte.valueOf("127"));
            sqlPstmt.setByte(9, Byte.valueOf("127"));

            sqlPstmt.executeUpdate();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void populateNumericTableWithNull() {

        try {
            String sql = "insert into " + numericTable + " values( " + "?,?,?" + ",?,?,?" + ",?,?,?" + ")";
            SQLServerPreparedStatement sqlPstmt = (SQLServerPreparedStatement) ((SQLServerConnection) connection).prepareStatement(sql,
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, connection.getHoldability());
            sqlPstmt.setObject(1, null, java.sql.Types.BIT);
            sqlPstmt.setObject(2, null, java.sql.Types.BIT);
            sqlPstmt.setObject(3, null, java.sql.Types.BIT);

            sqlPstmt.setObject(4, null, java.sql.Types.TINYINT);
            sqlPstmt.setObject(5, null, java.sql.Types.TINYINT);
            sqlPstmt.setObject(6, null, java.sql.Types.TINYINT);

            sqlPstmt.setObject(7, null, java.sql.Types.SMALLINT);
            sqlPstmt.setObject(8, null, java.sql.Types.SMALLINT);
            sqlPstmt.setObject(9, null, java.sql.Types.SMALLINT);

            sqlPstmt.executeUpdate();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void printDateTable() throws SQLException {

        stmt = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("select * from " + dateTable);

        while (rs.next()) {
            System.out.println(rs.getObject(1));
        }
    }

    private void printCharTable() throws SQLException {
        stmt = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("select * from " + charTable);

        while (rs.next()) {
            System.out.println(rs.getObject(1));
            System.out.println(rs.getObject(2));
            System.out.println(rs.getObject(3));
            System.out.println(rs.getObject(4));
            System.out.println(rs.getObject(5));
            System.out.println(rs.getObject(6));
        }

    }

    private void printNumericTable() throws SQLException {
        stmt = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("select * from " + numericTable);

        while (rs.next()) {
            System.out.println(rs.getObject(1));
            System.out.println(rs.getObject(2));
            System.out.println(rs.getObject(3));
            System.out.println(rs.getObject(4));
            System.out.println(rs.getObject(5));
            System.out.println(rs.getObject(6));
        }

    }

    private void createDateTable() throws SQLException {

        String sql = "create table " + dateTable + " ("
                + "RandomizedDate date ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL," + ");";

        try {
            stmt.execute(sql);
        }
        catch (SQLException e) {
            System.out.println(e);
        }
    }

    private void createCharTable() throws SQLException {
        String sql = "create table " + charTable + " (" + "PlainChar char(20) null,"
                + "RandomizedChar char(20) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"
                + "DeterministicChar char(20) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"

                + "PlainVarchar varchar(50) null,"
                + "RandomizedVarchar varchar(50) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"
                + "DeterministicVarchar varchar(50) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"

                + ");";

        try {
            stmt.execute(sql);
        }
        catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private void createNumericTable() throws SQLException {
        String sql = "create table " + numericTable + " (" + "PlainBit bit null,"
                + "RandomizedBit bit ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"
                + "DeterministicBit bit ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"

                + "PlainTinyint tinyint null,"
                + "RandomizedTinyint tinyint ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"
                + "DeterministicTinyint tinyint ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"

                + "PlainSmallint smallint null,"
                + "RandomizedSmallint smallint ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"
                + "DeterministicSmallint smallint ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"

                + ");";

        try {
            stmt.execute(sql);
        }
        catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private void dropTable() throws SQLException {
        stmt.executeUpdate("if object_id('" + dateTable + "','U') is not null" + " drop table " + dateTable);
        stmt.executeUpdate("if object_id('" + charTable + "','U') is not null" + " drop table " + charTable);
        stmt.executeUpdate("if object_id('" + numericTable + "','U') is not null" + " drop table " + numericTable);
    }
}
*/