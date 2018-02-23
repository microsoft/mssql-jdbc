/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.ShardingKey;
import java.util.Enumeration;
import java.util.stream.Stream;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;
import javax.sql.XAConnection;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.opentest4j.TestAbortedException;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.util.Util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Tests JDBC 4.3 APIs
 *
 */
@RunWith(JUnitPlatform.class)
public class JDBC43Test extends AbstractTest {
    ShardingKey superShardingKey = null;
    ShardingKey shardingKey = null;

    /**
     * Tests that we are throwing the unsupported exception for connectionBuilder()
     * @throws SQLException 
     * @throws TestAbortedException 
     * 
     * @since 1.9
     */
    @Test
    public void connectionBuilderTest() throws TestAbortedException, SQLException {
        assumeTrue(Util.supportJDBC43(connection));
        SQLServerDataSource ds = new SQLServerDataSource();
        try {
            superShardingKey = ds.createShardingKeyBuilder().subkey("EASTERN_REGION", JDBCType.VARCHAR).build();
        }
        catch (SQLException e) {
            assert (e.getMessage().contains("not implemented"));
        }

        try {
            shardingKey = ds.createShardingKeyBuilder().subkey("PITTSBURGH_BRANCH", JDBCType.VARCHAR).build();
        }
        catch (SQLException e) {
            assert (e.getMessage().contains("not implemented"));
        }

        try {
            Connection con = ds.createConnectionBuilder().user("rafa").password("tennis").shardingKey(shardingKey).superShardingKey(superShardingKey)
                    .build();
        }
        catch (SQLException e) {
            assert (e.getMessage().contains("not implemented"));
        }
    }

    /**
     * Tests that we are throwing the unsupported exception for connectionBuilder()
     * @throws SQLException 
     * @throws TestAbortedException 
     * 
     * @since 1.9
     */
    @Test
    public void xaConnectionBuilderTest() throws TestAbortedException, SQLException {
        assumeTrue(Util.supportJDBC43(connection));
        SQLServerXADataSource ds = new SQLServerXADataSource();
        try {
            superShardingKey = ds.createShardingKeyBuilder().subkey("EASTERN_REGION", JDBCType.VARCHAR).build();
        }
        catch (SQLException e) {
            assert (e.getMessage().contains("not implemented"));
        }

        try {
            shardingKey = ds.createShardingKeyBuilder().subkey("PITTSBURGH_BRANCH", JDBCType.VARCHAR).build();
        }
        catch (SQLException e) {
            assert (e.getMessage().contains("not implemented"));
        }

        try {
            XAConnection con = ds.createXAConnectionBuilder().user("rafa").password("tennis").shardingKey(shardingKey)
                    .superShardingKey(superShardingKey).build();
        }
        catch (SQLException e) {
            assert (e.getMessage().contains("not implemented"));
        }
    }

    /**
     * Tests that we are throwing the unsupported exception for createPooledConnectionBuilder()
     * @throws SQLException 
     * @throws TestAbortedException 
     * @since 1.9
     */
    @Test
    public void connectionPoolDataSourceTest() throws TestAbortedException, SQLException {
        assumeTrue(Util.supportJDBC43(connection));
        ConnectionPoolDataSource ds = new SQLServerConnectionPoolDataSource();
        try {
            superShardingKey = ds.createShardingKeyBuilder().subkey("EASTERN_REGION", JDBCType.VARCHAR).build();
        }
        catch (SQLException e) {
            assert (e.getMessage().contains("not implemented"));
        }

        try {
            shardingKey = ds.createShardingKeyBuilder().subkey("PITTSBURGH_BRANCH", JDBCType.VARCHAR).build();
        }
        catch (SQLException e) {
            assert (e.getMessage().contains("not implemented"));
        }
        try {
            PooledConnection con = ds.createPooledConnectionBuilder().user("rafa").password("tennis").shardingKey(shardingKey)
                    .superShardingKey(superShardingKey).build();
        }
        catch (SQLException e) {
            assert (e.getMessage().contains("not implemented"));
        }
    }
    
    /**
     * Tests that we are throwing the unsupported exception for setShardingKeyIfValid()
     * @throws SQLException 
     * @throws TestAbortedException 
     * @since 1.9
     */
    @Test
    public void setShardingKeyIfValidTest() throws TestAbortedException, SQLException {
        assumeTrue(Util.supportJDBC43(connection));
        SQLServerConnection connection43 = (SQLServerConnection43) DriverManager.getConnection(connectionString);
        try {
            connection43.setShardingKeyIfValid(shardingKey, 10);
        }
        catch (SQLException e) {
            assert (e.getMessage().contains("not implemented"));
        }
        try {
            connection43.setShardingKeyIfValid(shardingKey, superShardingKey, 10);
        }
        catch (SQLException e) {
            assert (e.getMessage().contains("not implemented"));
        }
       
    }
    
    /**
     * Tests that we are throwing the unsupported exception for setShardingKey()
     * @throws SQLException 
     * @throws TestAbortedException 
     * @since 1.9
     */
    @Test
    public void setShardingKeyTest() throws TestAbortedException, SQLException {
        assumeTrue(Util.supportJDBC43(connection));
        SQLServerConnection connection43 = (SQLServerConnection43) DriverManager.getConnection(connectionString);
        try {
            connection43.setShardingKey(shardingKey);
        }
        catch (SQLException e) {
            assert (e.getMessage().contains("not implemented"));
        }
        try {
            connection43.setShardingKey(shardingKey, superShardingKey);
        }
        catch (SQLException e) {
            assert (e.getMessage().contains("not implemented"));
        }
       
    }

    /**
     * Tests the stream<Driver> drivers() methods in java.sql.DriverManager
     * 
     * @since 1.9
     * @throws ClassNotFoundException
     */
    @Test
    public void driversTest() throws ClassNotFoundException {
        Stream<Driver> drivers = DriverManager.drivers();
        Object[] driversArray = drivers.toArray();
        assertEquals(driversArray[0].getClass(), Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver"));
    }

    /**
     * Tests deregister Driver
     * 
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    @Test
    public void deregisterDriverTest() throws SQLException, ClassNotFoundException {
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        Driver current = null;
        while (drivers.hasMoreElements()) {
            current = drivers.nextElement();
            DriverManager.deregisterDriver(current);
        }
        Stream<Driver> currentDrivers = DriverManager.drivers();
        Object[] driversArray = currentDrivers.toArray();
        assertEquals(0, driversArray.length);

        DriverManager.registerDriver(current);
    }

}