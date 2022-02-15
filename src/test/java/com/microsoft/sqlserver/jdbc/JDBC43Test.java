/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.ShardingKey;

import javax.sql.ConnectionPoolDataSource;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.opentest4j.TestAbortedException;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


/**
 * Tests JDBC 4.3 APIs
 *
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.xJDBC42)
public class JDBC43Test extends AbstractTest {
    ShardingKey superShardingKey = null;
    ShardingKey shardingKey = null;

    @BeforeAll
    public static void setupTests() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"trustServerCertificate", "true");
        setConnection();
    }

    /**
     * Tests that we are throwing the unsupported exception for connectionBuilder()
     * 
     * @throws SQLException
     * @throws TestAbortedException
     * 
     * @since 1.9
     */
    @Test
    public void connectionBuilderTest() throws TestAbortedException, SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();
        try {
            superShardingKey = ds.createShardingKeyBuilder().subkey("EASTERN_REGION", JDBCType.VARCHAR).build();
        } catch (SQLException e) {
            assert (e.getMessage().contains(TestResource.getResource("R_notImplemented")));
        }

        try {
            shardingKey = ds.createShardingKeyBuilder().subkey("PITTSBURGH_BRANCH", JDBCType.VARCHAR).build();
        } catch (SQLException e) {
            assert (e.getMessage().contains(TestResource.getResource("R_notImplemented")));
        }

        try {
            ds.createConnectionBuilder().user("rafa").password("tennis").shardingKey(shardingKey)
                    .superShardingKey(superShardingKey).build();
        } catch (SQLException e) {
            assert (e.getMessage().contains(TestResource.getResource("R_notImplemented")));
        }
    }

    /**
     * Tests that we are throwing the unsupported exception for connectionBuilder()
     * 
     * @throws SQLException
     * @throws TestAbortedException
     * 
     * @since 1.9
     */
    @Test
    public void xaConnectionBuilderTest() throws TestAbortedException, SQLException {
        SQLServerXADataSource ds = new SQLServerXADataSource();
        try {
            superShardingKey = ds.createShardingKeyBuilder().subkey("EASTERN_REGION", JDBCType.VARCHAR).build();
        } catch (SQLException e) {
            assert (e.getMessage().contains(TestResource.getResource("R_notImplemented")));
        }

        try {
            shardingKey = ds.createShardingKeyBuilder().subkey("PITTSBURGH_BRANCH", JDBCType.VARCHAR).build();
        } catch (SQLException e) {
            assert (e.getMessage().contains(TestResource.getResource("R_notImplemented")));
        }

        try {
            ds.createXAConnectionBuilder().user("rafa").password("tennis").shardingKey(shardingKey)
                    .superShardingKey(superShardingKey).build();
        } catch (SQLException e) {
            assert (e.getMessage().contains(TestResource.getResource("R_notImplemented")));
        }
    }

    /**
     * Tests that we are throwing the unsupported exception for createPooledConnectionBuilder()
     * 
     * @throws SQLException
     * @throws TestAbortedException
     * @since 1.9
     */
    @Test
    public void connectionPoolDataSourceTest() throws TestAbortedException, SQLException {
        ConnectionPoolDataSource ds = new SQLServerConnectionPoolDataSource();
        try {
            superShardingKey = ds.createShardingKeyBuilder().subkey("EASTERN_REGION", JDBCType.VARCHAR).build();
        } catch (SQLException e) {
            assert (e.getMessage().contains(TestResource.getResource("R_notImplemented")));
        }

        try {
            shardingKey = ds.createShardingKeyBuilder().subkey("PITTSBURGH_BRANCH", JDBCType.VARCHAR).build();
        } catch (SQLException e) {
            assert (e.getMessage().contains(TestResource.getResource("R_notImplemented")));
        }
        try {
            ds.createPooledConnectionBuilder().user("rafa").password("tennis").shardingKey(shardingKey)
                    .superShardingKey(superShardingKey).build();
        } catch (SQLException e) {
            assert (e.getMessage().contains(TestResource.getResource("R_notImplemented")));
        }
    }

    /**
     * Tests that we are throwing the unsupported exception for setShardingKeyIfValid()
     * 
     * @throws SQLException
     * @throws TestAbortedException
     * @since 1.9
     */
    @Test
    public void setShardingKeyIfValidTest() throws TestAbortedException, SQLException {
        try (SQLServerConnection connection43 = (SQLServerConnection43) getConnection()) {
            try {
                connection43.setShardingKeyIfValid(shardingKey, 10);
            } catch (SQLException e) {
                assert (e.getMessage().contains(TestResource.getResource("R_operationNotSupported")));
            }
            try {
                connection43.setShardingKeyIfValid(shardingKey, superShardingKey, 10);
            } catch (SQLException e) {
                assert (e.getMessage().contains(TestResource.getResource("R_operationNotSupported")));
            }
        }
    }

    /**
     * Tests that we are throwing the unsupported exception for setShardingKey()
     * 
     * @throws SQLException
     * @throws TestAbortedException
     * @since 1.9
     */
    @Test
    public void setShardingKeyTest() throws TestAbortedException, SQLException {
        try (SQLServerConnection connection43 = (SQLServerConnection43) getConnection()) {
            try {
                connection43.setShardingKey(shardingKey);
            } catch (SQLException e) {
                assert (e.getMessage().contains(TestResource.getResource("R_operationNotSupported")));
            }
            try {
                connection43.setShardingKey(shardingKey, superShardingKey);
            } catch (SQLException e) {
                assert (e.getMessage().contains(TestResource.getResource("R_operationNotSupported")));
            }
        }
    }
}
