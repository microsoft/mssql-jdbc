/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.fedauth;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.sql.PooledConnection;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
@Tag("slow")
@Tag(Constants.fedAuth)
public class PooledConnectionTest extends FedauthCommon {

    static String charTable = TestUtils.escapeSingleQuotes(
            AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("JDBC_PooledConnection")));

    @Test
    public void testPooledConnectionAccessTokenExpiredThenReconnectADPassword() throws SQLException {
        // suspend 5 mins
        testPooledConnectionAccessTokenExpiredThenReconnect((long) 5 * 60, SqlAuthentication.ActiveDirectoryPassword);

        // get another token
        getFedauthInfo();

        // suspend until access token expires
        testPooledConnectionAccessTokenExpiredThenReconnect(secondsBeforeExpiration,
                SqlAuthentication.ActiveDirectoryPassword);
    }

    @Test
    public void testPooledConnectionAccessTokenExpiredThenReconnectADIntegrated() throws SQLException {
        org.junit.Assume.assumeTrue(isWindows && enableADIntegrated);

        // suspend 5 mins
        testPooledConnectionAccessTokenExpiredThenReconnect((long) 5 * 60, SqlAuthentication.ActiveDirectoryIntegrated);

        // get another token
        getFedauthInfo();

        // suspend until access token expires
        testPooledConnectionAccessTokenExpiredThenReconnect(secondsBeforeExpiration,
                SqlAuthentication.ActiveDirectoryIntegrated);
    }

    private void testPooledConnectionAccessTokenExpiredThenReconnect(long testingTimeInSeconds,
            SqlAuthentication authentication) throws SQLException {
        SQLServerConnectionPoolDataSource ds = new SQLServerConnectionPoolDataSource();

        if (SqlAuthentication.ActiveDirectoryIntegrated != authentication) {
            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setUser(azureUserName);
            ds.setPassword(azurePassword);
            ds.setAuthentication(SqlAuthentication.ActiveDirectoryPassword.toString());
        } else {
            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setAuthentication(SqlAuthentication.ActiveDirectoryIntegrated.toString());
        }

        try {
            // create pooled connection
            PooledConnection pc = ds.getPooledConnection();

            // get first connection from pool
            try (Connection connection1 = pc.getConnection(); Statement stmt = connection1.createStatement()) {
                testUserName(connection1, azureUserName, authentication);

                try {
                    TestUtils.dropTableIfExists(charTable, stmt);
                    createTable(stmt, charTable);
                    populateCharTable(connection1, charTable);
                    testChar(stmt, charTable);
                } finally {
                    TestUtils.dropTableIfExists(charTable, stmt);
                }
            }
            Thread.sleep(TimeUnit.SECONDS.toMillis(testingTimeInSeconds));
            Thread.sleep(TimeUnit.SECONDS.toMillis(2)); // give 2 mins more to make sure the access token is expired.

            // get second connection from pool
            try (Connection connection2 = pc.getConnection(); Statement stmt = connection2.createStatement()) {
                testUserName(connection2, azureUserName, authentication);

                try {
                    TestUtils.dropTableIfExists(charTable, stmt);
                    createTable(stmt, charTable);
                    populateCharTable(connection2, charTable);
                    testChar(stmt, charTable);
                } finally {
                    TestUtils.dropTableIfExists(charTable, stmt);
                }
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testPooledConnectionMultiThreadADPassword() throws SQLException {
        testPooledConnectionMultiThread(secondsBeforeExpiration, SqlAuthentication.ActiveDirectoryPassword);
    }

    @Test
    public void testPooledConnectionMultiThreadADIntegrated() throws SQLException {
        org.junit.Assume.assumeTrue(isWindows && enableADIntegrated);

        testPooledConnectionMultiThread(secondsBeforeExpiration, SqlAuthentication.ActiveDirectoryIntegrated);
    }

    private void testPooledConnectionMultiThread(long testingTimeInSeconds,
            SqlAuthentication authentication) throws SQLException {
        SQLServerConnectionPoolDataSource ds = new SQLServerConnectionPoolDataSource();

        if (SqlAuthentication.ActiveDirectoryIntegrated != authentication) {
            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setUser(azureUserName);
            ds.setPassword(azurePassword);
            ds.setAuthentication(SqlAuthentication.ActiveDirectoryPassword.toString());
        } else {
            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setAuthentication(SqlAuthentication.ActiveDirectoryIntegrated.toString());
        }

        try {
            // create pooled connection
            final PooledConnection pc = ds.getPooledConnection();

            // get first connection from pool
            try (Connection connection1 = pc.getConnection(); Statement stmt = connection1.createStatement()) {
                testUserName(connection1, azureUserName, authentication);
            }
            Thread.sleep(TimeUnit.SECONDS.toMillis(testingTimeInSeconds));
            Thread.sleep(TimeUnit.SECONDS.toMillis(2)); // give 2 mins more to make sure the access token is expired.

            Callable<Void> c = () -> {
                try (Connection connection2 = pc.getConnection()) {
                    testUserName(connection2, azureUserName, authentication);
                } catch (SQLException e) {
                    assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(),
                            e.getMessage().contains(ERR_MSG_CONNECTION_CLOSED)
                                    || e.getMessage().contains(ERR_MSG_CONNECTION_IS_CLOSED)
                                    || e.getMessage().contains(ERR_MSG_HAS_CLOSED)
                                    || e.getMessage().contains(ERR_MSG_HAS_BEEN_CLOSED)
                                    || e.getMessage().contains(ERR_MSG_SOCKET_CLOSED));
                }
                return null;
            };

            Random rand = new Random();
            int numberOfThreadsForEachType = (rand.nextInt(15) + 1) * 3; // 3 to 45
            ExecutorService es = Executors.newFixedThreadPool(3);
            List<Future<Void>> results = new ArrayList<>(numberOfThreadsForEachType);
            for (int i = 0; i < numberOfThreadsForEachType; i++) {
                results.add(es.submit(c));
            }

            // get is blocking, will wait for thread to finish
            for (Future<Void> f : results) {
                f.get();
            }
            es.shutdown();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    // suspend until access token expires
    @Test
    public void testPooledConnectionWithAccessToken() throws SQLException {
        try {
            SQLServerConnectionPoolDataSource ds = new SQLServerConnectionPoolDataSource();

            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setAccessToken(accessToken);

            // create pooled connection
            final PooledConnection pc = ds.getPooledConnection();

            // get first connection from pool
            try (Connection connection1 = pc.getConnection()) {
                testUserName(connection1, azureUserName, SqlAuthentication.NotSpecified);
            }

            Callable<Void> r = () -> {
                try (Connection connection2 = pc.getConnection()) {
                    testUserName(connection2, azureUserName, SqlAuthentication.NotSpecified);
                } catch (SQLException e) {
                    fail(e.getMessage());
                }
                return null;
            };

            Random rand = new Random();
            int numberOfThreadsForEachType = (rand.nextInt(15) + 1) * 3; // 3 to 45
            ExecutorService es = Executors.newFixedThreadPool(3);
            List<Future<Void>> results = new ArrayList<>(numberOfThreadsForEachType);
            for (int i = 0; i < numberOfThreadsForEachType; i++) {
                results.add(es.submit(r));
            }

            // get is blocking, will wait for thread to finish
            for (Future<Void> f : results) {
                f.get();
            }
            es.shutdown();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @AfterAll
    public static void terminate() throws SQLException {
        try (Connection conn = DriverManager.getConnection(adPasswordConnectionStr);
                Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(charTable, stmt);
        }
    }
}
