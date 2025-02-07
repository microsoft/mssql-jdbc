/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.fedauth;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.sql.PooledConnection;

import com.microsoft.aad.msal4j.ClientCredentialFactory;
import com.microsoft.aad.msal4j.ClientCredentialParameters;
import com.microsoft.aad.msal4j.ConfidentialClientApplication;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import com.microsoft.aad.msal4j.IClientCredential;
import com.microsoft.sqlserver.jdbc.SQLServerAccessTokenCallback;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SqlAuthenticationToken;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.testframework.AbstractTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerPooledConnection;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
@Tag("slow")
@Tag(Constants.fedAuth)
@Tag(Constants.requireSecret)
public class PooledConnectionTest extends FedauthCommon {

    static String charTable = TestUtils.escapeSingleQuotes(
            AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("JDBC_PooledConnection")));

    private static String accessTokenCallbackConnectionString;

    public static class AccessTokenCallbackClass implements SQLServerAccessTokenCallback {
        @Override
        public SqlAuthenticationToken getAccessToken(String spn, String stsurl) {
            String scope = spn + "/.default";
            Set<String> scopes = new HashSet<>();
            scopes.add(scope);

            try {
                ExecutorService executorService = Executors.newSingleThreadExecutor();
                IClientCredential credential = ClientCredentialFactory.createFromSecret(applicationKey);
                ConfidentialClientApplication clientApplication = ConfidentialClientApplication
                        .builder(applicationClientID, credential).executorService(executorService)
                        .setTokenCacheAccessAspect(FedauthTokenCache.getInstance()).authority(stsurl).build();

                CompletableFuture<IAuthenticationResult> future = clientApplication
                        .acquireToken(ClientCredentialParameters.builder(scopes).build());

                IAuthenticationResult authenticationResult = future.get();
                String accessToken = authenticationResult.accessToken();
                long expiresOn = authenticationResult.expiresOnDate().getTime();

                return new SqlAuthenticationToken(accessToken, expiresOn);
            } catch (Exception e) {
                fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
            }
            return null;
        }
    }

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
        accessTokenCallbackConnectionString = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";";
    }

    @Test
    public void testPooledConnectionAccessTokenExpiredThenReconnectADPassword() throws SQLException {
        // suspend 60 secs
        testPooledConnectionAccessTokenExpiredThenReconnect(60, SqlAuthentication.ActiveDirectoryPassword);

        // get another token
        getFedauthInfo();

        // suspend until access token expires
        testPooledConnectionAccessTokenExpiredThenReconnect(60, SqlAuthentication.ActiveDirectoryPassword);
    }

    @Test
    public void testPooledConnectionAccessTokenExpiredThenReconnectADIntegrated() throws SQLException {
        org.junit.Assume.assumeTrue(enableADIntegrated);

        // suspend 60 secs
        testPooledConnectionAccessTokenExpiredThenReconnect(60, SqlAuthentication.ActiveDirectoryIntegrated);

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
            SQLServerPooledConnection spc = (SQLServerPooledConnection) pc;
            Field physicalConnectionField = SQLServerPooledConnection.class.getDeclaredField("physicalConnection");
            physicalConnectionField.setAccessible(true);
            Object con = physicalConnectionField.get(spc);
            TestUtils.setAccessTokenExpiry(con, accessToken);
            secondsBeforeExpiration = TestUtils.TEST_TOKEN_EXPIRY_SECONDS;

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
            Thread.sleep(TimeUnit.SECONDS.toMillis(testingTimeInSeconds + 5)); // give 5 more to make sure the access token is expired.

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
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testPooledConnectionMultiThreadADPassword() throws SQLException {
        testPooledConnectionMultiThread(secondsBeforeExpiration, SqlAuthentication.ActiveDirectoryPassword);
    }

    @Test
    public void testPooledConnectionMultiThreadADIntegrated() throws SQLException {
        org.junit.Assume.assumeTrue(enableADIntegrated);

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
            SQLServerPooledConnection spc = (SQLServerPooledConnection) pc;
            Field physicalConnectionField = SQLServerPooledConnection.class.getDeclaredField("physicalConnection");
            physicalConnectionField.setAccessible(true);
            Object con = physicalConnectionField.get(spc);
            TestUtils.setAccessTokenExpiry(con, accessToken);
            secondsBeforeExpiration = TestUtils.TEST_TOKEN_EXPIRY_SECONDS;

            // get first connection from pool
            try (Connection connection1 = pc.getConnection(); Statement stmt = connection1.createStatement()) {
                testUserName(connection1, azureUserName, authentication);
            }
            Thread.sleep(TimeUnit.SECONDS.toMillis(testingTimeInSeconds + 5)); // give 5 more to make sure the access token is expired.

            Callable<Void> c = () -> {
                try (Connection connection2 = pc.getConnection()) {
                    testUserName(connection2, azureUserName, authentication);
                } catch (SQLException e) {
                    assertTrue(INVALID_EXCEPTION_MSG + ": " + e.getMessage(),
                            e.getMessage().contains(ERR_MSG_CONNECTION_CLOSED)
                                    || e.getMessage().contains(ERR_MSG_CONNECTION_IS_CLOSED)
                                    || e.getMessage().contains(ERR_MSG_HAS_CLOSED)
                                    || e.getMessage().contains(ERR_MSG_HAS_BEEN_CLOSED)
                                    || e.getMessage().contains(ERR_MSG_SOCKET_CLOSED));
                }
                return null;
            };

            Random rand = new Random();
            int numberOfThreadsForEachType = (rand.nextInt(5) + 1) * 3; // 3 to 15
            ExecutorService es = Executors.newFixedThreadPool(3);
            List<Future<Void>> results = new ArrayList<>(numberOfThreadsForEachType);
            for (int i = 0; i < numberOfThreadsForEachType; i++) {
                results.add(es.submit(c));
            }

            // get is blocking, will wait for thread to finish
            for (Future<Void> f : results) {
                f.get(TestUtils.TEST_TOKEN_EXPIRY_SECONDS, TimeUnit.SECONDS);
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
            SQLServerPooledConnection spc = (SQLServerPooledConnection) pc;
            Field physicalConnectionField = SQLServerPooledConnection.class.getDeclaredField("physicalConnection");
            physicalConnectionField.setAccessible(true);
            Object con = physicalConnectionField.get(spc);
            TestUtils.setAccessTokenExpiry(con, accessToken);
            secondsBeforeExpiration = TestUtils.TEST_TOKEN_EXPIRY_SECONDS;

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
                f.get(TestUtils.TEST_TOKEN_EXPIRY_SECONDS, TimeUnit.SECONDS);
            }
            es.shutdown();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Tag(Constants.xSQLv11)
    @Tag(Constants.xSQLv12)
    @Tag(Constants.xSQLv14)
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.reqExternalSetup)
    @Test
    public void testDSPooledConnectionAccessTokenCallback() throws Exception {
        SQLServerConnectionPoolDataSource ds = new SQLServerConnectionPoolDataSource();

        // User/password is not required for access token callback
        AbstractTest.updateDataSource(accessTokenCallbackConnectionString, ds);
        ds.setAccessTokenCallback(TestUtils.accessTokenCallback);

        TestUtils.expireTokenToggle = false;
        SQLServerPooledConnection pc = (SQLServerPooledConnection) ds.getPooledConnection();
        String conn1ID;
        String conn2ID;

        // Callback should provide valid token on connection open for all new connections
        // When the access token hasn't expired, the connection ID should be the same
        try (Connection conn1 = pc.getConnection()) {}
        conn1ID = TestUtils.getConnectionID(pc);
        try (Connection conn2 = pc.getConnection()) {}
        conn2ID = TestUtils.getConnectionID(pc);
        assertEquals(conn1ID, conn2ID);
    }

    @Tag(Constants.xSQLv11)
    @Tag(Constants.xSQLv12)
    @Tag(Constants.xSQLv14)
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.reqExternalSetup)
    @Test
    public void testDSPooledConnectionMergeAccessTokenCallbackProperty() throws Exception {
        SQLServerConnectionPoolDataSource ds = new SQLServerConnectionPoolDataSource();

        // Access token callback property from ds.setAccessTokenCallback should merge supplied properties from URL
        ds.setURL(accessTokenCallbackConnectionString);
        ds.setAccessTokenCallback(TestUtils.accessTokenCallback);

        SQLServerPooledConnection pc = (SQLServerPooledConnection) ds.getPooledConnection();
        String conn1ID;
        String conn2ID;

        try (Connection conn1 = pc.getConnection()) {}
        conn1ID = TestUtils.getConnectionID(pc);
        try (Connection conn2 = pc.getConnection()) {}
        conn2ID = TestUtils.getConnectionID(pc);
        assertEquals(conn1ID, conn2ID);
    }

    @Tag(Constants.xSQLv11)
    @Tag(Constants.xSQLv12)
    @Tag(Constants.xSQLv14)
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.reqExternalSetup)
    @Test
    public void testDSPooledConnectionAccessTokenCallbackExpiredToken() throws Exception {
        SQLServerConnectionPoolDataSource ds = new SQLServerConnectionPoolDataSource();

        // User/password is not required for access token callback
        AbstractTest.updateDataSource(accessTokenCallbackConnectionString, ds);
        ds.setAccessTokenCallback(TestUtils.accessTokenCallback);

        SQLServerPooledConnection pc = (SQLServerPooledConnection) ds.getPooledConnection();
        String conn1ID;
        String conn2ID;

        // When token expires after first connection, it should create a new connection to get a new token.
        // Connection ID should not be the same.
        TestUtils.expireTokenToggle = true;
        pc = (SQLServerPooledConnection) ds.getPooledConnection();
        try (Connection conn1 = pc.getConnection()) {
            TestUtils.setAccessTokenExpiry(conn1);
            secondsBeforeExpiration = TestUtils.TEST_TOKEN_EXPIRY_SECONDS;
        }
        conn1ID = TestUtils.getConnectionID(pc);
        // Sleep until token expires
        Thread.sleep(secondsBeforeExpiration);
        try (Connection conn2 = pc.getConnection()) {}
        conn2ID = TestUtils.getConnectionID(pc);

        assertNotEquals(conn1ID, conn2ID);
    }

    @Tag(Constants.xSQLv11)
    @Tag(Constants.xSQLv12)
    @Tag(Constants.xSQLv14)
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.reqExternalSetup)
    @Test
    public void testDSPooledConnectionAccessTokenCallbackClass() throws Exception {
        SQLServerConnectionPoolDataSource ds = new SQLServerConnectionPoolDataSource();

        // User/password is not required for access token callback
        AbstractTest.updateDataSource(accessTokenCallbackConnectionString, ds);
        ds.setAccessTokenCallbackClass(AccessTokenCallbackClass.class.getName());

        SQLServerPooledConnection pc = (SQLServerPooledConnection) ds.getPooledConnection();
        String conn1ID;
        String conn2ID;

        // Callback should provide valid token on connection open for all new connections
        // When the access token hasn't expired, the connection ID should be the same
        try (Connection conn1 = pc.getConnection()) {}
        conn1ID = TestUtils.getConnectionID(pc);
        try (Connection conn2 = pc.getConnection()) {}
        conn2ID = TestUtils.getConnectionID(pc);
        assertEquals(conn1ID, conn2ID);
    }

    @Tag(Constants.xSQLv11)
    @Tag(Constants.xSQLv12)
    @Tag(Constants.xSQLv14)
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.reqExternalSetup)
    @Test
    public void testDSPooledConnectionAccessTokenCallbackClassExceptions() throws Exception {
        SQLServerConnectionPoolDataSource ds = new SQLServerConnectionPoolDataSource();

        // User/password is not required for access token callback
        AbstractTest.updateDataSource(accessTokenCallbackConnectionString, ds);

        ds.setAccessTokenCallbackClass(AccessTokenCallbackClass.class.getName());
        ds.setUser("user");
        ds.setPassword(UUID.randomUUID().toString());
        SQLServerPooledConnection pc;

        pc = (SQLServerPooledConnection) ds.getPooledConnection();
        try (Connection conn1 = pc.getConnection()) {
            assertNotNull(conn1);
        }

        // Should fail with invalid accessTokenCallbackClass value
        ds.setAccessTokenCallbackClass("Invalid");
        ds.setUser("");
        ds.setPassword("");
        try {
            pc = (SQLServerPooledConnection) ds.getPooledConnection();
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_InvalidAccessTokenCallbackClass")));
        }

        // Should pass with no user or password set
        ds.setAccessTokenCallbackClass(AccessTokenCallbackClass.class.getName());
        pc = (SQLServerPooledConnection) ds.getPooledConnection();
        try (Connection conn1 = pc.getConnection()) {}
    }

    @AfterAll
    public static void terminate() throws SQLException {
        try (Connection conn = DriverManager.getConnection(adPasswordConnectionStr);
                Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(charTable, stmt);
        }
    }
}
