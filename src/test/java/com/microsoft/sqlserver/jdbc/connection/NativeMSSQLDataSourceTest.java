/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.connection;

import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.microsoft.aad.msal4j.IClientCredential;
import com.microsoft.aad.msal4j.ClientCredentialFactory;
import com.microsoft.aad.msal4j.ConfidentialClientApplication;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import com.microsoft.aad.msal4j.ClientCredentialParameters;
import com.microsoft.sqlserver.jdbc.SQLServerXADataSource;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerAccessTokenCallback;
import com.microsoft.sqlserver.jdbc.SqlAuthenticationToken;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.ISQLServerDataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
public class NativeMSSQLDataSourceTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @Test
    public void testNativeMSSQLDataSource() throws SQLException {
        SQLServerXADataSource ds = new SQLServerXADataSource();
        ds.setLastUpdateCount(true);
        assertTrue(ds.getLastUpdateCount());
    }

    @Test
    public void testSerialization() throws IOException {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                ObjectOutput objectOutput = new ObjectOutputStream(outputStream)) {
            SQLServerDataSource ds = new SQLServerDataSource();
            PrintWriter out = new PrintWriter(new ByteArrayOutputStream());
            ds.setLogWriter(out);
            objectOutput.writeObject(ds);
            objectOutput.flush();

            assertTrue(out == ds.getLogWriter());
        }
    }

    @Test
    public void testDSNormal() throws ClassNotFoundException, IOException, SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setURL(connectionString);
        try (Connection conn = ds.getConnection()) {}
        ds = testSerial(ds);
        try (Connection conn = ds.getConnection()) {}
    }

    @Tag(Constants.xSQLv11)
    @Tag(Constants.xSQLv12)
    @Tag(Constants.xSQLv14)
    @Tag(Constants.xSQLv15)
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.reqExternalSetup)
    @Test
    public void testDSPooledConnectionAccessTokenCallback() throws SQLException {
        SQLServerAccessTokenCallback callback = new SQLServerAccessTokenCallback() {
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
                            .authority(stsurl).build();
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
        };

        SQLServerConnectionPoolDataSource ds = new SQLServerConnectionPoolDataSource();
        AbstractTest.updateDataSource(connectionString, ds);
        ds.setUser("");
        ds.setPassword("");
        ds.setAccessTokenCallback(callback);

        // Callback should provide valid token on connection open for all new connections
        try (Connection conn1 = (SQLServerConnection) ds.getConnection()) {}
        try (Connection conn2 = (SQLServerConnection) ds.getConnection()) {}
    }

    @Test
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.xAzureSQLDB)
    public void testDSTSPassword() throws ClassNotFoundException, IOException, SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();
        System.setProperty("java.net.preferIPv6Addresses", Boolean.TRUE.toString());
        ds.setURL(connectionString);
        ds.setTrustStorePassword("wrong_password");
        try (Connection conn = ds.getConnection()) {} catch (SQLException e) {
            assert (e.getMessage().contains(TestResource.getResource("R_keystorePassword")));
        }
        ds = testSerial(ds);
        try (Connection conn = ds.getConnection()) {} catch (SQLException e) {
            assertEquals(TestResource.getResource("R_trustStorePasswordNotSet"), e.getMessage());
        }
    }

    @Test
    public void testInterfaceWrapping() throws ClassNotFoundException, SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();
        assertEquals(true, ds.isWrapperFor(Class.forName(Constants.MSSQL_JDBC_PACKAGE + ".ISQLServerDataSource")));
        assertEquals(true, ds.isWrapperFor(Class.forName(Constants.MSSQL_JDBC_PACKAGE + ".SQLServerDataSource")));
        assertEquals(true, ds.isWrapperFor(Class.forName("javax.sql.CommonDataSource")));
        ISQLServerDataSource ids = (ISQLServerDataSource) (ds
                .unwrap(Class.forName(Constants.MSSQL_JDBC_PACKAGE + ".ISQLServerDataSource")));
        ids.setApplicationName("AppName");

        SQLServerConnectionPoolDataSource poolDS = new SQLServerConnectionPoolDataSource();
        assertEquals(true, poolDS.isWrapperFor(Class.forName(Constants.MSSQL_JDBC_PACKAGE + ".ISQLServerDataSource")));
        assertEquals(true, poolDS.isWrapperFor(Class.forName(Constants.MSSQL_JDBC_PACKAGE + ".SQLServerDataSource")));
        assertEquals(true, poolDS
                .isWrapperFor(Class.forName(Constants.MSSQL_JDBC_PACKAGE + ".SQLServerConnectionPoolDataSource")));
        assertEquals(true, poolDS.isWrapperFor(Class.forName("javax.sql.CommonDataSource")));
        ISQLServerDataSource ids2 = (ISQLServerDataSource) (poolDS
                .unwrap(Class.forName(Constants.MSSQL_JDBC_PACKAGE + ".ISQLServerDataSource")));
        ids2.setApplicationName("AppName");

        SQLServerXADataSource xaDS = new SQLServerXADataSource();
        assertEquals(true, xaDS.isWrapperFor(Class.forName(Constants.MSSQL_JDBC_PACKAGE + ".ISQLServerDataSource")));
        assertEquals(true, xaDS.isWrapperFor(Class.forName(Constants.MSSQL_JDBC_PACKAGE + ".SQLServerDataSource")));
        assertEquals(true,
                xaDS.isWrapperFor(Class.forName(Constants.MSSQL_JDBC_PACKAGE + ".SQLServerConnectionPoolDataSource")));
        assertEquals(true, xaDS.isWrapperFor(Class.forName(Constants.MSSQL_JDBC_PACKAGE + ".SQLServerXADataSource")));
        assertEquals(true, xaDS.isWrapperFor(Class.forName("javax.sql.CommonDataSource")));
        ISQLServerDataSource ids3 = (ISQLServerDataSource) (xaDS
                .unwrap(Class.forName(Constants.MSSQL_JDBC_PACKAGE + ".ISQLServerDataSource")));
        ids3.setApplicationName("AppName");
    }

    @Test
    public void testDSReference() {
        SQLServerDataSource ds = new SQLServerDataSource();
        assertTrue(ds.getReference().getClassName().equals("com.microsoft.sqlserver.jdbc.SQLServerDataSource"));
    }

    private SQLServerDataSource testSerial(SQLServerDataSource ds) throws IOException, ClassNotFoundException {
        try (java.io.ByteArrayOutputStream outputStream = new java.io.ByteArrayOutputStream();
                java.io.ObjectOutput objectOutput = new java.io.ObjectOutputStream(outputStream)) {
            objectOutput.writeObject(ds);
            objectOutput.flush();

            try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(outputStream.toByteArray()))) {
                SQLServerDataSource dtn;
                dtn = (SQLServerDataSource) in.readObject();
                return dtn;
            }
        }
    }
}
