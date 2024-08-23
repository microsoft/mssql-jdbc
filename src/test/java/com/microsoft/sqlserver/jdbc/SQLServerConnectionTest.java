/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.Reader;
import java.lang.management.ManagementFactory;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import javax.sql.ConnectionEvent;
import javax.sql.PooledConnection;

import org.junit.Assume;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.aad.msal4j.TokenCache;
import com.microsoft.aad.msal4j.TokenCacheAccessContext;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


@RunWith(JUnitPlatform.class)
public class SQLServerConnectionTest extends AbstractTest {
    // If no retry is done, the function should at least exit in 5 seconds
    static int threshHoldForNoRetryInMilliseconds = 5000;
    static int loginTimeOutInSeconds = 10;
    static String tnirHost = getConfiguredProperty("tnirHost");

    String randomServer = RandomUtil.getIdentifier("Server");

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    /**
     * Test connection properties with SQLServerDataSource
     * 
     * @throws SQLServerException
     */
    @Test
    public void testDataSource() throws SQLServerException {
        SQLServerDataSource ds = new SQLServerDataSource();
        String stringPropValue = "stringPropValue";
        boolean booleanPropValue = true;
        int intPropValue = 1;

        ds.setInstanceName(stringPropValue);
        assertEquals(stringPropValue, ds.getInstanceName(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setUser(stringPropValue);
        assertEquals(stringPropValue, ds.getUser(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setPassword(stringPropValue);
        assertEquals(stringPropValue, ds.getPassword(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setApplicationName(stringPropValue);
        assertEquals(stringPropValue, ds.getApplicationName(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setDatabaseName(stringPropValue);
        assertEquals(stringPropValue, ds.getDatabaseName(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setPortNumber(intPropValue);
        assertEquals(intPropValue, ds.getPortNumber(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setIPAddressPreference(stringPropValue);
        assertEquals(stringPropValue, ds.getIPAddressPreference(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setURL(stringPropValue);
        assertEquals(stringPropValue, ds.getURL(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setDescription(stringPropValue);
        assertEquals(stringPropValue, ds.getDescription(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setPacketSize(intPropValue);
        assertEquals(intPropValue, ds.getPacketSize(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setQueryTimeout(intPropValue);
        assertEquals(intPropValue, ds.getQueryTimeout(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setCancelQueryTimeout(intPropValue);
        assertEquals(intPropValue, ds.getCancelQueryTimeout(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setEnablePrepareOnFirstPreparedStatementCall(booleanPropValue);
        assertEquals(booleanPropValue, ds.getEnablePrepareOnFirstPreparedStatementCall(),
                TestResource.getResource("R_valuesAreDifferent"));

        ds.setEnablePrepareOnFirstPreparedStatementCall(booleanPropValue);
        assertEquals(booleanPropValue, ds.getEnablePrepareOnFirstPreparedStatementCall(),
                TestResource.getResource("R_valuesAreDifferent"));

        ds.setServerPreparedStatementDiscardThreshold(intPropValue);
        assertEquals(intPropValue, ds.getServerPreparedStatementDiscardThreshold(),
                TestResource.getResource("R_valuesAreDifferent"));

        ds.setStatementPoolingCacheSize(intPropValue);
        assertEquals(intPropValue, ds.getStatementPoolingCacheSize(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setDisableStatementPooling(booleanPropValue);
        assertEquals(booleanPropValue, ds.getDisableStatementPooling(),
                TestResource.getResource("R_valuesAreDifferent"));

        ds.setSocketTimeout(intPropValue);
        assertEquals(intPropValue, ds.getSocketTimeout(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setUseBulkCopyForBatchInsert(booleanPropValue);
        assertEquals(booleanPropValue, ds.getUseBulkCopyForBatchInsert(),
                TestResource.getResource("R_valuesAreDifferent"));

        ds.setJAASConfigurationName(stringPropValue);
        assertEquals(stringPropValue, ds.getJAASConfigurationName(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setUseDefaultJaasConfig(booleanPropValue);
        assertEquals(booleanPropValue, ds.getUseDefaultJaasConfig(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setMSIClientId(stringPropValue);
        assertEquals(stringPropValue, ds.getMSIClientId(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setAuthenticationScheme(stringPropValue);
        // there is no corresponding getAuthenticationScheme

        ds.setAuthentication(stringPropValue);
        assertEquals(stringPropValue, ds.getAuthentication(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setAccessToken(stringPropValue);
        assertEquals(stringPropValue, ds.getAccessToken(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setLastUpdateCount(booleanPropValue);
        assertEquals(booleanPropValue, ds.getLastUpdateCount(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setTransparentNetworkIPResolution(booleanPropValue);
        assertEquals(booleanPropValue, ds.getTransparentNetworkIPResolution(),
                TestResource.getResource("R_valuesAreDifferent"));

        ds.setTrustServerCertificate(booleanPropValue);
        assertEquals(booleanPropValue, ds.getTrustServerCertificate(),
                TestResource.getResource("R_valuesAreDifferent"));

        ds.setTrustStoreType(stringPropValue);
        assertEquals(stringPropValue, ds.getTrustStoreType(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setTrustStore(stringPropValue);
        assertEquals(stringPropValue, ds.getTrustStore(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setTrustStorePassword(stringPropValue);
        assertEquals(stringPropValue, ds.getTrustStorePassword(), TestResource.getResource("R_valuesAreDifferent"));

        // verify encrypt=true options
        ds.setEncrypt(EncryptOption.MANDATORY.toString());
        assertEquals("True", EncryptOption.valueOfString(ds.getEncrypt()).toString(),
                TestResource.getResource("R_valuesAreDifferent"));
        ds.setEncrypt(EncryptOption.TRUE.toString());
        assertEquals("True", EncryptOption.valueOfString(ds.getEncrypt()).toString(),
                TestResource.getResource("R_valuesAreDifferent"));

        // verify encrypt=false options
        ds.setEncrypt(EncryptOption.OPTIONAL.toString());
        assertEquals("False", EncryptOption.valueOfString(ds.getEncrypt()).toString(),
                TestResource.getResource("R_valuesAreDifferent"));
        ds.setEncrypt(EncryptOption.FALSE.toString());
        assertEquals("False", EncryptOption.valueOfString(ds.getEncrypt()).toString(),
                TestResource.getResource("R_valuesAreDifferent"));
        ds.setEncrypt(EncryptOption.NO.toString());
        assertEquals("False", EncryptOption.valueOfString(ds.getEncrypt()).toString(),
                TestResource.getResource("R_valuesAreDifferent"));

        // verify enrypt=strict options
        ds.setEncrypt(EncryptOption.STRICT.toString());
        assertEquals("Strict", EncryptOption.valueOfString(ds.getEncrypt()).toString(),
                TestResource.getResource("R_valuesAreDifferent"));

        ds.setEncrypt(booleanPropValue);
        assertEquals(Boolean.toString(booleanPropValue), ds.getEncrypt(),
                TestResource.getResource("R_valuesAreDifferent"));

        ds.setUseDefaultGSSCredential(booleanPropValue);
        assertEquals(booleanPropValue, ds.getUseDefaultGSSCredential(),
                TestResource.getResource("R_valuesAreDifferent"));

        ds.setUseFlexibleCallableStatements(booleanPropValue);
        assertEquals(booleanPropValue, ds.getUseFlexibleCallableStatements(),
                TestResource.getResource("R_valuesAreDifferent"));
        ds.setCalcBigDecimalPrecision(booleanPropValue);
        assertEquals(booleanPropValue, ds.getCalcBigDecimalPrecision(),
                TestResource.getResource("R_valuesAreDifferent"));

        ds.setServerCertificate(stringPropValue);
        assertEquals(stringPropValue, ds.getServerCertificate(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setPrepareMethod(stringPropValue);
        assertEquals(stringPropValue, ds.getPrepareMethod(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setHostNameInCertificate(stringPropValue);
        assertEquals(stringPropValue, ds.getHostNameInCertificate(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setLockTimeout(intPropValue);
        assertEquals(intPropValue, ds.getLockTimeout(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setSelectMethod(stringPropValue);
        assertEquals(stringPropValue, ds.getSelectMethod(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setResponseBuffering(stringPropValue);
        assertEquals(stringPropValue, ds.getResponseBuffering(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setApplicationIntent(stringPropValue);
        assertEquals(stringPropValue, ds.getApplicationIntent(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setReplication(booleanPropValue);
        assertEquals(booleanPropValue, ds.getReplication(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setSendTimeAsDatetime(booleanPropValue);
        assertEquals(booleanPropValue, ds.getSendTimeAsDatetime(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setDatetimeParameterType("datetime2");
        assertEquals("datetime2", ds.getDatetimeParameterType(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setUseFmtOnly(booleanPropValue);
        assertEquals(booleanPropValue, ds.getUseFmtOnly(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setSendStringParametersAsUnicode(booleanPropValue);
        assertEquals(booleanPropValue, ds.getSendStringParametersAsUnicode(),
                TestResource.getResource("R_valuesAreDifferent"));

        ds.setServerNameAsACE(booleanPropValue);
        assertEquals(booleanPropValue, ds.getServerNameAsACE(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setServerName(stringPropValue);
        assertEquals(stringPropValue, ds.getServerName(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setRealm(stringPropValue);
        assertEquals(stringPropValue, ds.getRealm(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setServerSpn(stringPropValue);
        assertEquals(stringPropValue, ds.getServerSpn(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setFailoverPartner(stringPropValue);
        assertEquals(stringPropValue, ds.getFailoverPartner(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setMultiSubnetFailover(booleanPropValue);
        assertEquals(booleanPropValue, ds.getMultiSubnetFailover(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setWorkstationID(stringPropValue);
        assertEquals(stringPropValue, ds.getWorkstationID(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setXopenStates(booleanPropValue);
        assertEquals(booleanPropValue, ds.getXopenStates(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setFIPS(booleanPropValue);
        assertEquals(booleanPropValue, ds.getFIPS(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setSSLProtocol(stringPropValue);
        assertEquals(stringPropValue, ds.getSSLProtocol(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setTrustManagerClass(stringPropValue);
        assertEquals(stringPropValue, ds.getTrustManagerClass(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setTrustManagerConstructorArg(stringPropValue);
        assertEquals(stringPropValue, ds.getTrustManagerConstructorArg(),
                TestResource.getResource("R_valuesAreDifferent"));

        ds.setHostNameInCertificate(stringPropValue);
        assertEquals(stringPropValue, ds.getHostNameInCertificate(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setDomain(stringPropValue);
        assertEquals(stringPropValue, ds.getDomain(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setColumnEncryptionSetting(stringPropValue);
        assertEquals(stringPropValue, ds.getColumnEncryptionSetting(),
                TestResource.getResource("R_valuesAreDifferent"));

        ds.setKeyStoreAuthentication(stringPropValue);
        assertEquals(stringPropValue, ds.getKeyStoreAuthentication(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setKeyStoreSecret(stringPropValue);
        // there is no corresponding getKeyStoreSecret

        ds.setKeyStoreLocation(stringPropValue);
        assertEquals(stringPropValue, ds.getKeyStoreLocation(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setEnclaveAttestationUrl(stringPropValue);
        assertTrue(ds.getEnclaveAttestationUrl().equals(stringPropValue));

        ds.setEnclaveAttestationProtocol(stringPropValue);
        assertTrue(ds.getEnclaveAttestationProtocol().equals(stringPropValue));

        ds.setKeyVaultProviderClientId(stringPropValue);
        assertTrue(ds.getKeyVaultProviderClientId().equals(stringPropValue));

        ds.setKeyVaultProviderClientKey(stringPropValue);
        // there is no corresponding getKeyVaultProviderClientKey

        ds.setKeyStorePrincipalId(stringPropValue);
        assertTrue(ds.getKeyStorePrincipalId().equals(stringPropValue));
    }

    @Test
    public void testDSConnection() {
        SQLServerDataSource ds = new SQLServerDataSource();
        updateDataSource(connectionString, ds);

        // getPassword can only be accessed within package
        try (Connection con = ds.getConnection(ds.getUser(), ds.getPassword())) {} catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
        }
    }

    @Test
    public void testEncryptedConnection() throws SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setApplicationName("User");
        ds.setURL(connectionString);
        if (encrypt == null) {
            ds.setEncrypt(Constants.TRUE);
        }
        if (trustServerCertificate == null) {
            ds.setTrustServerCertificate(true);
        }
        ds.setPacketSize(8192);
        try (Connection con = ds.getConnection()) {}
    }

    @Tag(Constants.xSQLv11)
    @Tag(Constants.xSQLv12)
    @Tag(Constants.xSQLv14)
    @Tag(Constants.xSQLv15)
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.xAzureSQLDB)
    @Test
    public void testEncryptedStrictConnection() throws SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setURL(connectionString);
        ds.setServerCertificate(serverCertificate);
        ds.setEncrypt(Constants.STRICT);

        try (Connection con = ds.getConnection()) {}
    }

    @Test
    public void testJdbcDataSourceMethod() throws SQLFeatureNotSupportedException {
        SQLServerDataSource fxds = new SQLServerDataSource();
        Logger logger = fxds.getParentLogger();
        assertEquals(logger.getName(), Constants.MSSQL_JDBC_PACKAGE,
                TestResource.getResource("R_parrentLoggerNameWrong"));
    }

    class MyEventListener implements javax.sql.ConnectionEventListener {
        boolean connClosed = false;
        boolean errorOccurred = false;

        public MyEventListener() {}

        public void connectionClosed(ConnectionEvent event) {
            connClosed = true;
        }

        public void connectionErrorOccurred(ConnectionEvent event) {
            errorOccurred = true;
        }
    }

    /**
     * Attach the Event listener and listen for connection events, fatal errors should not close the pooled connection
     * objects
     *
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.xAzureSQLDB)
    public void testConnectionEvents() throws SQLException {
        SQLServerConnectionPoolDataSource mds = new SQLServerConnectionPoolDataSource();
        mds.setURL(connectionString);
        PooledConnection pooledConnection = mds.getPooledConnection();

        // Attach the Event listener and listen for connection events.
        MyEventListener myE = new MyEventListener();
        pooledConnection.addConnectionEventListener(myE); // ConnectionListener implements ConnectionEventListener

        try (Connection con = pooledConnection.getConnection();
                Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
            boolean exceptionThrown = false;
            try {
                // raise a severe exception and make sure that the connection is not
                // closed.
                stmt.executeUpdate("RAISERROR ('foo', 20,1) WITH LOG");
            } catch (Exception e) {
                exceptionThrown = true;
            }
            assertTrue(exceptionThrown, TestResource.getResource("R_expectedExceptionNotThrown"));

            // Check to see if error occurred.
            assertTrue(myE.errorOccurred, TestResource.getResource("R_errorNotCalled"));
        } finally {
            // make sure that connection is closed.
            if (null != pooledConnection)
                pooledConnection.close();
        }
    }

    @Test
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.xAzureSQLDB)
    public void testConnectionPoolGetTwice() throws SQLException {
        SQLServerConnectionPoolDataSource mds = new SQLServerConnectionPoolDataSource();
        mds.setURL(connectionString);
        PooledConnection pooledConnection = mds.getPooledConnection();

        // Attach the Event listener and listen for connection events.
        MyEventListener myE = new MyEventListener();
        pooledConnection.addConnectionEventListener(myE); // ConnectionListener implements ConnectionEventListener

        try (Connection con = pooledConnection.getConnection();
                Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
            // raise a non severe exception and make sure that the connection is not closed.
            stmt.executeUpdate("RAISERROR ('foo', 3,1)");
            // not a serious error there should not be any errors.
            assertTrue(!myE.errorOccurred, TestResource.getResource("R_errorCalled"));
            // check to make sure that connection is not closed.
            assertTrue(!con.isClosed(), TestResource.getResource("R_connectionIsClosed"));
            stmt.close();
            con.close();
            // check to make sure that connection is closed.
            assertTrue(con.isClosed(), TestResource.getResource("R_connectionIsNotClosed"));
        } finally {
            // make sure that connection is closed.
            if (null != pooledConnection)
                pooledConnection.close();
        }
    }

    /**
     * Tests whether connectRetryCount and connectRetryInterval are properly respected in the login loop. As well, tests
     * that connection is retried the proper number of times.
     */
    @Test
    public void testConnectCountInLoginAndCorrectRetryCount() {
        long timerStart = 0;

        int connectRetryCount = 0;
        int connectRetryInterval = 60;
        int longLoginTimeout = loginTimeOutInSeconds * 3; // 90 seconds

        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setURL(connectionString);
            ds.setLoginTimeout(longLoginTimeout);
            ds.setConnectRetryCount(connectRetryCount);
            ds.setConnectRetryInterval(connectRetryInterval);
            ds.setDatabaseName(RandomUtil.getIdentifier("DataBase"));
            timerStart = System.currentTimeMillis();

            try (Connection con = ds.getConnection()) {
                assertTrue(con == null, TestResource.getResource("R_shouldNotConnect"));
            }
        } catch (Exception e) {
            assertTrue(
                    e.getMessage().contains(TestResource.getResource("R_cannotOpenDatabase"))
                            || (TestUtils.getProperty(connectionString, "msiClientId") != null && (e.getMessage()
                                    .toLowerCase().contains(TestResource.getResource("R_loginFailedMI").toLowerCase())
                                    || e.getMessage().toLowerCase()
                                            .contains(TestResource.getResource("R_MInotAvailable").toLowerCase()))),
                    e.getMessage());
            long totalTime = System.currentTimeMillis() - timerStart;

            // Maximum is unknown, but is needs to be less than longLoginTimeout or else this is an issue.
            assertTrue(totalTime < (longLoginTimeout * 1000L), TestResource.getResource("R_executionTooLong"));
        }
    }

    // Test connect retry 0 but should still connect to TNIR
    @Test
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.xAzureSQLDB)
    @Tag(Constants.reqExternalSetup)
    public void testConnectTnir() {
        org.junit.Assume.assumeTrue(isWindows);

        // no retries but should connect to TNIR (this assumes host is defined in host file
        try (Connection con = PrepUtil
                .getConnection(connectionString + ";transparentNetworkIPResolution=true;connectRetryCount=0;serverName="
                        + tnirHost);) {} catch (Exception e) {
            fail(e.getMessage());
        }
    }

    // Test connect retry 0 and TNIR disabled
    @Test
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.xAzureSQLDB)
    @Tag(Constants.reqExternalSetup)
    public void testConnectNoTnir() {
        org.junit.Assume.assumeTrue(isWindows);

        // no retries no TNIR should fail even tho host is defined in host file
        try (Connection con = PrepUtil.getConnection(connectionString
                + ";transparentNetworkIPResolution=false;connectRetryCount=0;serverName=" + tnirHost);) {
            assertTrue(con == null, TestResource.getResource("R_shouldNotConnect"));
        } catch (Exception e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_tcpipConnectionFailed"))
                    || ((isSqlAzure() || isSqlAzureDW())
                                                         ? e.getMessage().contains(
                                                                 TestResource.getResource("R_connectTimedOut"))
                                                         : false),
                    e.getMessage());
        }
    }

    @Test
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.xAzureSQLDB)
    public void testConnectionClosed() throws SQLException {
        SQLServerDataSource mds = new SQLServerDataSource();
        mds.setURL(connectionString);
        try (Connection con = mds.getConnection();
                Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
            boolean exceptionThrown = false;
            try {
                stmt.executeUpdate("RAISERROR ('foo', 20,1) WITH LOG");
            } catch (Exception e) {
                exceptionThrown = true;
            }
            assertTrue(exceptionThrown, TestResource.getResource("R_expectedExceptionNotThrown"));

            // check to make sure that connection is closed.
            assertTrue(con.isClosed(), TestResource.getResource("R_connectionIsNotClosed"));
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
        }
    }

    @Test
    public void testIsWrapperFor() throws SQLException, ClassNotFoundException {
        try (Connection conn = getConnection(); SQLServerConnection ssconn = (SQLServerConnection) conn) {
            boolean isWrapper;
            isWrapper = ssconn.isWrapperFor(ssconn.getClass());
            MessageFormat form = new MessageFormat(TestResource.getResource("R_supportUnwrapping"));
            Object[] msgArgs1 = {"SQLServerConnection"};

            assertTrue(isWrapper, form.format(msgArgs1));
            assertEquals(ISQLServerConnection.TRANSACTION_SNAPSHOT, ISQLServerConnection.TRANSACTION_SNAPSHOT,
                    TestResource.getResource("R_cantAccessSnapshot"));

            isWrapper = ssconn.isWrapperFor(Class.forName(Constants.MSSQL_JDBC_PACKAGE + ".ISQLServerConnection"));
            Object[] msgArgs2 = {"ISQLServerConnection"};
            assertTrue(isWrapper, form.format(msgArgs2));

            ssconn.unwrap(Class.forName(Constants.MSSQL_JDBC_PACKAGE + ".ISQLServerConnection"));
            assertEquals(ISQLServerConnection.TRANSACTION_SNAPSHOT, ISQLServerConnection.TRANSACTION_SNAPSHOT,
                    TestResource.getResource("R_cantAccessSnapshot"));

            ssconn.unwrap(Class.forName("java.sql.Connection"));
        }
    }

    @Test
    public void testNewConnection() throws SQLException {
        try (Connection conn = getConnection()) {
            assertTrue(conn.isValid(0), TestResource.getResource("R_newConnectionShouldBeValid"));
        }
    }

    @Test
    public void testClosedConnection() throws SQLException {
        try (Connection conn = getConnection()) {
            conn.close();
            assertTrue(!conn.isValid(0), TestResource.getResource("R_closedConnectionShouldBeInvalid"));

            // getting shared timer on closed connection should fail
            ((SQLServerConnection) conn).getSharedTimer();
            fail(TestResource.getResource("R_noExceptionClosedConnection"));
        } catch (SQLServerException e) {
            assertEquals(e.getMessage(), TestResource.getResource("R_connectionIsClosed"),
                    TestResource.getResource("R_wrongExceptionMessage"));
            assertEquals("08S01", e.getSQLState(), TestResource.getResource("R_wrongSqlState"));
        }
        try (Connection conn = getConnection()) {
            conn.close();
            ((SQLServerConnection) conn).checkClosed();
            fail(TestResource.getResource("R_noExceptionClosedConnection"));
        } catch (SQLServerException e) {
            assertEquals(e.getMessage(), TestResource.getResource("R_connectionIsClosed"),
                    TestResource.getResource("R_wrongExceptionMessage"));
            assertEquals("08S01", e.getSQLState(), TestResource.getResource("R_wrongSqlState"));
        }
    }

    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testSetTransactionIsolation() throws SQLException {
        try (Connection conn = getConnection()) {
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            assertTrue(conn.getTransactionIsolation() == Connection.TRANSACTION_READ_COMMITTED,
                    TestResource.getResource("R_expectedValue") + "Connection.TRANSACTION_READ_COMMITTED");
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            assertTrue(conn.getTransactionIsolation() == Connection.TRANSACTION_READ_UNCOMMITTED,
                    TestResource.getResource("R_expectedValue") + "Connection.TRANSACTION_READ_UNCOMMITTED");
            conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            assertTrue(conn.getTransactionIsolation() == Connection.TRANSACTION_REPEATABLE_READ,
                    TestResource.getResource("R_expectedValue") + "Connection.TRANSACTION_REPEATABLE_READ");
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            assertTrue(conn.getTransactionIsolation() == Connection.TRANSACTION_SERIALIZABLE,
                    TestResource.getResource("R_expectedValue") + "Connection.TRANSACTION_SERIALIZABLE");
            conn.setTransactionIsolation(SQLServerConnection.TRANSACTION_SNAPSHOT);
            assertTrue(conn.getTransactionIsolation() == SQLServerConnection.TRANSACTION_SNAPSHOT,
                    TestResource.getResource("R_expectedValue") + "SQLServerConnection.TRANSACTION_SNAPSHOT");
        }
    }

    @Test
    public void testNativeSQL() throws SQLException {
        try (Connection conn = getConnection()) {
            String nativeSql = conn.nativeSQL("SELECT @@version");
            assertTrue(nativeSql instanceof String);
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
        }
    }

    @Test
    public void testReadOnly() throws SQLException {
        try (Connection conn = getConnection()) {
            // not supported, will always return false
            conn.setReadOnly(false);
            assertTrue(!conn.isReadOnly(), TestResource.getResource("R_expectedValue") + Boolean.toString(false));
            conn.setReadOnly(true);
            assertTrue(!conn.isReadOnly(), TestResource.getResource("R_expectedValue") + Boolean.toString(false));
        }
    }

    @Test
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.xAzureSQLDB)
    @Tag(Constants.xAzureSQLMI)
    public void testCatalog() throws SQLException {
        try (Connection conn = getConnection()) {
            String catalog = "master";
            conn.setCatalog(catalog);
            assertTrue(conn.getCatalog().equals(catalog), TestResource.getResource("R_expectedValue") + catalog);
        }
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testTypeMap() throws SQLException {
        try (Connection conn = getConnection()) {
            java.util.HashMap map = new java.util.HashMap();
            conn.setTypeMap(map);
            assertTrue(conn.getTypeMap().isEmpty(),
                    TestResource.getResource("R_expectedValue") + Boolean.toString(true));
        }
    }

    @Test
    public void testNegativeTimeout() throws Exception {
        try (Connection conn = getConnection()) {
            try {
                conn.isValid(-42);
                fail(TestResource.getResource("R_noExceptionNegativeTimeout"));
            } catch (SQLException e) {
                MessageFormat form = new MessageFormat(TestResource.getResource("R_invalidQueryTimeout"));
                Object[] msgArgs = {"-42"};

                assertEquals(e.getMessage(), form.format(msgArgs), TestResource.getResource("R_wrongExceptionMessage"));
            }
        }
    }

    @Test
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.xAzureSQLDB)
    public void testDeadConnection() throws SQLException {
        String tableName = RandomUtil.getIdentifier("ConnectionTestTable");
        try (Connection conn = PrepUtil.getConnection(connectionString + ";responseBuffering=adaptive");
                Statement stmt = conn.createStatement()) {

            conn.setAutoCommit(false);
            stmt.executeUpdate(
                    "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (col1 int primary key)");
            for (int i = 0; i < 80; i++) {
                stmt.executeUpdate("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + "(col1) values ("
                        + i + ")");
            }
            conn.commit();
            try {
                stmt.execute("SELECT x1.col1 as foo, x2.col1 as bar, x1.col1 as eeep FROM "
                        + AbstractSQLGenerator.escapeIdentifier(tableName) + " as x1, "
                        + AbstractSQLGenerator.escapeIdentifier(tableName)
                        + " as x2; RAISERROR ('Oops', 21, 42) WITH LOG");
            } catch (SQLException e) {
                assertEquals(e.getMessage(), TestResource.getResource("R_connectionReset"),
                        TestResource.getResource("R_unknownException"));
            }
            assertEquals(conn.isValid(5), false, TestResource.getResource("R_deadConnection"));
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
        } finally {
            if (null != tableName) {
                try (Connection conn = PrepUtil.getConnection(connectionString + ";responseBuffering=adaptive");
                        Statement stmt = conn.createStatement()) {
                    stmt.execute("drop table " + AbstractSQLGenerator.escapeIdentifier(tableName));
                }
            }
        }
    }

    @Test
    public void testClientConnectionId() throws Exception {
        try (ISQLServerConnection conn = (ISQLServerConnection) getConnection()) {
            assertTrue(conn.getClientConnectionId() != null, TestResource.getResource("R_clientConnectionIdNull"));
            conn.close();
            try {
                // Call getClientConnectionId on a closed connection, should raise exception
                conn.getClientConnectionId();
                fail(TestResource.getResource("R_noExceptionClosedConnection"));
            } catch (SQLException e) {
                assertEquals(TestResource.getResource("R_connectionIsClosed"), e.getMessage(),

                        TestResource.getResource("R_wrongExceptionMessage"));
                assertEquals("08S01", e.getSQLState(), TestResource.getResource("R_wrongSqlState"));
            }
        }

        // Wrong database, ClientConnectionId should be available in error message
        try (Connection conn = PrepUtil.getConnection(connectionString + ";databaseName="
                + RandomUtil.getIdentifierForDB("DataBase") + Constants.SEMI_COLON)) {
            conn.close();

        } catch (SQLException e) {
            assertTrue(
                    (e.getMessage().indexOf("ClientConnectionId") != -1)
                            || ((isSqlAzure() || isSqlAzureDW())
                                                                 ? e.getMessage().contains(
                                                                         TestResource.getResource("R_connectTimedOut"))
                                                                 : false),
                    TestResource.getResource("R_unexpectedWrongDB") + ": " + e.getMessage());
        }

        // Non-existent host, ClientConnectionId should not be available in error message
        try (Connection conn = PrepUtil.getConnection(
                connectionString + ";instanceName=" + RandomUtil.getIdentifier("Instance") + ";logintimeout=5;")) {
            conn.close();

        } catch (SQLException e) {
            assertTrue(
                    (!(e.getMessage().indexOf("ClientConnectionId") != -1))
                            || ((isSqlAzure() || isSqlAzureDW())
                                                                 ? e.getMessage().contains(
                                                                         TestResource.getResource("R_connectTimedOut"))
                                                                 : false),
                    TestResource.getResource("R_unexpectedWrongHost") + ": " + e.getMessage());
        }
    }

    @Test
    public void testIncorrectDatabase() throws SQLException {
        long timerStart = 0;
        long timerEnd = 0;
        final long milsecs = threshHoldForNoRetryInMilliseconds;
        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setURL(connectionString);
            ds.setLoginTimeout(loginTimeOutInSeconds);
            ds.setDatabaseName(RandomUtil.getIdentifier("DataBase"));
            timerStart = System.currentTimeMillis();
            try (Connection con = ds.getConnection()) {

                long timeDiff = timerEnd - timerStart;
                assertTrue(con == null, TestResource.getResource("R_shouldNotConnect"));

                MessageFormat form = new MessageFormat(TestResource.getResource("R_exitedMoreSeconds"));
                Object[] msgArgs = {milsecs / 1000};
                assertTrue(timeDiff <= milsecs, form.format(msgArgs));
            }
        } catch (Exception e) {
            assertTrue(
                    e.getMessage().contains(TestResource.getResource("R_cannotOpenDatabase"))
                            || (TestUtils.getProperty(connectionString, "msiClientId") != null
                                    && e.getMessage().toLowerCase()
                                            .contains(TestResource.getResource("R_loginFailedMI").toLowerCase())),
                    e.getMessage());
            timerEnd = System.currentTimeMillis();
        }
    }

    @Test
    public void testIncorrectUserName() throws SQLException {
        String auth = TestUtils.getProperty(connectionString, "authentication");
        org.junit.Assume.assumeTrue(auth != null
                && (auth.equalsIgnoreCase("SqlPassword") || auth.equalsIgnoreCase("ActiveDirectoryPassword")));

        long timerStart = 0;
        long timerEnd = 0;
        final long milsecs = threshHoldForNoRetryInMilliseconds;
        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setURL(connectionString);
            ds.setLoginTimeout(loginTimeOutInSeconds);
            ds.setUser(RandomUtil.getIdentifier("User"));
            timerStart = System.currentTimeMillis();
            try (Connection con = ds.getConnection()) {
                long timeDiff = timerEnd - timerStart;
                assertTrue(con == null, TestResource.getResource("R_shouldNotConnect"));
                MessageFormat form = new MessageFormat(TestResource.getResource("R_exitedMoreSeconds"));
                Object[] msgArgs = {milsecs / 1000};
                assertTrue(timeDiff <= milsecs, form.format(msgArgs));
            }
        } catch (Exception e) {
            assertTrue(
                    e.getMessage().contains(TestResource.getResource("R_loginFailed"))
                            || (TestUtils.getProperty(connectionString, "msiClientId") != null
                                    && e.getMessage().toLowerCase()
                                            .contains(TestResource.getResource("R_loginFailedMI").toLowerCase())),
                    e.getMessage());
            timerEnd = System.currentTimeMillis();
        }
    }

    @Test
    public void testIncorrectPassword() throws SQLException {
        String auth = TestUtils.getProperty(connectionString, "authentication");
        org.junit.Assume.assumeTrue(auth != null
                && (auth.equalsIgnoreCase("SqlPassword") || auth.equalsIgnoreCase("ActiveDirectoryPassword")));

        long timerStart = 0;
        long timerEnd = 0;
        final long milsecs = threshHoldForNoRetryInMilliseconds;
        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setURL(connectionString);
            ds.setLoginTimeout(loginTimeOutInSeconds);
            ds.setPassword(RandomUtil.getIdentifier("Password"));
            timerStart = System.currentTimeMillis();
            try (Connection con = ds.getConnection()) {
                long timeDiff = timerEnd - timerStart;
                assertTrue(con == null, TestResource.getResource("R_shouldNotConnect"));
                MessageFormat form = new MessageFormat(TestResource.getResource("R_exitedMoreSeconds"));
                Object[] msgArgs = {milsecs / 1000};
                assertTrue(timeDiff <= milsecs, form.format(msgArgs));
            }
        } catch (Exception e) {
            assertTrue(
                    e.getMessage().contains(TestResource.getResource("R_loginFailed"))
                            || (TestUtils.getProperty(connectionString, "msiClientId") != null
                                    && e.getMessage().toLowerCase()
                                            .contains(TestResource.getResource("R_loginFailedMI").toLowerCase())),
                    e.getMessage());
            timerEnd = System.currentTimeMillis();
        }
    }

    @Test
    public void testInvalidCombination() throws SQLException {
        long timerStart = 0;
        long timerEnd = 0;
        final long milsecs = threshHoldForNoRetryInMilliseconds;
        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setURL(connectionString);
            ds.setLoginTimeout(loginTimeOutInSeconds);
            ds.setMultiSubnetFailover(true);
            ds.setFailoverPartner(RandomUtil.getIdentifier("FailoverPartner"));
            timerStart = System.currentTimeMillis();
            try (Connection con = ds.getConnection()) {
                long timeDiff = timerEnd - timerStart;
                assertTrue(con == null, TestResource.getResource("R_shouldNotConnect"));
                MessageFormat form = new MessageFormat(TestResource.getResource("R_exitedMoreSeconds"));
                Object[] msgArgs = {milsecs / 1000};
                assertTrue(timeDiff <= milsecs, form.format(msgArgs));
            }
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(TestResource.getResource("R_connectMirrored")));
            timerEnd = System.currentTimeMillis();
        }
    }

    @Test
    @Tag("slow")
    public void testIncorrectDatabaseWithFailoverPartner() throws SQLException {
        long timerStart = 0;
        long timerEnd = 0;
        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setURL(connectionString);
            ds.setLoginTimeout(loginTimeOutInSeconds);
            ds.setDatabaseName(RandomUtil.getIdentifierForDB("DB"));
            ds.setFailoverPartner(RandomUtil.getIdentifier("FailoverPartner"));
            timerStart = System.currentTimeMillis();
            try (Connection con = ds.getConnection()) {
                long timeDiff = timerEnd - timerStart;
                assertTrue(con == null, TestResource.getResource("R_shouldNotConnect"));
                MessageFormat form = new MessageFormat(TestResource.getResource("R_exitedLessSeconds"));
                Object[] msgArgs = {loginTimeOutInSeconds - 1};
                assertTrue(timeDiff >= ((loginTimeOutInSeconds - 1) * 1000), form.format(msgArgs));
            }
        } catch (Exception e) {
            timerEnd = System.currentTimeMillis();
        }
    }

    @Test
    public void testAbortBadParam() throws SQLException {
        try (Connection conn = getConnection()) {
            try {
                conn.abort(null);
            } catch (SQLException e) {
                assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_invalidArgument")));
            }
        }
    }

    @Test
    public void testAbort() throws SQLException {
        try (Connection conn = getConnection()) {
            Executor executor = Executors.newFixedThreadPool(2);
            conn.abort(executor);
            assert (conn.isClosed());
            // abort again on closed connection
            conn.abort(executor);
        }
    }

    @Test
    public void testSetSchema() throws SQLException {
        try (Connection conn = getConnection()) {
            conn.setSchema(RandomUtil.getIdentifier("schema"));
        }
    }

    @Test
    public void testGetSchema() throws SQLException {
        try (Connection conn = getConnection()) {
            conn.getSchema();
        }
    }

    @Test
    public void testSetDatetimeParameterTypeShouldAcceptDatetime() throws SQLException {
        String expected = "datetime";
        String actual = "";

        try (SQLServerConnection conn = getConnection()) {
            conn.setDatetimeParameterType("datetime");
            actual = conn.getDatetimeParameterType();
        }

        assertEquals(expected, actual, TestResource.getResource("R_valuesAreDifferent"));
    }

    @Test
    public void testSetDatetimeParameterTypeShouldAcceptDatetime2() throws SQLException {
        String expected = "datetime2";
        String actual = "";

        try (SQLServerConnection conn = getConnection()) {
            conn.setDatetimeParameterType("datetime2");
            actual = conn.getDatetimeParameterType();
        }

        assertEquals(expected, actual, TestResource.getResource("R_valuesAreDifferent"));
    }

    @Test
    public void testSetDatetimeParameterTypeShouldAcceptDatetimeoffset() throws SQLException {
        String expected = "datetimeoffset";
        String actual = "";

        try (SQLServerConnection conn = getConnection()) {
            conn.setDatetimeParameterType("datetimeoffset");
            actual = conn.getDatetimeParameterType();
        }

        assertEquals(expected, actual, TestResource.getResource("R_valuesAreDifferent"));
    }

    @Test
    public void testSetDatetimeParameterTypeThrowExceptionWhenBadValue() throws SQLException {
        try (SQLServerConnection conn = getConnection()) {
            assertThrows(SQLException.class, () -> {
                conn.setDatetimeParameterType("some_invalid_value");
            });
        }
    }

    @Test
    public void testGetDatetimeParameterTypeShouldReturnDatetime2AsDefault() throws SQLException {
        String expected = "datetime2";
        String actual = "";

        try (SQLServerConnection conn = getConnection()) {
            actual = conn.getDatetimeParameterType();
        }

        assertEquals(expected, actual, TestResource.getResource("R_valuesAreDifferent"));
    }

    @Test
    public void testGetDatetimeParameterTypeShouldConvertDatetimeParameterTypeToLowercase() throws SQLException {
        String expected = "datetime2";
        String actual = "";

        try (SQLServerConnection conn = getConnection()) {
            conn.setDatetimeParameterType("DATETIME2");
            actual = conn.getDatetimeParameterType();
        }

        assertEquals(expected, actual, TestResource.getResource("R_valuesAreDifferent"));
    }

    /**
     * Test thread's interrupt status is not cleared.
     *
     * @throws InterruptedException
     */
    @Test
    @Tag("slow")
    public void testThreadInterruptedStatus() throws InterruptedException {
        Runnable runnable = new Runnable() {
            public void run() {
                SQLServerDataSource ds = new SQLServerDataSource();

                ds.setURL(connectionString);
                ds.setServerName("invalidServerName" + UUID.randomUUID());
                ds.setLoginTimeout(30);
                ds.setConnectRetryCount(3);
                ds.setConnectRetryInterval(10);
                try (Connection con = ds.getConnection()) {} catch (SQLException e) {}
            }
        };

        ExecutorService executor = Executors.newFixedThreadPool(1);
        Future<?> future = executor.submit(runnable);

        Thread.sleep(1000);

        // interrupt the thread in the Runnable
        boolean status = future.cancel(true);
        Thread.sleep(8000);
        executor.shutdownNow();

        assertTrue(status && future.isCancelled(), TestResource.getResource("R_threadInterruptNotSet"));
    }

    /**
     * Test thread count when finding socket using threading.
     */
    @Test
    @Tag(Constants.xAzureSQLDB)
    @Tag(Constants.xAzureSQLDW)
    public void testThreadCountWhenFindingSocket() {
        ExecutorService executor = null;
        ManagementFactory.getThreadMXBean().resetPeakThreadCount();

        // First, check to see if there is a reachable local host, or else test will fail.
        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setServerName("localhost");
            Connection con = ds.getConnection();
        } catch (SQLServerException e) {
            // Assume this will be an error different than 'localhost is unreachable'. If it is 'localhost is
            // unreachable' abort and skip the test.
            Assume.assumeFalse(e.getMessage().startsWith(TestResource.getResource("R_tcpipConnectionToHost")));
        }

        try {
            executor = Executors.newSingleThreadExecutor(r -> new Thread(r, ""));
            executor.submit(() -> {
                try {
                    SQLServerDataSource ds = new SQLServerDataSource();
                    ds.setServerName("localhost");
                    Thread.sleep(5000);
                    Connection conn2 = ds.getConnection();
                } catch (Exception e) {
                    if (!(e instanceof SQLServerException)) {
                        fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
                    }
                }
            });
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setServerName("localhost");
            Connection conn = ds.getConnection();
            Thread.sleep(5000);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
            }
        } finally {
            if (executor != null) {
                executor.shutdown();
            }
        }

        // At this point, thread count has returned to normal. If the peak was more
        // than 2 times the current, this is an issue and the test should fail.
        if (ManagementFactory.getThreadMXBean().getPeakThreadCount() > 2
                * ManagementFactory.getThreadMXBean().getThreadCount()) {
            fail(TestResource.getResource("R_unexpectedThreadCount"));
        }
    }

    /**
     * Test calling method to get redirected server string.
     */
    @Test
    public void testRedirectedError() {
        try (SQLServerConnection conn = getConnection()) {
            assertTrue(conn.getServerNameString(null) == null);
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
        }
    }

    /*
     * Basic test to make sure lobs work with ConnectionPoolProxy as well
     */
    @Tag(Constants.xAzureSQLDW)
    @Test
    public void testConnectionPoolProxyWithLobs() throws SQLException, IOException {
        String cString = getConnectionString() + ";delayLoadingLobs=false;";
        String data = "testConnectionPoolProxyWithLobs";
        Clob c = null;
        try (Connection conn = PrepUtil.getConnection(cString);
                SQLServerConnectionPoolProxy proxy = new SQLServerConnectionPoolProxy((SQLServerConnection) conn)) {
            try (Statement stmt = proxy.createStatement()) {
                String tableName = RandomUtil.getIdentifier("streamingTest");
                stmt.execute(
                        "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (lob varchar(max))");
                stmt.execute(
                        "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " VALUES ('" + data + "')");
                try (ResultSet rs = stmt
                        .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                    while (rs.next()) {
                        c = rs.getClob(1);
                        try (Reader r = c.getCharacterStream()) {
                            long clobLength = c.length();
                            // read the Reader contents into a buffer and return the complete string
                            final StringBuilder stringBuilder = new StringBuilder((int) clobLength);
                            char[] buffer = new char[(int) clobLength];
                            int amountRead = -1;
                            while ((amountRead = r.read(buffer, 0, (int) clobLength)) != -1) {
                                stringBuilder.append(buffer, 0, amountRead);
                            }
                            String received = stringBuilder.toString();
                            assertTrue(data.equals(received),
                                    "Expected String: " + data + "\nReceived String: " + received);
                        }
                    }
                } finally {
                    TestUtils.dropTableIfExists(tableName, stmt);
                }
            }
        }
        // Read the lob after it's been closed
        try (Reader r = c.getCharacterStream()) {
            long clobLength = c.length();
            // read the Reader contents into a buffer and return the complete string
            final StringBuilder stringBuilder = new StringBuilder((int) clobLength);
            char[] buffer = new char[(int) clobLength];
            int amountRead = -1;
            while ((amountRead = r.read(buffer, 0, (int) clobLength)) != -1) {
                stringBuilder.append(buffer, 0, amountRead);
            }
            String received = stringBuilder.toString();
            assertTrue(data.equals(received), "Expected String: " + data + "\nReceived String: " + received);
        }
    }

    /*
     * Test PersistentTokenCacheAccessAspect methods - this test just executes the methods in the class it does not test
     * correct functionality as that requires manual interactive auth
     */
    @Test
    public void testPersistentTokenCacheAccessAspect() throws SQLException {
        TokenCacheAccessContext tokenCacheAccessContext = TokenCacheAccessContext.builder().clientId(null)
                .tokenCache(new TokenCache()).account(null).hasCacheChanged(true).build();

        PersistentTokenCacheAccessAspect persistentTokenAspect = PersistentTokenCacheAccessAspect.getInstance();
        persistentTokenAspect.afterCacheAccess(tokenCacheAccessContext);
        persistentTokenAspect.beforeCacheAccess(tokenCacheAccessContext);
        PersistentTokenCacheAccessAspect.clearUserTokenCache();
    }

    /**
     * test bad serverCertificate property
     * 
     * @throws SQLException
     */
    @Test
    public void testBadServerCert() throws SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setURL(connectionString);
        ds.setServerCertificate("badCert");
        ds.setEncrypt(Constants.STRICT);
        ds.setTrustServerCertificate(false);

        // test using datasource
        try (Connection con = ds.getConnection()) {
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (SQLException e) {
            // TODO: servers which do not support TDS 8 will return SSL failed error, test should be updated once server
            // available
            String errMsg = e.getMessage().replaceAll("\\r", "").replaceAll("\\n", "");
            assertTrue(errMsg.matches(TestUtils.formatErrorMsg("R_serverCertError"))
                    || errMsg.matches(TestUtils.formatErrorMsg("R_sslFailed")), e.getMessage());
        }

        // test connection string
        try (Connection con = PrepUtil.getConnection(
                connectionString + ";encrypt=strict;trustServerCertificate=false;serverCertificate=badCert")) {
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (SQLException e) {
            // TODO: servers which do not support TDS 8 will return SSL failed error, test should be updated once server
            // available
            String errMsg = e.getMessage().replaceAll("\\r", "").replaceAll("\\n", "");
            assertTrue(errMsg.matches(TestUtils.formatErrorMsg("R_serverCertError"))
                    || errMsg.matches(TestUtils.formatErrorMsg("R_sslFailed")), e.getMessage());
        }
    }

    /**
     * Test to make sure parameter metadata denials are handled correctly.
     * 
     * @throws SQLException
     * 
     * @throws SQLServerException
     */
    @Test
    public void testParameterMetadataAccessDenial() throws SQLException {
        try (SQLServerStatement stmt = (SQLServerStatement) connection.createStatement()) {
            CryptoCache cache = new CryptoCache();
            String userSql = "";
            Map<Integer, CekTableEntry> cekList = new HashMap<>();
            Parameter[] params = {new Parameter(false)};
            ArrayList<String> parameterNames = new ArrayList<>(1);
            parameterNames.add("testParameter");
            // Both will always return false
            try {
                ParameterMetaDataCache.addQueryMetadata(params, parameterNames, connection, stmt, userSql);
            } catch (SQLException e) {
                assertEquals(TestResource.getResource("R_CryptoCacheInaccessible"), e.getMessage(),
                        TestResource.getResource("R_wrongExceptionMessage"));
            }

            try {
                ParameterMetaDataCache.getQueryMetadata(params, parameterNames, connection, stmt, userSql);
            } catch (SQLException e) {
                assertEquals(TestResource.getResource("R_CryptoCacheInaccessible"), e.getMessage(),
                        TestResource.getResource("R_wrongExceptionMessage"));
            }
        }
    }

    @Test
    public void testServerNameField() throws SQLException {
        String subProtocol = "jdbc:sqlserver://";
        int indexOfFirstDelimiter = connectionString.indexOf(";");
        int indexOfLastDelimiter = connectionString.lastIndexOf(";");

        String[] serverNameAndPort = connectionString.substring(subProtocol.length(), indexOfFirstDelimiter).split(":");
        String connectionProperties = connectionString.substring(indexOfFirstDelimiter, indexOfLastDelimiter + 1);
        String loginTimeout = "loginTimout=15";

        // Server name field is empty but serverName connection property is set, should pass
        String emptyServerNameField = subProtocol + connectionProperties + "serverName=" + serverNameAndPort[0] + ";";

        // A loginTimeout connection property is passed into the server name field, should fail
        String invalidServerNameField = subProtocol + loginTimeout + connectionProperties;

        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(emptyServerNameField)) {}

        try (SQLServerConnection conn = (SQLServerConnection) DriverManager
                .getConnection(invalidServerNameField)) {} catch (SQLException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_errorServerName")));
        }
    }
}
