/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.Reader;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
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
import java.util.logging.Level;
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
import com.microsoft.sqlserver.jdbc.SQLServerConnection.SqlFedAuthInfo;
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

    SQLServerConnection mockConnection;
    Logger mockLogger;

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

        ds.setCalcBigDecimalPrecision(booleanPropValue);
        assertEquals(booleanPropValue, ds.getCalcBigDecimalPrecision(),
                TestResource.getResource("R_valuesAreDifferent"));
        ds.setRetryExec(stringPropValue);
        assertEquals(stringPropValue, ds.getRetryExec(), TestResource.getResource("R_valuesAreDifferent"));

        ds.setRetryConn(stringPropValue);
        assertEquals(stringPropValue, ds.getRetryConn(), TestResource.getResource("R_valuesAreDifferent"));

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
        
        ds.setQuotedIdentifier(stringPropValue);
        assertTrue(ds.getQuotedIdentifier().equals(stringPropValue));
        
        ds.setConcatNullYieldsNull(stringPropValue);
        assertTrue(ds.getConcatNullYieldsNull().equals(stringPropValue));
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
     * Test connection properties: CONCAT_NULL_YIELDS_NULL with SQLServerXADataSource for new connection and pooled connection
     * @throws SQLException
     */
    @Test
    public void testConcatNullYieldsNull() throws SQLException {
        // Server default is CONCAT_NULL_YIELDS_NULL = ON  
        int expectedResultFlagOff = 0;
        int expectedResultFlagOn = 1;
        String sessionPropertyName = "CONCAT_NULL_YIELDS_NULL";
        
        // Test for concatNullYieldsNull flag is OFF
        SQLServerDataSource dsWithOff = new SQLServerDataSource();
        dsWithOff.setURL(connectionString);
        dsWithOff.setConcatNullYieldsNull("OFF");
        testSessionPropertyValueHelper(dsWithOff.getConnection(), sessionPropertyName, expectedResultFlagOff);
        // Test pooled connections
        SQLServerXADataSource pdsWithOff = new SQLServerXADataSource();
        pdsWithOff.setURL(connectionString);
        pdsWithOff.setConcatNullYieldsNull("OFF");

        PooledConnection pcWithOff = pdsWithOff.getPooledConnection();
        try {
            testSessionPropertyValueHelper(pcWithOff.getConnection(), sessionPropertyName, expectedResultFlagOff);
            // Repeat getConnection to put the physical connection through a RESETCONNECTION
            testSessionPropertyValueHelper(pcWithOff.getConnection(), sessionPropertyName, expectedResultFlagOff);            
        } finally {
            if (null != pcWithOff) {
                pcWithOff.close();
            }
        }
        // Test for concatNullYieldsNull flag is ON
        SQLServerDataSource dsWithOn = new SQLServerDataSource();
        dsWithOn.setURL(connectionString);
        dsWithOn.setConcatNullYieldsNull("ON");
        testSessionPropertyValueHelper(dsWithOn.getConnection(), sessionPropertyName, expectedResultFlagOn);
        // Test pooled connections
        SQLServerXADataSource pdsWithOn = new SQLServerXADataSource();
        pdsWithOn.setURL(connectionString);
        pdsWithOn.setConcatNullYieldsNull("ON");

        PooledConnection pcWithOn = pdsWithOn.getPooledConnection();
        try {
            testSessionPropertyValueHelper(pcWithOn.getConnection(), sessionPropertyName, expectedResultFlagOn);
            // Repeat getConnection to put the physical connection through a RESETCONNECTION
            testSessionPropertyValueHelper(pcWithOn.getConnection(), sessionPropertyName, expectedResultFlagOn);            
        } finally {
            if (null != pcWithOn) {
                pcWithOn.close();
            }
        }
    }

    public void testSessionPropertyValueHelper(Connection con, String propName, int expectedResult) throws SQLException {
        String sqlSelect = "SELECT SESSIONPROPERTY('" + propName + "')";
        try (Statement statement = con.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sqlSelect)) {
                if (rs.next()) {
                    int actualResult = rs.getInt(1);
                    MessageFormat form1 = new MessageFormat(
                            TestResource.getResource("R_sessionPropertyFailed"));
                    Object[] msgArgs1 = {expectedResult, propName, actualResult};
                    assertEquals(expectedResult, actualResult, form1.format(msgArgs1));
                } else {
                    assertTrue(false, "Expected row of data was not found.");
                }
            }
        }
    }

    /**
     * Test connection properties: QUOTED_IDENTIFIER with SQLServerXADataSource for new connection and pooled connection
     * @throws SQLException
     */
    @Test
    public void testQuptedIdentifier() throws SQLException {
        // Server default is QUOTED_IDENTIFIER = ON  
        int expectedResultFlagOff = 0;
        int expectedResultFlagOn = 1;
        String sessionPropertyName = "QUOTED_IDENTIFIER";
        
        //Test for quotedIdentifier flag is OFF
        SQLServerDataSource dsWithOff = new SQLServerDataSource();
        dsWithOff.setURL(connectionString);
        dsWithOff.setQuotedIdentifier("OFF");
        testSessionPropertyValueHelper(dsWithOff.getConnection(), sessionPropertyName, expectedResultFlagOff);
        // Test pooled connections
        SQLServerXADataSource pdsWithOff = new SQLServerXADataSource();
        pdsWithOff.setURL(connectionString);
        pdsWithOff.setQuotedIdentifier("OFF");
        PooledConnection pcWithOff = pdsWithOff.getPooledConnection();
        try {
            testSessionPropertyValueHelper(pcWithOff.getConnection(), sessionPropertyName, expectedResultFlagOff);
            // Repeat getConnection to put the physical connection through a RESETCONNECTION
            testSessionPropertyValueHelper(pcWithOff.getConnection(), sessionPropertyName, expectedResultFlagOff);         
        } finally {
            if (null != pcWithOff) {
                pcWithOff.close();
            }
        }

        // Test for quotedIdentifier flag is ON
        SQLServerDataSource dsWithOn = new SQLServerDataSource();
        dsWithOn.setURL(connectionString);
        dsWithOn.setQuotedIdentifier("ON");
        testSessionPropertyValueHelper(dsWithOn.getConnection(), sessionPropertyName, expectedResultFlagOn);
        // Test pooled connections
        SQLServerXADataSource pdsWithOn = new SQLServerXADataSource();
        pdsWithOn.setURL(connectionString);
        pdsWithOn.setQuotedIdentifier("ON");
        PooledConnection pcWithOn = pdsWithOn.getPooledConnection();
        try {
            testSessionPropertyValueHelper(pcWithOn.getConnection(), sessionPropertyName, expectedResultFlagOn);
            // Repeat getConnection to put the physical connection through a RESETCONNECTION
            testSessionPropertyValueHelper(pcWithOn.getConnection(), sessionPropertyName, expectedResultFlagOn);         
        } finally {
            if (null != pcWithOn) {
                pcWithOn.close();
            }
        }
    }

    /**
     * Runs the `testConnectCountInLoginAndCorrectRetryCount` test several times with different values of
     * connectRetryCount.
     */
    @Test
    public void testConnectCountInLoginAndCorrectRetryCountForMultipleValues() {
        testConnectCountInLoginAndCorrectRetryCount(0);
        testConnectCountInLoginAndCorrectRetryCount(1);
        testConnectCountInLoginAndCorrectRetryCount(2);
    }

    /**
     * Tests whether connectRetryCount and connectRetryInterval are properly respected in the login loop. As well, tests
     * that connection is retried the proper number of times.
     */
    private void testConnectCountInLoginAndCorrectRetryCount(int connectRetryCount) {
        long timerStart = 0;

        int connectRetryInterval = 60;
        int longLoginTimeout = loginTimeOutInSeconds * 9; // 90 seconds

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

            // We should at least take as long as the retry interval between all retries past the first.
            // Of the above acceptable errors (R_cannotOpenDatabase, R_loginFailedMI, R_MInotAvailable), only
            // R_cannotOpenDatabase is transient, and can be used to measure multiple retries with retry interval. The
            // others will exit before they have a chance to wait, and min will be too low.
            if (e.getMessage().contains(TestResource.getResource("R_cannotOpenDatabase"))) {
                int minTimeInSecs = connectRetryInterval * (connectRetryCount - 1);
                assertTrue(totalTime > (minTimeInSecs * 1000L), TestResource.getResource("R_executionNotLong"));
            }
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
                ds.setConnectRetryCount(6);
                ds.setConnectRetryInterval(20);
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
    

    @Test
    public void testGetSqlFedAuthTokenFailure() throws SQLException {
        try (Connection conn = getConnection()){
            SqlFedAuthInfo fedAuthInfo = ((SQLServerConnection) conn).new SqlFedAuthInfo();
            fedAuthInfo.spn = "https://database.windows.net/";
            fedAuthInfo.stsurl = "https://login.windows.net/xxx";
            SqlAuthenticationToken fedAuthToken = SQLServerMSAL4JUtils.getSqlFedAuthToken(fedAuthInfo, "xxx",
                    "xxx",SqlAuthentication.ACTIVE_DIRECTORY_PASSWORD.toString(), 10);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            // test pass
            assertTrue(e.getMessage().contains(SQLServerException.getErrString("R_connectionTimedOut")), "Expected Timeout Exception was not thrown");
        }        
    }

    @Test
    public void testGetSqlFedAuthTokenFailureNoWaiting() throws SQLException {
        try (Connection conn = getConnection()){
            SqlFedAuthInfo fedAuthInfo = ((SQLServerConnection) conn).new SqlFedAuthInfo();
            fedAuthInfo.spn = "https://database.windows.net/";
            fedAuthInfo.stsurl = "https://login.windows.net/xxx";
            SqlAuthenticationToken fedAuthToken = SQLServerMSAL4JUtils.getSqlFedAuthToken(fedAuthInfo, "xxx",
                    "xxx",SqlAuthentication.ACTIVE_DIRECTORY_PASSWORD.toString(), 0);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            // test pass
            assertTrue(e.getMessage().contains(SQLServerException.getErrString("R_connectionTimedOut")), "Expected Timeout Exception was not thrown");
        }        
    }

    @Test
    public void testGetSqlFedAuthTokenFailureNagativeWaiting() throws SQLException {
        try (Connection conn = getConnection()){
            SqlFedAuthInfo fedAuthInfo = ((SQLServerConnection) conn).new SqlFedAuthInfo();
            fedAuthInfo.spn = "https://database.windows.net/";
            fedAuthInfo.stsurl = "https://login.windows.net/xxx";
            SqlAuthenticationToken fedAuthToken = SQLServerMSAL4JUtils.getSqlFedAuthToken(fedAuthInfo, "xxx",
                    "xxx",SqlAuthentication.ACTIVE_DIRECTORY_PASSWORD.toString(), -1);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            // test pass
            assertTrue(e.getMessage().contains(SQLServerException.getErrString("R_connectionTimedOut")), "Expected Timeout Exception was not thrown");
        }        
    }

    @Test
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.xAzureSQLMI)
    @Tag(Constants.xSQLv11)
    @Tag(Constants.xSQLv12)
    @Tag(Constants.xSQLv14)
    @Tag(Constants.xSQLv15)
    @Tag(Constants.xSQLv16)
    public void testManagedIdentityWithEncryptStrict() {
        SQLServerDataSource ds = new SQLServerDataSource();

        String connectionUrl = connectionString;
        if (connectionUrl.contains("user=")) {
            connectionUrl = TestUtils.removeProperty(connectionUrl, "user");
        }
        if (connectionUrl.contains("password=")) {
            connectionUrl = TestUtils.removeProperty(connectionUrl, "password");
        }

        ds.setURL(connectionUrl);
        ds.setAuthentication("ActiveDirectoryMSI");
        ds.setEncrypt("strict");
        ds.setHostNameInCertificate("*.database.windows.net"); 

        try (Connection con = ds.getConnection()) {
            assertNotNull(con);
        } catch (SQLException e) {
            fail("Connection failed: " + e.getMessage());
        }
    }

    public Method mockedConnectionRecoveryCheck() throws Exception {
        mockConnection = spy(new SQLServerConnection("test"));
        mockLogger = mock(Logger.class);
        doReturn(true).when(mockLogger).isLoggable(Level.WARNING);
        doNothing().when(mockConnection).terminate(anyInt(), anyString());

        Method method = SQLServerConnection.class.getDeclaredMethod("connectionReconveryCheck", boolean.class,
                boolean.class, ServerPortPlaceHolder.class);
        method.setAccessible(true);
        return method;
    }

    @Test
    void testConnectionRecoveryCheckThrowsWhenAllConditionsMet() throws Exception {
        Method method = mockedConnectionRecoveryCheck();
        method.invoke(mockConnection, true, false, null);
        verify(mockConnection, times(1)).terminate(eq(SQLServerException.DRIVER_ERROR_INVALID_TDS),
                eq(SQLServerException.getErrString("R_crClientNoRecoveryAckFromLogin")));
    }

    @Test
    void testConnectionRecoveryCheckDoesNotThrowWhenNotReconnectRunning() throws Exception {
        Method method = mockedConnectionRecoveryCheck();
        method.invoke(mockConnection, false, false, null);
        verify(mockConnection, never()).terminate(anyInt(), anyString());
    }

    @Test
    void testConnectionRecoveryCheckDoesNotThrowWhenRecoveryPossible() throws Exception {
        Method method = mockedConnectionRecoveryCheck();
        method.invoke(mockConnection, true, true, null);
        verify(mockConnection, never()).terminate(anyInt(), anyString());
    }

    @Test
    void testConnectionRecoveryCheckDoesNotThrowWhenRoutingDetailsNotNull() throws Exception {
        Method method = mockedConnectionRecoveryCheck();
        ServerPortPlaceHolder routingDetails = mock(ServerPortPlaceHolder.class);
        method.setAccessible(true);
        method.invoke(mockConnection, true, false, routingDetails);
        verify(mockConnection, never()).terminate(anyInt(), anyString());
    }

    @Test
    public void testIsAzureSynapseOnDemandEndpoint() throws Exception {
        // Use reflection to instantiate SQLServerConnection with a dummy argument
        java.lang.reflect.Constructor<SQLServerConnection> ctor = SQLServerConnection.class
                .getDeclaredConstructor(String.class);
        ctor.setAccessible(true);
        SQLServerConnection synapseConn = ctor.newInstance("test");
        java.util.Properties props = new java.util.Properties();
        // Typical Synapse OnDemand endpoint pattern
        props.setProperty("serverName", "myworkspace-ondemand.sql.azuresynapse.net");
        synapseConn.activeConnectionProperties = props;
        assertTrue(synapseConn.isAzureSynapseOnDemandEndpoint(), "Should detect Azure Synapse OnDemand endpoint");

        // Simulate a regular Azure SQL endpoint
        SQLServerConnection regularConn = ctor.newInstance("test");
        java.util.Properties props2 = new java.util.Properties();
        props2.setProperty("serverName", "myserver.database.windows.net");
        regularConn.activeConnectionProperties = props2;
        assertFalse(regularConn.isAzureSynapseOnDemandEndpoint(),
                "Should not detect regular Azure SQL as Synapse OnDemand endpoint");
    }

    /**
     * Test for validateMaxSQLLoginName: should throw if login name exceeds max length, otherwise not.
     */
    @Test
    public void testValidateMaxSQLLoginName() throws Exception {
        // Use reflection to access the method and constant
        java.lang.reflect.Constructor<SQLServerConnection> ctor = SQLServerConnection.class
                .getDeclaredConstructor(String.class);
        ctor.setAccessible(true);
        SQLServerConnection conn = ctor.newInstance("test");

        java.lang.reflect.Method method = SQLServerConnection.class.getDeclaredMethod("validateMaxSQLLoginName",
                String.class, String.class);
        method.setAccessible(true);

        // Get the max length constant
        java.lang.reflect.Field maxLenField = SQLServerConnection.class.getDeclaredField("MAX_SQL_LOGIN_NAME_WCHARS");
        maxLenField.setAccessible(true);
        int maxLen = maxLenField.getInt(conn);

        String propName = "user";
        String validValue = "a".repeat(maxLen);
        String tooLongValue = "b".repeat(maxLen + 1);

        // Should NOT throw for valid length
        method.invoke(conn, propName, validValue);

        // Should throw for too long value
        Exception ex = assertThrows(Exception.class, () -> {
            try {
                method.invoke(conn, propName, tooLongValue);
            } catch (java.lang.reflect.InvocationTargetException e) {
                // Unwrap
                throw e.getCause();
            }
        });
        // Should be SQLServerException
        assertTrue(ex instanceof com.microsoft.sqlserver.jdbc.SQLServerException,
                "Should throw SQLServerException for too long login name");
    }

    @Test
    public void testGetServerNameStringRedirected() throws Exception {
        // Use reflection to instantiate SQLServerConnection with a dummy argument
        java.lang.reflect.Constructor<SQLServerConnection> ctor = SQLServerConnection.class
                .getDeclaredConstructor(String.class);
        ctor.setAccessible(true);
        SQLServerConnection conn = ctor.newInstance("test");
        java.util.Properties props = new java.util.Properties();
        props.setProperty("serverName", "originalServer.database.windows.net");
        conn.activeConnectionProperties = props;

        // Simulate a redirect: pass a different serverName
        String redirectedServer = "redirectedServer.database.windows.net";
        String result = conn.getServerNameString(redirectedServer);
        // The expected format is: {0} (redirected from {1})
        String expected = redirectedServer + " (redirected from originalServer.database.windows.net)";
        assertEquals(expected, result);
    }

    @Test
    public void testFederatedAuthenticationFeatureExtensionDataSwitchCases() throws Exception {
        // Use reflection to access the inner class and constructor
        Class<?> innerClass = Class.forName(
                "com.microsoft.sqlserver.jdbc.SQLServerConnection$FederatedAuthenticationFeatureExtensionData");
        java.lang.reflect.Constructor<?> ctor = innerClass.getDeclaredConstructor(int.class, String.class,
                boolean.class);
        ctor.setAccessible(true);

        // All valid authentication strings and expected enum names
        String[][] cases = {{"ActiveDirectoryPassword", "ACTIVE_DIRECTORY_PASSWORD"},
                {"ActiveDirectoryIntegrated", "ACTIVE_DIRECTORY_INTEGRATED"},
                {"ActiveDirectoryManagedIdentity", "ACTIVE_DIRECTORY_MANAGED_IDENTITY"},
                {"ActiveDirectoryDefault", "ACTIVE_DIRECTORY_DEFAULT"},
                {"ActiveDirectoryServicePrincipal", "ACTIVE_DIRECTORY_SERVICE_PRINCIPAL"},
                {"ActiveDirectoryServicePrincipalCertificate", "ACTIVE_DIRECTORY_SERVICE_PRINCIPAL_CERTIFICATE"},
                {"ActiveDirectoryInteractive", "ACTIVE_DIRECTORY_INTERACTIVE"}};
        for (String[] c : cases) {
            Object obj = ctor.newInstance(1, c[0], true);
            java.lang.reflect.Field authField = innerClass.getDeclaredField("authentication");
            authField.setAccessible(true);
            Object authEnum = authField.get(obj);
            assertEquals(c[1], authEnum.toString(), "Enum for " + c[0]);
        }

        // Test default/callback case: if accessTokenCallback or hasAccessTokenCallbackClass is set, should use NOT_SPECIFIED
        // We'll use reflection to set the static field hasAccessTokenCallbackClass to true, then reset it
        java.lang.reflect.Field callbackField = SQLServerConnection.class
                .getDeclaredField("hasAccessTokenCallbackClass");
        callbackField.setAccessible(true);
        boolean oldValue = callbackField.getBoolean(null);
        try {
            callbackField.setBoolean(null, true);
            Object obj = ctor.newInstance(1, "", true);
            java.lang.reflect.Field authField = innerClass.getDeclaredField("authentication");
            authField.setAccessible(true);
            Object authEnum = authField.get(obj);
            assertEquals("NOT_SPECIFIED", authEnum.toString(), "Enum for callback case");
        } finally {
            callbackField.setBoolean(null, oldValue);
        }

        // Test invalid string throws exception if no callback
        callbackField.setBoolean(null, false);
        Exception ex = assertThrows(Exception.class, () -> {
            ctor.newInstance(1, "InvalidAuthType", true);
        });
        assertTrue(ex.getCause() instanceof com.microsoft.sqlserver.jdbc.SQLServerException,
                "Should throw SQLServerException for invalid auth string");
    }

    @Test
    public void testSetVectorTypeSupport_UserCase() throws SQLException {
        // Set to "v1" and set/update column encryption trusted master key paths
        String connStrV1 = connectionString + ";vectorTypeSupport=v1;";
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connStrV1)) {
            // This should succeed for valid input
            String keyPath = "C:/trusted/key/path";
            java.util.Map<String, java.util.List<String>> keyMap = new java.util.HashMap<>();
            keyMap.put("keyStoreProviderName", java.util.Collections.singletonList(keyPath));
            SQLServerConnection.setColumnEncryptionTrustedMasterKeyPaths(keyMap);
            Boolean[] found = new Boolean[1];
            java.util.List<String> paths = SQLServerConnection
                    .getColumnEncryptionTrustedMasterKeyPaths("keyStoreProviderName", found);
            assertTrue(paths.contains(keyPath), "Trusted master key path should be set when vectorTypeSupport is v1");
            assertTrue(Boolean.TRUE.equals(found[0]), "Key path should be found for provider");

            // Now update the trusted master key paths for the same provider
            String updatedKeyPath = "C:/trusted/key/updated";
            java.util.List<String> updatedKeyPaths = java.util.Collections.singletonList(updatedKeyPath);
            SQLServerConnection.updateColumnEncryptionTrustedMasterKeyPaths("keyStoreProviderName", updatedKeyPaths);
            Boolean[] foundUpdated = new Boolean[1];
            java.util.List<String> updatedPaths = SQLServerConnection
                    .getColumnEncryptionTrustedMasterKeyPaths("keyStoreProviderName", foundUpdated);
            assertTrue(updatedPaths.contains(updatedKeyPath), "Updated trusted master key path should be set");
            assertFalse(updatedPaths.contains(keyPath), "Old key path should not be present after update");
            assertTrue(Boolean.TRUE.equals(foundUpdated[0]), "Updated key path should be found for provider");
        }

        // Set to "off" and try to set column encryption trusted master key paths
        String connStrOff = connectionString + ";vectorTypeSupport=off;";
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connStrOff)) {
            String keyPath = "C:/trusted/key/path";
            java.util.Map<String, java.util.List<String>> keyMap = new java.util.HashMap<>();
            keyMap.put("keyStoreProviderName", java.util.Collections.singletonList(keyPath));
            try {
                SQLServerConnection.setColumnEncryptionTrustedMasterKeyPaths(keyMap);
                Boolean[] found = new Boolean[1];
                java.util.List<String> paths = SQLServerConnection
                        .getColumnEncryptionTrustedMasterKeyPaths("keyStoreProviderName", found);
                // If feature is disabled, paths should be empty or not contain the keyPath
                assertFalse(paths.contains(keyPath),
                        "Trusted master key path should not be set when vectorTypeSupport is off");
                assertFalse(Boolean.TRUE.equals(found[0]),
                        "Key path should not be found for provider when vectorTypeSupport is off");
            } catch (Exception e) {
                // Acceptable: feature may be disabled and throw
                assertTrue(e instanceof SQLException || e instanceof UnsupportedOperationException);
            }
        }

        // Invalid value in connection string should throw
        String invalidConnStr = connectionString + ";vectorTypeSupport=invalid;";
        assertThrows(SQLException.class, () -> {
            PrepUtil.getConnection(invalidConnStr);
        });
    }

    /**
     * User-case driven test: Attempt to connect using each supported authentication type.
     * This simulates a real user specifying authentication in the connection string.
     * Note: Actual authentication may fail if not configured, but the test ensures the code path is exercised.
     */
    @Test
    public void testFederatedAuthentication_UserCase() {
        String[] authTypes = {"ActiveDirectoryPassword", "ActiveDirectoryIntegrated", "ActiveDirectoryManagedIdentity",
                "ActiveDirectoryDefault", "ActiveDirectoryServicePrincipal",
                "ActiveDirectoryServicePrincipalCertificate", "ActiveDirectoryInteractive"};
        for (String auth : authTypes) {
            String connStr = connectionString + ";authentication=" + auth + ";";
            try {
                // We expect most of these to fail unless the environment is configured, but the point is to exercise the user path
                PrepUtil.getConnection(connStr);
            } catch (Exception e) {
                // Acceptable: just ensure the driver attempts to process the auth type
                assertTrue(e.getMessage().toLowerCase().contains("authentication") || e instanceof SQLException);
            }
        }

        // Invalid authentication type should throw
        String invalidConnStr = connectionString + ";authentication=InvalidAuthType;";
        assertThrows(SQLException.class, () -> {
            PrepUtil.getConnection(invalidConnStr);
        });
    }

    /**
     * User-case driven test: Remove column encryption trusted master key paths for a provider and verify removal.
     */
    @Test
    public void testRemoveColumnEncryptionTrustedMasterKeyPaths_UserCase() throws SQLException {

        String connStr = connectionString + ";vectorTypeSupport=v1;";
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connStr)) {
            String provider = "keyStoreProviderName";
            String keyPath = "C:/trusted/key/path";
            java.util.Map<String, java.util.List<String>> keyMap = new java.util.HashMap<>();
            keyMap.put(provider, java.util.Collections.singletonList(keyPath));
            SQLServerConnection.setColumnEncryptionTrustedMasterKeyPaths(keyMap);
            Boolean[] found = new Boolean[1];
            java.util.List<String> paths = SQLServerConnection.getColumnEncryptionTrustedMasterKeyPaths(provider,
                    found);
            assertTrue(paths.contains(keyPath), "Trusted master key path should be set before removal");
            assertTrue(Boolean.TRUE.equals(found[0]), "Key path should be found for provider before removal");

            // Now remove the trusted master key paths for the provider
            SQLServerConnection.removeColumnEncryptionTrustedMasterKeyPaths(provider);
            Boolean[] foundAfter = new Boolean[1];
            java.util.List<String> afterPaths = SQLServerConnection.getColumnEncryptionTrustedMasterKeyPaths(provider,
                    foundAfter);
            assertTrue(afterPaths == null || afterPaths.isEmpty(),
                    "Trusted master key path list should be empty after removal");
            assertFalse(Boolean.TRUE.equals(foundAfter[0]), "Key path should not be found for provider after removal");
        }

        // Test isDenaliOrLater utility if available
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            try {
                java.lang.reflect.Method method = SQLServerConnection.class.getDeclaredMethod("isDenaliOrLater");
                method.setAccessible(true);
                Object result = null;
                try {
                    result = method.invoke(conn);
                } catch (IllegalAccessException | java.lang.reflect.InvocationTargetException e) {
                    // If the method cannot be invoked, skip
                }
                // Just assert that the method returns a boolean (true/false)
                if (result != null) {
                    assertTrue(result instanceof Boolean, "isDenaliOrLater should return a boolean");
                }
            } catch (NoSuchMethodException e) {
                // Method does not exist, skip
            }
        }
    }

    /**
     * Test that checkClosed throws an exception when called on a closed connection.
     */
    @Test
    public void testCheckClosedThrowsOnClosedConnection() throws Exception {
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.close();
            java.lang.reflect.Method checkClosedMethod = SQLServerConnection.class.getDeclaredMethod("checkClosed");
            checkClosedMethod.setAccessible(true);
            Exception ex = assertThrows(Exception.class, () -> {
                try {
                    checkClosedMethod.invoke(conn);
                } catch (java.lang.reflect.InvocationTargetException e) {
                    // Unwrap the cause for assertion
                    throw e.getCause();
                }
            });
            // Should be SQLServerException
            assertTrue(ex instanceof com.microsoft.sqlserver.jdbc.SQLServerException,
                    "checkClosed should throw SQLServerException when called on closed connection");
        }
    }

    /**
     * Test the connect method for exception and logging coverage.
     * This uses reflection to invoke connect and simulates error scenarios.
     */
    @Test
    public void testConnectMethodExceptionAndLogging() throws Exception {
        // Use reflection to get the connect method
        java.lang.reflect.Constructor<SQLServerConnection> ctor = SQLServerConnection.class
                .getDeclaredConstructor(String.class);
        ctor.setAccessible(true);
        SQLServerConnection conn = ctor.newInstance("test");
        java.lang.reflect.Method connectMethod = SQLServerConnection.class.getDeclaredMethod("connect", String.class,
                java.util.Properties.class, String.class, String.class);
        connectMethod.setAccessible(true);

        // Set up logger to FINEST to ensure all logging paths are covered
        java.util.logging.Logger logger = java.util.logging.Logger.getLogger(SQLServerConnection.class.getName());
        java.util.logging.Level oldLevel = logger.getLevel();
        logger.setLevel(java.util.logging.Level.FINEST);
        try {
            // Simulate invalid arguments to trigger exception and logging
            Exception ex = assertThrows(Exception.class, () -> {
                try {
                    connectMethod.invoke(conn, "invalidUrl", new java.util.Properties(), null, null);
                } catch (java.lang.reflect.InvocationTargetException e) {
                    throw e.getCause();
                }
            });
            // Should be SQLServerException or IllegalArgumentException depending on code path
            assertTrue(
                    ex instanceof com.microsoft.sqlserver.jdbc.SQLServerException
                            || ex instanceof IllegalArgumentException,
                    "connect should throw SQLServerException or IllegalArgumentException for invalid input");
        } finally {
            logger.setLevel(oldLevel);
        }
    }

    /**
     * Test setKeyStoreSecretAndLocation for exception coverage via reflection.
     */
    @Test
    public void testSetKeyStoreSecretAndLocationException() throws Exception {
        java.lang.reflect.Constructor<SQLServerConnection> ctor = SQLServerConnection.class
                .getDeclaredConstructor(String.class);
        ctor.setAccessible(true);
        SQLServerConnection conn = ctor.newInstance("test");
        java.lang.reflect.Method method = SQLServerConnection.class.getDeclaredMethod("setKeyStoreSecretAndLocation",
                String.class, String.class);
        method.setAccessible(true);
        // Simulate invalid input (nulls or empty)
        Exception ex = assertThrows(Exception.class, () -> {
            try {
                method.invoke(conn, (String) null, (String) null);
            } catch (java.lang.reflect.InvocationTargetException e) {
                throw e.getCause();
            }
        });
        assertTrue(
                ex instanceof com.microsoft.sqlserver.jdbc.SQLServerException || ex instanceof IllegalArgumentException,
                "setKeyStoreSecretAndLocation should throw for invalid input");
    }

    /**
     * Test validateTimeout for exception coverage via reflection.
     */
    @Test
    public void testValidateTimeoutException() throws Exception {
        java.lang.reflect.Constructor<SQLServerConnection> ctor = SQLServerConnection.class
                .getDeclaredConstructor(String.class);
        ctor.setAccessible(true);
        SQLServerConnection conn = ctor.newInstance("test");
        java.lang.reflect.Method method = SQLServerConnection.class.getDeclaredMethod("validateTimeout", int.class);
        method.setAccessible(true);
        // Simulate invalid input (negative timeout)
        Exception ex = assertThrows(Exception.class, () -> {
            try {
                method.invoke(conn, -1);
            } catch (java.lang.reflect.InvocationTargetException e) {
                throw e.getCause();
            }
        });
        assertTrue(
                ex instanceof com.microsoft.sqlserver.jdbc.SQLServerException || ex instanceof IllegalArgumentException,
                "validateTimeout should throw for negative timeout");
    }

    /**
     * Test validateConnectionRetry for exception coverage via reflection.
     */
    @Test
    public void testValidateConnectionRetryException() throws Exception {
        java.lang.reflect.Constructor<SQLServerConnection> ctor = SQLServerConnection.class
                .getDeclaredConstructor(String.class);
        ctor.setAccessible(true);
        SQLServerConnection conn = ctor.newInstance("test");
        java.lang.reflect.Method method = SQLServerConnection.class.getDeclaredMethod("validateConnectionRetry",
                int.class, int.class);
        method.setAccessible(true);
        // Simulate invalid input (negative retry count or interval)
        Exception ex1 = assertThrows(Exception.class, () -> {
            try {
                method.invoke(conn, -1, 10);
            } catch (java.lang.reflect.InvocationTargetException e) {
                throw e.getCause();
            }
        });
        assertTrue(
                ex1 instanceof com.microsoft.sqlserver.jdbc.SQLServerException
                        || ex1 instanceof IllegalArgumentException,
                "validateConnectionRetry should throw for negative retry count");
        Exception ex2 = assertThrows(Exception.class, () -> {
            try {
                method.invoke(conn, 1, -10);
            } catch (java.lang.reflect.InvocationTargetException e) {
                throw e.getCause();
            }
        });
        assertTrue(
                ex2 instanceof com.microsoft.sqlserver.jdbc.SQLServerException
                        || ex2 instanceof IllegalArgumentException,
                "validateConnectionRetry should throw for negative retry interval");
    }

    /**
     * Test connectInternal for exception and logging coverage via reflection.
     */
    @Test
    public void testConnectInternalExceptionAndLogging() throws Exception {
        java.lang.reflect.Constructor<SQLServerConnection> ctor = SQLServerConnection.class
                .getDeclaredConstructor(String.class);
        ctor.setAccessible(true);
        SQLServerConnection conn = ctor.newInstance("test");
        java.lang.reflect.Method method = SQLServerConnection.class.getDeclaredMethod("connectInternal", String.class,
                java.util.Properties.class, String.class, String.class);
        method.setAccessible(true);
        // Set up logger to FINEST to ensure all logging paths are covered
        java.util.logging.Logger logger = java.util.logging.Logger.getLogger(SQLServerConnection.class.getName());
        java.util.logging.Level oldLevel = logger.getLevel();
        logger.setLevel(java.util.logging.Level.FINEST);
        try {
            // Simulate invalid arguments to trigger exception and logging
            Exception ex = assertThrows(Exception.class, () -> {
                try {
                    method.invoke(conn, "invalidUrl", new java.util.Properties(), null, null);
                } catch (java.lang.reflect.InvocationTargetException e) {
                    throw e.getCause();
                }
            });
            assertTrue(
                    ex instanceof com.microsoft.sqlserver.jdbc.SQLServerException
                            || ex instanceof IllegalArgumentException,
                    "connectInternal should throw SQLServerException or IllegalArgumentException for invalid input");
        } finally {
            logger.setLevel(oldLevel);
        }
    }

    /**
     * Use-case driven test for the login method to cover exception and edge cases.
     * This uses reflection to invoke login with various invalid and edge-case arguments.
     */
    @Test
    public void testLoginMethodUseCases() throws Exception {
        java.lang.reflect.Constructor<SQLServerConnection> ctor = SQLServerConnection.class
                .getDeclaredConstructor(String.class);
        ctor.setAccessible(true);
        SQLServerConnection conn = ctor.newInstance("test");
        java.lang.reflect.Method loginMethod = SQLServerConnection.class.getDeclaredMethod("login", String.class,
                java.util.Properties.class, String.class, String.class, boolean.class, boolean.class);
        loginMethod.setAccessible(true);

        // Set up logger to FINEST to ensure all logging paths are covered
        java.util.logging.Logger logger = java.util.logging.Logger.getLogger(SQLServerConnection.class.getName());
        java.util.logging.Level oldLevel = logger.getLevel();
        logger.setLevel(java.util.logging.Level.FINEST);
        try {
            // Case 1: All nulls/empty, expect exception
            Exception ex1 = assertThrows(Exception.class, () -> {
                try {
                    loginMethod.invoke(conn, null, null, null, null, false, false);
                } catch (java.lang.reflect.InvocationTargetException e) {
                    throw e.getCause();
                }
            });
            assertTrue(ex1 instanceof com.microsoft.sqlserver.jdbc.SQLServerException
                    || ex1 instanceof IllegalArgumentException, "login should throw for all null arguments");

            // Case 2: Invalid URL, expect exception
            Exception ex2 = assertThrows(Exception.class, () -> {
                try {
                    loginMethod.invoke(conn, "invalidUrl", new java.util.Properties(), null, null, false, false);
                } catch (java.lang.reflect.InvocationTargetException e) {
                    throw e.getCause();
                }
            });
            assertTrue(ex2 instanceof com.microsoft.sqlserver.jdbc.SQLServerException
                    || ex2 instanceof IllegalArgumentException, "login should throw for invalid URL");

            // Case 3: Valid URL but missing properties, expect exception
            Exception ex3 = assertThrows(Exception.class, () -> {
                try {
                    loginMethod.invoke(conn, "jdbc:sqlserver://localhost", new java.util.Properties(), null, null,
                            false, false);
                } catch (java.lang.reflect.InvocationTargetException e) {
                    throw e.getCause();
                }
            });
            assertTrue(
                    ex3 instanceof com.microsoft.sqlserver.jdbc.SQLServerException
                            || ex3 instanceof IllegalArgumentException,
                    "login should throw for missing required properties");

            // Case 4: Valid URL and properties but simulate login failure (e.g., wrong credentials)
            java.util.Properties props = new java.util.Properties();
            props.setProperty("user", "invalidUser");
            props.setProperty("password", "invalidPassword");
            Exception ex4 = assertThrows(Exception.class, () -> {
                try {
                    loginMethod.invoke(conn, "jdbc:sqlserver://localhost", props, null, null, false, false);
                } catch (java.lang.reflect.InvocationTargetException e) {
                    throw e.getCause();
                }
            });
            assertTrue(ex4 instanceof com.microsoft.sqlserver.jdbc.SQLServerException
                    || ex4 instanceof IllegalArgumentException, "login should throw for invalid credentials");

            // Case 5: Valid URL, properties, and simulate integrated security (should throw if not supported)
            java.util.Properties propsIntegrated = new java.util.Properties();
            propsIntegrated.setProperty("integratedSecurity", "true");
            Exception ex5 = assertThrows(Exception.class, () -> {
                try {
                    loginMethod.invoke(conn, "jdbc:sqlserver://localhost", propsIntegrated, null, null, false, false);
                } catch (java.lang.reflect.InvocationTargetException e) {
                    throw e.getCause();
                }
            });
            assertTrue(
                    ex5 instanceof com.microsoft.sqlserver.jdbc.SQLServerException
                            || ex5 instanceof IllegalArgumentException,
                    "login should throw for unsupported integrated security");

        } finally {
            logger.setLevel(oldLevel);
        }
    }

    /**
     * Test connectHelper for exception and logging code coverage via reflection.
     */
    @Test
    public void testConnectHelperExceptionCoverage() throws Exception {
        java.lang.reflect.Constructor<SQLServerConnection> ctor = SQLServerConnection.class
                .getDeclaredConstructor(String.class);
        ctor.setAccessible(true);
        SQLServerConnection conn = ctor.newInstance("test");
        java.lang.reflect.Method method = SQLServerConnection.class.getDeclaredMethod("connectHelper", String.class,
                java.util.Properties.class, String.class, String.class);
        method.setAccessible(true);
        // Pass nulls or invalid values to trigger exception paths
        try {
            method.invoke(conn, (String) null, null, null, null);
            fail("Expected exception not thrown");
        } catch (Exception e) {
            // Should throw due to invalid arguments or internal logic
            assertTrue(e.getCause() instanceof SQLException || e.getCause() instanceof NullPointerException);
        }
    }

    /**
     * Test resetNonRoutingEnvchangeValues for code coverage via reflection (already covered, but for completeness).
     */
    @Test
    public void testResetNonRoutingEnvchangeValuesExceptionCoverage() throws Exception {
        java.lang.reflect.Constructor<SQLServerConnection> ctor = SQLServerConnection.class
                .getDeclaredConstructor(String.class);
        ctor.setAccessible(true);
        SQLServerConnection conn = ctor.newInstance("test");
        java.lang.reflect.Method method = SQLServerConnection.class.getDeclaredMethod("resetNonRoutingEnvchangeValues");
        method.setAccessible(true);
        // Should not throw, but call for coverage
        method.invoke(conn);
    }

    /**
     * Test executeReconnect for exception and logging code coverage via reflection.
     */
    @Test
    public void testExecuteReconnectExceptionCoverage() throws Exception {
        java.lang.reflect.Constructor<SQLServerConnection> ctor = SQLServerConnection.class
                .getDeclaredConstructor(String.class);
        ctor.setAccessible(true);
        SQLServerConnection conn = ctor.newInstance("test");
        java.lang.reflect.Method method = SQLServerConnection.class.getDeclaredMethod("executeReconnect");
        method.setAccessible(true);
        // Simulate closed connection or invalid state
        java.lang.reflect.Field closedField = SQLServerConnection.class.getDeclaredField("isClosed");
        closedField.setAccessible(true);
        closedField.set(conn, true);
        try {
            method.invoke(conn);
            fail("Expected exception not thrown");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof SQLException || e.getCause() instanceof IllegalStateException);
        }
    }

    /**
     * Test executeCommand for exception and logging code coverage via reflection.
     */
    @Test
    public void testExecuteCommandExceptionCoverage() throws Exception {
        java.lang.reflect.Constructor<SQLServerConnection> ctor = SQLServerConnection.class
                .getDeclaredConstructor(String.class);
        ctor.setAccessible(true);
        SQLServerConnection conn = ctor.newInstance("test");
        java.lang.reflect.Method method = SQLServerConnection.class.getDeclaredMethod("executeCommand", String.class);
        method.setAccessible(true);
        // Pass null or invalid command to trigger exception
        try {
            method.invoke(conn, (String) null);
            fail("Expected exception not thrown");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof SQLException || e.getCause() instanceof NullPointerException);
        }
    }

    /**
     * Test abort for exception and logging code coverage.
     */
    @Test
    public void testAbortExceptionCoverage() throws Exception {
        java.lang.reflect.Constructor<SQLServerConnection> ctor = SQLServerConnection.class
                .getDeclaredConstructor(String.class);
        ctor.setAccessible(true);
        SQLServerConnection conn = ctor.newInstance("test");
        // Simulate closed connection
        java.lang.reflect.Field closedField = SQLServerConnection.class.getDeclaredField("isClosed");
        closedField.setAccessible(true);
        closedField.set(conn, true);
        try {
            conn.abort(null);
            fail("Expected exception not thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("closed") || e.getSQLState() != null);
        }
        // Also test abort with null executor on open connection
        closedField.set(conn, false);
        try {
            conn.abort(null);
            fail("Expected exception not thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage() != null);
        }
    }

    /**
     * Test processEnvChange for code coverage via reflection, including exception and edge cases.
     */
    @Test
    public void testProcessEnvChangeCoverage() throws Exception {
        java.lang.reflect.Constructor<SQLServerConnection> ctor = SQLServerConnection.class
                .getDeclaredConstructor(String.class);
        ctor.setAccessible(true);
        SQLServerConnection conn = ctor.newInstance("test");
        java.lang.reflect.Method method = SQLServerConnection.class.getDeclaredMethod("processEnvChange", Object.class);
        method.setAccessible(true);
        // Test with null argument (should handle gracefully or throw)
        try {
            method.invoke(conn, (Object) null);
        } catch (Exception e) {
            // Acceptable: method may throw for null input
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            assertTrue(cause instanceof SQLException || cause instanceof NullPointerException
                    || cause instanceof IllegalArgumentException);
        }
        // Test with a dummy object (simulate unexpected type)
        Object dummy = new Object();
        try {
            method.invoke(conn, dummy);
        } catch (Exception e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            assertTrue(cause instanceof SQLException || cause instanceof ClassCastException
                    || cause instanceof IllegalArgumentException);
        }
        // If possible, test with a mock or minimal valid env change object (if class is accessible)
        // This part is optional and will be skipped if the class is not public/accessible
        try {
            Class<?> envChangeClass = null;
            for (Class<?> inner : SQLServerConnection.class.getDeclaredClasses()) {
                if (inner.getSimpleName().equals("EnvChange")) {
                    envChangeClass = inner;
                    break;
                }
            }
            if (envChangeClass != null) {
                java.lang.reflect.Constructor<?> envCtor = envChangeClass.getDeclaredConstructors()[0];
                envCtor.setAccessible(true);
                // Try to create an instance with default or dummy values
                Object envChange = null;
                try {
                    // Try with zero args, else with nulls
                    if (envCtor.getParameterCount() == 0) {
                        envChange = envCtor.newInstance();
                    } else {
                        Object[] params = new Object[envCtor.getParameterCount()];
                        envChange = envCtor.newInstance(params);
                    }
                } catch (Exception ignore) {
                    // If cannot instantiate, skip
                }
                if (envChange != null) {
                    try {
                        method.invoke(conn, envChange);
                    } catch (Exception e) {
                        // Acceptable: may throw if not fully initialized
                        Throwable cause = e.getCause() != null ? e.getCause() : e;
                        assertTrue(cause instanceof SQLException || cause instanceof IllegalArgumentException
                                || cause instanceof NullPointerException);
                    }
                }
            }
        } catch (Exception ignore) {
            // If EnvChange class is not accessible, skip
        }
    }

    /**
     * Use-case driven test for executeDTCCommand method via reflection.
     * Covers exception and edge cases.
     */
    @Test
    public void testExecuteDTCCommandUseCases() throws Exception {
        java.lang.reflect.Constructor<SQLServerConnection> ctor = SQLServerConnection.class
                .getDeclaredConstructor(String.class);
        ctor.setAccessible(true);
        SQLServerConnection conn = ctor.newInstance("test");
        java.lang.reflect.Method method = SQLServerConnection.class.getDeclaredMethod("executeDTCCommand", int.class,
                byte[].class, int.class, int.class);
        method.setAccessible(true);
        // Case 1: Null byte array
        Exception ex1 = assertThrows(Exception.class, () -> {
            try {
                method.invoke(conn, 0, null, 0, 0);
            } catch (java.lang.reflect.InvocationTargetException e) {
                throw e.getCause();
            }
        });
        assertTrue(ex1 instanceof SQLException || ex1 instanceof NullPointerException
                || ex1 instanceof IllegalArgumentException);
        // Case 2: Empty byte array
        Exception ex2 = assertThrows(Exception.class, () -> {
            try {
                method.invoke(conn, 0, new byte[0], 0, 0);
            } catch (java.lang.reflect.InvocationTargetException e) {
                throw e.getCause();
            }
        });
        assertTrue(ex2 instanceof SQLException || ex2 instanceof IllegalArgumentException);
        // Case 3: Valid byte array but invalid state (simulate closed connection)
        java.lang.reflect.Field closedField = SQLServerConnection.class.getDeclaredField("isClosed");
        closedField.setAccessible(true);
        closedField.set(conn, true);
        Exception ex3 = assertThrows(Exception.class, () -> {
            try {
                method.invoke(conn, 0, new byte[] {1, 2, 3}, 0, 3);
            } catch (java.lang.reflect.InvocationTargetException e) {
                throw e.getCause();
            }
        });
        assertTrue(ex3 instanceof SQLException || ex3 instanceof IllegalStateException);
    }

    /**
     * Use-case driven test for prepareStatement methods.
     * Covers various overloads and exception/edge cases.
     */
    @Test
    public void testPrepareStatementUseCases() throws Exception {
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            // Case 1: Null SQL
            assertThrows(SQLException.class, () -> conn.prepareStatement(null));
            // Case 2: Empty SQL
            assertThrows(SQLException.class, () -> conn.prepareStatement(""));
            // Case 3: Valid SQL, default
            try (java.sql.PreparedStatement ps = conn.prepareStatement("SELECT 1")) {
                assertNotNull(ps);
            }
            // Case 4: Valid SQL, with auto-generated keys
            try (java.sql.PreparedStatement ps = conn.prepareStatement("SELECT 1",
                    java.sql.Statement.RETURN_GENERATED_KEYS)) {
                assertNotNull(ps);
            }
            // Case 5: Valid SQL, with column indexes
            try (java.sql.PreparedStatement ps = conn.prepareStatement("SELECT 1", new int[] {1})) {
                assertNotNull(ps);
            }
            // Case 6: Valid SQL, with column names
            try (java.sql.PreparedStatement ps = conn.prepareStatement("SELECT 1", new String[] {"col1"})) {
                assertNotNull(ps);
            }
            // Case 7: Valid SQL, with result set type, concurrency, holdability
            try (java.sql.PreparedStatement ps = conn.prepareStatement("SELECT 1", java.sql.ResultSet.TYPE_FORWARD_ONLY,
                    java.sql.ResultSet.CONCUR_READ_ONLY, java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
                assertNotNull(ps);
            }
        }
        // Case 8: Closed connection
        SQLServerConnection closedConn = (SQLServerConnection) PrepUtil.getConnection(connectionString);
        closedConn.close();
        assertThrows(SQLException.class, () -> closedConn.prepareStatement("SELECT 1"));
    }

    /**
     * Use-case driven test for setSavepoint and rollback methods.
     * Covers normal, edge, and exception cases.
     */
    @Test
    public void testSetSavepointAndRollbackCoverage() throws Exception {
        // Normal use-case: setSavepoint, rollback to savepoint, release savepoint
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setAutoCommit(false);
            try (java.sql.Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE #t (id int)");
                stmt.execute("INSERT INTO #t VALUES (1)");
                java.sql.Savepoint sp = conn.setSavepoint();
                stmt.execute("INSERT INTO #t VALUES (2)");
                conn.rollback(sp);
                stmt.execute("INSERT INTO #t VALUES (3)");
                conn.releaseSavepoint(sp);
                conn.commit();
                // Validate only 1 and 3 exist
                try (java.sql.ResultSet rs = stmt.executeQuery("SELECT id FROM #t ORDER BY id")) {
                    int count = 0;
                    int[] expected = {1, 3};
                    while (rs.next()) {
                        assertEquals(expected[count++], rs.getInt(1));
                    }
                    assertEquals(2, count);
                }
            }
        }
        // Named savepoint
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setAutoCommit(false);
            java.sql.Savepoint sp = conn.setSavepoint("mysave");
            conn.rollback(sp);
            conn.releaseSavepoint(sp);
            conn.commit();
        }
        // Exception: rollback to invalid savepoint
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setAutoCommit(false);
            java.sql.Savepoint sp = conn.setSavepoint();
            conn.releaseSavepoint(sp);
            assertThrows(SQLException.class, () -> conn.rollback(sp));
        }
        // Exception: setSavepoint/rollback on closed connection
        SQLServerConnection closedConn = (SQLServerConnection) PrepUtil.getConnection(connectionString);
        closedConn.close();
        assertThrows(SQLException.class, () -> closedConn.setSavepoint());
        assertThrows(SQLException.class, () -> closedConn.rollback());
    }

    /**
     * Test setNetworkTimeout for normal and exception coverage.
     */
    @Test
    public void testSetNetworkTimeoutCoverage() throws Exception {
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            // Normal: set network timeout with a valid executor
            java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor();
            conn.setNetworkTimeout(executor, 1000);
            assertEquals(1000, conn.getNetworkTimeout());
            executor.shutdownNow();
        }
        // Exception: setNetworkTimeout on closed connection
        SQLServerConnection closedConn = (SQLServerConnection) PrepUtil.getConnection(connectionString);
        closedConn.close();
        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        assertThrows(SQLException.class, () -> closedConn.setNetworkTimeout(executor, 1000));
        executor.shutdownNow();
        // Exception: setNetworkTimeout with null executor
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            assertThrows(SQLException.class, () -> conn.setNetworkTimeout(null, 1000));
        }
    }

    /**
     * Test supportsTransactions for coverage.
     */
    @Test
    public void testSupportsTransactionsCoverage() throws Exception {
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            // Should return true for SQL Server
            assertTrue(conn.supportsTransactions());
        }
        // Exception: supportsTransactions on closed connection
        SQLServerConnection closedConn = (SQLServerConnection) PrepUtil.getConnection(connectionString);
        closedConn.close();
        assertThrows(SQLException.class, () -> closedConn.supportsTransactions());
    }

    /**
     * Test setLockTimeout for normal and exception coverage.
     */
    @Test
    public void testSetLockTimeoutCoverage() throws Exception {
        // Use reflection to call setLockTimeout(int) if available, else skip
        java.lang.reflect.Method setLockTimeoutMethod = null;
        try {
            setLockTimeoutMethod = SQLServerConnection.class.getDeclaredMethod("setLockTimeout", int.class);
        } catch (NoSuchMethodException e) {
            // Method does not exist, skip test
            return;
        }
        // Normal: set lock timeout to 1000 ms
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            setLockTimeoutMethod.invoke(conn, 1000);
            // Should not throw, but validate by running a simple query
            try (java.sql.Statement stmt = conn.createStatement()) {
                stmt.execute("SELECT 1");
            }
        }
        // Exception: setLockTimeout on closed connection
        SQLServerConnection closedConn = (SQLServerConnection) PrepUtil.getConnection(connectionString);
        closedConn.close();
        SQLServerConnection finalClosedConn = closedConn;
        java.lang.reflect.Method finalSetLockTimeoutMethod = setLockTimeoutMethod;
        assertThrows(Exception.class, () -> finalSetLockTimeoutMethod.invoke(finalClosedConn, 1000));
        // Exception: setLockTimeout with negative value (should throw)
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            SQLServerConnection finalConn = conn;
            assertThrows(Exception.class, () -> finalSetLockTimeoutMethod.invoke(finalConn, -1));
        }
    }
}
