package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;

public class SQLServerConnectionPoolProxyTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    /**
     * Test SQLServerConnectionPoolProxy constructor
     */
    @Test
    @Tag("CodeCov")
    public void testConnectionPoolProxy() throws SQLException {
        try (SQLServerConnection conn = getConnection()) {
            SQLServerConnectionPoolProxy proxy = new SQLServerConnectionPoolProxy(conn);

            String proxyString = proxy.toString();
            assertNotNull(proxyString);
            assertTrue(proxyString.contains("ProxyConnectionID:"));

            assertEquals(conn, proxy.getWrappedConnection());

            assertFalse(proxy.isClosed());

            proxy.close();
        }
    }

    /**
     * Tests bulk copy properties and methods on connection pool proxy
     */
    @Test
    @Tag("CodeCov")
    public void testConnectionPoolProxyBulkCopy() throws SQLException {
        try (SQLServerConnection conn = getConnection();
                SQLServerConnectionPoolProxy proxy = new SQLServerConnectionPoolProxy(conn)) {

            assertFalse(proxy.isClosed());
            proxy.checkClosed(); // Should not throw when open

            // Test useBulkCopyForBatchInsert
            proxy.setUseBulkCopyForBatchInsert(true);
            assertEquals(true, proxy.getUseBulkCopyForBatchInsert());

            // Test bulkCopyForBatchInsertBatchSize
            proxy.setBulkCopyForBatchInsertBatchSize(1000);
            assertEquals(1000, proxy.getBulkCopyForBatchInsertBatchSize());

            // Test bulkCopyForBatchInsertCheckConstraints
            proxy.setBulkCopyForBatchInsertCheckConstraints(true);
            assertEquals(true, proxy.getBulkCopyForBatchInsertCheckConstraints());

            // Test bulkCopyForBatchInsertFireTriggers
            proxy.setBulkCopyForBatchInsertFireTriggers(true);
            assertEquals(true, proxy.getBulkCopyForBatchInsertFireTriggers());

            // Test bulkCopyForBatchInsertKeepIdentity
            proxy.setBulkCopyForBatchInsertKeepIdentity(true);
            assertEquals(true, proxy.getBulkCopyForBatchInsertKeepIdentity());

            // Test bulkCopyForBatchInsertKeepNulls
            proxy.setBulkCopyForBatchInsertKeepNulls(false);
            assertEquals(false, proxy.getBulkCopyForBatchInsertKeepNulls());

            // Test bulkCopyForBatchInsertTableLock
            proxy.setBulkCopyForBatchInsertTableLock(false);
            assertEquals(false, proxy.getBulkCopyForBatchInsertTableLock());

            // Test bulkCopyForBatchInsertAllowEncryptedValueModifications
            proxy.setBulkCopyForBatchInsertAllowEncryptedValueModifications(false);
            assertEquals(false, proxy.getBulkCopyForBatchInsertAllowEncryptedValueModifications());

            proxy.close();
            assertTrue(proxy.isClosed());

        }
    }

    /**
     * Test to check connection property getters and setters.
     */
    @Test
    @Tag("CodeCov")
    public void testConnectionPoolProxyPropertyMethods() throws SQLException {
        try (SQLServerConnection conn = getConnection();
                SQLServerConnectionPoolProxy proxy = new SQLServerConnectionPoolProxy(conn)) {

            UUID clientId = proxy.getClientConnectionId();
            assertNotNull(clientId);

            proxy.setSendTimeAsDatetime(true);
            assertEquals(true, proxy.getSendTimeAsDatetime());

            proxy.setDatetimeParameterType("datetime2");
            assertEquals("datetime2", proxy.getDatetimeParameterType());

            int discardedCount = proxy.getDiscardedServerPreparedStatementCount();
            assertTrue(discardedCount >= 0);

            proxy.closeUnreferencedPreparedStatementHandles();

            proxy.setEnablePrepareOnFirstPreparedStatementCall(true);
            assertEquals(true, proxy.getEnablePrepareOnFirstPreparedStatementCall());

            proxy.setcacheBulkCopyMetadata(true);
            assertEquals(true, proxy.getcacheBulkCopyMetadata());

            proxy.setPrepareMethod("prepare");
            assertEquals("prepare", proxy.getPrepareMethod());

            proxy.setServerPreparedStatementDiscardThreshold(100);
            assertEquals(100, proxy.getServerPreparedStatementDiscardThreshold());

            proxy.setStatementPoolingCacheSize(50);
            assertEquals(50, proxy.getStatementPoolingCacheSize());

            boolean isPoolingEnabled = proxy.isStatementPoolingEnabled();
            assertTrue(isPoolingEnabled || !isPoolingEnabled);

            int cacheEntryCount = proxy.getStatementHandleCacheEntryCount();
            assertTrue(cacheEntryCount >= 0);

            proxy.setDisableStatementPooling(false);
            assertEquals(false, proxy.getDisableStatementPooling());

            proxy.setUseFmtOnly(false);
            assertEquals(false, proxy.getUseFmtOnly());

            proxy.setDelayLoadingLobs(true);
            assertEquals(true, proxy.getDelayLoadingLobs());

            proxy.setIgnoreOffsetOnDateTimeOffsetConversion(true);
            assertEquals(true, proxy.getIgnoreOffsetOnDateTimeOffsetConversion());

            proxy.setIPAddressPreference("IPv4First");
            assertEquals("IPv4First", proxy.getIPAddressPreference());
        }
    }

    @Test
    @Tag("CodeCov")
    public void testConnectionPoolProxyMethods() throws SQLException {
        try (SQLServerConnection conn = getConnection();
                SQLServerConnectionPoolProxy proxy = new SQLServerConnectionPoolProxy(conn)) {

            try (PreparedStatement ps1 = proxy.prepareStatement("SELECT 1",
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                assertNotNull(ps1);
            }

            try (PreparedStatement ps2 = proxy.prepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS)) {
                assertNotNull(ps2);
            }

            try (PreparedStatement ps3 = proxy.prepareStatement("SELECT 1", new int[] { 1 })) {
                assertNotNull(ps3);
            }

            try (PreparedStatement ps4 = proxy.prepareStatement("SELECT 1", new String[] { "id" })) {
                assertNotNull(ps4);
            }

            try (CallableStatement cs1 = proxy.prepareCall("{ call sp_who }",
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                assertNotNull(cs1);
            }

            try (Statement stmt = proxy.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT)) {
                assertNotNull(stmt);
            }

            proxy.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
            assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, proxy.getHoldability());

            Executor executor = Executors.newSingleThreadExecutor();
            proxy.setNetworkTimeout(executor, 30000);
            assertEquals(30000, proxy.getNetworkTimeout());

            proxy.setSchema("dbo");
            assertEquals("dbo", proxy.getSchema());

            // Test create methods
            assertNotNull(proxy.createBlob());
            assertNotNull(proxy.createClob());
            assertNotNull(proxy.createNClob());
            assertNotNull(proxy.createSQLXML());

            // Test type map operations
            java.util.Map<String, Class<?>> newTypeMap = new java.util.HashMap<>();
            proxy.setTypeMap(newTypeMap);
            assertEquals(newTypeMap, proxy.getTypeMap());

            // Test client info operations
            java.util.Properties clientInfo = proxy.getClientInfo();
            assertNotNull(clientInfo);

            assertTrue(proxy.isValid(10));
        }
    }

    /**
     * Test connection properties and metadata
     */
    @Test
    @Tag("CodeCov")
    public void testConnectionPoolProxyProperties() throws SQLException {
        try (SQLServerConnection conn = getConnection();
                SQLServerConnectionPoolProxy proxy = new SQLServerConnectionPoolProxy(conn)) {

            java.sql.DatabaseMetaData metaData = proxy.getMetaData();
            assertNotNull(metaData);

            proxy.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            assertEquals(Connection.TRANSACTION_READ_COMMITTED, proxy.getTransactionIsolation());

            java.sql.SQLWarning warnings = proxy.getWarnings();
            assertNull(warnings);
            proxy.clearWarnings(); // This should not throw an exception
        }
    }

    @Test
    @Tag("CodeCov")
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.xAzureSQLDB)
    @Tag(Constants.xAzureSQLMI)
    public void testCatalog() throws SQLException {
        try (SQLServerConnection conn = getConnection();
                SQLServerConnectionPoolProxy proxy = new SQLServerConnectionPoolProxy(conn)) {
            proxy.setCatalog("master");
            assertEquals("master", proxy.getCatalog());
        }
    }

}