/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.DummyKeyStoreProvider;


/**
 * Test key store provider registration.
 */
@RunWith(JUnitPlatform.class)
public class RegisterKeyStoreProviderTest extends AbstractTest {

    private static final String dummyProviderName1 = "DummyProvider1";
    private static final String dummyProviderName2 = "DummyProvider2";
    private static final String dummyProviderName3 = "DummyProvider3";
    private static final String dummyProviderName4 = "DummyProvider4";

    private static Map<String, SQLServerColumnEncryptionKeyStoreProvider> singleKeyStoreProvider = new HashMap<>();
    private static Map<String, SQLServerColumnEncryptionKeyStoreProvider> multipleKeyStoreProviders = new HashMap<>();
    private static Map<String, SQLServerColumnEncryptionKeyStoreProvider> globalKeyStoreProviders = null;

    @BeforeAll
    public static void testSetup() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"trustServerCertificate", "true");
        setConnection();

        singleKeyStoreProvider.put(dummyProviderName1, new DummyKeyStoreProvider());

        multipleKeyStoreProviders.put(dummyProviderName2, new DummyKeyStoreProvider());
        multipleKeyStoreProviders.put(dummyProviderName3, new DummyKeyStoreProvider());
        multipleKeyStoreProviders.put(dummyProviderName4, new DummyKeyStoreProvider());

        if (null != SQLServerConnection.globalCustomColumnEncryptionKeyStoreProviders) {
            globalKeyStoreProviders = new HashMap<String, SQLServerColumnEncryptionKeyStoreProvider>(
                    SQLServerConnection.globalCustomColumnEncryptionKeyStoreProviders);
        }

        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();
    }

    @Test
    public void testNullProviderMap() {
        Map<String, SQLServerColumnEncryptionKeyStoreProvider> providers = null;
        assertExceptionIsThrownForAllCustomProviderCaches(providers,
                TestUtils.formatErrorMsg("R_CustomKeyStoreProviderMapNull"));
    }

    @Test
    public void testInvalidProviderNameOnGlobal() {
        Map<String, SQLServerColumnEncryptionKeyStoreProvider> providers = new HashMap<>();
        providers.put("MSSQL_DUMMY", new DummyKeyStoreProvider());

        assertExceptionIsThrownForGlobalCustomProviderCache(providers,
                TestUtils.formatErrorMsg("R_InvalidCustomKeyStoreProviderName"));
    }

    @Test
    public void testInvalidProviderNameAllLevels() {
        Map<String, SQLServerColumnEncryptionKeyStoreProvider> providers = new HashMap<>();
        providers.put(Constants.WINDOWS_KEY_STORE_NAME, new DummyKeyStoreProvider());
        assertExceptionIsThrownForAllCustomProviderCaches(providers,
                TestUtils.formatErrorMsg("R_InvalidCustomKeyStoreProviderName"));
    }

    @Test
    public void testNullProviderAllLevels() {
        Map<String, SQLServerColumnEncryptionKeyStoreProvider> providers = new HashMap<>();
        providers.put("DUMMY_PROVIDER", null);
        assertExceptionIsThrownForAllCustomProviderCaches(providers,
                TestUtils.formatErrorMsg("R_CustomKeyStoreProviderValueNull"));

    }

    @Test
    public void testEmptyProviderNameAllLevels() {
        Map<String, SQLServerColumnEncryptionKeyStoreProvider> providers = new HashMap<>();
        providers.put(" ", new DummyKeyStoreProvider());
        assertExceptionIsThrownForAllCustomProviderCaches(providers,
                TestUtils.formatErrorMsg("R_EmptyCustomKeyStoreProviderName"));
    }

    @Test
    public void testSetProviderOnlyOnceOnGlobal() {
        Map<String, SQLServerColumnEncryptionKeyStoreProvider> providers = new HashMap<>();
        providers.put("DUMMY_PROVIDER", new DummyKeyStoreProvider());

        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();

        try {
            SQLServerConnection.registerColumnEncryptionKeyStoreProviders(providers);
        } catch (Exception e) {
            fail(e.getMessage());
        }

        assertExceptionIsThrownForGlobalCustomProviderCache(providers,
                TestUtils.formatErrorMsg("R_CustomKeyStoreProviderSetOnce"));
        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();
    }

    @Test
    public void testSetProviderMoreThanOnceOnConnection() {
        try (SQLServerConnection conn = getConnection()) {
            conn.registerColumnEncryptionKeyStoreProvidersOnConnection(singleKeyStoreProvider);
            assertProviderCacheContainsExpectedProviders(conn.connectionColumnEncryptionKeyStoreProvider,
                    singleKeyStoreProvider);

            conn.registerColumnEncryptionKeyStoreProvidersOnConnection(multipleKeyStoreProviders);
            assertProviderCacheContainsExpectedProviders(conn.connectionColumnEncryptionKeyStoreProvider,
                    multipleKeyStoreProviders);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testSetProviderMoreThanOnceOnStatement() {
        try (SQLServerConnection conn = getConnection();
                SQLServerStatement stmt = (SQLServerStatement) conn.createStatement()) {
            stmt.registerColumnEncryptionKeyStoreProvidersOnStatement(singleKeyStoreProvider);
            assertProviderCacheContainsExpectedProviders(stmt.statementColumnEncryptionKeyStoreProviders,
                    singleKeyStoreProvider);

            stmt.registerColumnEncryptionKeyStoreProvidersOnStatement(multipleKeyStoreProviders);
            assertProviderCacheContainsExpectedProviders(stmt.statementColumnEncryptionKeyStoreProviders,
                    multipleKeyStoreProviders);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @AfterAll
    public static void testClearUp() throws Exception {
        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();

        if (null != globalKeyStoreProviders) {
            SQLServerConnection.registerColumnEncryptionKeyStoreProviders(globalKeyStoreProviders);
        }
    }

    private void assertProviderCacheContainsExpectedProviders(
            Map<String, SQLServerColumnEncryptionKeyStoreProvider> providerCache,
            Map<String, SQLServerColumnEncryptionKeyStoreProvider> expectedProviders) {
        assertEquals(providerCache.size(), expectedProviders.size());

        for (String key : expectedProviders.keySet()) {
            assertTrue(providerCache.containsKey(key));
        }
    }

    private void assertExceptionIsThrownForAllCustomProviderCaches(
            Map<String, SQLServerColumnEncryptionKeyStoreProvider> providers, String expectedString) {
        assertExceptionIsThrownForGlobalCustomProviderCache(providers, expectedString);

        try (SQLServerConnection conn = getConnection();
                SQLServerStatement stmt = (SQLServerStatement) conn.createStatement()) {
            try {
                conn.registerColumnEncryptionKeyStoreProvidersOnConnection(providers);
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            } catch (SQLServerException ex) {
                assertTrue(ex.getMessage().matches(expectedString));
            }

            try {
                stmt.registerColumnEncryptionKeyStoreProvidersOnStatement(providers);
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            } catch (SQLServerException ex) {
                assertTrue(ex.getMessage().matches(expectedString));
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }

    }

    private void assertExceptionIsThrownForGlobalCustomProviderCache(
            Map<String, SQLServerColumnEncryptionKeyStoreProvider> providers, String expectedString) {
        try {
            SQLServerConnection.registerColumnEncryptionKeyStoreProviders(providers);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException ex) {
            assertTrue(ex.getMessage().matches(expectedString));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

}
