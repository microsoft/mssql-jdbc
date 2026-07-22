/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.testframework.Constants;


/**
 * Regression tests for issue #2957: CEK decryption must be served from
 * {@link SQLServerSymmetricKeyCache} on both the non-enclave AE path and the enclave AE path. Each path should call the
 * underlying CMK provider's {@code decryptColumnEncryptionKey} at most once per unique
 * {@code (serverName, encryptedCEK, keyStoreName)} triple.
 *
 * Pure unit test — no SQL Server, no Key Vault, no network. Lives in package
 * {@code com.microsoft.sqlserver.jdbc} to reach the package-private types
 * ({@link EncryptionKeyInfo}, {@link SQLServerSymmetricKeyCache}, default method
 * {@code ISQLServerEnclaveProvider#processSDPEv1}).
 */
public class EnclaveCacheUnitTest {

    private static final String PROVIDER_NAME = "ENCLAVE_CACHE_TEST_PROVIDER";
    private static final String SERVER = "test-server";
    private static final String KEY_PATH = "https://vault/keys/k/v";
    private static final String ALGO = "RSA_OAEP";
    private static final byte[] PLAINTEXT_CEK = new byte[32]; // not the cache key; bytes value is irrelevant

    // Distinct wrapped-CEK bytes per test method so the singleton cache cannot satisfy
    // a lookup from a previously-run test.
    private static final byte[] ENC_CEK_NON_ENCLAVE = {1, 1, 1, 1};
    private static final byte[] ENC_CEK_ENCLAVE = {2, 2, 2, 2};

    private final AtomicInteger decryptCalls = new AtomicInteger();

    private final SQLServerColumnEncryptionKeyStoreProvider provider = new SQLServerColumnEncryptionKeyStoreProvider() {
        private String name = PROVIDER_NAME;

        @Override
        public void setName(String n) {
            this.name = n;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public byte[] decryptColumnEncryptionKey(String p, String a, byte[] enc) {
            decryptCalls.incrementAndGet();
            return PLAINTEXT_CEK;
        }

        @Override
        public byte[] encryptColumnEncryptionKey(String p, String a, byte[] cek) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean verifyColumnMasterKeyMetadata(String p, boolean e, byte[] s) {
            return true;
        }
    };

    @BeforeEach
    public void registerProvider() throws SQLServerException {
        // Each test starts with a clean global registration. unregister is a no-op if nothing is registered.
        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();
        SQLServerConnection
                .registerColumnEncryptionKeyStoreProviders(Collections.singletonMap(PROVIDER_NAME, provider));
        decryptCalls.set(0);
    }

    @AfterEach
    public void unregisterProvider() throws SQLServerException {
        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();
    }

    // ----------------------------------------------------------------
    // Non-enclave path
    // ----------------------------------------------------------------

    /**
     * The non-enclave path goes through {@link SQLServerSymmetricKeyCache#getKey} via
     * {@code SQLServerSecurityUtility.decryptSymmetricKey}. N lookups for the same wrapped CEK must call the provider
     * exactly once.
     */
    @Test
    public void nonEnclavePath_hitsCacheAfterFirstLookup() throws Exception {
        SQLServerConnection conn = mock(SQLServerConnection.class);
        when(conn.getTrustedServerNameAE()).thenReturn(SERVER);
        when(conn.getSystemOrGlobalColumnEncryptionKeyStoreProvider(PROVIDER_NAME)).thenReturn(provider);

        EncryptionKeyInfo keyInfo = new EncryptionKeyInfo(ENC_CEK_NON_ENCLAVE, 1, 1, 1, new byte[8], KEY_PATH,
                PROVIDER_NAME, ALGO);

        for (int i = 0; i < 50; i++) {
            SQLServerSymmetricKey k = SQLServerSymmetricKeyCache.getInstance().getKey(keyInfo, conn);
            assertEquals(32, k.getRootKey().length);
        }

        assertEquals(1, decryptCalls.get(),
                "Non-enclave path: provider must be called once for 50 lookups of same CEK");
    }

    // ----------------------------------------------------------------
    // Enclave path
    // ----------------------------------------------------------------

    /**
     * The enclave path runs through {@link ISQLServerEnclaveProvider#processSDPEv1}. Before issue #2957's fix to
     * "Site 4", every call invoked {@code provider.decryptColumnEncryptionKey} directly with no cache, so N invocations
     * produced ~N+1 provider calls. After the fix, both the enclave CEK lookup and the parameter CEK lookup share
     * {@link SQLServerSymmetricKeyCache}, so a single provider call satisfies all N invocations.
     */
    @Test
    public void enclavePath_hitsCacheAfterFirstLookup() throws Exception {
        final int iterations = 25;
        for (int i = 0; i < iterations; i++) {
            invokeProcessSDPEv1OnceWithOneEnclaveCek();
        }

        assertEquals(1, decryptCalls.get(), "Enclave path: provider must be called once for " + iterations
                + " enclave-query invocations of same CEK");
    }

    /**
     * Drives a single {@code processSDPEv1} call with a fabricated {@code sp_describe_parameter_encryption} result set
     * that describes one encrypted parameter whose CEK is required by the enclave.
     */
    private void invokeProcessSDPEv1OnceWithOneEnclaveCek() throws Exception {
        SQLServerConnection conn = mock(SQLServerConnection.class);
        when(conn.getTrustedServerNameAE()).thenReturn(SERVER);
        when(conn.getSystemOrGlobalColumnEncryptionKeyStoreProvider(PROVIDER_NAME)).thenReturn(provider);
        when(conn.getServerColumnEncryptionVersion()).thenReturn(ColumnEncryptionVersion.AE_V2);

        SQLServerStatement sqlStmt = mock(SQLServerStatement.class);
        when(sqlStmt.hasColumnEncryptionKeyStoreProvidersRegistered()).thenReturn(false);

        SQLServerPreparedStatement prepStmt = mock(SQLServerPreparedStatement.class);

        // ---- ResultSet #1: one CEK row, requested-by-enclave = true ----
        ResultSet rs1 = mock(ResultSet.class);
        when(rs1.next()).thenReturn(true, false);
        when(rs1.getInt(DescribeParameterEncryptionResultSet1.KEYORDINAL.value())).thenReturn(0);
        when(rs1.getString(DescribeParameterEncryptionResultSet1.PROVIDERNAME.value())).thenReturn(PROVIDER_NAME);
        when(rs1.getString(DescribeParameterEncryptionResultSet1.KEYENCRYPTIONALGORITHM.value())).thenReturn(ALGO);
        when(rs1.getString(DescribeParameterEncryptionResultSet1.KEYPATH.value())).thenReturn(KEY_PATH);
        when(rs1.getInt(DescribeParameterEncryptionResultSet1.DBID.value())).thenReturn(1);
        when(rs1.getBytes(DescribeParameterEncryptionResultSet1.KEYMDVERSION.value())).thenReturn(new byte[8]);
        when(rs1.getInt(DescribeParameterEncryptionResultSet1.KEYID.value())).thenReturn(1);
        when(rs1.getInt(DescribeParameterEncryptionResultSet1.KEYVERSION.value())).thenReturn(1);
        when(rs1.getBytes(DescribeParameterEncryptionResultSet1.ENCRYPTEDKEY.value())).thenReturn(ENC_CEK_ENCLAVE);
        when(rs1.getBoolean(DescribeParameterEncryptionResultSet1.ISREQUESTEDBYENCLAVE.value())).thenReturn(true);
        when(rs1.getBytes(DescribeParameterEncryptionResultSet1.ENCLAVECMKSIGNATURE.value())).thenReturn(new byte[64]);

        // ---- ResultSet #2: parameter -> CEK ordinal mapping ----
        ResultSet rs2 = mock(ResultSet.class);
        when(rs2.next()).thenReturn(true, false);
        when(rs2.getString(DescribeParameterEncryptionResultSet2.PARAMETERNAME.value())).thenReturn("@p0");
        when(rs2.getInt(DescribeParameterEncryptionResultSet2.COLUMNENCRYPTIONKEYORDINAL.value())).thenReturn(0);
        when(rs2.getInt(DescribeParameterEncryptionResultSet2.COLUMNENCRYPTIONTYPE.value()))
                .thenReturn((int) SQLServerEncryptionType.RANDOMIZED.value);
        when(rs2.getInt(DescribeParameterEncryptionResultSet2.COLUMNENCRYPTIONALGORITHM.value()))
                .thenReturn(TDS.AEAD_AES_256_CBC_HMAC_SHA256);
        when(rs2.getInt(DescribeParameterEncryptionResultSet2.NORMALIZATIONRULEVERSION.value())).thenReturn(1);

        when(prepStmt.getMoreResults()).thenReturn(true);
        when(prepStmt.getResultSet()).thenReturn(rs2);

        Parameter param = new Parameter(false);
        Parameter[] params = {param};
        ArrayList<String> paramNames = new ArrayList<>();
        paramNames.add("@p0");
        ArrayList<byte[]> enclaveCEKs = new ArrayList<>();

        // Any concrete enclave provider works; processSDPEv1 is a default method on the interface.
        ISQLServerEnclaveProvider ep = new SQLServerNoneEnclaveProvider();
        ep.processSDPEv1("SELECT 1", "@p0 int", params, paramNames, conn, sqlStmt, prepStmt, rs1, enclaveCEKs);

        assertEquals(1, enclaveCEKs.size(), "expected one enclave CEK entry per call");
    }

    // ----------------------------------------------------------------
    // Manual performance test (requires external Azure SQL + AKV setup)
    // ----------------------------------------------------------------

    // Fill in connection details for your test environment:
    // - Azure SQL with VBS enclave enabled (DC-series or GP_S with enclave config)
    // - AKV key for CMK, app registration with KeyVault Crypto User role
    // - Table: dbo.TestEnclave (Id INT PK, SSN NVARCHAR(50) COLLATE Latin1_General_BIN2, Salary INT)
    //   with SSN and Salary encrypted (Randomized, AEAD_AES_256_CBC_HMAC_SHA_256)
    private static final String PERF_TEST_CONNECTION_URL = "jdbc:sqlserver://<your-server>.database.windows.net;"
            + "database=<your-database>;"
            + "user=<your-user>;"
            + "password=<your-password>;"
            + "columnEncryptionSetting=Enabled;"
            + "enclaveAttestationProtocol=NONE;"
            + "keyStoreAuthentication=KeyVaultClientSecret;"
            + "keyStorePrincipalId=<your-app-client-id>;"
            + "keyStoreSecret=<your-app-client-secret>;"
            + "encrypt=true;"
            + "trustServerCertificate=true;";

    /**
     * Performance test for enclave CEK cache fix (issue #2957).
     *
     * Validates that enclave queries reuse cached CEKs on subsequent executions instead of
     * re-fetching from Azure Key Vault on every call. With the fix, Run 1+ should be ~5x
     * faster than Run 0 (cache hit vs. cold AKV fetch).
     *
     * Expected results:
     *   With fix:    Run 0 ~1500-2000ms (cold AKV fetch), Run 1+ ~400ms (cache hit)
     *   Without fix: ALL runs ~2000ms (CEK refetched every time)
     */
    @Disabled("Manual performance test — requires external Azure SQL + AKV setup")
    @Tag(Constants.reqExternalSetup)
    @Test
    public void testEnclaveCekCachePerformance() throws Exception {
        System.out.println("Connecting...");
        try (Connection conn = DriverManager.getConnection(PERF_TEST_CONNECTION_URL)) {
            System.out.println("Connected! Server: " + conn.getMetaData().getDatabaseProductVersion());

            // --- Insert test data ---
            System.out.println("\n--- Inserting test data ---");
            try (PreparedStatement ps = conn.prepareStatement(
                    "IF NOT EXISTS (SELECT 1 FROM dbo.TestEnclave WHERE Id=?) "
                            + "INSERT INTO dbo.TestEnclave (Id, SSN, Salary) VALUES (?, ?, ?)")) {
                int[][] data = {{1, 85000}, {2, 92000}, {3, 75000}};
                String[] ssns = {"123-45-6789", "987-65-4321", "111-22-3333"};
                for (int i = 0; i < 3; i++) {
                    ps.setInt(1, data[i][0]);
                    ps.setInt(2, data[i][0]);
                    ps.setNString(3, ssns[i]);
                    ps.setInt(4, data[i][1]);
                    ps.executeUpdate();
                }
            }
            System.out.println("Data inserted.");

            // --- Enclave query: LIKE on randomized column (requires enclave) ---
            System.out.println("\n--- Enclave query timing (LIKE on randomized SSN) ---");
            System.out.println("  With fix: Run 0 ~1500-2000ms (cold AKV fetch), Run 1+ ~400ms (cache hit)");
            System.out.println("  Without fix: ALL runs ~2000ms (CEK refetched every time)");
            String sql = "SELECT * FROM dbo.TestEnclave WHERE SSN LIKE ?";

            for (int i = 0; i < 10; i++) {
                long start = System.nanoTime();
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setNString(1, "%45%");
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            // consume results
                        }
                    }
                }
                long ms = (System.nanoTime() - start) / 1_000_000;
                System.out.println("  Run " + i + ": " + ms + " ms");
            }

            // --- Non-enclave query for baseline comparison ---
            System.out.println("\n--- Non-enclave equality query timing (baseline) ---");
            String eqSql = "SELECT * FROM dbo.TestEnclave WHERE Id = ?";
            for (int i = 0; i < 5; i++) {
                long start = System.nanoTime();
                try (PreparedStatement ps = conn.prepareStatement(eqSql)) {
                    ps.setInt(1, 1);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            // consume
                        }
                    }
                }
                long ms = (System.nanoTime() - start) / 1_000_000;
                System.out.println("  Run " + i + ": " + ms + " ms");
            }
        }
        System.out.println("\nDone.");
    }
}
