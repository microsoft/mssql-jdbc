/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.AlwaysEncrypted;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionKeyStoreProvider;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * Regression test for the Always Encrypted secure-enclaves CEK caching bug: provider CEK cache disabled at
 * registration; enclave path bypasses {@code SQLServerSymmetricKeyCache}.
 *
 * <p>
 * All diagnostic output uses {@link System#out} so it appears unconditionally in test stdout (surefire console,
 * Azure DevOps job log, IDE Run window) regardless of JDK logging configuration. Run with:
 *
 * <pre>
 * mvn test -Pjre11 \
 *   -Dgroups=alwaysEncrypted \
 *   -DexcludedGroups=MSI,NTLM,clientCertAuth,fedAuth,kerberos \
 *   -Dtest=EnclaveCEKCachingTest
 * </pre>
 *
 * <p>
 * <b>Pre/Post fix expectations</b>
 *
 * <pre>
 *   Before fix:  decrypt calls during loop ≈ N (number of enclave queries)
 *   After fix :  decrypt calls during loop ≤ 2 (warmup miss + at most one structural lookup)
 * </pre>
 */
@RunWith(Parameterized.class)
@Tag(Constants.xSQLv11)
@Tag(Constants.xSQLv12)
@Tag(Constants.xSQLv14)
@Tag(Constants.xAzureSQLDW)
@Tag(Constants.xAzureSQLDB)
@Tag(Constants.reqExternalSetup)
@Tag(Constants.requireSecret)
@Tag(Constants.alwaysEncrypted)
public class EnclaveCEKCachingTest extends AESetup {

    private static final String TAG = "[ENCLAVE-CEK]";
    private static final String BAR =
            "======================================================================";
    private static final String DASH =
            "----------------------------------------------------------------------";

    /** Number of enclave queries to execute in the measurement loop. */
    private static final int QUERY_ITERATIONS = 50;

    /**
     * Maximum number of {@code decryptColumnEncryptionKey} invocations we tolerate across
     * {@link #QUERY_ITERATIONS} identical enclave queries when the fix is in place.
     */
    private static final int MAX_ALLOWED_DECRYPT_CALLS_AFTER_FIX = 2;

    /** Name of the table created for the test. */
    private static final String TABLE_NAME = "EnclaveCEKCachingRepro";

    /** Seed row contents. The {@code LIKE 'test_%'} predicate matches this. */
    private static final String SEED_VALUE = "test_value_for_enclave_caching";

    /** Counting wrapper around the Azure Key Vault provider; populated per test. */
    private static CountingKeyStoreProvider countingProvider;

    /* =====================================================================
     * Counting wrapper. We register this in place of the AKV provider via
     * SQLServerConnection.registerColumnEncryptionKeyStoreProviders(...) so that the bug's registration-time
     * setColumnEncryptionCacheTtl(0) call fires against THIS instance and we can observe everything.
     * ===================================================================== */
    private static final class CountingKeyStoreProvider extends SQLServerColumnEncryptionKeyStoreProvider {

        private final SQLServerColumnEncryptionKeyStoreProvider delegate;
        private final String providerName;

        final AtomicInteger decryptCalls = new AtomicInteger();
        final AtomicInteger encryptCalls = new AtomicInteger();
        final AtomicInteger verifyCalls = new AtomicInteger();
        final AtomicInteger setTtlCalls = new AtomicInteger();
        volatile Duration lastTtlSet;

        CountingKeyStoreProvider(SQLServerColumnEncryptionKeyStoreProvider delegate, String name) {
            this.delegate = delegate;
            this.providerName = name;
        }

        @Override
        public void setName(String name) {
            // provider name is fixed at registration; nothing to do
        }

        @Override
        public String getName() {
            return providerName;
        }

        @Override
        public byte[] decryptColumnEncryptionKey(String masterKeyPath, String encryptionAlgorithm,
                byte[] encryptedColumnEncryptionKey) throws SQLServerException {
            int n = decryptCalls.incrementAndGet();
            long t0 = System.nanoTime();
            byte[] result = delegate.decryptColumnEncryptionKey(masterKeyPath, encryptionAlgorithm,
                    encryptedColumnEncryptionKey);
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
            System.out.println(TAG + "[provider.decrypt #" + n + "] keyPath='" + masterKeyPath + "' algo='"
                    + encryptionAlgorithm + "' encKeyLen=" + encryptedColumnEncryptionKey.length
                    + " elapsedMs=" + elapsedMs + " <-- KV round-trip");
            return result;
        }

        @Override
        public byte[] encryptColumnEncryptionKey(String masterKeyPath, String encryptionAlgorithm,
                byte[] columnEncryptionKey) throws SQLServerException {
            int n = encryptCalls.incrementAndGet();
            System.out.println(TAG + "[provider.encrypt #" + n + "] keyPath='" + masterKeyPath + "'");
            return delegate.encryptColumnEncryptionKey(masterKeyPath, encryptionAlgorithm, columnEncryptionKey);
        }

        @Override
        public boolean verifyColumnMasterKeyMetadata(String masterKeyPath, boolean allowEnclaveComputations,
                byte[] signature) throws SQLServerException {
            int n = verifyCalls.incrementAndGet();
            long t0 = System.nanoTime();
            boolean result = delegate.verifyColumnMasterKeyMetadata(masterKeyPath, allowEnclaveComputations,
                    signature);
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
            System.out.println(TAG + "[provider.verify  #" + n + "] keyPath='" + masterKeyPath + "' enclave="
                    + allowEnclaveComputations + " sigLen=" + (signature == null ? 0 : signature.length)
                    + " elapsedMs=" + elapsedMs + " result=" + result);
            return result;
        }

        @Override
        public void setColumnEncryptionCacheTtl(Duration duration) {
            int n = setTtlCalls.incrementAndGet();
            Duration previous = lastTtlSet;
            lastTtlSet = duration;
            System.out.println(TAG + "[provider.setTtl  #" + n + "] previous=" + previous + " new=" + duration
                    + (Duration.ZERO.equals(duration)
                            ? "  <-- POISON: provider's internal SimpleTtlCache disabled"
                            : ""));
            delegate.setColumnEncryptionCacheTtl(duration);
        }

        @Override
        public Duration getColumnEncryptionKeyCacheTtl() {
            return delegate.getColumnEncryptionKeyCacheTtl();
        }

        void resetCounters() {
            decryptCalls.set(0);
            encryptCalls.set(0);
            verifyCalls.set(0);
            setTtlCalls.set(0);
        }

        @Override
        public String toString() {
            return "Counts[decrypt=" + decryptCalls.get() + ", encrypt=" + encryptCalls.get() + ", verify="
                    + verifyCalls.get() + ", setTtl=" + setTtlCalls.get() + ", lastTtl=" + lastTtlSet + "]";
        }
    }

    /* =====================================================================
     * Setup helpers
     * ===================================================================== */

    private static CountingKeyStoreProvider installCountingProvider() throws SQLServerException {
        assumeTrue(null != akvProvider, "Azure Key Vault provider is not configured; skipping. "
                + "Set applicationClientID/applicationKey/keyID system properties to enable.");

        System.out.println(BAR);
        System.out.println(TAG + " Installing CountingKeyStoreProvider in place of the global AKV provider");
        System.out.println(BAR);
        System.out.println(TAG + " Existing globally-registered AKV provider class : "
                + akvProvider.getClass().getName());
        System.out.println(TAG + " Existing AKV provider TTL (before swap)         : "
                + akvProvider.getColumnEncryptionKeyCacheTtl());

        CountingKeyStoreProvider counting = new CountingKeyStoreProvider(akvProvider, Constants.AZURE_KEY_VAULT_NAME);
        System.out.println(TAG + " Wrapper TTL (before re-register)                : "
                + counting.getColumnEncryptionKeyCacheTtl());

        System.out.println(TAG + " Calling SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders()");
        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();

        Map<String, SQLServerColumnEncryptionKeyStoreProvider> map = new HashMap<>();
        map.put(Constants.AZURE_KEY_VAULT_NAME, counting);

        System.out.println(TAG + " Calling SQLServerConnection.registerColumnEncryptionKeyStoreProviders(...)");
        System.out.println(TAG + "   --> EXPECT bug-site-1 println: provider.setColumnEncryptionCacheTtl(ZERO)");
        SQLServerConnection.registerColumnEncryptionKeyStoreProviders(map);

        System.out.println(TAG + " Post-registration wrapper state : " + counting);
        System.out.println(TAG + " Post-registration TTL on wrapper: " + counting.lastTtlSet
                + "    <-- should be PT0S; this confirms BUG-SITE-1 fired");
        System.out.println(BAR);
        return counting;
    }

    @AfterAll
    public static void restoreOriginalProvider() throws SQLServerException {
        if (null == akvProvider) {
            return;
        }
        System.out.println(TAG + " @AfterAll: restoring original AKV provider so other test classes are unaffected");
        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();
        Map<String, SQLServerColumnEncryptionKeyStoreProvider> map = new HashMap<>();
        map.put(Constants.AZURE_KEY_VAULT_NAME, akvProvider);
        SQLServerConnection.registerColumnEncryptionKeyStoreProviders(map);
    }

    /* =====================================================================
     * The test itself
     * ===================================================================== */

    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testEnclaveCEKIsCached(String serverName, String url, String protocol) throws Exception {

        assumeTrue(null != serverName && null != url && null != protocol,
                "No enclave server/url/protocol configured; cannot exercise enclave path.");

        System.out.println();
        System.out.println(BAR);
        System.out.println(TAG + " ENCLAVE CEK CACHING TEST");
        System.out.println(BAR);
        System.out.println(TAG + " serverName             = " + serverName);
        System.out.println(TAG + " enclaveAttestationUrl  = " + url);
        System.out.println(TAG + " enclaveAttestationProto= " + protocol);
        System.out.println(TAG + " iterations             = " + QUERY_ITERATIONS);
        System.out.println(TAG + " maxAllowedAfterFix     = " + MAX_ALLOWED_DECRYPT_CALLS_AFTER_FIX);
        System.out.println(BAR);

        setAEConnectionString(serverName, url, protocol);
        System.out.println(TAG + " AETestConnectionString = " + AETestConnectionString);

        countingProvider = installCountingProvider();
        countingProvider.resetCounters();
        System.out.println(TAG + " Counters reset BEFORE running the test workload. State now: " + countingProvider);

        assertNotNull(cekAkv, "cekAkv must have been created by AESetup");
        System.out.println(TAG + " Using CEK created by AESetup: cekAkv = " + cekAkv);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {

            System.out.println(TAG + " Dropping pre-existing table (if any): " + TABLE_NAME);
            TestUtils.dropTableIfExists(TABLE_NAME, stmt);

            // varcharTableSimple is "Varchar varchar(20) COLLATE LATIN1_GENERAL_BIN2" -- exactly the BIN2 collation
            // required to make LIKE engage the enclave on a RANDOMIZED column.
            System.out.println(TAG + " Creating table " + TABLE_NAME + " with varcharTableSimple + cekAkv");
            createTable(TABLE_NAME, cekAkv, varcharTableSimple);

            // ---- Phase A: INSERT (standard AE path; NOT the enclave path) ----
            System.out.println(DASH);
            System.out.println(TAG + " PHASE A : INSERT (standard AE path; no enclave)");
            System.out.println(DASH);
            try (PreparedStatement insert = con
                    .prepareStatement("INSERT INTO " + TABLE_NAME + " VALUES (?,?,?)")) {
                insert.setString(1, "a");
                insert.setString(2, "b");
                insert.setString(3, SEED_VALUE);
                insert.execute();
            }
            int decryptAfterInsert = countingProvider.decryptCalls.get();
            int verifyAfterInsert = countingProvider.verifyCalls.get();
            int setTtlAfterInsert = countingProvider.setTtlCalls.get();
            System.out.println(TAG + " After INSERT warmup           : " + countingProvider);
            System.out.println(TAG + "   decrypt=" + decryptAfterInsert + "  verify=" + verifyAfterInsert
                    + "  setTtl=" + setTtlAfterInsert + "  (standard path warmed driver cache)");

            // ---- Phase B: ENCLAVE LIKE LOOP ----
            System.out.println(DASH);
            System.out.println(TAG + " PHASE B : ENCLAVE LIKE loop (" + QUERY_ITERATIONS + " iterations)");
            System.out.println(TAG + "           Query: SELECT * FROM " + TABLE_NAME
                    + " WHERE RANDOMIZEDVarchar LIKE 'test_%'");
            System.out.println(TAG + "           BUGGY behaviour : N decrypt calls, ~hundreds of ms per query");
            System.out.println(TAG + "           FIXED behaviour : ≤" + MAX_ALLOWED_DECRYPT_CALLS_AFTER_FIX
                    + " decrypt calls, ~tens of ms per query");
            System.out.println(DASH);

            long startNanos = System.nanoTime();
            int matchedRows = 0;
            for (int i = 0; i < QUERY_ITERATIONS; i++) {
                int decryptBefore = countingProvider.decryptCalls.get();
                long iterStart = System.nanoTime();
                try (PreparedStatement pstmt = con.prepareStatement(
                        "SELECT * FROM " + TABLE_NAME + " WHERE RANDOMIZEDVarchar LIKE ?")) {
                    pstmt.setString(1, "test_%");
                    try (ResultSet rs = pstmt.executeQuery()) {
                        while (rs.next()) {
                            matchedRows++;
                        }
                    }
                }
                long iterMs = (System.nanoTime() - iterStart) / 1_000_000L;
                int decryptDelta = countingProvider.decryptCalls.get() - decryptBefore;
                // Print every iteration: per-iteration decrypt delta + wall time make the bug obvious.
                System.out.println(TAG + "   iter " + String.format("%2d", i) + " : decryptDelta="
                        + decryptDelta + "  iterMs=" + iterMs
                        + (decryptDelta == 0 ? "  (cache hit)" : "  (Key Vault round-trip)"));
            }
            long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;

            int decryptDuringLoop = countingProvider.decryptCalls.get() - decryptAfterInsert;
            int verifyDuringLoop = countingProvider.verifyCalls.get() - verifyAfterInsert;
            int setTtlDuringLoop = countingProvider.setTtlCalls.get() - setTtlAfterInsert;
            double msPerQuery = elapsedMillis / (double) QUERY_ITERATIONS;

            // ---- Phase C: Tally ----
            System.out.println();
            System.out.println(BAR);
            System.out.println(TAG + " FINAL TALLY");
            System.out.println(BAR);
            System.out.println(TAG + "   Server                : " + serverName);
            System.out.println(TAG + "   Iterations            : " + QUERY_ITERATIONS);
            System.out.println(TAG + "   Total elapsed         : " + elapsedMillis + " ms");
            System.out.println(TAG + "   Avg per query         : " + String.format("%.2f", msPerQuery) + " ms");
            System.out.println(TAG + "   Matched rows (sanity) : " + matchedRows
                    + "   (expected: " + QUERY_ITERATIONS + ", one match per iteration)");
            System.out.println(TAG + "   decrypt calls IN LOOP : " + decryptDuringLoop + "    (cumulative "
                    + countingProvider.decryptCalls.get() + ")");
            System.out.println(TAG + "   verify  calls IN LOOP : " + verifyDuringLoop + "    (cumulative "
                    + countingProvider.verifyCalls.get() + ")");
            System.out.println(TAG + "   setTtl  calls IN LOOP : " + setTtlDuringLoop + "    (cumulative "
                    + countingProvider.setTtlCalls.get() + ", last value " + countingProvider.lastTtlSet + ")");
            System.out.println(DASH);
            System.out.println(TAG + "   Pre-fix expectation   : decryptDuringLoop ≈ " + QUERY_ITERATIONS
                    + "  (Key Vault round-trip on every query)");
            System.out.println(TAG + "   Post-fix expectation  : decryptDuringLoop ≤ "
                    + MAX_ALLOWED_DECRYPT_CALLS_AFTER_FIX + "  (cache absorbs all but warmup)");
            System.out.println(TAG + "   Observed              : decryptDuringLoop = " + decryptDuringLoop
                    + "  ==> " + (decryptDuringLoop <= MAX_ALLOWED_DECRYPT_CALLS_AFTER_FIX
                            ? "FIXED (cache is working)"
                            : "BUG PRESENT (cache is bypassed)"));
            System.out.println(BAR);

            // ---- Assertions ----
            assertTrue(decryptDuringLoop <= MAX_ALLOWED_DECRYPT_CALLS_AFTER_FIX,
                    String.format(
                            "ENCLAVE CEK CACHING NOT FIXED: Observed %d calls to decryptColumnEncryptionKey across %d "
                                    + "identical enclave queries against the SAME key path. Expected <= %d. "
                                    + "The enclave path in ISQLServerEnclaveProvider#processSDPEv1 is calling "
                                    + "provider.decryptColumnEncryptionKey directly, bypassing "
                                    + "SQLServerSymmetricKeyCache, while the provider's own SimpleTtlCache is "
                                    + "disabled by setColumnEncryptionCacheTtl(Duration.ZERO).",
                            decryptDuringLoop, QUERY_ITERATIONS, MAX_ALLOWED_DECRYPT_CALLS_AFTER_FIX));

            assertTrue(verifyDuringLoop <= MAX_ALLOWED_DECRYPT_CALLS_AFTER_FIX,
                    String.format(
                            "Regression in CMK signature verify caching: %d verifyColumnMasterKeyMetadata calls "
                                    + "across %d enclave queries (expected <= %d).",
                            verifyDuringLoop, QUERY_ITERATIONS, MAX_ALLOWED_DECRYPT_CALLS_AFTER_FIX));

            try (Statement s = con.createStatement()) {
                TestUtils.dropTableIfExists(TABLE_NAME, s);
            }
        }
    }
}
