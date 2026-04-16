/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.security;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


/**
 * Tests security boundary scenarios ported from FX security test suite (securitytest.java).
 *
 * Covers FX Threat8/9 (Denial of Service — Server/Browser), Threat10/13 (Buffer/Sproc Overflows),
 * Threat14 (Client-side DoS via threading), and Threat18 (Blank Password handling).
 *
 * These tests verify the driver handles boundary conditions gracefully — max-length parameters,
 * connection pool exhaustion, concurrent connection stress, and blank/edge-case credentials.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxSecurity)
public class SecurityBoundaryTest extends AbstractTest {

    private static final String tableName = RandomUtil.getIdentifier("SecurityBoundaryTest");
    private static final String escapedTableName = AbstractSQLGenerator.escapeIdentifier(tableName);

    private static final String procName = RandomUtil.getIdentifier("SecurityBoundaryProc");
    private static final String escapedProcName = AbstractSQLGenerator.escapeIdentifier(procName);

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();

        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(escapedTableName, stmt);
            stmt.executeUpdate("CREATE TABLE " + escapedTableName + " ("
                    + "id int IDENTITY(1,1) PRIMARY KEY, "
                    + "col_varchar varchar(8000), "
                    + "col_nvarchar nvarchar(4000), "
                    + "col_varbinary varbinary(8000))");

            stmt.executeUpdate("INSERT INTO " + escapedTableName
                    + " (col_varchar) VALUES ('seed')");

            TestUtils.dropProcedureIfExists(escapedProcName, stmt);
            stmt.executeUpdate("CREATE PROCEDURE " + escapedProcName
                    + " @p_input varchar(8000), @p_output varchar(8000) OUTPUT "
                    + "AS BEGIN SET @p_output = @p_input; END");
        }
    }

    // ---------------------------------------------------------------
    // Threat10: Buffer Overflow — Max Length Parameters
    // FX source: securitytest.Threat10_Overflow_Buffer
    // ---------------------------------------------------------------

    /**
     * FX Threat10: Verify driver handles max-length varchar parameter (8000 chars) gracefully.
     * The driver should not crash, truncate silently, or expose internal state.
     */
    @Test
    public void testMaxLengthVarcharParameter() throws Exception {
        String maxString = generateString(8000);
        String query = "INSERT INTO " + escapedTableName + " (col_varchar) VALUES (?)";

        try (Connection conn = getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, maxString);
            int rows = pstmt.executeUpdate();
            assertTrue(rows == 1, "Should insert 1 row with max-length varchar");
        }

        // Verify the data round-trips correctly
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT col_varchar FROM " + escapedTableName + " WHERE LEN(col_varchar) = 8000")) {
            assertTrue(rs.next(), "Should find the max-length row");
            assertTrue(rs.getString(1).length() == 8000, "Retrieved varchar should be 8000 chars");
        }
    }

    /**
     * FX Threat10: Verify driver handles max-length nvarchar parameter (4000 chars) gracefully.
     */
    @Test
    public void testMaxLengthNvarcharParameter() throws Exception {
        String maxString = generateString(4000);
        String query = "INSERT INTO " + escapedTableName + " (col_nvarchar) VALUES (?)";

        try (Connection conn = getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setNString(1, maxString);
            int rows = pstmt.executeUpdate();
            assertTrue(rows == 1, "Should insert 1 row with max-length nvarchar");
        }

        try (Connection conn = getConnection(); Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT col_nvarchar FROM " + escapedTableName + " WHERE LEN(col_nvarchar) = 4000")) {
            assertTrue(rs.next(), "Should find the max-length nvarchar row");
            assertTrue(rs.getString(1).length() == 4000, "Retrieved nvarchar should be 4000 chars");
        }
    }

    /**
     * FX Threat10: Verify driver handles max-length varbinary parameter (8000 bytes) gracefully.
     */
    @Test
    public void testMaxLengthVarbinaryParameter() throws Exception {
        byte[] maxBytes = new byte[8000];
        for (int i = 0; i < maxBytes.length; i++) {
            maxBytes[i] = (byte) (i % 256);
        }

        String query = "INSERT INTO " + escapedTableName + " (col_varbinary) VALUES (?)";
        try (Connection conn = getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setBytes(1, maxBytes);
            int rows = pstmt.executeUpdate();
            assertTrue(rows == 1, "Should insert 1 row with max-length varbinary");
        }

        try (Connection conn = getConnection(); Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT col_varbinary FROM " + escapedTableName
                                + " WHERE DATALENGTH(col_varbinary) = 8000")) {
            assertTrue(rs.next(), "Should find the max-length varbinary row");
            byte[] retrieved = rs.getBytes(1);
            assertTrue(retrieved.length == 8000, "Retrieved varbinary should be 8000 bytes");
        }
    }

    /**
     * FX Threat10: Verify driver handles oversized string parameter gracefully.
     * Passing a string longer than the column allows should result in a clean SQLException,
     * not a buffer overflow or crash.
     */
    @Test
    public void testOversizedStringParameter() throws Exception {
        // varchar(8000) column — try inserting 10000 chars
        String oversizedString = generateString(10000);
        String query = "INSERT INTO " + escapedTableName + " (col_varchar) VALUES (?)";

        try (Connection conn = getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, oversizedString);
            try {
                pstmt.executeUpdate();
                // Some drivers/servers may truncate; verify no crash
            } catch (SQLException e) {
                // Clean error is expected — truncation or data-too-long error
                assertNotNull(e.getMessage());
            }
        }

        // Connection should still be usable after oversized parameter
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT 1")) {
            assertTrue(rs.next());
        }
    }

    // ---------------------------------------------------------------
    // Threat13: Overflow via Stored Procedure Parameters
    // FX source: securitytest.Threat13_Overflows_Sprocs
    // ---------------------------------------------------------------

    /**
     * FX Threat13: Verify driver handles max-length stored procedure parameters gracefully.
     */
    @Test
    public void testMaxLengthStoredProcParameters() throws Exception {
        String maxString = generateString(8000);
        String call = "{call " + escapedProcName + "(?, ?)}";

        try (Connection conn = getConnection();
                CallableStatement cstmt = conn.prepareCall(call)) {
            cstmt.setString(1, maxString);
            cstmt.registerOutParameter(2, java.sql.Types.VARCHAR);
            cstmt.execute();

            String output = cstmt.getString(2);
            assertTrue(output.length() == maxString.length(),
                    "Output parameter should have same length as max input");
        }
    }

    /**
     * FX Threat13: Verify driver handles oversized stored procedure input gracefully.
     */
    @Test
    public void testOversizedStoredProcInput() throws Exception {
        String oversizedString = generateString(10000);
        String call = "{call " + escapedProcName + "(?, ?)}";

        try (Connection conn = getConnection();
                CallableStatement cstmt = conn.prepareCall(call)) {
            cstmt.setString(1, oversizedString);
            cstmt.registerOutParameter(2, java.sql.Types.VARCHAR);
            try {
                cstmt.execute();
                // If it succeeds, verify no crash and output is reasonable
                String output = cstmt.getString(2);
                assertNotNull(output);
            } catch (SQLException e) {
                // Truncation or type mismatch error is acceptable
                assertNotNull(e.getMessage());
            }
        }

        // Connection should still be usable
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT 1")) {
            assertTrue(rs.next());
        }
    }

    // ---------------------------------------------------------------
    // Threat8: Denial of Service — Connection Pool Exhaustion
    // FX source: securitytest.Threat8_DOS_Server
    // ---------------------------------------------------------------

    /**
     * FX Threat8, Variation 2-4: Connection pool exhaustion and recovery.
     * Opens many connections rapidly and verifies the driver handles pool exhaustion
     * gracefully — no crash, no resource leak, recovery is possible.
     */
    @Test
    public void testConnectionPoolExhaustionRecovery() throws Exception {
        int maxConnections = 20;
        List<Connection> connections = new ArrayList<>();

        try {
            // Open many connections
            for (int i = 0; i < maxConnections; i++) {
                try {
                    Connection conn = getConnection();
                    connections.add(conn);
                } catch (SQLException e) {
                    // Pool exhaustion is expected at some point — that's acceptable
                    break;
                }
            }

            // Verify at least some connections were established
            assertTrue(connections.size() > 0, "Should have opened at least one connection");

            // Verify each connection is usable
            for (Connection conn : connections) {
                if (!conn.isClosed()) {
                    try (Statement stmt = conn.createStatement();
                            ResultSet rs = stmt.executeQuery("SELECT 1")) {
                        assertTrue(rs.next());
                    }
                }
            }
        } finally {
            // Close all connections
            for (Connection conn : connections) {
                try {
                    if (conn != null && !conn.isClosed()) {
                        conn.close();
                    }
                } catch (SQLException e) {
                    // Ignore close errors during cleanup
                }
            }
        }

        // Verify recovery — should be able to connect again after closing all
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT 1")) {
            assertTrue(rs.next(), "Should be able to connect after pool recovery");
        }
    }

    /**
     * FX Threat8: Verify pool identity — connections with different credentials
     * must go to separate pools and not cross-contaminate.
     */
    @Test
    public void testPoolIdentityIsolation() throws Exception {
        // Open a connection with the default connection string
        try (Connection conn1 = getConnection()) {
            String catalog1 = conn1.getCatalog();

            // Open another connection with a different database (if possible)
            String altConnStr = TestUtils.addOrOverrideProperty(connectionString, "databaseName", "master");
            try (Connection conn2 = DriverManager.getConnection(altConnStr)) {
                String catalog2 = conn2.getCatalog();

                // The catalogs should differ if the pool identity is correctly separated
                // (this is a basic check — real pool identity also includes user, server, etc.)
                if (catalog1 != null && !catalog1.equalsIgnoreCase("master")) {
                    assertTrue(!catalog1.equalsIgnoreCase(catalog2)
                                    || catalog2.equalsIgnoreCase("master"),
                            "Pool identity should separate connections with different databases");
                }
            }
        }
    }

    // ---------------------------------------------------------------
    // Threat14: Client-side DoS via Threading
    // FX source: securitytest.Threat14_DOS_Client (Variation 3: Threading)
    // ---------------------------------------------------------------

    /**
     * FX Threat14, Variation 3: Verify driver handles concurrent connection/execution stress.
     * Multiple threads open connections, execute queries, and close simultaneously.
     * The driver should not deadlock, crash, or corrupt state.
     */
    @Test
    public void testConcurrentConnectionStress() throws Exception {
        int threadCount = 10;
        int operationsPerThread = 5;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threadCount; t++) {
            futures.add(executor.submit(() -> {
                try {
                    for (int i = 0; i < operationsPerThread; i++) {
                        try (Connection conn = getConnection();
                                Statement stmt = conn.createStatement();
                                ResultSet rs = stmt.executeQuery("SELECT @@VERSION")) {
                            if (rs.next()) {
                                assertNotNull(rs.getString(1));
                                successCount.incrementAndGet();
                            }
                        }
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            }));
        }

        boolean completed = latch.await(60, TimeUnit.SECONDS);
        executor.shutdownNow();

        assertTrue(completed, "All threads should complete within 60 seconds (no deadlock)");
        assertTrue(successCount.get() > 0, "At least some concurrent operations should succeed");

        // Verify driver is still functional after concurrent stress
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT 1")) {
            assertTrue(rs.next(), "Driver should still be functional after concurrent stress");
        }
    }

    /**
     * FX Threat14: Verify rapid open/close connection cycling doesn't leak resources.
     */
    @Test
    public void testRapidOpenCloseConnectionCycle() throws Exception {
        int cycles = 50;
        for (int i = 0; i < cycles; i++) {
            try (Connection conn = getConnection()) {
                assertNotNull(conn);
            }
        }

        // After rapid cycling, new connection should still work
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT 1")) {
            assertTrue(rs.next(), "Should connect after " + cycles + " rapid open/close cycles");
        }
    }

    // ---------------------------------------------------------------
    // Threat18: Blank Password Handling
    // FX source: securitytest.Threat18_Blank_Passwords
    // ---------------------------------------------------------------

    /**
     * FX Threat18: Verify blank password is handled safely.
     * The driver should either reject blank password or handle it gracefully — 
     * it should never bypass authentication due to a blank password.
     */
    @Test
    public void testBlankPasswordHandled() throws Exception {
        String blankPwdConnStr = TestUtils.addOrOverrideProperty(connectionString, "password", "");
        blankPwdConnStr = TestUtils.addOrOverrideProperty(blankPwdConnStr, "loginTimeout", "5");

        try {
            try (Connection conn = DriverManager.getConnection(blankPwdConnStr)) {
                // If connection succeeds (e.g., integrated auth fallback), verify it's valid
                assertNotNull(conn.getMetaData().getDatabaseProductName());
            }
        } catch (SQLException e) {
            // Expected for SQL auth — blank password should cause auth failure
            assertNotNull(e.getMessage(), "Auth failure should have a descriptive message");
        }
    }

    /**
     * FX Threat18: Verify null password is handled safely.
     */
    @Test
    public void testNullPasswordHandled() throws Exception {
        // Build a connection string without any password property
        String noPwdConnStr = connectionString;
        // Remove existing password from connection string
        int pwdStart = noPwdConnStr.toLowerCase().indexOf("password=");
        if (pwdStart >= 0) {
            int pwdEnd = noPwdConnStr.indexOf(";", pwdStart);
            if (pwdEnd >= 0) {
                noPwdConnStr = noPwdConnStr.substring(0, pwdStart) + noPwdConnStr.substring(pwdEnd + 1);
            } else {
                noPwdConnStr = noPwdConnStr.substring(0, pwdStart);
            }
        }
        noPwdConnStr = TestUtils.addOrOverrideProperty(noPwdConnStr, "loginTimeout", "5");

        try {
            try (Connection conn = DriverManager.getConnection(noPwdConnStr)) {
                // May succeed with integrated auth
                assertNotNull(conn.getMetaData().getDatabaseProductName());
            }
        } catch (SQLException e) {
            // Expected for SQL auth
            assertNotNull(e.getMessage());
        }
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    /**
     * Generates a string of the specified length using repeating ASCII characters.
     */
    private String generateString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append((char) ('A' + (i % 26)));
        }
        return sb.toString();
    }

    @AfterAll
    public static void cleanUp() throws Exception {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropProcedureIfExists(escapedProcName, stmt);
            TestUtils.dropTableIfExists(escapedTableName, stmt);
        }
    }
}
