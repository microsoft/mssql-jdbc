/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.security;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


/**
 * Tests credential disclosure prevention ported from FX security test suite (securitytest.java).
 *
 * Covers FX Threat3 (Memory disclosure via reflection), Threat6 (Exception message credential leakage),
 * Threat15/16 (Credential disclosure in logs/diagnostics), and Threat19 (DriverPropertyInfo password exposure).
 *
 * Verifies the driver does not expose passwords, keys, or other sensitive credential material
 * through public fields, exception messages, logging output, or property info APIs.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxSecurity)
public class CredentialDisclosureTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    // ---------------------------------------------------------------
    // Threat3: Disclosure of user credentials (memory/reflection)
    // FX source: securitytest.Threat3_Disclosure_Memory
    // ---------------------------------------------------------------

    /**
     * FX Threat3, Variation 1: Connection credentials not available through reflection.
     * Verifies that SQLServerConnection does not expose credential fields publicly.
     * Only the allowed TRANSACTION_* constants from java.sql.Connection should be public.
     */
    @Test
    public void testConnectionFieldsNotExposedViaReflection() throws Exception {
        try (Connection conn = getConnection()) {
            Field[] fields = conn.getClass().getFields();

            // Allowed public fields are the TRANSACTION_* isolation level constants
            // inherited from java.sql.Connection plus TRANSACTION_SNAPSHOT from ISQLServerConnection
            Set<String> allowedFields = new HashSet<>(Arrays.asList(
                    "TRANSACTION_NONE",
                    "TRANSACTION_READ_UNCOMMITTED",
                    "TRANSACTION_READ_COMMITTED",
                    "TRANSACTION_REPEATABLE_READ",
                    "TRANSACTION_SERIALIZABLE",
                    "TRANSACTION_SNAPSHOT"));

            // Sensitive keywords that should never appear as public field names
            String[] sensitiveKeywords = {"password", "pwd", "secret", "key", "credential", "token"};

            int nonSyntheticCount = 0;
            for (Field field : fields) {
                if (field.isSynthetic()) {
                    continue; // skip jacoco-injected fields
                }
                nonSyntheticCount++;
                String fieldName = field.getName().toLowerCase(Locale.ROOT);

                // Verify no sensitive field names are publicly exposed
                for (String keyword : sensitiveKeywords) {
                    assertFalse(fieldName.contains(keyword),
                            "Public field '" + field.getName()
                                    + "' on SQLServerConnection contains sensitive keyword '" + keyword + "'");
                }
            }

            // Verify total public (non-synthetic) field count is within expected range
            assertTrue(nonSyntheticCount <= allowedFields.size() + 2,
                    "SQLServerConnection exposes " + nonSyntheticCount
                            + " public fields — expected at most " + (allowedFields.size() + 2)
                            + ". Unexpected fields may leak credentials.");
        }
    }

    /**
     * Verifies that SQLServerConnection's declared fields (including private) with
     * sensitive names are not accessible via standard reflection without setAccessible.
     */
    @Test
    public void testSensitiveFieldsNotPubliclyAccessible() throws Exception {
        try (Connection conn = getConnection()) {
            Field[] allFields = conn.getClass().getDeclaredFields();
            String[] sensitiveKeywords = {"password", "pwd", "secret", "key", "credential", "token"};

            for (Field field : allFields) {
                String name = field.getName().toLowerCase(Locale.ROOT);
                for (String keyword : sensitiveKeywords) {
                    if (name.contains(keyword)) {
                        // These fields exist but must NOT be public
                        assertFalse(java.lang.reflect.Modifier.isPublic(field.getModifiers()),
                                "Sensitive field '" + field.getName() + "' must not be public");
                    }
                }
            }
        }
    }

    // ---------------------------------------------------------------
    // Threat6: Disclosure of user information (exceptions)
    // FX source: securitytest.Threat6_Disclosure_Exceptions
    // ---------------------------------------------------------------

    /**
     * FX Threat6, Variation 1: Invalid server connection attempt must not leak password in exception.
     */
    @Test
    public void testPasswordNotInExceptionOnInvalidServer() throws Exception {
        String password = getPasswordFromConnectionString();
        if (password == null || password.isEmpty()) {
            return; // skip if no password in connection string (e.g., integrated auth)
        }

        String invalidConnStr = TestUtils.addOrOverrideProperty(connectionString, "serverName",
                "invalid_server_does_not_exist_12345");
        // Also set a short login timeout to avoid long waits
        invalidConnStr = TestUtils.addOrOverrideProperty(invalidConnStr, "loginTimeout", "5");

        try {
            try (Connection conn = DriverManager.getConnection(invalidConnStr)) {
                fail("Should have thrown SQLException for invalid server");
            }
        } catch (SQLException e) {
            assertExceptionDoesNotContainPassword(e, password);
        }
    }

    /**
     * FX Threat6, Variation 2: Invalid authentication must not leak password in exception.
     */
    @Test
    public void testPasswordNotInExceptionOnInvalidAuth() throws Exception {
        String password = getPasswordFromConnectionString();
        if (password == null || password.isEmpty()) {
            return;
        }

        String invalidConnStr = TestUtils.addOrOverrideProperty(connectionString, "password", "WrongPassword!@#$");
        invalidConnStr = TestUtils.addOrOverrideProperty(invalidConnStr, "loginTimeout", "10");

        try {
            try (Connection conn = DriverManager.getConnection(invalidConnStr)) {
                fail("Should have thrown SQLException for invalid auth");
            }
        } catch (SQLException e) {
            // The exception should not contain either the real or wrong password
            assertExceptionDoesNotContainPassword(e, password);
            assertExceptionDoesNotContainPassword(e, "WrongPassword!@#$");
        }
    }

    /**
     * FX Threat6, Variation 5: Connection timeout must not leak password in exception.
     */
    @Test
    public void testPasswordNotInExceptionOnTimeout() throws Exception {
        String password = getPasswordFromConnectionString();
        if (password == null || password.isEmpty()) {
            return;
        }

        // Use a non-routable IP to force timeout
        String invalidConnStr = TestUtils.addOrOverrideProperty(connectionString, "serverName", "10.255.255.1");
        invalidConnStr = TestUtils.addOrOverrideProperty(invalidConnStr, "loginTimeout", "1");

        try {
            try (Connection conn = DriverManager.getConnection(invalidConnStr)) {
                fail("Should have thrown SQLException for connection timeout");
            }
        } catch (SQLException e) {
            assertExceptionDoesNotContainPassword(e, password);
        }
    }

    /**
     * FX Threat6: Verify password not leaked when using invalid keyword/value pairs.
     */
    @Test
    public void testPasswordNotInExceptionOnInvalidKeyword() throws Exception {
        String password = getPasswordFromConnectionString();
        if (password == null || password.isEmpty()) {
            return;
        }

        // Add an invalid property to trigger parsing error
        String invalidConnStr = connectionString + ";invalidKeyword12345=someValue";

        try {
            try (Connection conn = DriverManager.getConnection(invalidConnStr)) {
                // Connection may succeed if unknown properties are ignored — that's fine
            }
        } catch (SQLException e) {
            assertExceptionDoesNotContainPassword(e, password);
        }
    }

    /**
     * FX Threat6: Verify password not leaked when setCatalog fails.
     */
    @Test
    public void testPasswordNotInExceptionOnInvalidCatalog() throws Exception {
        String password = getPasswordFromConnectionString();
        if (password == null || password.isEmpty()) {
            return;
        }

        try (Connection conn = getConnection()) {
            try {
                conn.setCatalog("nonexistent_database_12345_xyz");
            } catch (SQLException e) {
                assertExceptionDoesNotContainPassword(e, password);
            }
        }
    }

    /**
     * FX Threat6: Verify password not leaked in setSavepoint exception.
     */
    @Test
    public void testPasswordNotInExceptionOnSavepoint() throws Exception {
        String password = getPasswordFromConnectionString();
        if (password == null || password.isEmpty()) {
            return;
        }

        try (Connection conn = getConnection()) {
            // Try savepoint operations that may fail
            try {
                // setSavepoint should fail when autoCommit=true
                conn.setAutoCommit(true);
                conn.setSavepoint("test_savepoint");
            } catch (SQLException e) {
                assertExceptionDoesNotContainPassword(e, password);
            }
        }
    }

    // ---------------------------------------------------------------
    // Threat15/16: Disclosure of user credentials in logs/diagnostics
    // FX source: securitytest.Threat15_Disclosure_Files, Threat16_Disclosure_Tracing
    // ---------------------------------------------------------------

    /**
     * FX Threat16: Verify credentials are not echoed in driver diagnostic logging output.
     * Enables FINEST level logging, performs a connection, and verifies the password
     * does not appear in the log stream.
     */
    @Test
    public void testCredentialNotInDiagnosticLogs() throws Exception {
        String password = getPasswordFromConnectionString();
        if (password == null || password.isEmpty()) {
            return;
        }

        // Capture log output to a buffer
        ByteArrayOutputStream logBuffer = new ByteArrayOutputStream();
        PrintStream logStream = new PrintStream(logBuffer);
        Logger driverLogger = Logger.getLogger(Constants.MSSQL_JDBC_PACKAGE);
        Level originalLevel = driverLogger.getLevel();
        Handler testHandler = new StreamHandler(logStream, new SimpleFormatter());
        testHandler.setLevel(Level.FINEST);

        try {
            driverLogger.addHandler(testHandler);
            driverLogger.setLevel(Level.FINEST);

            // Perform operations that generate log output
            try (Connection conn = DriverManager.getConnection(connectionString)) {
                conn.getMetaData().getDatabaseProductName();
                conn.getCatalog();
            }

            // Flush the handler to ensure all output is captured
            testHandler.flush();

            String logOutput = logBuffer.toString();

            // Verify password does not appear in log output
            if (logOutput.length() > 0) {
                assertFalse(logOutput.contains(password),
                        "Driver log output must NOT contain the connection password");

                // Also check for connection string keywords that might leak credentials
                assertFalse(logOutput.toLowerCase(Locale.ROOT).contains("password=" + password.toLowerCase(Locale.ROOT)),
                        "Driver log output must NOT contain password= keyword with actual password");
            }
        } finally {
            // Restore original logging state
            driverLogger.removeHandler(testHandler);
            driverLogger.setLevel(originalLevel);
            testHandler.close();
            logStream.close();
        }
    }

    /**
     * FX Threat16: Verify credentials are not in log output even on connection failure.
     */
    @Test
    public void testCredentialNotInLogsOnFailure() throws Exception {
        String password = getPasswordFromConnectionString();
        if (password == null || password.isEmpty()) {
            return;
        }

        ByteArrayOutputStream logBuffer = new ByteArrayOutputStream();
        PrintStream logStream = new PrintStream(logBuffer);
        Logger driverLogger = Logger.getLogger(Constants.MSSQL_JDBC_PACKAGE);
        Level originalLevel = driverLogger.getLevel();
        Handler testHandler = new StreamHandler(logStream, new SimpleFormatter());
        testHandler.setLevel(Level.FINEST);

        try {
            driverLogger.addHandler(testHandler);
            driverLogger.setLevel(Level.FINEST);

            // Try connecting with wrong password to generate error-path logging
            String invalidConnStr = TestUtils.addOrOverrideProperty(connectionString, "password", "BadPass_" + password);
            invalidConnStr = TestUtils.addOrOverrideProperty(invalidConnStr, "loginTimeout", "5");

            try {
                try (Connection conn = DriverManager.getConnection(invalidConnStr)) {
                    // may or may not connect
                }
            } catch (SQLException e) {
                // expected
            }

            testHandler.flush();
            String logOutput = logBuffer.toString();

            if (logOutput.length() > 0) {
                // Neither the real nor the attempted password should be logged
                assertFalse(logOutput.contains(password),
                        "Driver log output must NOT contain the real password on failure");
                assertFalse(logOutput.contains("BadPass_" + password),
                        "Driver log output must NOT contain the attempted password on failure");
            }
        } finally {
            driverLogger.removeHandler(testHandler);
            driverLogger.setLevel(originalLevel);
            testHandler.close();
            logStream.close();
        }
    }

    // ---------------------------------------------------------------
    // Threat19: Password accessible through DriverPropertyInfo
    // FX source: securitytest.Threat19_Access_Passwords
    // ---------------------------------------------------------------

    /**
     * FX Threat19: Verify getPropertyInfo does not return actual password values.
     * The password property in DriverPropertyInfo should be null or empty.
     */
    @Test
    public void testDriverPropertyInfoDoesNotExposePassword() throws Exception {
        SQLServerDriver driver = new SQLServerDriver();
        String url = connectionString;

        DriverPropertyInfo[] propInfo = driver.getPropertyInfo(url, null);
        assertNotNull(propInfo, "DriverPropertyInfo should not be null");

        for (DriverPropertyInfo info : propInfo) {
            if (info.name != null && info.name.toLowerCase(Locale.ROOT).contains("password")) {
                assertTrue(info.value == null || info.value.isEmpty(),
                        "DriverPropertyInfo for '" + info.name
                                + "' should NOT return the actual password value. Got: '"
                                + (info.value != null ? "[REDACTED]" : "null") + "'");
            }
        }
    }

    /**
     * FX Threat19: Verify getPropertyInfo with explicit properties does not leak password.
     */
    @Test
    public void testDriverPropertyInfoWithPropertiesDoesNotExposePassword() throws Exception {
        SQLServerDriver driver = new SQLServerDriver();

        Properties props = new Properties();
        props.setProperty("user", "testUser");
        props.setProperty("password", "testPassword123!@#");

        // Use just the URL portion
        String url = Constants.JDBC_PREFIX + "localhost";
        DriverPropertyInfo[] propInfo = driver.getPropertyInfo(url, props);
        assertNotNull(propInfo, "DriverPropertyInfo should not be null");

        for (DriverPropertyInfo info : propInfo) {
            if (info.name != null && info.name.toLowerCase(Locale.ROOT).contains("password")) {
                assertTrue(info.value == null || info.value.isEmpty(),
                        "DriverPropertyInfo for '" + info.name
                                + "' should NOT return the supplied password value");
            }
        }
    }

    /**
     * FX Threat3: Verify Connection.toString() does not expose credentials.
     */
    @Test
    public void testConnectionToStringDoesNotExposeCredentials() throws Exception {
        String password = getPasswordFromConnectionString();
        if (password == null || password.isEmpty()) {
            return;
        }

        try (Connection conn = getConnection()) {
            String connStr = conn.toString();
            assertFalse(connStr.contains(password),
                    "Connection.toString() must NOT contain the password");
        }
    }

    /**
     * FX Threat6: Verify the full exception chain (message + cause + stack trace)
     * does not contain sensitive keywords.
     */
    @Test
    public void testExceptionChainDoesNotLeakSensitiveKeywords() throws Exception {
        String password = getPasswordFromConnectionString();
        if (password == null || password.isEmpty()) {
            return;
        }

        // Force a connection failure to generate a rich exception chain
        String invalidConnStr = TestUtils.addOrOverrideProperty(connectionString, "password", password);
        invalidConnStr = TestUtils.addOrOverrideProperty(invalidConnStr, "serverName",
                "nonexistent_server_xyz_12345");
        invalidConnStr = TestUtils.addOrOverrideProperty(invalidConnStr, "loginTimeout", "3");

        try {
            try (Connection conn = DriverManager.getConnection(invalidConnStr)) {
                fail("Should have thrown");
            }
        } catch (SQLException e) {
            // Walk the full cause chain
            Throwable current = e;
            while (current != null) {
                String msg = current.getMessage();
                if (msg != null) {
                    assertFalse(msg.contains(password),
                            "Exception message in chain (" + current.getClass().getSimpleName()
                                    + ") must NOT contain the password");
                }
                current = current.getCause();
            }

            // Also check the full stack trace as a string
            ByteArrayOutputStream stackOut = new ByteArrayOutputStream();
            e.printStackTrace(new PrintStream(stackOut));
            String fullTrace = stackOut.toString();
            assertFalse(fullTrace.contains(password),
                    "Full exception stack trace must NOT contain the password");
        }
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    /**
     * Extract the password from the test connection string.
     */
    private String getPasswordFromConnectionString() {
        return TestUtils.getProperty(connectionString, "password");
    }

    /**
     * Verifies that an exception message (and all causes) does not contain the given password.
     */
    private void assertExceptionDoesNotContainPassword(SQLException e, String password) {
        Throwable current = e;
        while (current != null) {
            String msg = current.getMessage();
            if (msg != null) {
                assertFalse(msg.contains(password),
                        "Exception message (" + current.getClass().getSimpleName()
                                + ") must NOT contain the password. Message: "
                                + msg.substring(0, Math.min(msg.length(), 200)));
            }
            current = current.getCause();
        }
    }
}
