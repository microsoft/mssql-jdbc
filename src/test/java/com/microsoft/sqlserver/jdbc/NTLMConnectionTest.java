package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;


/**
 * A class for testing basic NTLMv2 functionality.
 */
@RunWith(JUnitPlatform.class)
public class NTLMConnectionTest extends AbstractTest {
    private static SQLServerDataSource dsNTLMLocal = null;

    private static boolean ntlmPropsDefined = false;

    private static String connectionStringNTLM = connectionString;

    @BeforeAll
    public static void setUp() {
        java.util.logging.Logger logger = java.util.logging.Logger.getLogger("");
        logger.removeHandler(logger.getHandlers()[0]);

        // if these properties are defined then NTLM is desired, modify connection string accordingly
        String domain = System.getProperty("domain");
        String user = System.getProperty("userNTLM");
        String password = System.getProperty("passwordNTLM");

        if (null != domain) {
            connectionStringNTLM = addOrOverrideProperty(connectionStringNTLM, "domain", domain);
            ntlmPropsDefined = true;
        }

        if (null != user) {
            connectionStringNTLM = addOrOverrideProperty(connectionStringNTLM, "user", user);
            ntlmPropsDefined = true;
        }

        if (null != password) {
            connectionStringNTLM = addOrOverrideProperty(connectionStringNTLM, "password", password);
            ntlmPropsDefined = true;
        }

        if (ntlmPropsDefined) {
            connectionStringNTLM = addOrOverrideProperty(connectionStringNTLM, "authenticationScheme", "NTLM");
            connectionStringNTLM = addOrOverrideProperty(connectionStringNTLM, "integratedSecurity", "true");

            dsNTLMLocal = new SQLServerDataSource();
            updateDataSource(connectionStringNTLM, dsNTLMLocal);
        }
    }

    /**
     * Tests basic NTLM authentication.
     * 
     * @throws SQLException
     */
    @Test
    public void testNTLMBasicConnection() throws SQLException {
        if (ntlmPropsDefined) {
            try (Connection conn1 = dsNTLMLocal.getConnection();
                    Connection conn2 = DriverManager.getConnection(connectionStringNTLM)) {
                verifyNTLM(conn1);
                verifyNTLM(conn2);
            }
        }
    }

    /**
     * Tests invalid connection property combinations.
     */
    @Test
    public void testNTLMInvalidProperties() {
        if (ntlmPropsDefined) {
            // NTLM without user name
            testInvalidProperties(addOrOverrideProperty(connectionStringNTLM, "user", ""),
                    "R_NtlmNoUserPasswordDomain");

            // NTLM without password
            testInvalidProperties(addOrOverrideProperty(connectionStringNTLM, "password", ""),
                    "R_NtlmNoUserPasswordDomain");

            // NTLM with integratedSecurity property
            testInvalidProperties(
                    addOrOverrideProperty(connectionStringNTLM, "authentication", "ActiveDirectoryIntegrated "),
                    "R_SetAuthenticationWhenIntegratedSecurityTrue");
        }
    }

    /**
     * Tests NTLM authentication when the connection is encrypted.
     * 
     * @throws SQLException
     */
    @Test
    public void testNTLMEncryptedConnection() throws SQLException {
        if (ntlmPropsDefined) {
            try (Connection conn = DriverManager
                    .getConnection(connectionStringNTLM + ";encrypt=true;trustServerCertificate=true")) {
                verifyNTLM(conn);
            }
        }
    }

    /**
     * Tests NTLM authentication against a non-default database.
     * 
     * @throws SQLException
     */
    @Test
    public void testNTLMNonDeafultDatabase() throws SQLException {
        if (ntlmPropsDefined) {
            String databaseName = "tempdb";
            try (Connection conn = DriverManager
                    .getConnection(addOrOverrideProperty(connectionStringNTLM, "database", databaseName))) {
                verifyNTLM(conn);
                verifyDatabase(conn, databaseName);
            }
        }
    }

    /**
     * Tests NTLM authentication when connection pooling is enabled.
     * 
     * @throws SQLException
     */
    @Test
    public void testNTLMHikariCP() throws SQLException {
        if (ntlmPropsDefined) {
            HikariConfig config1 = new HikariConfig();
            HikariConfig config2 = new HikariConfig();
            config1.setDataSource(dsNTLMLocal);
            config2.setJdbcUrl(connectionStringNTLM);
            try (HikariDataSource dsCP1 = new HikariDataSource(config1);
                    HikariDataSource dsCP2 = new HikariDataSource(config2); Connection conn1 = dsCP1.getConnection();
                    Connection conn2 = dsCP2.getConnection()) {
                verifyNTLM(conn1);
                verifyNTLM(conn2);
            }
        }
    }

    /**
     * Validates that loginTimeout connection property works with NTLM authentication.
     */
    // @Test
    public void testNTLMLoginTimeout() {
        if (ntlmPropsDefined) {
            long timerStart = 0;
            long timerEnd = 0;
            int loginTimeout = 10;

            timerStart = System.currentTimeMillis();
            try (Connection conn = DriverManager.getConnection(
                    connectionStringNTLM + ";servername=bogus;instanceName=;loginTimeout=" + loginTimeout)) {
                fail();
            } catch (SQLException e) {
                timerEnd = System.currentTimeMillis();
            }
            long elapsedTime = (timerEnd - timerStart) / 1000;
            assertTrue(elapsedTime < loginTimeout + 1);
        }
    }

    @Test
    public void testNTLMBadInit() throws SQLException {

        try {
            SSPIAuthentication auth = new NTLMAuthentication(new SQLServerConnection(""), "serverName", "domainName",
                    "hostname");
        } catch (Exception e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_ntlmInitError")));

        }
    }

    @Test
    public void testNTLMBadBlob() throws SQLException {

        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionStringNTLM)) {
            SSPIAuthentication auth = new NTLMAuthentication(conn, "serverName", "domainName", "hostname");
            boolean[] done = {false};

            byte[] secBlobOut;
            try {
                // start with a good blob with a MIC flag
                byte[] sspiBlob = {78, 84, 76, 77, 83, 83, 80, 0, 2, 0, 0, 0, 12, 0, 12, 0, 56, 0, 0, 0, 21, -126, -127,
                        2, 78, -76, 68, 118, 41, -30, -71, 100, 0, 0, 0, 0, 0, 0, 0, 0, -108, 0, -108, 0, 68, 0, 0, 0,
                        6, 3, -128, 37, 0, 0, 0, 15, 71, 0, 65, 0, 76, 0, 65, 0, 88, 0, 89, 0, 2, 0, 12, 0, 71, 0, 65,
                        0, 76, 0, 65, 0, 88, 0, 89, 0, 1, 0, 22, 0, 83, 0, 81, 0, 76, 0, 45, 0, 50, 0, 75, 0, 49, 0, 54,
                        0, 45, 0, 48, 0, 49, 0, 4, 0, 20, 0, 68, 0, 79, 0, 77, 0, 65, 0, 73, 0, 78, 0, 78, 0, 65, 0, 77,
                        0, 69, 0, 3, 0, 20, 0, 115, 0, 101, 0, 114, 0, 118, 0, 101, 0, 114, 0, 78, 0, 97, 0, 109, 0,
                        101, 0, 5, 0, 18, 0, 103, 0, 97, 0, 108, 0, 97, 0, 120, 0, 121, 0, 46, 0, 97, 0, 100, 0, 7, 0,
                        8, 0, 122, -115, 18, 50, -5, -32, -44, 1, 6, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
                secBlobOut = auth.generateClientContext(sspiBlob, done);

            } catch (Exception e) {
                fail(e.getMessage());
            }

            try {
                // blob with a bad signature
                byte[] sspiBlob = {88, 84, 76, 77, 83, 83, 80, 0, 2, 0, 0, 0, 12, 0, 12, 0, 56, 0, 0, 0, 21, -126, -127,
                        2, 78, -76, 68, 118, 41, -30, -71, 100, 0, 0, 0, 0, 0, 0, 0, 0, -108, 0, -108, 0, 68, 0, 0, 0,
                        6, 3, -128, 37, 0, 0, 0, 15, 71, 0, 65, 0, 76, 0, 65, 0, 88, 0, 89, 0, 2, 0, 12, 0, 71, 0, 65,
                        0, 76, 0, 65, 0, 88, 0, 89, 0, 1, 0, 22, 0, 83, 0, 81, 0, 76, 0, 45, 0, 50, 0, 75, 0, 49, 0, 54,
                        0, 45, 0, 48, 0, 49, 0, 4, 0, 20, 0, 68, 0, 79, 0, 77, 0, 65, 0, 73, 0, 78, 0, 78, 0, 65, 0, 77,
                        0, 69, 0, 3, 0, 20, 0, 115, 0, 101, 0, 114, 0, 118, 0, 101, 0, 114, 0, 78, 0, 97, 0, 109, 0,
                        101, 0, 5, 0, 18, 0, 103, 0, 97, 0, 108, 0, 97, 0, 120, 0, 121, 0, 46, 0, 97, 0, 100, 0, 7, 0,
                        8, 0, 122, -115, 18, 50, -5, -32, -44, 1, 6, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
                secBlobOut = auth.generateClientContext(sspiBlob, done);

            } catch (Exception e) {
                assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_ntlmSignatureError")));
            }

            try {
                // blob with a bad message type
                byte[] sspiBlob = {78, 84, 76, 77, 83, 83, 80, 0, 0, 0, 0, 0, 12, 0, 12, 0, 56, 0, 0, 0, 21, -126, -127,
                        2, 78, -76, 68, 118, 41, -30, -71, 100, 0, 0, 0, 0, 0, 0, 0, 0, -108, 0, -108, 0, 68, 0, 0, 0,
                        6, 3, -128, 37, 0, 0, 0, 15, 71, 0, 65, 0, 76, 0, 65, 0, 88, 0, 89, 0, 2, 0, 12, 0, 71, 0, 65,
                        0, 76, 0, 65, 0, 88, 0, 89, 0, 1, 0, 22, 0, 83, 0, 81, 0, 76, 0, 45, 0, 50, 0, 75, 0, 49, 0, 54,
                        0, 45, 0, 48, 0, 49, 0, 4, 0, 20, 0, 68, 0, 79, 0, 77, 0, 65, 0, 73, 0, 78, 0, 78, 0, 65, 0, 77,
                        0, 69, 0, 3, 0, 20, 0, 115, 0, 101, 0, 114, 0, 118, 0, 101, 0, 114, 0, 78, 0, 97, 0, 109, 0,
                        101, 0, 5, 0, 18, 0, 103, 0, 97, 0, 108, 0, 97, 0, 120, 0, 121, 0, 46, 0, 97, 0, 100, 0, 7, 0,
                        8, 0, 122, -115, 18, 50, -5, -32, -44, 1, 6, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
                secBlobOut = auth.generateClientContext(sspiBlob, done);

            } catch (Exception e) {
                assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_ntlmMessageTypeError")));
            }

            try {
                // blob with a bad target info len
                byte[] sspiBlob = {78, 84, 76, 77, 83, 83, 80, 0, 2, 0, 0, 0, 12, 0, 12, 0, 56, 0, 0, 0, 21, -126, -127,
                        2, 78, -76, 68, 118, 41, -30, -71, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -108, 0, 68, 0, 0, 0, 6,
                        3, -128, 37, 0, 0, 0, 15, 71, 0, 65, 0, 76, 0, 65, 0, 88, 0, 89, 0, 2, 0, 12, 0, 71, 0, 65, 0,
                        76, 0, 65, 0, 88, 0, 89, 0, 1, 0, 22, 0, 83, 0, 81, 0, 76, 0, 45, 0, 50, 0, 75, 0, 49, 0, 54, 0,
                        45, 0, 48, 0, 49, 0, 4, 0, 20, 0, 68, 0, 79, 0, 77, 0, 65, 0, 73, 0, 78, 0, 78, 0, 65, 0, 77, 0,
                        69, 0, 3, 0, 20, 0, 115, 0, 101, 0, 114, 0, 118, 0, 101, 0, 114, 0, 78, 0, 97, 0, 109, 0, 101,
                        0, 5, 0, 18, 0, 103, 0, 97, 0, 108, 0, 97, 0, 120, 0, 121, 0, 46, 0, 97, 0, 100, 0, 7, 0, 8, 0,
                        122, -115, 18, 50, -5, -32, -44, 1, 6, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
                secBlobOut = auth.generateClientContext(sspiBlob, done);

            } catch (Exception e) {
                assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_ntlmNoTargetInfo")));
            }

            try {
                // blob with bad domain
                byte[] sspiBlob = {78, 84, 76, 77, 83, 83, 80, 0, 2, 0, 0, 0, 12, 0, 12, 0, 56, 0, 0, 0, 21, -126, -127,
                        2, 78, -76, 68, 118, 41, -30, -71, 100, 0, 0, 0, 0, 0, 0, 0, 0, -108, 0, -108, 0, 68, 0, 0, 0,
                        6, 3, -128, 37, 0, 0, 0, 15, 71, 0, 65, 0, 76, 0, 65, 0, 88, 0, 89, 0, 2, 0, 12, 0, 71, 0, 65,
                        0, 76, 0, 65, 0, 88, 0, 89, 0, 1, 0, 22, 0, 83, 0, 81, 0, 76, 0, 45, 0, 50, 0, 75, 0, 49, 0, 54,
                        0, 45, 0, 48, 0, 49, 0, 4, 0, 20, 0, 0, 0, 79, 0, 77, 0, 65, 0, 73, 0, 78, 0, 78, 0, 65, 0, 77,
                        0, 69, 0, 3, 0, 20, 0, 115, 0, 101, 0, 114, 0, 118, 0, 101, 0, 114, 0, 78, 0, 97, 0, 109, 0,
                        101, 0, 5, 0, 18, 0, 103, 0, 97, 0, 108, 0, 97, 0, 120, 0, 121, 0, 46, 0, 97, 0, 100, 0, 7, 0,
                        8, 0, 122, -115, 18, 50, -5, -32, -44, 1, 6, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
                secBlobOut = auth.generateClientContext(sspiBlob, done);

            } catch (Exception e) {
                assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_ntlmBadDomain")));
            }

            try {
                // blob with bad computer name
                byte[] sspiBlob = {78, 84, 76, 77, 83, 83, 80, 0, 2, 0, 0, 0, 12, 0, 12, 0, 56, 0, 0, 0, 21, -126, -127,
                        2, 78, -76, 68, 118, 41, -30, -71, 100, 0, 0, 0, 0, 0, 0, 0, 0, -108, 0, -108, 0, 68, 0, 0, 0,
                        6, 3, -128, 37, 0, 0, 0, 15, 71, 0, 65, 0, 76, 0, 65, 0, 88, 0, 89, 0, 2, 0, 12, 0, 71, 0, 65,
                        0, 76, 0, 65, 0, 88, 0, 89, 0, 1, 0, 22, 0, 83, 0, 81, 0, 76, 0, 45, 0, 50, 0, 75, 0, 49, 0, 54,
                        0, 45, 0, 48, 0, 49, 0, 4, 0, 20, 0, 68, 0, 79, 0, 77, 0, 65, 0, 73, 0, 78, 0, 78, 0, 65, 0, 77,
                        0, 69, 0, 3, 0, 20, 0, 0, 0, 101, 0, 114, 0, 118, 0, 101, 0, 114, 0, 78, 0, 97, 0, 109, 0, 101,
                        0, 5, 0, 18, 0, 103, 0, 97, 0, 108, 0, 97, 0, 120, 0, 121, 0, 46, 0, 97, 0, 100, 0, 7, 0, 8, 0,
                        122, -115, 18, 50, -5, -32, -44, 1, 6, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
                secBlobOut = auth.generateClientContext(sspiBlob, done);

            } catch (Exception e) {
                assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_ntlmBadComputer")));
            }

            try {
                // blob with bad avid
                byte[] sspiBlob = {78, 84, 76, 77, 83, 83, 80, 0, 2, 0, 0, 0, 12, 0, 12, 0, 56, 0, 0, 0, 21, -126, -127,
                        2, 78, -76, 68, 118, 41, -30, -71, 100, 0, 0, 0, 0, 0, 0, 0, 0, -108, 0, -108, 0, 68, 0, 0, 0,
                        6, 3, -128, 37, 0, 0, 0, 15, 71, 0, 65, 0, 76, 0, 65, 0, 88, 0, 89, 0, -1, 0, 12, 0, 71, 0, 65,
                        0, 76, 0, 65, 0, 88, 0, 89, 0, 1, 0, 22, 0, 83, 0, 81, 0, 76, 0, 45, 0, 50, 0, 75, 0, 49, 0, 54,
                        0, 45, 0, 48, 0, 49, 0, 4, 0, 20, 0, 68, 0, 79, 0, 77, 0, 65, 0, 73, 0, 78, 0, 78, 0, 65, 0, 77,
                        0, 69, 0, 3, 0, 20, 0, 115, 0, 101, 0, 114, 0, 118, 0, 101, 0, 114, 0, 78, 0, 97, 0, 109, 0,
                        101, 0, 5, 0, 18, 0, 103, 0, 97, 0, 108, 0, 97, 0, 120, 0, 121, 0, 46, 0, 97, 0, 100, 0, 7, 0,
                        8, 0, 122, -115, 18, 50, -5, -32, -44, 1, 6, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
                secBlobOut = auth.generateClientContext(sspiBlob, done);

            } catch (Exception e) {
                assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_ntlmUnknownValue")));
            }

            try {
                // blob with no timestamp
                byte[] sspiBlob = {78, 84, 76, 77, 83, 83, 80, 0, 2, 0, 0, 0, 12, 0, 12, 0, 56, 0, 0, 0, 21, -126, -127,
                        2, 78, -76, 68, 118, 41, -30, -71, 100, 0, 0, 0, 0, 0, 0, 0, 0, -108, 0, -108, 0, 68, 0, 0, 0,
                        6, 3, -128, 37, 0, 0, 0, 15, 71, 0, 65, 0, 76, 0, 65, 0, 88, 0, 89, 0, 2, 0, 12, 0, 71, 0, 65,
                        0, 76, 0, 65, 0, 88, 0, 89, 0, 1, 0, 22, 0, 83, 0, 81, 0, 76, 0, 45, 0, 50, 0, 75, 0, 49, 0, 54,
                        0, 45, 0, 48, 0, 49, 0, 4, 0, 20, 0, 68, 0, 79, 0, 77, 0, 65, 0, 73, 0, 78, 0, 78, 0, 65, 0, 77,
                        0, 69, 0, 3, 0, 20, 0, 115, 0, 101, 0, 114, 0, 118, 0, 101, 0, 114, 0, 78, 0, 97, 0, 109, 0,
                        101, 0, 5, 0, 18, 0, 103, 0, 97, 0, 108, 0, 97, 0, 120, 0, 121, 0, 46, 0, 97, 0, 100, 0, 7, 0,
                        8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0};
                secBlobOut = auth.generateClientContext(sspiBlob, done);

            } catch (Exception e) {
                assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_ntlmNoTimestamp")));
            }
        }

    }

    private void testInvalidProperties(String connectionString, String resourceKeyword) {
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg(resourceKeyword)));
        }
    }

    private void verifyNTLM(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt
                .executeQuery("select auth_scheme from sys.dm_exec_connections where session_id=@@spid")) {
            while (rs.next()) {
                assertEquals("NTLM", rs.getString(1));
            }
        }
    }

    private void verifyDatabase(Connection conn, String databaseName) throws SQLException {
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("SELECT DB_NAME()")) {
            while (rs.next()) {
                assertEquals(databaseName, rs.getString(1));
            }
        }
    }

    // TODO Remove when this method is merged to dev in PR #968
    private static SQLServerDataSource updateDataSource(String connectionString, SQLServerDataSource ds) {
        String prefix = "jdbc:sqlserver://";
        if (null != connectionString && connectionString.startsWith(prefix)) {
            String extract = connectionString.substring(prefix.length());
            String[] identifiers = extract.split(";");
            String server = identifiers[0];
            // Check if serverName contains instance name
            if (server.contains("\\")) {
                int i = identifiers[0].indexOf('\\');
                ds.setServerName(extractPort(server.substring(0, i), ds));
                ds.setInstanceName(server.substring(i + 1));
            } else {
                ds.setServerName(extractPort(server, ds));
            }
            for (String prop : identifiers) {
                if (prop.contains("=")) {
                    int index = prop.indexOf("=");
                    String name = prop.substring(0, index);
                    String value = prop.substring(index + 1);
                    switch (name.toUpperCase()) {
                        case "USER":
                        case "USERNAME":
                            ds.setUser(value);
                            break;
                        case "PORT":
                        case "PORTNUMBER":
                            ds.setPortNumber(Integer.parseInt(value));
                            break;
                        case "PASSWORD":
                            ds.setPassword(value);
                            break;
                        case "DOMAIN":
                            ds.setDomain(value);
                            break;
                        case "DATABASE":
                        case "DATABASENAME":
                            ds.setDatabaseName(value);
                            break;
                        case "COLUMNENCRYPTIONSETTING":
                            ds.setColumnEncryptionSetting(value);
                            break;
                        case "DISABLESTATEMENTPOOLING":
                            ds.setDisableStatementPooling(Boolean.parseBoolean(value));
                            break;
                        case "STATEMENTPOOLINGCACHESIZE":
                            ds.setStatementPoolingCacheSize(Integer.parseInt(value));
                            break;
                        case "AUTHENTICATION":
                            ds.setAuthentication(value);
                            break;
                        case "AUTHENTICATIONSCHEME":
                            ds.setAuthenticationScheme(value);
                            break;
                        case "CANCELQUERYTIMEOUT":
                            ds.setCancelQueryTimeout(Integer.parseInt(value));
                            break;
                        case "ENCRYPT":
                            ds.setEncrypt(Boolean.parseBoolean(value));
                            break;
                        case "HOSTNAMEINCERTIFICATE":
                            ds.setHostNameInCertificate(value);
                            break;
                        case "INTEGRATEDSECURITY":
                            ds.setIntegratedSecurity(Boolean.valueOf(value));
                            break;
                        default:
                            break;
                    }
                }
            }
        }
        return ds;
    }

    static String extractPort(String server, SQLServerDataSource ds) {
        if (server.contains(":")) {
            ds.setPortNumber(Integer.parseInt(server.substring(server.indexOf(":") + 1)));
            server = server.substring(0, server.indexOf(":"));
        }
        return server;
    }

    /**
     * Adds or updates the value of the given connection property in the connection string by overriding property.
     * 
     * @param connectionString
     *        original connection string
     * @param property
     *        name of the property
     * @param value
     *        value of the property
     * @return The updated connection string
     */
    private static String addOrOverrideProperty(String connectionString, String property, String value) {
        return connectionString + ";" + property + "=" + value + ";";
    }
}
