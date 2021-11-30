package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.LogManager;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;


/**
 * A class for testing basic NTLMv2 functionality.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDW)
@Tag(Constants.xAzureSQLDB)
@Tag(Constants.xAzureSQLMI)
@Tag(Constants.NTLM)
public class NTLMConnectionTest extends AbstractTest {

    private static SQLServerDataSource dsNTLMLocal = null;
    private static SQLServerDataSource dsNTLMXA = null;
    private static SQLServerDataSource dsNTLMPool = null;

    private static String serverFqdn;

    /**
     * Setup environment for test.
     * 
     * @throws Exception
     *         when an error occurs
     */
    @BeforeAll
    public static void setUp() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"trustServerCertificate", "true");
        setConnection();

        // reset logging to avoid servere logs due to negative testing
        LogManager.getLogManager().reset();

        dsNTLMLocal = new SQLServerDataSource();
        AbstractTest.updateDataSource(connectionStringNTLM, dsNTLMLocal);

        dsNTLMXA = new SQLServerXADataSource();
        AbstractTest.updateDataSource(connectionStringNTLM, dsNTLMXA);

        dsNTLMPool = new SQLServerConnectionPoolDataSource();
        AbstractTest.updateDataSource(connectionStringNTLM, dsNTLMPool);

        // grant view server state permission - needed to verify NTLM
        try (Connection con = PrepUtil
                .getConnection(TestUtils.addOrOverrideProperty(connectionString, "database", "master"));
                Statement stmt = con.createStatement()) {
            stmt.executeUpdate("GRANT VIEW SERVER STATE TO PUBLIC");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Tests basic NTLM authentication.
     * 
     * @throws SQLException
     *         when an error occurs
     */
    @Test
    public void testNTLMBasicConnection() throws SQLException {
        try (Connection con1 = dsNTLMLocal.getConnection(); Connection con2 = dsNTLMXA.getConnection();
                Connection con3 = dsNTLMPool.getConnection();
                Connection con4 = PrepUtil.getConnection(connectionStringNTLM)) {
            verifyNTLM(con1);
            verifyNTLM(con2);
            verifyNTLM(con3);
            verifyNTLM(con4);
        }
    }

    /**
     * Tests invalid connection property combinations.
     */
    @Test
    public void testNTLMInvalidProperties() {
        // NTLM without user name
        testInvalidProperties(TestUtils.addOrOverrideProperty(connectionStringNTLM, "user", ""),
                "R_NtlmNoUserPasswordDomain");

        // NTLM without password
        testInvalidProperties(TestUtils.addOrOverrideProperty(connectionStringNTLM, "password", ""),
                "R_NtlmNoUserPasswordDomain");

        // NTLM with integratedSecurity property
        testInvalidProperties(
                TestUtils.addOrOverrideProperty(connectionStringNTLM, "authentication", "ActiveDirectoryIntegrated "),
                "R_SetAuthenticationWhenIntegratedSecurityTrue");
    }

    /**
     * Tests NTLM authentication when the connection is encrypted.
     * 
     * @throws SQLException
     *         when an error occurs
     */
    @Test
    public void testNTLMEncryptedConnection() throws SQLException {
        try (Connection con = PrepUtil
                .getConnection(connectionStringNTLM + ";encrypt=true;trustServerCertificate=true")) {
            verifyNTLM(con);
        }
    }

    /**
     * Tests NTLM authentication against a non-default database.
     * 
     * @throws SQLException
     *         when an error occurs
     */
    @Test
    public void testNTLMNonDefaultDatabase() throws SQLException {
        String databaseName = "tempdb";
        try (Connection con = PrepUtil
                .getConnection(TestUtils.addOrOverrideProperty(connectionStringNTLM, "database", databaseName))) {
            verifyNTLM(con);
            verifyDatabase(con, databaseName);
        }
    }

    /**
     * Tests NTLM authentication when connection pooling is enabled.
     * 
     * @throws SQLException
     *         when an error occurs
     */
    @Test
    public void testNTLMHikariCP() throws SQLException {
        HikariConfig config1 = new HikariConfig();
        HikariConfig config2 = new HikariConfig();
        config1.setDataSource(dsNTLMLocal);
        config2.setJdbcUrl(connectionStringNTLM);
        try (HikariDataSource dsCP1 = new HikariDataSource(config1);
                HikariDataSource dsCP2 = new HikariDataSource(config2); Connection con1 = dsCP1.getConnection();
                Connection con2 = dsCP2.getConnection()) {
            verifyNTLM(con1);
            verifyNTLM(con2);
        }
    }

    /**
     * Test NTLM connection with IP address
     * 
     * @throws SQLException
     *         when an error occurs
     */
    @Test
    public void testNTLMipAddr() throws SQLException {
        String ipAddr;
        try {
            ipAddr = InetAddress.getByName(serverFqdn).getHostAddress();
            try (Connection con = PrepUtil.getConnection(connectionStringNTLM + ";servername=" + ipAddr)) {
                verifyNTLM(con);
            }
        } catch (UnknownHostException e) {
            fail(e.getMessage());
        }
    }

    /**
     * The following testNTLMBad* tests use the following hardcoded challenge message token and modifies the fields with
     * generateClientToken to trigger errors.
     * 
     * <pre>
     * A good challenge token to start: 
     * {
     * 78, 84, 76, 77, 83, 83, 80, 0,                                               // NTLMSSP\0
     * 2, 0, 0, 0,                                                                  // messageType (2 for challenge msg)              
     * 12, 0,                                                                       // target name len
     * 12, 0,                                                                       // target name max len
     * 56, 0, 0, 0,                                                                 // target name buffer offset
     * 21, -126, -127, 2,                                                           // negotiate flags
     * 78, -76, 68, 118, 41, -30, -71, 100,                                         // server challenge
     * 0, 0, 0, 0, 0, 0, 0, 0,                                                      // reserved
     * -88, 0,                                                                      // target info len
     * -88, 0,                                                                      // target info max len
     * 68, 0, 0, 0,                                                                 // target info buffer offset
     * 0, 0, 0, 0, 0, 0, 0, 0,                                                      // version
     * 71, 0, 65, 0, 76, 0, 65, 0, 88, 0, 89, 0,                                    // target name
     * 2, 0,                                                                        // MsvAvNbDomainName avid
     * 12, 0,                                                                       // MsvAvNbDomainName avlen
     * 71, 0, 65, 0, 76, 0, 65, 0, 88, 0, 89, 0,                                    // domain name
     * 1, 0,                                                                        // MsvAvNbComputerName avid
     * 30, 0,                                                                       // MsvAvNbComputerName avlen
     * 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 // computer name
     * 4, 0,                                                                        // MsvAvDnsDomainName avid
     * 20, 0,                                                                       // MsvAvDnsDomainName avlen
     * 68, 0, 79, 0, 77, 0, 65, 0, 73, 0, 78, 0, 78, 0, 65, 0, 77, 0, 69, 0,        // domain name
     * 3, 0,                                                                        // MsvAvDnsComputerName avid
     * [filled in test]                                                             // MsvAvDnsComputerName len                                     
     * [filled in test]                                                             // computer name
     * 5, 0,                                                                        // MsvAvDnsTreeName avid
     * 18, 0,                                                                       // MsvAvDnsTreeName avlen
     * 103, 0, 97, 0, 108, 0, 97, 0, 120, 0, 121, 0, 46, 0, 97, 0, 100, 0,          // tree name
     * 7, 0,                                                                        // MsvAvTimestamp avid
     * 8, 0,                                                                        // MsvAvTimestamp avlen
     * 122, -115, 18, 50, -5, -32, -44, 1,                                          // timestamp
     * 0, 0, 0, 0                                                                   // MsvAvEOL avid
     * };
     * 
     * challengeTokenPart1 is everything up to targetinfo
     * challengeTargetInfo1 is everything in targetinfo up to MsvAvDnsComputerName id
     * challengeTargetInfo2 is everything in targetinfo after computer (server) name
     * 
     * NTLM_CHALLENGE_*_OFFSETS are defined offsets for the fields in the challenge token
     * </pre>
     */
    private byte[] challengeToken1stPart = {78, 84, 76, 77, 83, 83, 80, 0, 2, 0, 0, 0, 12, 0, 12, 0, 56, 0, 0, 0, 21,
            -126, -127, 2, 78, -76, 68, 118, 41, -30, -71, 100, 0, 0, 0, 0, 0, 0, 0, 0, -88, 0, -88, 0, 68, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 71, 0, 65, 0, 76, 0, 65, 0, 88, 0, 89, 0};

    private byte[] challengeTargetInfo1 = {2, 0, 12, 0, 71, 0, 65, 0, 76, 0, 65, 0, 88, 0, 89, 0, 1, 0, 30, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 20, 0, 68, 0, 79, 0,
            77, 0, 65, 0, 73, 0, 78, 0, 78, 0, 65, 0, 77, 0, 69, 0, 3, 0};

    private byte[] challengeTargetInfo2 = {5, 0, 18, 0, 103, 0, 97, 0, 108, 0, 97, 0, 120, 0, 121, 0, 46, 0, 97, 0, 100,
            0, 7, 0, 8, 0, 122, -115, 18, 50, -5, -32, -44, 1, 0, 0, 0, 0};

    private final int ntlmChallengeSignatureOffset = 0;
    private final int ntlmChallengeMessageTypeOffset = 8;

    private final int ntlmChallengeTargetInfoLenOffset = 40;
    private final int ntlmChallengeTargetInfoOffset = 68;

    // offsets for targetinfo av pairs
    private final int ntlmChallengeMsvAvTimestampOffset = -16;

    /**
     * Test NTLM Bad Signature
     */
    @Test
    public void testNTLMBadSignature() {
        try {
            byte[] badSignature = {0, 0, 0, 0, 0, 0, 0, 0};
            sendBadToken(badSignature, ntlmChallengeSignatureOffset);
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (Exception e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_ntlmSignatureError")));
        }
    }

    /**
     * Test NTLM Bad Message Type
     */
    @Test
    public void testNTLMBadMessageType() {
        try {
            byte[] badMessageType = {0, 0, 0, 0};
            sendBadToken(badMessageType, ntlmChallengeMessageTypeOffset);
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (Exception e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_ntlmMessageTypeError")));
        }
    }

    /**
     * Test NTLM Bad Target info length
     */
    @Test
    public void testNTLMBadTargetInfoLen() {
        try {
            byte[] badTargetInfoLen = {0, 0};
            sendBadToken(badTargetInfoLen, ntlmChallengeTargetInfoLenOffset);
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (Exception e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_ntlmNoTargetInfo")));
        }
    }

    /**
     * Test NTLM Bad Avid
     */
    @Test
    public void testNTLMBadAvid() {
        try {
            byte[] badAvid = {-1, 0};
            sendBadToken(badAvid, ntlmChallengeTargetInfoOffset);
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (Exception e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_ntlmUnknownValue")));
        }
    }

    /**
     * Test NTLM Bad Timestamp
     */
    @Test
    public void testNTLMBadTimestamp() {
        try {
            byte[] badTimestamp = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
            sendBadToken(badTimestamp, ntlmChallengeMsvAvTimestampOffset);
        } catch (Exception e) {
            // this should just generate a log but not fail
            fail(e.getMessage());
        }
    }

    /**
     * Get Server FQDN from connection
     */
    private void getServerFqdn(SQLServerConnection con) {
        try {
            serverFqdn = InetAddress
                    .getByName(con.activeConnectionProperties
                            .getProperty(SQLServerDriverStringProperty.SERVER_NAME.toString()))
                    .getCanonicalHostName().toUpperCase();

        } catch (UnknownHostException e) {
            fail("Error getting server FQDN: " + e.getMessage());
        }
    }

    /**
     * Send a bad NTLM Challenge Token
     */
    private void sendBadToken(byte[] badField, int offset) throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(connectionStringNTLM)) {
            getServerFqdn(con);
            SSPIAuthentication auth = new NTLMAuthentication(con, "domainName", "userName", new byte[1], "hostname");
            boolean[] done = {false};
            byte[] badToken = getChallengeToken(offset, badField);
            auth.generateClientContext(badToken, done);
        }
    }

    /**
     * Get bad challenge token for testNTLMBad* tests
     * 
     * @param offset
     *        if > 0 offset from beginning of token if < 0 offset from end of token
     * @param badBytes
     *        bad bytes to write to the challenge token
     * @return
     */
    private byte[] getChallengeToken(int offset, byte[] badBytes) {
        byte[] serverBytes = serverFqdn.getBytes(java.nio.charset.StandardCharsets.UTF_16LE);

        // add 4 for MSVAVDNSCOMPUTERNAME avlen
        int targetInfoLen = challengeTargetInfo1.length + 4 + serverBytes.length + challengeTargetInfo2.length;
        int tokenLen = challengeToken1stPart.length + targetInfoLen;

        ByteBuffer token = ByteBuffer.allocate(tokenLen).order(ByteOrder.LITTLE_ENDIAN);
        token.put(challengeToken1stPart);

        token.put(challengeTargetInfo1);

        token.putShort((short) serverBytes.length);
        token.put(serverBytes);

        token.put(challengeTargetInfo2);

        /*
         * update target info - the cast is necessary as JDK 8 calls method Buffer.position(I)LBuffer while in JDK 9+
         * calls method ByteBuffer.position(I)LByteBuffer
         */
        ((java.nio.Buffer) token).position(ntlmChallengeTargetInfoLenOffset);
        token.putShort((short) targetInfoLen); // update target info len
        token.putShort((short) targetInfoLen); // update target info maxlen

        // write bad bytes
        if (0 <= offset) {
            ((java.nio.Buffer) token).position(offset);
            token.put(badBytes);
        } else {
            ((java.nio.Buffer) token).position(tokenLen + offset);
            token.put(badBytes);
        }

        return token.array();
    }

    /*
     * Test invalid properties
     */
    private void testInvalidProperties(String connectionStringNTLM, String resourceKeyword) {
        try (Connection con = PrepUtil.getConnection(connectionStringNTLM)) {
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (SQLException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg(resourceKeyword)));
        }
    }

    /*
     * Verify NTLM authentication
     */
    private void verifyNTLM(Connection con) throws SQLException {
        try (Statement stmt = con.createStatement(); ResultSet rs = stmt
                .executeQuery("select auth_scheme from sys.dm_exec_connections where session_id=@@spid")) {
            while (rs.next()) {
                assertEquals("NTLM", rs.getString(1));
            }
        }
    }

    /*
     * Verify Database name
     */
    private void verifyDatabase(Connection con, String databaseName) throws SQLException {
        try (Statement stmt = con.createStatement(); ResultSet rs = stmt.executeQuery("SELECT DB_NAME()")) {
            while (rs.next()) {
                assertEquals(databaseName, rs.getString(1));
            }
        }
    }
}
