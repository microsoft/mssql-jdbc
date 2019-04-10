package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.util.logging.StreamHandler;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
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
@Tag("xAzureSQLDW")
@Tag("xAzureSQLDB")
@Tag("xAzureSQLMSI")
public class NTLMConnectionTest extends AbstractTest {

    // custom formatter that contains nothing but the log message only
    private static class CustomFormatter extends Formatter {
        @Override
        public String format(LogRecord record) {
            StringBuffer sb = new StringBuffer();
            sb.append(record.getMessage());
            return sb.toString();
        }
    }

    private static SQLServerDataSource dsNTLMLocal = null;

    // NTLM properties defined
    private static boolean ntlmPropsDefined = false;

    // updated connection strong with NTLM properties
    private static String connectionStringNTLM = connectionString;

    private static String serverFqdn;

    private static java.util.logging.Logger ntlmLogger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.internals.NTLMAuthentication");

    private static ByteArrayOutputStream loggerContent = new ByteArrayOutputStream();
    private static StreamHandler handler = new StreamHandler(new PrintStream(loggerContent), new CustomFormatter());

    @BeforeAll
    public static void setUp() {
        // remove all other logging and add our own handler
        java.util.logging.Logger logger = java.util.logging.Logger.getLogger("");
        logger.removeHandler(logger.getHandlers()[0]);
        ntlmLogger.addHandler(handler);

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
            try (Connection con1 = dsNTLMLocal.getConnection();
                    Connection con2 = DriverManager.getConnection(connectionStringNTLM)) {
                verifyNTLM(con1);
                verifyNTLM(con2);
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
            try (Connection con = DriverManager
                    .getConnection(connectionStringNTLM + ";encrypt=true;trustServerCertificate=true")) {
                verifyNTLM(con);
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
            try (Connection con = DriverManager
                    .getConnection(addOrOverrideProperty(connectionStringNTLM, "database", databaseName))) {
                verifyNTLM(con);
                verifyDatabase(con, databaseName);
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
                    HikariDataSource dsCP2 = new HikariDataSource(config2); Connection con1 = dsCP1.getConnection();
                    Connection con2 = dsCP2.getConnection()) {
                verifyNTLM(con1);
                verifyNTLM(con2);
            }
        }
    }

    /**
     * TODO: random timeout failure with this test
     * 
     * Validates that loginTimeout connection property works with NTLM authentication.
     */
    // @Test
    public void testNTLMLoginTimeout() {
        if (ntlmPropsDefined) {
            long timerStart = 0;
            long timerEnd = 0;
            int loginTimeout = 10;

            timerStart = System.currentTimeMillis();
            try (Connection con = DriverManager.getConnection(
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
    /**
     * Test NTLM connection with IP address
     * 
     * @throws SQLException
     */
    public void testNTLMipAddr() throws SQLException {
        if (ntlmPropsDefined) {
            String ipAddr;
            try {
                ipAddr = InetAddress.getByName(serverFqdn).getHostAddress();
                try (Connection con = DriverManager.getConnection(connectionStringNTLM + ";servername=" + ipAddr)) {
                    verifyNTLM(con);

                }
            } catch (UnknownHostException e) {
                fail(e.getMessage());
            }
        }
    }

    @Test
    /**
     * Test Bad NTLM Initialization
     * 
     * @throws SQLException
     */
    public void testNTLMBadInit() throws SQLException {
        try {
            SSPIAuthentication auth = new NTLMAuthentication(new SQLServerConnection(""), "serverName", "domainName",
                    "hostname");
        } catch (Exception e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_ntlmUnknownServer")));
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

    private final int NTLM_CHALLENGE_SIGNATURE_OFFSET = 0;
    private final int NTLM_CHALLENGE_MESSAGETYPE_OFFSET = 8;

    private final int NTLM_CHALLENGE_TARGETINFOLEN_OFFSET = 40;
    private final int NTLM_CHALLENGE_TARGETINFO_OFFSET = 68;

    // offsets for targetinfo av pairs
    private final int NTLM_CHALLENGE_MSVAVDNSDOMAINNAME_OFFSET = NTLM_CHALLENGE_TARGETINFO_OFFSET + 54;
    private final int NTLM_CHALLENGE_MSVAVDNSCOMPUTERNAME_OFFSET = NTLM_CHALLENGE_TARGETINFO_OFFSET + 78;
    private final int NTLM_CHALLENGE_MSVAVTIMESTAMP_OFFSET = -16;

    @Test
    public void testNTLMBadSignature() throws SQLException {
        if (ntlmPropsDefined) {
            try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(connectionStringNTLM)) {
                getServerFqdn(con);
                SSPIAuthentication auth = new NTLMAuthentication(con, serverFqdn, "domainName", "hostname");
                boolean[] done = {false};

                try {
                    byte[] badSignature = {0, 0, 0, 0, 0, 0, 0, 0};
                    byte[] badToken = getChallengeToken(NTLM_CHALLENGE_SIGNATURE_OFFSET, badSignature);

                    auth.generateClientContext(badToken, done);
                } catch (Exception e) {
                    assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_ntlmSignatureError")));
                }
            }
        }
    }

    @Test
    public void testNTLMBadMessageType() throws SQLException {
        if (ntlmPropsDefined) {
            try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(connectionStringNTLM)) {
                getServerFqdn(con);
                SSPIAuthentication auth = new NTLMAuthentication(con, serverFqdn, "domainName", "hostname");
                boolean[] done = {false};

                try {
                    byte[] badMessageType = {0, 0, 0, 0};
                    byte[] badToken = getChallengeToken(NTLM_CHALLENGE_MESSAGETYPE_OFFSET, badMessageType);

                    auth.generateClientContext(badToken, done);

                } catch (Exception e) {
                    assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_ntlmMessageTypeError")));
                }
            }
        }
    }

    @Test
    public void testNTLMBadTargetInfoLen() throws SQLException {
        if (ntlmPropsDefined) {
            try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(connectionStringNTLM)) {
                getServerFqdn(con);
                SSPIAuthentication auth = new NTLMAuthentication(con, serverFqdn, "domainName", "hostname");
                boolean[] done = {false};

                try {
                    byte[] badTargetInfoLen = {0, 0};
                    byte[] badToken = getChallengeToken(NTLM_CHALLENGE_TARGETINFOLEN_OFFSET, badTargetInfoLen);

                    auth.generateClientContext(badToken, done);

                } catch (Exception e) {
                    assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_ntlmNoTargetInfo")));
                }
            }
        }
    }

    @Test
    public void testNTLMBadDomain() throws SQLException {
        if (ntlmPropsDefined) {
            loggerContent.reset();

            try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(connectionStringNTLM)) {
                getServerFqdn(con);
                SSPIAuthentication auth = new NTLMAuthentication(con, serverFqdn, "domainName", "hostname");
                boolean[] done = {false};

                try {
                    byte[] badDomain = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
                    byte[] badToken = getChallengeToken(NTLM_CHALLENGE_MSVAVDNSDOMAINNAME_OFFSET, badDomain);

                    auth.generateClientContext(badToken, done);

                } catch (Exception e) {
                    // this should just generate a warning but not fail
                    fail(e.getMessage());
                }
            }

            // verify message was logged
            handler.flush();
            assertTrue(loggerContent.toString().matches(TestUtils.formatErrorMsg("R_ntlmBadDomain")));
        }
    }

    @Test
    public void testNTLMBadComputerName() throws SQLException {
        if (ntlmPropsDefined) {
            loggerContent.reset();

            try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(connectionStringNTLM)) {
                getServerFqdn(con);
                SSPIAuthentication auth = new NTLMAuthentication(con, serverFqdn, "domainName", "hostname");
                boolean[] done = {false};
                try {
                    // modify token with bad computer name
                    byte[] badComputer = {0, 0, 0, 0, 0, 0, 0, 0, 0};
                    byte[] badToken = getChallengeToken(NTLM_CHALLENGE_MSVAVDNSCOMPUTERNAME_OFFSET, badComputer);

                    auth.generateClientContext(badToken, done);

                } catch (Exception e) {
                    // this should just generate a warning but not fail
                    fail(e.getMessage());
                }

                // verify message was logged
                handler.flush();
                assertTrue(loggerContent.toString().matches(TestUtils.formatErrorMsg("R_ntlmBadComputer")));
            }
        }
    }

    @Test
    public void testNTLMBadAvid() throws SQLException {
        if (ntlmPropsDefined) {
            try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(connectionStringNTLM)) {
                getServerFqdn(con);
                SSPIAuthentication auth = new NTLMAuthentication(con, serverFqdn, "domainName", "hostname");
                boolean[] done = {false};
                try {
                    byte[] badAvid = {-1, 0};
                    byte[] badToken = getChallengeToken(NTLM_CHALLENGE_TARGETINFO_OFFSET, badAvid);

                    auth.generateClientContext(badToken, done);

                } catch (Exception e) {
                    assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_ntlmUnknownValue")));
                }
            }
        }
    }

    @Test
    public void testNTLMBadTimestamp() throws SQLException {
        if (ntlmPropsDefined) {
            loggerContent.reset();

            try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(connectionStringNTLM)) {
                getServerFqdn(con);
                SSPIAuthentication auth = new NTLMAuthentication(con, serverFqdn, "domainName", "hostname");
                boolean[] done = {false};

                try {

                    byte[] badTimestamp = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
                    byte[] badToken = getChallengeToken(NTLM_CHALLENGE_MSVAVTIMESTAMP_OFFSET, badTimestamp);

                    auth.generateClientContext(badToken, done);

                } catch (Exception e) {
                    // this should just generate a warning but not fail
                    fail(e.getMessage());
                }
            }

            // verify message was logged
            handler.flush();
            assertTrue(loggerContent.toString().matches(TestUtils.formatErrorMsg("R_ntlmNoTimestamp")));
        }
    }

    /*
     * Get Server FQDN from connection
     */
    private void getServerFqdn(SQLServerConnection con) throws SQLException {
        try {
            serverFqdn = InetAddress.getByName(con.currentConnectPlaceHolder.getServerName()).getCanonicalHostName()
                    .toUpperCase();
        } catch (UnknownHostException e) {
            fail("Error getting server FQDN: " + e.getMessage());
            e.printStackTrace();
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

        // update targetinfo len
        token.position(NTLM_CHALLENGE_TARGETINFOLEN_OFFSET);
        token.putShort((short) targetInfoLen); // len
        token.putShort((short) targetInfoLen); // maxlen

        // write bad bytes
        if (0 <= offset) {
            token.position(offset);
            token.put(badBytes);
        } else {
            token.position(tokenLen + offset);
            token.put(badBytes);
        }

        return token.array();
    }

    /*
     * Test invalid properties
     */
    private void testInvalidProperties(String connectionString, String resourceKeyword) {
        try (Connection con = DriverManager.getConnection(connectionString)) {
            fail();
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
