package com.microsoft.sqlserver.jdbc.connection;

import static org.junit.Assume.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Before;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;


/**
 * A class for testing basic NTLMv2 functionality.
 */
@RunWith(JUnitPlatform.class)
public class NTLMConnectionTest extends AbstractTest {
    private static SQLServerDataSource dsNTLMLocal = null;

    @BeforeAll
    public static void setUp() {
        if (connectionStringNTLM != null) {
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
        if (connectionStringNTLM != null) {
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
        if (connectionStringNTLM != null) {
            // NTLM without domain.
            testInvalidProperties(addOrOverrideProperty(connectionStringNTLM, "domain", ""),
                    "R_NoUserPasswordForDomain");

            // NTLM without user name
            testInvalidProperties(addOrOverrideProperty(connectionStringNTLM, "user", ""), "R_NoUserPasswordForDomain");

            // NTLM without password
            testInvalidProperties(addOrOverrideProperty(connectionStringNTLM, "password", ""),
                    "R_NoUserPasswordForDomain");

            // NTLM with integratedSecurity property
            testInvalidProperties(addOrOverrideProperty(connectionStringNTLM, "integratedSecurity", "true"),
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
        if (connectionStringNTLM != null) {
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
        if (connectionStringNTLM != null) {
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
        if (connectionStringNTLM != null) {
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
    @Test
    public void testNTLMLoginTimeout() {
        if (connectionStringNTLM != null) {
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
    private String addOrOverrideProperty(String connectionString, String property, String value) {
        return connectionString + ";" + property + "=" + value + ";";
    }
}
