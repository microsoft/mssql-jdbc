/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) 2016 Microsoft Corporation All rights reserved. This program is
 * made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;

import com.microsoft.sqlserver.jdbc.ISQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionAzureKeyVaultProvider;
import com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionJavaKeyStoreProvider;
import com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionKeyStoreProvider;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.microsoft.sqlserver.jdbc.SQLServerXADataSource;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;


/**
 * Think about following things:
 * <li>Connection pool
 * <li>Configured Property file instead of passing from args.
 * <li>Think of different property files for different settings. / flag etc.
 * <Li>Think about what kind of logging we are going use it. <B>util.logging</B> will be preference.
 * 
 * @since 6.1.2
 */
public abstract class AbstractTest {

    protected static String applicationClientID = null;
    protected static String applicationKey = null;
    protected static String tenantID;
    protected static String[] keyIDs = null;

    protected static String[] enclaveServer = null;
    protected static String[] enclaveAttestationUrl = null;
    protected static String[] enclaveAttestationProtocol = null;

    protected static String clientCertificate = null;
    protected static String clientKey = null;
    protected static String clientKeyPassword = "";

    protected static String trustStore = "";
    protected static String trustStorePassword = "";

    protected static String encrypt = "";
    protected static String trustServerCertificate = "";

    protected static String windowsKeyPath = null;
    protected static String javaKeyPath = null;
    protected static String javaKeyAliases = null;
    protected static SQLServerColumnEncryptionKeyStoreProvider jksProvider = null;
    protected static SQLServerColumnEncryptionAzureKeyVaultProvider akvProvider = null;
    static boolean isKspRegistered = false;

    // properties needed for Managed Identity
    protected static String managedIdentityClientId = null;
    protected static String keyStorePrincipalId = null;
    protected static String keyStoreSecret = null;

    protected static SQLServerConnection connection = null;
    protected static ISQLServerDataSource ds = null;
    protected static ISQLServerDataSource dsXA = null;
    protected static ISQLServerDataSource dsPool = null;

    protected static Connection connectionAzure = null;
    protected static String connectionString = null;
    protected static String connectionStringNTLM;

    private static boolean determinedSqlAzureOrSqlServer = false;
    private static boolean determinedSqlOS = false;
    private static boolean isSqlAzure = false;
    private static boolean isSqlAzureDW = false;
    private static boolean isSqlLinux = false;

    /**
     * Byte Array containing streamed logging output. Content can be retrieved using toByteArray() or toString()
     */
    private static ByteArrayOutputStream logOutputStream = null;
    private static PrintStream logPrintStream = null;
    private static Properties configProperties = null;

    protected static boolean isWindows = System.getProperty("os.name").startsWith("Windows");

    public static Properties properties = null;

    /**
     * This will take care of all initialization before running the Test Suite.
     * 
     * @throws Exception
     *         when an error occurs
     */
    @BeforeAll
    public static void setup() throws Exception {
        // Invoke fine logging...
        invokeLogging();

        // get Properties from config file
        try (InputStream input = new FileInputStream(Constants.CONFIG_PROPERTIES_FILE)) {
            configProperties = new Properties();
            configProperties.load(input);
        } catch (FileNotFoundException | SecurityException e) {
            // no config file used
        }

        connectionString = getConfiguredPropertyOrEnv(Constants.MSSQL_JDBC_TEST_CONNECTION_PROPERTIES);

        applicationClientID = getConfiguredProperty("applicationClientID");
        applicationKey = getConfiguredProperty("applicationKey");
        tenantID = getConfiguredProperty("tenantID");

        encrypt = getConfiguredProperty("encrypt", "false");
        connectionString = TestUtils.addOrOverrideProperty(connectionString, "encrypt", encrypt);

        trustServerCertificate = getConfiguredProperty("trustServerCertificate", "true");
        connectionString = TestUtils.addOrOverrideProperty(connectionString, "trustServerCertificate",
                trustServerCertificate);

        javaKeyPath = TestUtils.getCurrentClassPath() + Constants.JKS_NAME;

        keyIDs = getConfiguredProperty("keyID", "").split(Constants.SEMI_COLON);
        windowsKeyPath = getConfiguredProperty("windowsKeyPath");

        String prop;
        prop = getConfiguredProperty("enclaveServer", null);
        if (null == prop) {
            // default to server in connection string
            String serverName = (connectionString.substring(Constants.JDBC_PREFIX.length())
                    .split(Constants.SEMI_COLON)[0]).split(":")[0];
            enclaveServer = new String[1];
            enclaveServer[0] = new String(serverName);
        } else {
            enclaveServer = prop.split(Constants.SEMI_COLON);
        }

        prop = getConfiguredProperty("enclaveAttestationUrl", null);
        enclaveAttestationUrl = null != prop ? prop.split(Constants.SEMI_COLON) : null;

        prop = getConfiguredProperty("enclaveAttestationProtocol", null);
        enclaveAttestationProtocol = null != prop ? prop.split(Constants.SEMI_COLON) : null;

        clientCertificate = getConfiguredProperty("clientCertificate", null);

        clientKey = getConfiguredProperty("clientKey", null);

        clientKeyPassword = getConfiguredProperty("clientKeyPassword", "");

        trustStore = getConfiguredProperty("trustStore", "");
        if (!trustStore.trim().isEmpty()) {
            connectionString = TestUtils.addOrOverrideProperty(connectionString, "trustStore", trustStore);
        }

        trustStorePassword = getConfiguredProperty("trustStorePassword", "");
        if (!trustStorePassword.trim().isEmpty()) {
            connectionString = TestUtils.addOrOverrideProperty(connectionString, "trustStorePassword",
                    trustStorePassword);
        }

        Map<String, SQLServerColumnEncryptionKeyStoreProvider> map = new HashMap<String, SQLServerColumnEncryptionKeyStoreProvider>();
        if (null == jksProvider) {
            jksProvider = new SQLServerColumnEncryptionJavaKeyStoreProvider(javaKeyPath,
                    Constants.JKS_SECRET.toCharArray());
            map.put(Constants.CUSTOM_KEYSTORE_NAME, jksProvider);
        }

        if (null == akvProvider && null != applicationClientID && null != applicationKey) {
            File file = null;
            try {
                file = new File(Constants.MSSQL_JDBC_PROPERTIES);
                try (OutputStream os = new FileOutputStream(file);) {
                    Properties props = new Properties();
                    // Append to the list of hardcoded endpoints.
                    props.setProperty(Constants.AKV_TRUSTED_ENDPOINTS_KEYWORD, ";vault.azure.net");
                    props.store(os, "");
                }
                akvProvider = new SQLServerColumnEncryptionAzureKeyVaultProvider(applicationClientID, applicationKey);
                map.put(Constants.AZURE_KEY_VAULT_NAME, akvProvider);
            } finally {
                if (null != file) {
                    file.delete();
                }
            }
        }

        if (!isKspRegistered) {
            SQLServerConnection.registerColumnEncryptionKeyStoreProviders(map);
            isKspRegistered = true;
        }

        // MSI properties
        managedIdentityClientId = getConfiguredProperty("msiClientId");
        keyStorePrincipalId = getConfiguredProperty("keyStorePrincipalId");
        keyStoreSecret = getConfiguredProperty("keyStoreSecret");
    }

    protected static void setupConnectionString() {
        connectionStringNTLM = connectionString;

        // if these properties are defined then NTLM is desired, modify connection string accordingly
        String domain = getConfiguredProperty("domainNTLM");
        String user = getConfiguredProperty("userNTLM");
        String password = getConfiguredProperty("passwordNTLM");

        if (null != domain) {
            connectionStringNTLM = TestUtils.addOrOverrideProperty(connectionStringNTLM, "domain", domain);
        }

        if (null != user) {
            connectionStringNTLM = TestUtils.addOrOverrideProperty(connectionStringNTLM, "user", user);
        }

        if (null != password) {
            connectionStringNTLM = TestUtils.addOrOverrideProperty(connectionStringNTLM, "password", password);
        }

        if (null != user && null != password) {
            connectionStringNTLM = TestUtils.addOrOverrideProperty(connectionStringNTLM, "authenticationScheme",
                    "NTLM");
            connectionStringNTLM = TestUtils.addOrOverrideProperty(connectionStringNTLM, "integratedSecurity", "true");
        }

        ds = updateDataSource(connectionString, new SQLServerDataSource());
        dsXA = updateDataSource(connectionString, new SQLServerXADataSource());
        dsPool = updateDataSource(connectionString, new SQLServerConnectionPoolDataSource());
    }

    protected static void setConnection() throws Exception {
        setupConnectionString();

        Assertions.assertNotNull(connectionString, TestResource.getResource("R_ConnectionStringNull"));
        Class.forName(Constants.MSSQL_JDBC_PACKAGE + ".SQLServerDriver");
        if (!SQLServerDriver.isRegistered()) {
            SQLServerDriver.register();
        }
        if (null == connection || connection.isClosed()) {
            connection = getConnection();
        }
        isSqlAzureOrAzureDW(connection);

        checkSqlOS(connection);
    }

    /**
     * Covers only connection properties required for testing. Does not cover all connection properties - add more
     * properties if needed.
     * 
     * @param ds
     *        DataSource to be configured
     * @return ISQLServerDataSource
     */
    protected static ISQLServerDataSource updateDataSource(String connectionString, ISQLServerDataSource ds) {
        if (null != connectionString && connectionString.startsWith(Constants.JDBC_PREFIX)) {
            String extract = connectionString.substring(Constants.JDBC_PREFIX.length());
            String[] identifiers = extract.split(Constants.SEMI_COLON);
            String server = identifiers[0];

            // Check if serverName contains instance name
            if (server.contains(Constants.BACK_SLASH)) {
                int i = identifiers[0].indexOf(Constants.BACK_SLASH);
                ds.setServerName(extractPort(server.substring(0, i), ds));
                ds.setInstanceName(server.substring(i + 1));
            } else {
                ds.setServerName(extractPort(server, ds));
            }
            for (String prop : identifiers) {
                if (prop.contains(Constants.EQUAL_TO)) {
                    int index = prop.indexOf(Constants.EQUAL_TO);
                    String name = prop.substring(0, index);
                    String value = prop.substring(index + 1);
                    switch (name.toUpperCase()) {
                        case Constants.INTEGRATED_SECURITY:
                            ds.setIntegratedSecurity(Boolean.parseBoolean(value));
                            break;
                        case Constants.USER:
                        case Constants.USER_NAME:
                            ds.setUser(value);
                            break;
                        case Constants.PORT:
                        case Constants.PORT_NUMBER:
                            ds.setPortNumber(Integer.parseInt(value));
                            break;
                        case Constants.PASSWORD:
                            ds.setPassword(value);
                            break;
                        case Constants.DOMAIN:
                        case Constants.DOMAIN_NAME:
                            ds.setDomain(value);
                            break;
                        case Constants.DATABASE:
                        case Constants.DATABASE_NAME:
                            ds.setDatabaseName(value);
                            break;
                        case Constants.COLUMN_ENCRYPTION_SETTING:
                            ds.setColumnEncryptionSetting(value);
                            break;
                        case Constants.DISABLE_STATEMENT_POOLING:
                            ds.setDisableStatementPooling(Boolean.parseBoolean(value));
                            break;
                        case Constants.STATEMENT_POOLING_CACHE_SIZE:
                            ds.setStatementPoolingCacheSize(Integer.parseInt(value));
                            break;
                        case Constants.AUTHENTICATION:
                            ds.setAuthentication(value);
                            break;
                        case Constants.AUTHENTICATION_SCHEME:
                            ds.setAuthenticationScheme(value);
                            break;
                        case Constants.CANCEL_QUERY_TIMEOUT:
                            ds.setCancelQueryTimeout(Integer.parseInt(value));
                            break;
                        case Constants.ENCRYPT:
                            ds.setEncrypt(value);
                            break;
                        case Constants.TRUST_SERVER_CERTIFICATE:
                            ds.setTrustServerCertificate(Boolean.parseBoolean(value));
                            break;
                        case Constants.TRUST_STORE:
                            ds.setTrustStore(value);
                            break;
                        case Constants.TRUST_STORE_SECRET_PROPERTY:
                            ds.setTrustStorePassword(value);
                            break;
                        case Constants.TRUST_STORE_TYPE:
                            ds.setTrustStoreType(value);
                            break;
                        case Constants.HOST_NAME_IN_CERTIFICATE:
                            ds.setHostNameInCertificate(value);
                            break;
                        case Constants.ENCLAVE_ATTESTATIONURL:
                            ds.setEnclaveAttestationUrl(value);
                            break;
                        case Constants.ENCLAVE_ATTESTATIONPROTOCOL:
                            ds.setEnclaveAttestationProtocol(value);
                            break;
                        case Constants.KEYVAULTPROVIDER_CLIENTID:
                            ds.setKeyVaultProviderClientId(value);
                            break;
                        case Constants.KEYVAULTPROVIDER_CLIENTKEY:
                            ds.setKeyVaultProviderClientKey(value);
                            break;
                        case Constants.KEYSTORE_AUTHENTICATION:
                            ds.setKeyStoreAuthentication(value);
                            break;
                        case Constants.KEYSTORE_PRINCIPALID:
                            ds.setKeyStorePrincipalId(value);
                            break;
                        case Constants.KEYSTORE_SECRET:
                            ds.setKeyStoreSecret(value);
                            break;
                        case Constants.MSICLIENTID:
                            ds.setMSIClientId(value);
                            break;
                        case Constants.CLIENT_CERTIFICATE:
                            ds.setClientCertificate(value);
                            break;
                        case Constants.CLIENT_KEY:
                            ds.setClientKey(value);
                            break;
                        case Constants.CLIENT_KEY_PASSWORD:
                            ds.setClientKeyPassword(value);
                            break;
                        case Constants.AAD_SECURE_PRINCIPAL_ID:
                            ds.setAADSecurePrincipalId(value);
                            break;
                        case Constants.AAD_SECURE_PRINCIPAL_SECRET:
                            ds.setAADSecurePrincipalSecret(value);
                            break;
                        case Constants.SEND_TEMPORAL_DATATYPES_AS_STRING_FOR_BULK_COPY:
                            ds.setSendTemporalDataTypesAsStringForBulkCopy(Boolean.parseBoolean(value));
                            break;
                        case Constants.MAX_RESULT_BUFFER:
                            ds.setMaxResultBuffer(value);
                            break;
                        case Constants.CONNECT_RETRY_COUNT:
                            ds.setConnectRetryCount(Integer.parseInt(value));
                            break;
                        case Constants.CONNECT_RETRY_INTERVAL:
                            ds.setConnectRetryInterval(Integer.parseInt(value));
                            break;
                        case Constants.PREPARE_METHOD:
                            ds.setPrepareMethod(value);
                            break;
                        default:
                            break;
                    }
                }
            }
        }
        return ds;
    }

    static String extractPort(String server, ISQLServerDataSource ds) {
        if (server.contains(Constants.COLON)) {
            ds.setPortNumber(Integer.parseInt(server.substring(server.indexOf(Constants.COLON) + 1)));
            server = server.substring(0, server.indexOf(Constants.COLON));
        }
        return server;
    }

    /**
     * Get the connection String
     * 
     * @return connectionString
     */
    public static String getConnectionString() {
        return connectionString;
    }

    /**
     * Retrieves connection using default configured connection string
     * 
     * @return
     * @throws SQLException
     */
    protected static SQLServerConnection getConnection() throws SQLException {
        return PrepUtil.getConnection(connectionString);
    }

    /**
     * This will take care of all clean ups after running the Test Suite.
     * 
     * @throws Exception
     *         when an error occurs
     */
    @AfterAll
    public static void teardown() throws Exception {
        try {
            if (null != connection && !connection.isClosed()) {
                connection.close();
            }

            if (null != logOutputStream) {
                logOutputStream.close();
            }

            if (null != logPrintStream) {
                logPrintStream.close();
            }
        } finally {
            connection = null;
            logOutputStream = null;
            logPrintStream = null;
        }
    }

    /**
     * Invoke logging.
     */
    public static void invokeLogging() {
        Handler handler = null;

        // enable logging to stream by default for tests
        String enableLogging = getConfiguredPropertyOrEnv(Constants.MSSQL_JDBC_LOGGING, Boolean.TRUE.toString());

        // If logging is not enable then return.
        if (!Boolean.TRUE.toString().equalsIgnoreCase(enableLogging)) {
            return;
        }

        String loggingHandler = getConfiguredPropertyOrEnv(Constants.MSSQL_JDBC_LOGGING_HANDLER,
                Constants.LOGGING_HANDLER_STREAM);

        try {
            if (Constants.LOGGING_HANDLER_CONSOLE.equalsIgnoreCase(loggingHandler)) {
                handler = new ConsoleHandler();
                handler.setFormatter(new SimpleFormatter());
            } else if (Constants.LOGGING_HANDLER_FILE.equalsIgnoreCase(loggingHandler)) {
                handler = new FileHandler(Constants.DEFAULT_DRIVER_LOG);
                handler.setFormatter(new SimpleFormatter());
                System.out.println("Look for Driver.log file in your classpath for detail logs");
            } else if (Constants.LOGGING_HANDLER_STREAM.equalsIgnoreCase(loggingHandler)) {
                logOutputStream = new ByteArrayOutputStream();
                logPrintStream = new PrintStream(logOutputStream);
                handler = new StreamHandler(logPrintStream, new SimpleFormatter());
            }

            if (handler != null) {
                handler.setLevel(Level.FINEST);
                Logger.getLogger(Constants.MSSQL_JDBC_LOGGING_HANDLER).addHandler(handler);
            }

            /*
             * By default, Loggers also send their output to their parent logger. Typically the root Logger is
             * configured with a set of Handlers that essentially act as default handlers for all loggers.
             */
            Logger logger = Logger.getLogger(Constants.MSSQL_JDBC_PACKAGE);
            logger.setLevel(Level.FINEST);
        } catch (Exception e) {
            System.err.println("Could not invoke logging: " + e.getMessage());
        }
    }

    /**
     * Returns if target Server is SQL Azure Database
     * 
     * @return true/false
     */
    public static boolean isSqlAzure() {
        return isSqlAzure;
    }

    /**
     * Returns if target Server is SQL Azure DW
     * 
     * @return true/false
     */
    public static boolean isSqlAzureDW() {
        return isSqlAzureDW;
    }

    /**
     * Returns if target Server is SQL Linux
     *
     * @return true/false
     */
    public static boolean isSqlLinux() {
        return isSqlLinux;
    }

    /**
     * Determines the server's type.
     * 
     * @param con
     *        connection to server
     * @return void
     * @throws SQLException
     */
    private static void isSqlAzureOrAzureDW(Connection con) throws SQLException {
        if (determinedSqlAzureOrSqlServer) {
            return;
        }

        try (Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT CAST(SERVERPROPERTY('EngineEdition') as INT)")) {
            rs.next();
            int engineEdition = rs.getInt(1);
            isSqlAzure = (engineEdition == Constants.ENGINE_EDITION_FOR_SQL_AZURE
                    || engineEdition == Constants.ENGINE_EDITION_FOR_SQL_AZURE_DW);
            isSqlAzureDW = (engineEdition == Constants.ENGINE_EDITION_FOR_SQL_AZURE_DW);
            determinedSqlAzureOrSqlServer = true;
        }
    }

    /**
     * Determines the server's OSF
     *
     * @param con
     * @throws SQLException
     */
    private static void checkSqlOS(Connection con) throws SQLException {
        if (determinedSqlOS) {
            return;
        }

        try (Statement stmt = con.createStatement(); ResultSet rs = stmt.executeQuery("SELECT @@VERSION")) {
            rs.next();
            isSqlLinux = rs.getString(1).contains("Linux");
            determinedSqlOS = true;
        }
    }

    /**
     * Read property from system or config properties file
     * 
     * @param key
     * @return property value
     */
    protected static String getConfiguredProperty(String key) {
        String value = System.getProperty(key);

        if (null == value && null != configProperties) {
            return configProperties.getProperty(key);
        }

        return value;
    }

    /**
     * Read property from system or config properties file or read from env var
     * 
     * @param key
     * @return property value
     */
    private static String getConfiguredPropertyOrEnv(String key) {
        String value = getConfiguredProperty(key);

        if (null == value) {
            return System.getenv(key);
        }

        return value;
    }

    /**
     * Read property from system or config properties file if not set return default value
     * 
     * @param key
     * @return property value or default value
     */
    protected static String getConfiguredProperty(String key, String defaultValue) {
        String value = getConfiguredProperty(key);

        if (null == value) {
            return defaultValue;
        }

        return value;
    }

    /**
     * Read property from system or config properties file or env var if not set return default value
     * 
     * @param key
     * @return property value or default value
     */
    private static String getConfiguredPropertyOrEnv(String key, String defaultValue) {
        String value = getConfiguredProperty(key);

        if (null == value) {
            value = System.getenv(key);
        }

        if (null == value) {
            value = defaultValue;
        }

        return value;
    }
}
