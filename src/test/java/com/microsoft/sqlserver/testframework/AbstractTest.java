/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) 2016 Microsoft Corporation All rights reserved. This program is
 * made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;

import com.microsoft.sqlserver.jdbc.ISQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.microsoft.sqlserver.jdbc.SQLServerXADataSource;
import com.microsoft.sqlserver.jdbc.TestUtils;


/**
 * Think about following things:
 * <li>Connection pool
 * <li>Configured Property file instead of passing from args.
 * <li>Think of different property files for different settings. / flag etc.
 * <Li>Think about what kind of logging we are going use it. <B>util.logging<B> will be preference.
 * 
 * @since 6.1.2
 */
public abstract class AbstractTest {

    protected static final Logger logger = Logger.getLogger("AbstractTest");
    protected static final ThreadLocalRandom random = ThreadLocalRandom.current();
    protected static final String secretstrJks = "changeit";

    protected static String applicationClientID = null;
    protected static String applicationKey = null;
    protected static String[] keyIDs = null;

    protected static String[] jksPaths = null;
    protected static String[] javaKeyAliases = null;
    protected static String windowsKeyPath = null;

    protected static SQLServerConnection connection = null;
    protected static ISQLServerDataSource ds = null;
    protected static ISQLServerDataSource dsXA = null;
    protected static ISQLServerDataSource dsPool = null;

    protected static Connection connectionAzure = null;

    protected static String connectionString = null;

    protected static Properties info = new Properties();

    private final static int ENGINE_EDITION_FOR_SQL_AZURE = 5;
    private final static int ENGINE_EDITION_FOR_SQL_AZURE_DW = 6;
    private static boolean _determinedSqlAzureOrSqlServer = false;
    private static boolean _isSqlAzure = false;
    private static boolean _isSqlAzureDW = false;

    /**
     * This will take care of all initialization before running the Test Suite.
     * 
     * @throws Exception
     */
    @BeforeAll
    public static void setup() throws Exception {
        // Invoke fine logging...
        invokeLogging();

        applicationClientID = getConfiguredProperty("applicationClientID");
        applicationKey = getConfiguredProperty("applicationKey");
        keyIDs = getConfiguredProperty("keyID", "").split(";");

        connectionString = getConfiguredProperty("mssql_jdbc_test_connection_properties");
        ds = updateDataSource(new SQLServerDataSource());
        dsXA = updateDataSource(new SQLServerXADataSource());
        dsPool = updateDataSource(new SQLServerConnectionPoolDataSource());

        jksPaths = getConfiguredProperty("jksPaths", "").split(";");
        javaKeyAliases = getConfiguredProperty("javaKeyAliases", "").split(";");
        windowsKeyPath = getConfiguredProperty("windowsKeyPath");

        // info.setProperty("ColumnEncryptionSetting", "Enabled"); // May be we
        // can use parameterized way to change this value
        if (!jksPaths[0].isEmpty()) {
            info.setProperty("keyStoreAuthentication", "JavaKeyStorePassword");
            info.setProperty("keyStoreLocation", jksPaths[0]);
            info.setProperty("keyStoreSecret", secretstrJks);
        }

        try {
            Assertions.assertNotNull(connectionString, "Connection String should not be null");
            connection = PrepUtil.getConnection(connectionString, info);
            isSqlAzureOrAzureDW(connection);
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * Covers only connection properties required for testing. Does not cover all connection properties - add more
     * properties if needed.
     * 
     * @param ds
     *        DataSource to be configured
     * @return ISQLServerDataSource
     */
    private static ISQLServerDataSource updateDataSource(ISQLServerDataSource ds) {
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

    static String extractPort(String server, ISQLServerDataSource ds) {
        if (server.contains(":")) {
            ds.setPortNumber(Integer.parseInt(server.substring(server.indexOf(":") + 1)));
            server = server.substring(0, server.indexOf(":"));
        }
        return server;
    }

    /**
     * Get the connection String
     * 
     * @return
     */
    public static String getConnectionString() {
        return connectionString;
    }

    /**
     * This will take care of all clean ups after running the Test Suite.
     * 
     * @throws Exception
     */
    @AfterAll
    public static void teardown() throws Exception {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (Exception e) {
            connection.close();
        } finally {
            connection = null;
        }
    }

    @BeforeAll
    public static void registerDriver() throws Exception {
        SQLServerDriver.register();
    }

    /**
     * Read variable from property files if found null try to read from env.
     * 
     * @param key
     * @return Value
     */
    public static String getConfiguredProperty(String key) {
        return TestUtils.getConfiguredProperty(key);
    }

    /**
     * Convenient method for {@link #getConfiguredProperty(String)}
     * 
     * @param key
     * @return Value
     */
    public static String getConfiguredProperty(String key, String defaultValue) {
        return TestUtils.getConfiguredProperty(key, defaultValue);
    }

    /**
     * Invoke logging.
     */
    public static void invokeLogging() {
        Handler handler = null;

        String enableLogging = getConfiguredProperty("mssql_jdbc_logging", "false");

        // If logging is not enable then return.
        if (!"true".equalsIgnoreCase(enableLogging)) {
            return;
        }

        String loggingHandler = getConfiguredProperty("mssql_jdbc_logging_handler", "not_configured");

        try {
            // handler = new FileHandler("Driver.log");
            if ("console".equalsIgnoreCase(loggingHandler)) {
                handler = new ConsoleHandler();
            } else if ("file".equalsIgnoreCase(loggingHandler)) {
                handler = new FileHandler("Driver.log");
                System.out.println("Look for Driver.log file in your classpath for detail logs");
            }

            if (handler != null) {
                handler.setFormatter(new SimpleFormatter());
                handler.setLevel(Level.FINEST);
                Logger.getLogger("").addHandler(handler);
            }
            // By default, Loggers also send their output to their parent logger.
            // Typically the root Logger is configured with a set of Handlers that essentially act as default handlers
            // for all loggers.
            Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc");
            logger.setLevel(Level.FINEST);
        } catch (Exception e) {
            System.err.println("Some how could not invoke logging: " + e.getMessage());
        }
    }

    public static boolean isSqlAzure() {
        return _isSqlAzure;
    }

    public static boolean isSqlAzureDW() {
        return _isSqlAzureDW;
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
        if (_determinedSqlAzureOrSqlServer) {
            return;
        }

        try (Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT CAST(SERVERPROPERTY('EngineEdition') as INT)")) {
            rs.next();
            int engineEdition = rs.getInt(1);
            _isSqlAzure = (engineEdition == ENGINE_EDITION_FOR_SQL_AZURE
                    || engineEdition == ENGINE_EDITION_FOR_SQL_AZURE_DW);
            _isSqlAzureDW = (engineEdition == ENGINE_EDITION_FOR_SQL_AZURE_DW);
            _determinedSqlAzureOrSqlServer = true;
        }
    }
}
