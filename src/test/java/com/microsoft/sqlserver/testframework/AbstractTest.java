/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) 2016 Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework;

import java.sql.Connection;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;

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

    protected static Logger logger = Logger.getLogger("AbstractTest");

    protected static String secretstrJks = "changeit";

    protected static String applicationClientID = null;
    protected static String applicationKey = null;
    protected static String[] keyIDs = null;

    protected static String[] jksPaths = null;
    protected static String[] javaKeyAliases = null;
    protected static String windowsKeyPath = null;

    protected static SQLServerConnection connection = null;
    protected static Connection connectionAzure = null;

    protected static String connectionString = null;

    protected static Properties info = new Properties();

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
        logger.info("In AbstractTest:setup");

        try {
            Assertions.assertNotNull(connectionString, "Connection String should not be null");
            connection = PrepUtil.getConnection(connectionString, info);

        }
        catch (Exception e) {
            throw e;
        }
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
        }
        catch (Exception e) {
            connection.close();
        }
        finally {
            connection = null;
        }
    }

    /**
     * Read variable from property files if found null try to read from env.
     * 
     * @param key
     * @return Value
     */
    public static String getConfiguredProperty(String key) {
        return Utils.getConfiguredProperty(key);
    }

    /**
     * Convenient method for {@link #getConfiguredProperty(String)}
     * 
     * @param key
     * @return Value
     */
    public static String getConfiguredProperty(String key,
            String defaultValue) {
        return Utils.getConfiguredProperty(key, defaultValue);
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
            }
            else if ("file".equalsIgnoreCase(loggingHandler)) {
                handler = new FileHandler("Driver.log");
                System.out.println("Look for Driver.log file in your classpath for detail logs");
            }

            if (handler != null) {
                handler.setFormatter(new SimpleFormatter());
                handler.setLevel(Level.FINEST);
                Logger.getLogger("").addHandler(handler);
            }
            // By default, Loggers also send their output to their parent logger.  
            // Typically the root Logger is configured with a set of Handlers that essentially act as default handlers for all loggers. 
            Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc");
            logger.setLevel(Level.FINEST);
        }
        catch (Exception e) {
            System.err.println("Some how could not invoke logging: " + e.getMessage());
        }
    }

}
