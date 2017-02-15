/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Generic Utility class which we can access by test classes.
 * 
 * @since 6.1.2
 */
public class Utils {
    public static final Logger log = Logger.getLogger("Utils");

    // 'SQL' represents SQL Server, while 'SQLAzure' represents SQL Azure.
    public static final String SERVER_TYPE_SQL_SERVER = "SQL";
    public static final String SERVER_TYPE_SQL_AZURE = "SQLAzure";

    /**
     * Returns serverType
     * @return
     */
    public static String getServerType() {
        String serverType = null;

        String serverTypeProperty = getConfiguredProperty("server.type");
        if (null == serverTypeProperty) {
            // default to SQL Server
            serverType = SERVER_TYPE_SQL_SERVER;
        }
        else if (serverTypeProperty.equalsIgnoreCase(SERVER_TYPE_SQL_AZURE)) {
            serverType = SERVER_TYPE_SQL_AZURE;
        }
        else if (serverTypeProperty.equalsIgnoreCase(SERVER_TYPE_SQL_SERVER)) {
            serverType = SERVER_TYPE_SQL_SERVER;
        }
        else {
            if (log.isLoggable(Level.FINE)) {
                log.fine("Server.type '" + serverTypeProperty + "' is not supported yet. Default to SQL Server");
            }
            serverType = SERVER_TYPE_SQL_SERVER;
        }
        return serverType;
    }

    /**
     * Read variable from property files if found null try to read from env.
     * 
     * @param key
     * @return Value
     */
    public static String getConfiguredProperty(String key) {
        String value = System.getProperty(key);

        if (value == null) {
            value = System.getenv(key);
        }

        return value;
    }

    /**
     * Convenient method for {@link #getConfiguredProperty(String)}
     * 
     * @param key
     * @return Value
     */
    public static String getConfiguredProperty(String key,
            String defaultValue) {
        String value = getConfiguredProperty(key);

        if (value == null) {
            value = defaultValue;
        }

        return value;
    }
}
