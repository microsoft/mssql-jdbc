/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation
 * All rights reserved.
 * 
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.util;

/**
 * Common Code which we can use across driver code.
 */
public class Utils {

    /**
     * Read variable from property files if found null try to read from env.
     * 
     * @param key
     * @return Value
     */
    public static String getSystemEnvOrProperty(String key) {
        String value = System.getProperty(key);

        if (value == null) {
            value = System.getenv(key);
        }

        return value;
    }

    /**
     * Convenient method for {@link #getSystemEnvOrProperty(String)}
     * 
     * @param key           Key
     * @param defaultValue  Default value
     * @return              Return value for configured propery.
     */
    public static String getSystemEnvOrProperty(String key,
            String defaultValue) {
        String value = getSystemEnvOrProperty(key);

        if (value == null) {
            value = defaultValue;
        }

        return value;
    }
}
