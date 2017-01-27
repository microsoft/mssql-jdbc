/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework.util;

import java.util.UUID;

/**
 * Utility Class
 */
public class RandomUtil {

    static int _maxIdentifier = 128;	// Max identifier allowed by SQL Server

    static public String getIdentifier(String prefix) {
        return getIdentifier(prefix, _maxIdentifier, true, false);
    }

    public static String getIdentifierForDB(String prefix) {
        return getIdentifierForDB(prefix, _maxIdentifier, true);
    }

    /**
     * 
     * @param prefix
     *            Prefix
     * @param maxLength
     *            max length
     * @param unique
     *            Includes UUID.
     * @param isDatabase
     *            Do you want for db name.
     * @return
     */
    static public String getIdentifier(String prefix,
            int maxLength,
            boolean unique,
            boolean isDatabase) {
        String identifier;
        StringBuilder sb = new StringBuilder();
        sb.append(prefix);
        sb.append("_");
        sb.append("jdbc_");
        sb.append(System.getProperty("user.name"));
        sb.append("_");
        if (unique) {
            // Create UUID.
            sb.append(UUID.randomUUID().toString());
        }

        identifier = sb.toString();

        if (maxLength < identifier.length()) {
            identifier = identifier.substring(0, maxLength);
        }
        return identifier;
    }

    /**
     * Get Identifier for DB Name.
     * 
     * @param prefix
     *            Prefix
     * @param maxLength
     *            max length
     * @param unique
     *            Includes UUID.
     * @return
     */
    public static String getIdentifierForDB(String prefix,
            int maxLength,
            boolean unique) {
        String identifier = getIdentifier(prefix, maxLength, unique, true);
        return removeInvalidDBChars(identifier);
    }

    /**
     * 
     * @param prefix
     *            Prefix
     * @param maxLength
     *            max length
     * @param isDatabase
     *            Do you want for db name.
     * @return
     */
    public static String getUniqueIdentifier(String prefix,
            int maxLength,
            boolean isDatabase) {
        return getIdentifier(prefix, maxLength, true, isDatabase);
    }

    /**
     * Remove Invalid DB Chars.
     * 
     * @param s
     * @return
     */
    private static String removeInvalidDBChars(String s) {
        return s.replaceAll("[:-]", "_");
    }

}
