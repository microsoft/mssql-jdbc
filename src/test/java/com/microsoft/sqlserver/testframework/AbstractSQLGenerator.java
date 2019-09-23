/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework;

import com.microsoft.sqlserver.jdbc.TestUtils;


/**
 * Common methods needed for any implementation for {@link SQLGeneratorIF}
 */
public abstract class AbstractSQLGenerator {// implements ISQLGenerator {

    static String WRAP_IDENTIFIER = Constants.DEFAULT_WRAP_IDENTIFIER;
    static String OPEN_ESCAPE_IDENTIFIER = "[";
    static String CLOSE_ESCAPE_IDENTIFIER = "]";

    /**
     * @return the wrapIdentifier
     */
    public static String getWrapIdentifier() {
        return WRAP_IDENTIFIER;
    }

    /**
     * @param wrapIdentifier
     *        the wrapIdentifier to set
     */
    public static void setWrapIdentifier(String wrapIdentifier) {
        AbstractSQLGenerator.WRAP_IDENTIFIER = wrapIdentifier;
    }

    // TODO: should provide more detail to distinguish between wrap and escape
    // identifier
    /**
     * It will wrap provided string with wrap identifier.
     * 
     * @param name
     * @return
     */
    public String wrapName(String name) {
        StringBuffer wrap = new StringBuffer();
        wrap.append(getWrapIdentifier());
        wrap.append(TestUtils.escapeSingleQuotes(name));
        wrap.append(getWrapIdentifier());
        return wrap.toString();
    }

    /**
     * Variable used to escape the Identifiers
     * 
     * @param openIdentifier
     * @param closeIdentifier
     */
    public static void setEscapeIdentifier(String openIdentifier, String closeIdentifier) {
        AbstractSQLGenerator.OPEN_ESCAPE_IDENTIFIER = openIdentifier;
        AbstractSQLGenerator.CLOSE_ESCAPE_IDENTIFIER = closeIdentifier;
    }

    /**
     * 
     * @param value
     *        to escape
     * @return escaped literal
     */
    public static String escapeIdentifier(String value) {
        return OPEN_ESCAPE_IDENTIFIER + value + CLOSE_ESCAPE_IDENTIFIER;
    }

}
