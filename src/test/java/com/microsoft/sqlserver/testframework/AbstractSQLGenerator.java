/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework;

/**
 * Common methods needed for any implementation for {@link SQLGeneratorIF}
 */
public abstract class AbstractSQLGenerator {// implements ISQLGenerator {

    protected static final String CREATE_TABLE = "CREATE TABLE";
    protected static final String SPACE_CHAR = " ";
    protected static final String OPEN_BRACKET = "(";
    protected static final String CLOSE_BRACKET = ")";
    protected static final String NOT = "NOT";
    protected static final String NULL = "NULL";
    protected static final String PRIMARY_KEY = "PRIMARY KEY";
    protected static final String DEFAULT = "DEFAULT";
    protected static final String COMMA = ",";
    protected static final String QUESTION_MARK = "?";

    // FIXME: Find good word for '. Better replaced by wrapIdentifier.
    protected static final String TICK = "'";

    protected static final String defaultWrapIdentifier = "\'";

    protected static String wrapIdentifier = defaultWrapIdentifier;

    protected static String openEscapeIdentifier = "[";

    protected static String closeEscapeIdentifier = "]";

    /**
     * @return the wrapIdentifier
     */
    public static String getWrapIdentifier() {
        return wrapIdentifier;
    }

    /**
     * @param wrapIdentifier
     *            the wrapIdentifier to set
     */
    public static void setWrapIdentifier(String wrapIdentifier) {
        AbstractSQLGenerator.wrapIdentifier = wrapIdentifier;
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
        wrap.append(name);
        wrap.append(getWrapIdentifier());
        return wrap.toString();
    }

    /**
     * Variable used to escape the Identifiers
     * 
     * @param openIdentifier
     * @param closeIdentifier
     */
    public static void setEscapeIdentifier(String openIdentifier,
            String closeIdentifier) {
        AbstractSQLGenerator.openEscapeIdentifier = openIdentifier;
        AbstractSQLGenerator.closeEscapeIdentifier = closeIdentifier;
    }

    /**
     * 
     * @param value
     *            to escape
     * @return escaped literal
     */
    public static String escapeIdentifier(String value) {
        return openEscapeIdentifier + value + closeEscapeIdentifier;
    }

}
