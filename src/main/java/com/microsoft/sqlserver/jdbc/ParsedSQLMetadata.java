/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * Used for caching of meta data from parsed SQL text.
 */
final class ParsedSQLCacheItem {
    /** The SQL text AFTER processing. */
    String processedSQL;
    int[] parameterPositions;
    String procedureName;
    boolean bReturnValueSyntax;
    boolean callEscape;
    boolean execEscape;
    boolean embeddedParam;

    ParsedSQLCacheItem(String processedSQL, int[] parameterPositions, String procedureName,
            boolean bReturnValueSyntax, boolean callEscape, boolean execEscape, boolean embeddedParam) {
        this.processedSQL = processedSQL;
        this.parameterPositions = parameterPositions;
        this.procedureName = procedureName;
        this.bReturnValueSyntax = bReturnValueSyntax;
        this.callEscape = callEscape;
        this.execEscape = execEscape;
        this.embeddedParam = embeddedParam;
    }
}
