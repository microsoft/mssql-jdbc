/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * Used for caching of meta data from parsed SQL text.
 */
final class ParsedSQLCacheItem {
    /** The SQL text AFTER processing. */
    String processedSQL;
    int parameterCount; 
    String procedureName;
    boolean bReturnValueSyntax; 
    
    ParsedSQLCacheItem(String processedSQL, int parameterCount, String procedureName, boolean bReturnValueSyntax) {
        this.processedSQL = processedSQL;
        this.parameterCount = parameterCount;
        this.procedureName = procedureName;
        this.bReturnValueSyntax = bReturnValueSyntax;
    }
}

