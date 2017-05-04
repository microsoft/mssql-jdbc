/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import org.apache.commons.codec.digest.DigestUtils;
import java.nio.ByteBuffer;

/**
 * Used to keep track of parsed SQL text and its properties for prepared statements.
 */
final class ParsedSQLCacheItem {
    /** The SQL text AFTER processing. */
    String preparedSQLText;
    int parameterCount; 
    String procedureName;
    boolean bReturnValueSyntax; 
    
    ParsedSQLCacheItem(String preparedSQLText, int parameterCount, String procedureName, boolean bReturnValueSyntax) {
        this.preparedSQLText = preparedSQLText;
        this.parameterCount = parameterCount;
        this.procedureName = procedureName;
        this.bReturnValueSyntax = bReturnValueSyntax;
    }

    static ByteBuffer generateHash(String originalKey) {
        try {
            if(null == originalKey)
                return null;
            else 
                return ByteBuffer.wrap(DigestUtils.sha1(originalKey));
        }
        catch(Exception e) {
            return null;
        }
    }
}

