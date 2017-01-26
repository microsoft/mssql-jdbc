/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * Integrated Authentication master file. Common items for kerb and JNI auth are in this interface.
 */

abstract class SSPIAuthentication {
    abstract byte[] GenerateClientContext(byte[] pin,
            boolean[] done) throws SQLServerException;

    abstract int ReleaseClientContext() throws SQLServerException;
}
