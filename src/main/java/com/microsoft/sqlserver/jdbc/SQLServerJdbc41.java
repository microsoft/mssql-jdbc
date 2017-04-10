/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * Shims for JDBC 4.1 JAR.
 *
 * JDBC 4.1 public methods should always check the SQLServerJdbcVersion first to make sure that they are not operable in any earlier driver version.
 * That is, they should throw an exception, be a no-op, or whatever.
 */

final class DriverJDBCVersion {
    static final int major = 4;
    static final int minor = 1;

    static final void checkSupportsJDBC42() {
        throw new UnsupportedOperationException(SQLServerException.getErrString("R_notSupported"));
    }

    // Stub for the new overloaded method in BatchUpdateException in JDBC 4.2
    static final void throwBatchUpdateException(SQLServerException lastError,
            long[] updateCounts) {
        throw new UnsupportedOperationException(SQLServerException.getErrString("R_notSupported"));
    }
}
