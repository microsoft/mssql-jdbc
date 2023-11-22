/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.BatchUpdateException;


/**
 * Shims for JDBC 4.2 JAR.
 *
 * JDBC 4.2 public methods should always check the SQLServerJdbcVersion first to make sure that they are not operable in
 * any earlier driver version. That is, they should throw an exception, be a no-op, or whatever.
 */

final class DriverJDBCVersion {
    // The 4.2 driver is compliant to JDBC 4.2.
    static final int MAJOR = 4;
    static final int MINOR = 2;

    private DriverJDBCVersion() {
        throw new UnsupportedOperationException(SQLServerException.getErrString("R_notSupported"));
    }

    static final boolean checkSupportsJDBC43() {
        return false;
    }

    static final void throwBatchUpdateException(SQLServerException lastError,
            long[] updateCounts) throws BatchUpdateException {
        throw new BatchUpdateException(lastError.getMessage(), lastError.getSQLState(), lastError.getErrorCode(),
                updateCounts, new Throwable(lastError.getMessage()));
    }

    static SQLServerConnection getSQLServerConnection(String parentInfo) throws SQLServerException {
        return new SQLServerConnection(parentInfo);
    }

    /** Client process ID sent during login */
    private static int pid = 0;

    static int getProcessId() {
        return pid;
    }
}
