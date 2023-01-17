/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketOption;
import java.sql.BatchUpdateException;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import jdk.net.ExtendedSocketOptions;


/**
 * Shims for JDBC 4.3 JAR.
 *
 * JDBC 4.3 public methods should always check the SQLServerJdbcVersion first to make sure that they are not operable in
 * any earlier driver version. That is, they should throw an exception, be a no-op, or whatever.
 */

final class DriverJDBCVersion {
    // The 4.3 driver is compliant to JDBC 4.3.
    static final int MAJOR = 4;
    static final int MINOR = 3;

    private DriverJDBCVersion() {
        throw new UnsupportedOperationException(SQLServerException.getErrString("R_notSupported"));
    }

    private static final Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.DriverJDBCVersion");

    static final boolean checkSupportsJDBC43() {
        return true;
    }

    static final void throwBatchUpdateException(SQLServerException lastError,
            long[] updateCounts) throws BatchUpdateException {
        throw new BatchUpdateException(lastError.getMessage(), lastError.getSQLState(), lastError.getErrorCode(),
                updateCounts, new Throwable(lastError.getMessage()));
    }

    private static double jvmVersion = Double.parseDouble(Util.SYSTEM_SPEC_VERSION);

    static SQLServerConnection getSQLServerConnection(String parentInfo) throws SQLServerException {
        return jvmVersion >= 9 ? new SQLServerConnection43(parentInfo) : new SQLServerConnection(parentInfo);
    }

    /** Client process ID sent during login */
    private static int pid = 0;

    static {
        long pidLong = 0;
        try {
            pidLong = ProcessHandle.current().pid();
        } catch (NoClassDefFoundError e) { // ProcessHandle is Java 9+
        }
        pid = (pidLong > Integer.MAX_VALUE) ? 0 : (int) pidLong;
    }

    static int getProcessId() {
        return pid;
    }

    static void setSocketOptions(Socket tcpSocket, TDSChannel channel) throws IOException {
        Set<SocketOption<?>> options = tcpSocket.supportedOptions();
        if (options.contains(ExtendedSocketOptions.TCP_KEEPIDLE)
                && options.contains(ExtendedSocketOptions.TCP_KEEPINTERVAL)) {
            if (logger.isLoggable(Level.FINER)) {
                logger.finer(channel.toString() + ": Setting KeepAlive extended socket options.");
            }
            tcpSocket.setOption(ExtendedSocketOptions.TCP_KEEPIDLE, 30); // 30 seconds
            tcpSocket.setOption(ExtendedSocketOptions.TCP_KEEPINTERVAL, 1); // 1 second
        } else if (logger.isLoggable(Level.FINER)) {
            logger.finer(channel.toString() + ": KeepAlive extended socket options not supported on this platform.");
        }
    }
}
