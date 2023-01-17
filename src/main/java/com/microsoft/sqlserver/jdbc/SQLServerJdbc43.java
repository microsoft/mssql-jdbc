/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketOption;
import java.sql.BatchUpdateException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;


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

    @SuppressWarnings("unchecked")
    static void setSocketOptions(Socket tcpSocket, TDSChannel channel) throws IOException {
        Method setOptionMethod = null;
        SocketOption<Integer> keepIdleOption = null;
        SocketOption<Integer> keepIntervalOption = null;

        try {
            setOptionMethod = Socket.class.getMethod("setOption", SocketOption.class, Object.class);
            Class<?> clazz = Class.forName("jdk.net.ExtendedSocketOptions");
            keepIdleOption = (SocketOption<Integer>) clazz.getDeclaredField("TCP_KEEPIDLE").get(null);
            keepIntervalOption = (SocketOption<Integer>) clazz.getDeclaredField("TCP_KEEPINTERVAL").get(null);

            if (setOptionMethod != null && keepIdleOption != null && keepIntervalOption != null) {
                if (logger.isLoggable(Level.FINER)) {
                    logger.finer(channel.toString() + ": Setting KeepAlive extended socket options.");
                }

                setOptionMethod.invoke(tcpSocket, keepIdleOption, 30); // 30 seconds
                setOptionMethod.invoke(tcpSocket, keepIntervalOption, 1); // 1 second

            } else if (logger.isLoggable(Level.FINER)) {
                logger.finer(
                        channel.toString() + ": KeepAlive extended socket options not supported on this platform.");
            }
        } catch (ClassNotFoundException | NoSuchMethodException | NoSuchFieldException | IllegalAccessException
                | InvocationTargetException e) {
            if (logger.isLoggable(Level.FINER)) {
                logger.finer(
                        channel.toString() + ": KeepAlive extended socket options not supported on this platform.");
            }
        }
    }

}
