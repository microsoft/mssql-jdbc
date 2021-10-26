/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * Abstract implementation of a command that can be timed out using the {@link TimeoutPoller}
 */
abstract class TimeoutCommand<T> {
    private final long startTime;
    private final int timeout;
    private final T command;
    private final SQLServerConnection sqlServerConnection;

    TimeoutCommand(int timeout, T command, SQLServerConnection sqlServerConnection) {
        this.timeout = timeout;
        this.command = command;
        this.sqlServerConnection = sqlServerConnection;
        this.startTime = System.currentTimeMillis();
    }

    protected boolean canTimeout() {
        long currentTime = System.currentTimeMillis();
        return ((currentTime - startTime) / 1000) >= timeout;
    }

    protected T getCommand() {
        return command;
    }

    protected SQLServerConnection getSqlServerConnection() {
        return sqlServerConnection;
    }

    /**
     * The implementation for interrupting this timeout command
     */
    protected abstract void interrupt();
}
