/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;


/**
 * The TDS default implementation of a timeout command
 */
class TDSTimeoutTask implements Runnable {
    private static final AtomicLong COUNTER = new AtomicLong(0);

    private final UUID connectionId;
    private final TDSCommand command;
    private final SQLServerConnection sqlServerConnection;

    public TDSTimeoutTask(TDSCommand command, SQLServerConnection sqlServerConnection) {
        this.connectionId = sqlServerConnection == null ? null : sqlServerConnection.getClientConIdInternal();
        this.command = command;
        this.sqlServerConnection = sqlServerConnection;
    }

    @Override
    public final void run() {
        // Create a new thread to run the interrupt to ensure that blocking operations performed
        // by the interrupt do not hang the primary timer thread.
        String name = "mssql-timeout-task-" + COUNTER.incrementAndGet() + "-" + connectionId;
        Thread thread = new Thread(this::interrupt, name);
        thread.setDaemon(true);
        thread.start();
    }

    protected void interrupt() {
        try {
            // If TCP Connection to server is silently dropped, exceeding the query timeout
            // on the same connection does not throw SQLTimeoutException
            // The application stops responding instead until SocketTimeoutException is
            // thrown. In this case, we must manually terminate the connection.
            if (null == command) {
                if (null != sqlServerConnection) {
                    sqlServerConnection.terminate(SQLServerException.DRIVER_ERROR_IO_FAILED,
                            SQLServerException.getErrString("R_connectionIsClosed"));
                }
            } else {
                // If the timer wasn't canceled before it ran out of
                // time then interrupt the registered command.
                command.interrupt(SQLServerException.getErrString("R_queryTimedOut"));
            }
        } catch (SQLServerException e) {
            // Unfortunately, there's nothing we can do if we fail to time out the request. There
            // is no way to report back what happened.
            assert null != command;
            command.log(Level.WARNING, "Command could not be timed out. Reason: " + e.getMessage());
        }
    }
}
