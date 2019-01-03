/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Abstract implementation of a command that can be timed out using the {@link SQLServerTimeoutManager}
 */
abstract class TimeoutCommand<T> {
    private final long startTime;
    private final int timeout;
    private final T command;
    private final SQLServerConnection sqlServerConnection;
    private ScheduledFuture<?> timeoutTask;
    static AtomicLong uniqueId = new AtomicLong();
    private final long id;

    TimeoutCommand(int timeout, T command, SQLServerConnection sqlServerConnection) {
        this.timeout = timeout;
        this.command = command;
        this.sqlServerConnection = sqlServerConnection;
        this.startTime = System.currentTimeMillis();
        this.id = uniqueId.getAndIncrement();
    }

    public boolean canTimeout() {
        long currentTime = System.currentTimeMillis();
        return ((currentTime - startTime) / 1000) >= timeout;
    }

    public T getCommand() {
        return command;
    }

    public SQLServerConnection getSqlServerConnection() {
        return sqlServerConnection;
    }

    public int getTimeout() {
        return timeout;
    }

    public boolean isTimeoutTaskComplete() {
        return this.timeoutTask.isCancelled() || this.timeoutTask.isDone();
    }

    public void cancelTimeoutTask() {
        this.timeoutTask.cancel(true);
    }

    public void setTimeoutTask(ScheduledFuture<?> timeoutTask) {
        this.timeoutTask = timeoutTask;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (id ^ (id >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TimeoutCommand other = (TimeoutCommand) obj;
        if (id != other.id)
            return false;
        return true;
    }

    /**
     * The implementation for interrupting this timeout command
     */
    public abstract void interrupt();
}
