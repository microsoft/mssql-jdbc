package com.microsoft.sqlserver.jdbc;

/**
 * Abstract implementation of a command that can be timed out using the {@link TimeoutPoller}
 */
public abstract class TimeoutCommand<T> {
    private final long startTime;
    private final int timeout;
    private final T command;
    private final SQLServerConnection sqlServerConnection;

    public TimeoutCommand(int timeout, T command, SQLServerConnection sqlServerConnection) {
        this.timeout = timeout;
        this.command = command;
        this.sqlServerConnection = sqlServerConnection;
        this.startTime = System.currentTimeMillis();
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

    /**
     * The implementation for interrupting this timeout command
     */
    public abstract void interrupt();
}
