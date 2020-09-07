package com.microsoft.sqlserver.jdbc;

public interface Counter {

    void increaseCounter(long bytes) throws SQLServerException;

    void resetCounter();
}
