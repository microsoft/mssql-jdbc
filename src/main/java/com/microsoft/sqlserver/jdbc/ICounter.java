/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * Interface for MaxResultBufferCounter
 */
public interface ICounter {

    /**
     * Increases the state of Counter
     *
     * @param bytes
     *        Number of bytes to increase state
     * @throws SQLServerException
     *         Exception is thrown, when limit of Counter is exceeded
     */
    void increaseCounter(long bytes) throws SQLServerException;

    /**
     * Resets the state of Counter
     */
    void resetCounter();
}
