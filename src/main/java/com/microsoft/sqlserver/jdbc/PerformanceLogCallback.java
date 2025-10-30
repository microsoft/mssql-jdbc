/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * Callback interface for publishing performance logs.
 */
public interface PerformanceLogCallback {

    /**
     * Publish a performance log entry.
     * @param activity        The type of activity being logged.
     * @param connectionId    The ID of the connection.
     * @param durationMs      The duration of the operation in milliseconds.
     * @param exception       An exception, if an error occurred.
     */
    void publish(PerformanceActivity activity, int connectionId, long durationMs, Exception exception) throws Exception;

}