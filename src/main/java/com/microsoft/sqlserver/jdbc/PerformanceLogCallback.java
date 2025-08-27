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
     *
     * @param durationMs      The duration of the operation in milliseconds.
     * @param activity        The type of activity being logged.
     * @param exceptionMessage An exception message, if an error occurred.
     */
    void publish(long durationMs, PerformanceActivity activity, String exceptionMessage);

}