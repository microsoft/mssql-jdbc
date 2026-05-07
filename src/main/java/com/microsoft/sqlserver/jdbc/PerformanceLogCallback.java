/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * Callback interface for publishing performance logs.
 *
 * <p>The {@code duration} parameter in {@link #publish} contains the operation duration
 * in milliseconds by default. To receive nanosecond granularity instead, override
 * {@link #useNanoseconds()} to return {@code true}.</p>
 */
public interface PerformanceLogCallback {

    /**
     * Publish performance log for connection-level activities.
     * @param activity        The type of activity being logged.
     * @param connectionId    The ID of the connection.
     * @param duration        The duration of the operation (milliseconds by default, nanoseconds if
     *                        {@link #useNanoseconds()} returns true).
     * @param exception       An exception, if an error occurred.
     */
    void publish(PerformanceActivity activity, int connectionId, long duration, Exception exception) throws Exception;

    /**
     * Publish performance log for statement-level activities.
     * @param activity        The type of activity being logged.
     * @param connectionId    The ID of the connection.
     * @param statementId     The ID of the statement (if applicable).
     * @param duration        The duration of the operation (milliseconds by default, nanoseconds if
     *                        {@link #useNanoseconds()} returns true).
     * @param exception       An exception, if an error occurred.
     */
    void publish(PerformanceActivity activity, int connectionId, int statementId, long duration, Exception exception) throws Exception;

    /**
     * Indicates whether the callback wants duration values in nanoseconds.
     * Override this method to return {@code true} to receive nanosecond granularity
     * in the {@code duration} parameter of {@link #publish}.
     * The default is {@code false} (milliseconds).
     *
     * @return true if duration should be reported in nanoseconds, false for milliseconds.
     */
    default boolean useNanoseconds() {
        return false;
    }

}