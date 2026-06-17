/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * Callback interface for publishing performance logs.
 *
 * The {@code duration} parameter in {@link #publish(PerformanceActivity, int, long, Exception)}
 * and {@link #publish(PerformanceActivity, int, int, long, Exception)} contains the operation duration
 * in milliseconds by default. To receive nanosecond granularity instead, override
 * {@link #useNanoseconds()} to return {@code true}.
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
    void publish(PerformanceActivity activity, int connectionId, long duration, Exception exception);

    /**
     * Publish performance log for statement-level activities.
     * @param activity        The type of activity being logged.
     * @param connectionId    The ID of the connection.
     * @param statementId     The ID of the statement (if applicable).
     * @param duration        The duration of the operation (milliseconds by default, nanoseconds if
     *                        {@link #useNanoseconds()} returns true).
     * @param exception       An exception, if an error occurred.
     */
    void publish(PerformanceActivity activity, int connectionId, int statementId, long duration, Exception exception);

    /**
     * Publish performance log for connection-level activities with explicit start/end timestamps.
     * The unit of {@code startTime} and {@code endTime} matches {@link #useNanoseconds()}:
     * {@code System.currentTimeMillis()} values when false (default), {@code System.nanoTime()}
     * values when true. The default implementation delegates to
     * {@link #publish(PerformanceActivity, int, long, Exception)} with {@code endTime - startTime}.
     * Override this when you need the exact wall-clock placement of the event (e.g. emitting
     * trace spans), not just its duration.
     *
     * @param activity        The type of activity being logged.
     * @param connectionId    The ID of the connection.
     * @param startTime       Start timestamp captured when the activity began.
     * @param endTime         End timestamp captured when the activity completed.
     * @param exception       An exception, if an error occurred.
     */
    default void publish(PerformanceActivity activity, int connectionId, long startTime, long endTime,
            Exception exception) {
        publish(activity, connectionId, endTime - startTime, exception);
    }

    /**
     * Publish performance log for statement-level activities with explicit start/end timestamps.
     * See {@link #publish(PerformanceActivity, int, long, long, Exception)} for the timestamp unit.
     *
     * @param activity        The type of activity being logged.
     * @param connectionId    The ID of the connection.
     * @param statementId     The ID of the statement.
     * @param startTime       Start timestamp captured when the activity began.
     * @param endTime         End timestamp captured when the activity completed.
     * @param exception       An exception, if an error occurred.
     */
    default void publish(PerformanceActivity activity, int connectionId, int statementId, long startTime, long endTime,
            Exception exception) {
        publish(activity, connectionId, statementId, endTime - startTime, exception);
    }

    /**
     * Indicates whether the callback wants duration values in nanoseconds.
     * Override this method to return {@code true} to receive nanosecond granularity
     * in the {@code duration} parameter of {@link #publish(PerformanceActivity, int, long, Exception)}
     * and {@link #publish(PerformanceActivity, int, int, long, Exception)}.
     * The default is {@code false} (milliseconds).
     *
     * @return true if duration should be reported in nanoseconds, false for milliseconds.
     */
    default boolean useNanoseconds() {
        return false;
    }

    /**
     * Returns the SQL text for the current performance event.
     * Only valid inside a {@link #publish} callback invocation.
     * Returns {@code null} for connection-level activities or when called outside {@code publish()}.
     *
     * @return the user SQL text, or null if not available.
     */
    default String getCurrentUserSql() {
        return PerformanceLog.currentUserSql.get();
    }

    /**
     * Returns the statement type for the current performance event.
     * Only valid inside a {@link #publish} callback invocation.
     * Returns {@code null} for connection-level activities or when called outside {@code publish()}.
     *
     * @return the {@link StatementType}, or null if not applicable.
     */
    default StatementType getCurrentStatementType() {
        return PerformanceLog.currentStatementType.get();
    }

}