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
 *
 * <p>To receive the application name for each event, override the overloaded variants
 * {@link #publish(PerformanceActivity, int, String, long, Exception)} and
 * {@link #publish(PerformanceActivity, int, String, int, long, Exception)} instead.
 * The default implementations of those overloads delegate to the original abstract methods,
 * so existing implementations continue to work without modification.</p>
 */
public interface PerformanceLogCallback {

    /**
     * Publish performance log for connection-level activities.
     *
     * @param activity     The type of activity being logged.
     * @param connectionId The ID of the connection.
     * @param duration     The duration of the operation (milliseconds by default,
     *                     nanoseconds if
     *                     {@link #useNanoseconds()} returns true).
     * @param exception    An exception, if an error occurred.
     * @throws Exception if the callback cannot publish the performance event.
     */
    void publish(PerformanceActivity activity, int connectionId, long duration, Exception exception) throws Exception;

    /**
     * Publish performance log for statement-level activities.
     *
     * @param activity     The type of activity being logged.
     * @param connectionId The ID of the connection.
     * @param statementId  The ID of the statement (if applicable).
     * @param duration     The duration of the operation (milliseconds by default,
     *                     nanoseconds if
     *                     {@link #useNanoseconds()} returns true).
     * @param exception    An exception, if an error occurred.
     * @throws Exception if the callback cannot publish the performance event.
     */
    void publish(PerformanceActivity activity, int connectionId, int statementId, long duration, Exception exception) throws Exception;

    /**
     * Publish performance log for connection-level activities, including the application name.
     *
     * <p>Override this method to receive the {@code applicationName} connection property alongside
     * the other event fields. The default implementation delegates to
     * {@link #publish(PerformanceActivity, int, long, Exception)}, so existing implementations
     * that only override the original method continue to work without change.</p>
     *
     * @param activity        The type of activity being logged.
     * @param connectionId    The ID of the connection.
     * @param applicationName The application name from the connection string, or {@code null} if
     *                        not available.
     * @param duration        The duration of the operation (milliseconds by default,
     *                        nanoseconds if {@link #useNanoseconds()} returns true).
     * @param exception       An exception, if an error occurred.
     * @throws Exception if the callback cannot publish the performance event.
     */
    default void publish(PerformanceActivity activity, int connectionId, String applicationName,
            long duration, Exception exception) throws Exception {
        publish(activity, connectionId, duration, exception);
    }

    /**
     * Publish performance log for statement-level activities, including the application name.
     *
     * <p>Override this method to receive the {@code applicationName} connection property alongside
     * the other event fields. The default implementation delegates to
     * {@link #publish(PerformanceActivity, int, int, long, Exception)}, so existing implementations
     * that only override the original method continue to work without change.</p>
     *
     * @param activity        The type of activity being logged.
     * @param connectionId    The ID of the connection.
     * @param applicationName The application name from the connection string, or {@code null} if
     *                        not available.
     * @param statementId     The ID of the statement (if applicable).
     * @param duration        The duration of the operation (milliseconds by default,
     *                        nanoseconds if {@link #useNanoseconds()} returns true).
     * @param exception       An exception, if an error occurred.
     * @throws Exception if the callback cannot publish the performance event.
     */
    default void publish(PerformanceActivity activity, int connectionId, String applicationName,
            int statementId, long duration, Exception exception) throws Exception {
        publish(activity, connectionId, statementId, duration, exception);
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