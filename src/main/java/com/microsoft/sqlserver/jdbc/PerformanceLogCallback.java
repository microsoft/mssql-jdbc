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
     * Publishes the START and END boundaries of an accurately bounded connection activity, independently of JUL
     * logging level. Use {@link PerformanceLogEvent#getType()} to distinguish the boundaries. The same callback
     * instance receives both, even if registration changes. Broad legacy LOGIN and TOKEN_ACQUISITION wrappers and
     * statement activities do not invoke this overload.
     *
     * START has zero end time and duration and no failure. END is published once, including for successful activities.
     * Export filtering and buffering belong to the callback. Times are always nanoseconds, regardless of
     * {@link #useNanoseconds()}.
     * Only the originating failure scope carries detailed error attributes; enclosing failed scopes carry a summary.
     * The default implementation is a no-op for backward compatibility. This overload does not change the legacy
     * close-time {@code publish} calls or their duration units.
     *
     * @param event
     *        immutable boundary snapshot; the exception is an in-process diagnostic and must not be exported raw
     * @throws Exception
     *         if the callback fails; the driver isolates callback failures from SQL operations and still performs
     *         legacy publication and scope cleanup
     */
    default void publish(PerformanceLogEvent event) throws Exception {}

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

    /**
     * Returns the application name of the connection associated with the current performance event.
     * This is the value of the {@code applicationName} connection property, which defaults to
     * "Microsoft JDBC Driver for SQL Server" when not set. Applications using a connection pool can
     * set this property on the pool's data source to identify which pool the event originated from.
     * Unlike {@link #getCurrentUserSql()}, this value is available for both connection-level and
     * statement-level activities.
     * Only valid inside a {@link #publish} callback invocation.
     * Returns {@code null} when called outside {@code publish()}, or when the connection properties
     * have not been parsed yet (for example, a connection that fails before the property is resolved).
     *
     * @return the application name, or null if not available.
     */
    default String getCurrentApplicationName() {
        return PerformanceLog.currentApplicationName.get();
    }

}