/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import java.util.Properties;


/**
 * Optional service-provider interface for automatic, connection-specific user telemetry. Implementations are discovered
 * using {@link java.util.ServiceLoader} only when {@code otelEndpoint} is configured. No OpenTelemetry SDK is required
 * by the core driver. A provider must not register or replace the application's global performance callback.
 */
public interface UserOtelTelemetryProvider {
    /**
     * Creates a lightweight callback before the connection-open scope starts, on the application thread with its original
     * context class loader. Capture that loader for later resolution of the optional {@link SQLServerAccessTokenCallback}
     * class. Constructors, static initialization and this method must not initialize an SDK, acquire tokens, perform
     * network I/O or wait for export; that work must be asynchronous. Callback methods must be nonblocking too.
     *
     * The options contain only validated {@code otelEndpoint}, {@code otelLogLevel} (default {@code "0"}), and, when
     * supplied, {@code otelAccessTokenCallback}, {@code otelConnectionSpanDurationThreshold} and
     * {@code otelStatementSpanDurationThreshold}. Thresholds are normalized finite nonnegative decimal seconds within
     * the signed-long nanosecond range. Absence disables duration filtering, including for zero-duration roots; when
     * present a tree is exported only if its root duration is strictly greater than the threshold. Metrics are never
     * duration-filtered. Level 0 exports metrics only, level 1 also exports failed-root trees, and level 2 exports all
     * trees. Filtering and bounded tree buffering belong to the provider, not the SQL execution thread.
     *
     * Only lifecycle {@link PerformanceLogEvent} boundaries are delivered to this callback; legacy publish overloads
     * must be no-ops to prevent duplicate metrics. Never export these options, endpoint, tokens, raw SQL or raw exceptions
     * as attributes. The optional module owns a bounded shared exporter registry and its lifecycle; closing a SQL
     * connection does not close an exporter. There is no automatic Microsoft endpoint or token discovery.
     *
     * @param options
     *        a private copy of the allowlisted normalized options, not the SQL connection properties
     * @return the connection callback, or {@code null} if this provider declines the configuration
     */
    PerformanceLogCallback create(Properties options);
}
