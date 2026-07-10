/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * Narrow bridge contract between the core JDBC driver and an optional telemetry module such as
 * {@code mssql-jdbc-otel}. The core driver emits a {@link TelemetryEvent} and the bridge decides how to
 * convert that payload into metrics, spans, logs, or other observability signals.
 *
 * <p>The bridge should treat auth-related data as opaque and only use it for correlation or propagation when
 * it is explicitly needed by the hosting telemetry backend.</p>
 */
public interface TelemetryBridge {

    /**
     * Publish a telemetry event produced by the core driver.
     *
     * @param event the event payload to publish
     * @throws Exception if the bridge cannot publish the event
     */
    void publish(TelemetryEvent event) throws Exception;
}
