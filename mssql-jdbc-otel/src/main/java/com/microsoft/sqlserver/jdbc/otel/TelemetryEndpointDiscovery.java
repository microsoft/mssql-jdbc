/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.otel;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.Properties;


/** Explicit demo/startup discovery, never invoked by driver opens or by the telemetry bootstrap. */
public final class TelemetryEndpointDiscovery {
    private static final String QUERY = "SELECT TOP 1 DemoLocalOtelEndpoint, AzureResourceId, "
            + "AzureRegion FROM msdb.dbo.SQLServerAzureArcProperties";

    private TelemetryEndpointDiscovery() {}

    /**
     * Queries the demo discovery table on a separately obtained startup connection. This does not create telemetry,
     * register a callback, close the connection, or alter transactions. The table must have one authoritative row:
     * the fork-compatible TOP 1 query has no ordering and cannot detect multiple underlying rows.
     *
     * <p>
     * Results are untrusted routing hints. The caller must approve the destination before supplying credentials and
     * creating {@link OtlpConnectionTelemetry}, which validates the endpoint and headers. No insecure-transport opt-in
     * or authentication settings are returned. The region is informational, not exported as a resource attribute.
     * Query cancellation depends on the JDBC implementation; configure connection/socket deadlines separately.
     *
     * @param connection
     *        caller-owned startup connection
     * @param timeoutSeconds
     *        positive statement query timeout in seconds (zero/unlimited is not accepted)
     * @return properties named otelEndpoint, otelArmResourceId and otelDiscoveredArmRegion, omitting absent values;
     *         empty when the table has no row or the endpoint is blank
     * @throws SQLException
     *         if discovery fails, with a fixed message and no server diagnostics or chained exception
     * @throws IllegalArgumentException
     *         if the timeout is not positive
     */
    public static Properties discover(Connection connection, int timeoutSeconds) throws SQLException {
        Objects.requireNonNull(connection, "connection");
        if (timeoutSeconds <= 0) {
            throw new IllegalArgumentException("Discovery timeout must be positive");
        }
        Properties properties = new Properties();
        try (Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(timeoutSeconds);
            try (ResultSet result = statement.executeQuery(QUERY)) {
                if (result.next()) {
                    put(properties, "otelEndpoint", result.getString(1));
                    if (properties.containsKey("otelEndpoint")) {
                        put(properties, "otelArmResourceId", result.getString(2));
                        put(properties, "otelDiscoveredArmRegion", result.getString(3));
                    }
                }
            }
        } catch (SQLException e) {
            throw new SQLException("Telemetry endpoint discovery failed");
        }
        return properties;
    }

    private static void put(Properties properties, String key, String value) {
        if (value != null && !value.trim().isEmpty()) {
            properties.setProperty(key, value.trim());
        }
    }
}
