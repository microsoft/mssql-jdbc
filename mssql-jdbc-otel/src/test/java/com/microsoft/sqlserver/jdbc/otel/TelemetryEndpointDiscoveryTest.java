/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.otel;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.Test;


class TelemetryEndpointDiscoveryTest {
    @Test
    void explicitDiscoveryUsesFixedQueryTimeoutAndClosesOnlyOwnedResources() throws Exception {
        List<String> calls = new ArrayList<>();
        Connection connection = connection(calls, false, false);
        Properties properties = TelemetryEndpointDiscovery.discover(connection, 7);
        assertEquals("https://collector.example", properties.getProperty("otelEndpoint"));
        assertEquals("/resource", properties.getProperty("otelArmResourceId"));
        assertEquals("region", properties.getProperty("otelDiscoveredArmRegion"));
        assertEquals(3, properties.size());
        assertEquals(Arrays.asList("timeout:7",
                "SELECT TOP 1 DemoLocalOtelEndpoint, AzureResourceId, AzureRegion FROM msdb.dbo.SQLServerAzureArcProperties",
                "result.close", "statement.close"), calls);
    }

    @Test
    void emptyDiscoveryIsExplicitAndSqlErrorsAreRedacted() throws Exception {
        List<String> calls = new ArrayList<>();
        assertTrue(TelemetryEndpointDiscovery.discover(connection(calls, true, false), 1).isEmpty());
        SQLException failure = assertThrows(SQLException.class,
                () -> TelemetryEndpointDiscovery.discover(connection(calls, false, true), 1));
        assertFalse(failure.toString().contains("SECRET"));
        assertNull(failure.getCause());
        assertNull(failure.getNextException());
        assertThrows(IllegalArgumentException.class,
                () -> TelemetryEndpointDiscovery.discover(connection(calls, false, false), 0));
    }

    private static Connection connection(List<String> calls, boolean empty, boolean fail) {
        ResultSet result = (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(),
                new Class<?>[] {ResultSet.class}, (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "next":
                            return !empty;
                        case "getString":
                            return new String[] {"https://collector.example", "/resource", "region"}[(Integer) args[0]
                                    - 1];
                        case "close":
                            calls.add("result.close");
                            return null;
                        default:
                            throw new AssertionError(method.getName());
                    }
                });
        Statement statement = (Statement) Proxy.newProxyInstance(Statement.class.getClassLoader(),
                new Class<?>[] {Statement.class}, (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "setQueryTimeout":
                            calls.add("timeout:" + args[0]);
                            return null;
                        case "executeQuery":
                            calls.add((String) args[0]);
                            if (fail) {
                                throw new SQLException("SECRET SQL diagnostics");
                            }
                            return result;
                        case "close":
                            calls.add("statement.close");
                            return null;
                        default:
                            throw new AssertionError(method.getName());
                    }
                });
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[] {Connection.class},
                (proxy, method, args) -> {
                    if ("createStatement".equals(method.getName())) {
                        return statement;
                    }
                    throw new AssertionError(method.getName());
                });
    }
}
