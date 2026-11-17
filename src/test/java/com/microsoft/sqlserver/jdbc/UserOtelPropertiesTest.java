/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.DriverPropertyInfo;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.junit.jupiter.api.Test;


class UserOtelPropertiesTest {
    @Test
    void registersExactPropertyNames() throws Exception {
        Set<String> names = new HashSet<>();
        for (DriverPropertyInfo info : new SQLServerDriver().getPropertyInfo("jdbc:sqlserver://localhost", null)) {
            names.add(info.name);
        }
        assertTrue(names.containsAll(Arrays.asList("otelEndpoint", "otelAccessTokenCallback", "otelLogLevel",
                "otelConnectionSpanDurationThreshold", "otelStatementSpanDurationThreshold")));
        assertFalse(names.contains("otelAccessTokenCallbackClass"));
    }

    @Test
    void invalidExplicitOptionsFailBeforeUnrelatedConfigurationWithoutLeakingValues() {
        String[][] invalid = {{"otelEndpoint", "https://SECRET@example.com/path"},
                {"otelEndpoint", "http://example.com/?SECRET"}, {"otelEndpoint", "https://example.com/#SECRET"},
                {"otelEndpoint", "file:///SECRET"}, {"otelEndpoint", "https:///SECRET"}, {"otelLogLevel", "3"},
                {"otelLogLevel", "-1"}, {"otelLogLevel", "NaN"}, {"otelConnectionSpanDurationThreshold", "-1"},
                {"otelConnectionSpanDurationThreshold", "NaN"}, {"otelConnectionSpanDurationThreshold", "Infinity"},
                {"otelStatementSpanDurationThreshold", "9223372036.854775808"},
                {"otelStatementSpanDurationThreshold", "1e1000"}, {"otelAccessTokenCallback", "not a class"}};
        for (String[] option : invalid) {
            SQLServerException error = assertThrows(SQLServerException.class,
                    () -> new SQLServerDriver().connect(
                            "jdbc:sqlserver://localhost;loginTimeout=INVALID;" + option[0] + "=" + option[1],
                            new Properties()));
            assertEquals("Invalid user telemetry configuration.", error.getMessage(), option[0]);
            assertNull(error.getCause());
        }
    }

    @Test
    void dataSourceDefaultsAndDecimalStringRoundTrip() throws Exception {
        SQLServerDataSource ds = new SQLServerDataSource();
        assertEquals(0, ds.getClass().getMethod("getOtelLogLevel").invoke(ds));
        for (String suffix : Arrays.asList("Endpoint", "AccessTokenCallback", "ConnectionSpanDurationThreshold",
                "StatementSpanDurationThreshold")) {
            assertNull(ds.getClass().getMethod("getOtel" + suffix).invoke(ds));
            String value = suffix.endsWith("Threshold") ? "0.000000001" : "value";
            ds.getClass().getMethod("setOtel" + suffix, String.class).invoke(ds, value);
            assertEquals(value, ds.getClass().getMethod("getOtel" + suffix).invoke(ds));
        }
    }
}
