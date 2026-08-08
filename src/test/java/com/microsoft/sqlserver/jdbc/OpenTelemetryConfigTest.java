/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Unit tests for {@link OpenTelemetryConfig}, covering region-to-endpoint lookup and the four telemetry delivery
 * modes (DISABLED, CUSTOM, ARC, PAAS). These tests do not require a SQL Server instance.
 */
class OpenTelemetryConfigTest {

    @Test
    void testEndpointForRegionKnownRegion() {
        assertEquals("https://eastus.telemetry.sql.azure.com", OpenTelemetryConfig.endpointForRegion("eastus"));
        assertEquals("https://westeurope.telemetry.sql.azure.com",
                OpenTelemetryConfig.endpointForRegion("westeurope"));
    }

    @Test
    void testEndpointForRegionIsCaseInsensitiveAndTrimmed() {
        assertEquals("https://eastus.telemetry.sql.azure.com", OpenTelemetryConfig.endpointForRegion("  EastUS  "));
    }

    @Test
    void testEndpointForRegionUnknownOrNull() {
        assertNull(OpenTelemetryConfig.endpointForRegion("nowhere"));
        assertNull(OpenTelemetryConfig.endpointForRegion(null));
    }

    @Test
    void testResolveNullConnectionIsDisabled() {
        OpenTelemetryConfig config = OpenTelemetryConfig.resolve(null);
        assertEquals(OpenTelemetryConfig.Mode.DISABLED, config.getMode());
        assertFalse(config.isEnabled());
        assertNull(config.getEndpoint());
        assertTrue(config.getAuthHeaders().isEmpty());
    }

    @Test
    void testResolveFeatureOffIsDisabled() {
        SQLServerConnection connection = mock(SQLServerConnection.class);
        when(connection.getOtelEndpoint()).thenReturn(null);
        when(connection.isClientOpenTelemetryEnabled()).thenReturn(false);

        OpenTelemetryConfig config = OpenTelemetryConfig.resolve(connection);
        assertEquals(OpenTelemetryConfig.Mode.DISABLED, config.getMode());
        assertFalse(config.isEnabled());
        assertNull(config.getEndpoint());
    }

    @Test
    void testResolveCustomEndpointWithoutCallback() {
        SQLServerConnection connection = mock(SQLServerConnection.class);
        when(connection.getOtelEndpoint()).thenReturn("https://custom.collector:4318");
        when(connection.getOtelAccessTokenCallbackClass()).thenReturn(null);

        OpenTelemetryConfig config = OpenTelemetryConfig.resolve(connection);
        assertEquals(OpenTelemetryConfig.Mode.CUSTOM, config.getMode());
        assertTrue(config.isEnabled());
        assertEquals("https://custom.collector:4318", config.getEndpoint());
        assertTrue(config.getAuthHeaders().isEmpty());
    }

    @Test
    void testResolveCustomEndpointTakesPrecedenceOverServerToggle() {
        SQLServerConnection connection = mock(SQLServerConnection.class);
        when(connection.getOtelEndpoint()).thenReturn("https://custom.collector:4318");
        when(connection.isClientOpenTelemetryEnabled()).thenReturn(true);
        when(connection.getOtelServerRegion()).thenReturn("eastus");
        when(connection.getOtelServerArmResourceId()).thenReturn("/subscriptions/abc");

        OpenTelemetryConfig config = OpenTelemetryConfig.resolve(connection);
        assertEquals(OpenTelemetryConfig.Mode.CUSTOM, config.getMode());
        assertEquals("https://custom.collector:4318", config.getEndpoint());
    }

    @Test
    void testResolveArcWhenRegionAndArmIdEmpty() {
        SQLServerConnection connection = mock(SQLServerConnection.class);
        when(connection.getOtelEndpoint()).thenReturn(null);
        when(connection.isClientOpenTelemetryEnabled()).thenReturn(true);
        when(connection.getOtelServerRegion()).thenReturn("");
        when(connection.getOtelServerArmResourceId()).thenReturn("");

        OpenTelemetryConfig config = OpenTelemetryConfig.resolve(connection);
        assertEquals(OpenTelemetryConfig.Mode.ARC, config.getMode());
        assertTrue(config.isEnabled());
        assertEquals(OpenTelemetryConfig.ARC_ENDPOINT, config.getEndpoint());
        assertEquals("http://localhost:5555", config.getEndpoint());
        assertTrue(config.getAuthHeaders().isEmpty());
    }

    @Test
    void testResolvePaasDeducesEndpointAndAttachesHeaders() {
        SQLServerConnection connection = mock(SQLServerConnection.class);
        when(connection.getOtelEndpoint()).thenReturn(null);
        when(connection.isClientOpenTelemetryEnabled()).thenReturn(true);
        when(connection.getOtelServerRegion()).thenReturn("eastus");
        when(connection.getOtelServerArmResourceId())
                .thenReturn("/subscriptions/abc/resourceGroups/rg/providers/Microsoft.Sql/servers/s");
        when(connection.getOtelAuth()).thenReturn("");

        SqlAuthenticationToken fakeToken = new SqlAuthenticationToken("fake-token",
                System.currentTimeMillis() + 3600000L);

        try (MockedStatic<SQLServerSecurityUtility> util = Mockito.mockStatic(SQLServerSecurityUtility.class)) {
            util.when(() -> SQLServerSecurityUtility.getDefaultAzureCredAuthToken(
                    Mockito.eq(OpenTelemetryConfig.PAAS_TOKEN_SCOPE), Mockito.isNull(), Mockito.anyInt()))
                    .thenReturn(fakeToken);

            OpenTelemetryConfig config = OpenTelemetryConfig.resolve(connection);

            assertEquals(OpenTelemetryConfig.Mode.PAAS, config.getMode());
            assertTrue(config.isEnabled());
            assertEquals("https://eastus.telemetry.sql.azure.com", config.getEndpoint());
            assertEquals("/subscriptions/abc/resourceGroups/rg/providers/Microsoft.Sql/servers/s",
                    config.getAuthHeaders().get(OpenTelemetryConfig.ARM_RESOURCE_ID_HEADER));
            assertEquals("Bearer fake-token", config.getAuthHeaders().get(OpenTelemetryConfig.AUTHORIZATION_HEADER));
        }
    }
}
