/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Resolves the effective OpenTelemetry export configuration for a connection.
 *
 * <p>The driver deduces the telemetry destination and authentication automatically, in priority order:</p>
 * <ol>
 * <li><b>CUSTOM</b> — {@code otelEndpoint} is supplied on the connection string. Telemetry is sent to that
 * endpoint regardless of any server-side setting. If {@code otelAccessTokenCallbackClass} is also supplied, the
 * driver loads it from the classpath and calls it to obtain a bearer token for the {@code Authorization}
 * header.</li>
 * <li><b>ARC</b> — the server negotiated the Client OpenTelemetry feature (toggle ON) but returned an empty ARM
 * resource id and region. Telemetry is sent to the Arc agent on {@code localhost:ARC_PORT} with no
 * authentication.</li>
 * <li><b>PAAS</b> — the server negotiated the feature and returned a non-empty ARM resource id and region
 * (Azure SQL DB). The driver deduces the endpoint from the region-to-endpoint map, acquires an Azure token,
 * and attaches the ARM resource id plus the bearer token to the telemetry auth headers.</li>
 * <li><b>DISABLED</b> — none of the above applies.</li>
 * </ol>
 */
final class OpenTelemetryConfig {

    private static final Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.Telemetry");

    /** Local port where the Arc agent exposes its OpenTelemetry collector endpoint. */
    static final int ARC_PORT = 5555;

    /** Arc collector endpoint on the local host. */
    static final String ARC_ENDPOINT = "http://localhost:" + ARC_PORT;

    /** Placeholder Azure token scope for the PaaS telemetry endpoint (POC). */
    static final String PAAS_TOKEN_SCOPE = "https://monitor.azure.com/.default";

    /** Authorization header name. */
    static final String AUTHORIZATION_HEADER = "Authorization";

    /** ARM resource id header name attached to PaaS telemetry messages. */
    static final String ARM_RESOURCE_ID_HEADER = "x-ms-arm-resource-id";

    /**
     * Mocked region-to-endpoint map for PaaS (Azure SQL DB). In a real deployment this would be a curated,
     * possibly per-cloud list. For the POC these are placeholder endpoints keyed by region name.
     */
    private static final Map<String, String> REGION_ENDPOINT_MAP;

    static {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("eastus", "https://eastus.telemetry.sql.azure.com");
        map.put("eastus2", "https://eastus2.telemetry.sql.azure.com");
        map.put("westus", "https://westus.telemetry.sql.azure.com");
        map.put("westus2", "https://westus2.telemetry.sql.azure.com");
        map.put("westus3", "https://westus3.telemetry.sql.azure.com");
        map.put("centralus", "https://centralus.telemetry.sql.azure.com");
        map.put("northeurope", "https://northeurope.telemetry.sql.azure.com");
        map.put("westeurope", "https://westeurope.telemetry.sql.azure.com");
        map.put("southeastasia", "https://southeastasia.telemetry.sql.azure.com");
        map.put("eastasia", "https://eastasia.telemetry.sql.azure.com");
        map.put("uksouth", "https://uksouth.telemetry.sql.azure.com");
        map.put("australiaeast", "https://australiaeast.telemetry.sql.azure.com");
        REGION_ENDPOINT_MAP = Collections.unmodifiableMap(map);
    }

    /** Telemetry delivery mode resolved for a connection. */
    enum Mode {
        DISABLED, CUSTOM, ARC, PAAS
    }

    private final Mode mode;
    private final boolean enabled;
    private final String endpoint;
    private final Map<String, String> authHeaders;

    private OpenTelemetryConfig(Mode mode, String endpoint, Map<String, String> authHeaders) {
        this.mode = mode;
        this.enabled = (mode != Mode.DISABLED);
        this.endpoint = endpoint;
        this.authHeaders = (authHeaders == null || authHeaders.isEmpty()) ? Collections.emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(authHeaders));
    }

    Mode getMode() {
        return mode;
    }

    boolean isEnabled() {
        return enabled;
    }

    String getEndpoint() {
        return endpoint;
    }

    Map<String, String> getAuthHeaders() {
        return authHeaders;
    }

    /**
     * Returns the mocked telemetry endpoint for a region, or {@code null} if the region is not mapped.
     *
     * @param region
     *        the region name
     * @return the endpoint, or {@code null}
     */
    static String endpointForRegion(String region) {
        if (region == null) {
            return null;
        }
        return REGION_ENDPOINT_MAP.get(region.trim().toLowerCase(java.util.Locale.ROOT));
    }

    /**
     * Resolves the effective OpenTelemetry configuration for a connection.
     *
     * @param connection
     *        the connection whose properties and negotiated feature state drive resolution
     * @return the resolved configuration (never {@code null})
     */
    static OpenTelemetryConfig resolve(SQLServerConnection connection) {
        if (connection == null) {
            return new OpenTelemetryConfig(Mode.DISABLED, null, null);
        }

        // 1. CUSTOM: bring-your-own endpoint takes precedence over any server setting.
        String customEndpoint = connection.getOtelEndpoint();
        if (customEndpoint != null && !customEndpoint.isEmpty()) {
            Map<String, String> headers = new LinkedHashMap<>();
            String callbackClass = connection.getOtelAccessTokenCallbackClass();
            if (callbackClass != null && !callbackClass.isEmpty()) {
                String token = acquireTokenFromCallback(callbackClass, customEndpoint);
                if (token != null && !token.isEmpty()) {
                    headers.put(AUTHORIZATION_HEADER, "Bearer " + token);
                }
            }
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("OpenTelemetry resolved to CUSTOM endpoint: " + customEndpoint);
            }
            return new OpenTelemetryConfig(Mode.CUSTOM, customEndpoint, headers);
        }

        // 2 & 3. Server-driven: only when the server negotiated the feature with the toggle ON.
        if (!connection.isClientOpenTelemetryEnabled()) {
            return new OpenTelemetryConfig(Mode.DISABLED, null, null);
        }

        String region = connection.getOtelServerRegion();
        String armResourceId = connection.getOtelServerArmResourceId();
        boolean paas = (region != null && !region.isEmpty()) && (armResourceId != null && !armResourceId.isEmpty());

        if (!paas) {
            // ARC: send to the local Arc agent, no authentication.
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("OpenTelemetry resolved to ARC endpoint: " + ARC_ENDPOINT);
            }
            return new OpenTelemetryConfig(Mode.ARC, ARC_ENDPOINT, null);
        }

        // PAAS: deduce endpoint from region and attach ARM id + acquired token.
        String endpoint = endpointForRegion(region);
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put(ARM_RESOURCE_ID_HEADER, armResourceId);
        String token = acquireAzureToken(connection.getOtelAuth());
        if (token != null && !token.isEmpty()) {
            headers.put(AUTHORIZATION_HEADER, "Bearer " + token);
        }
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("OpenTelemetry resolved to PAAS endpoint: " + endpoint + " for region: " + region);
        }
        return new OpenTelemetryConfig(Mode.PAAS, endpoint, headers);
    }

    /**
     * Loads the supplied access-token callback class from the classpath and invokes it to obtain a token.
     * Failures are logged and swallowed (telemetry auth is best-effort).
     */
    private static String acquireTokenFromCallback(String callbackClass, String endpoint) {
        try {
            Object[] msgArgs = {"otelAccessTokenCallbackClass",
                    "com.microsoft.sqlserver.jdbc.SQLServerAccessTokenCallback"};
            SQLServerAccessTokenCallback callbackInstance = Util.newInstance(SQLServerAccessTokenCallback.class,
                    callbackClass, null, msgArgs);
            SqlAuthenticationToken token = callbackInstance.getAccessToken(endpoint, endpoint);
            return token == null ? null : token.getAccessToken();
        } catch (Exception e) {
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("Failed to acquire OpenTelemetry token from callback class " + callbackClass + ": "
                        + e.getMessage());
            }
            return null;
        }
    }

    /**
     * Acquires an Azure token for the PaaS telemetry endpoint using the mechanism selected by {@code otelAuth}.
     * Failures are logged and swallowed (telemetry auth is best-effort).
     */
    private static String acquireAzureToken(String otelAuth) {
        String mechanism = (otelAuth == null) ? "" : otelAuth.trim().toUpperCase(java.util.Locale.ROOT);
        try {
            SqlAuthenticationToken token;
            if ("MANAGEDIDENTITY".equals(mechanism)) {
                token = SQLServerSecurityUtility.getManagedIdentityCredAuthToken(PAAS_TOKEN_SCOPE, null,
                        TOKEN_WAIT_MS);
            } else {
                // DEFAULT (or omitted) and all other values fall back to DefaultAzureCredential for the POC.
                token = SQLServerSecurityUtility.getDefaultAzureCredAuthToken(PAAS_TOKEN_SCOPE, null,
                        (int) TOKEN_WAIT_MS);
            }
            return token == null ? null : token.getAccessToken();
        } catch (Exception e) {
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("Failed to acquire Azure OpenTelemetry token (mechanism=" + mechanism + "): "
                        + e.getMessage());
            }
            return null;
        }
    }

    private static final long TOKEN_WAIT_MS = 20000L;
}
