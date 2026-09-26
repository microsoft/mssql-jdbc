/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.otel;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Map;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.AzureCliCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.microsoft.sqlserver.jdbc.SQLServerAccessTokenCallback;
import com.microsoft.sqlserver.jdbc.SqlAuthenticationToken;


/**
 * Demo-only telemetry credential provider. The OTLP bootstrap constructs and invokes this class on its exporter
 * worker, never on the JDBC connection callback. It does not authenticate SQL connections, initiate interactive
 * login, infer audiences, or print provider diagnostics. Azure Identity is a test-scope dependency only.
 */
public final class DemoTokenCallback implements SQLServerAccessTokenCallback {
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(3);
    private static final Duration PROCESS_TIMEOUT = Duration.ofSeconds(2);
    private final TokenCredential credential;
    private final String scope;
    private final String authority;
    private final Duration timeout;

    /** Creates a provider from the process environment without acquiring a token. */
    public DemoTokenCallback() {
        this(System.getenv(), null, REQUEST_TIMEOUT);
    }

    DemoTokenCallback(Map<String, String> env, TokenCredential injected, Duration timeout) {
        scope = ConnectionErrorDemo.required(env, "OTEL_ACCESS_TOKEN_SCOPE");
        authority = authority(env);
        this.timeout = timeout;
        if (injected != null) {
            credential = injected;
            return;
        }
        switch (ConnectionErrorDemo.value(env, "OTEL_AUTH_MODE", "none")) {
            case "azure_cli":
                // The CLI uses its already selected cloud/session, not the callback authority.
                credential = new AzureCliCredentialBuilder().processTimeout(PROCESS_TIMEOUT).build();
                break;
            case "managed_identity":
                ManagedIdentityCredentialBuilder managed = new ManagedIdentityCredentialBuilder();
                String clientId = ConnectionErrorDemo.value(env, "AZURE_CLIENT_ID", null);
                if (clientId != null) {
                    managed.clientId(clientId);
                }
                credential = managed.build();
                break;
            case "default":
                credential = new DefaultAzureCredentialBuilder().authorityHost(authority)
                        .credentialProcessTimeout(PROCESS_TIMEOUT).build();
                break;
            default:
                throw ConnectionErrorDemo.invalidConfiguration();
        }
    }

    /**
     * Requests only the explicitly configured telemetry scope, with a finite reactive wait. Provider failures have
     * no attached cause or sensitive text; the bootstrap drops the batch without an unauthenticated fallback.
     *
     * @param spn
     *        exact configured telemetry scope (not a SQL SPN or the collector URL)
     * @param stsurl
     *        validated authority supplied by the bootstrap
     * @return a token with its actual expiry
     */
    @Override
    public SqlAuthenticationToken getAccessToken(String spn, String stsurl) {
        try {
            if (!scope.equals(spn) || !authority.equals(stsurl)) {
                throw new IllegalStateException();
            }
            AccessToken token = credential.getToken(new TokenRequestContext().addScopes(scope)).block(timeout);
            if (token == null || token.isExpired()) {
                throw new IllegalStateException();
            }
            return new SqlAuthenticationToken(token.getToken(), token.getExpiresAt().toInstant().toEpochMilli());
        } catch (RuntimeException e) {
            throw new IllegalStateException("Telemetry authentication unavailable");
        }
    }

    static String authority(Map<String, String> env) {
        // Explicit Azure public-cloud default, not a tenant or a value derived from a JDBC/OTLP endpoint.
        String authority = ConnectionErrorDemo.value(env, "OTEL_TOKEN_AUTHORITY",
                ConnectionErrorDemo.value(env, "AZURE_AUTHORITY_HOST", "https://login.microsoftonline.com/"));
        try {
            URI uri = new URI(authority);
            if (!"https".equalsIgnoreCase(uri.getScheme()) || uri.getHost() == null || uri.getRawUserInfo() != null
                    || uri.getRawQuery() != null || uri.getRawFragment() != null
                    || (uri.getPort() != -1 && uri.getPort() != 443)) {
                throw ConnectionErrorDemo.invalidConfiguration();
            }
            return authority;
        } catch (URISyntaxException e) {
            throw ConnectionErrorDemo.invalidConfiguration();
        }
    }
}
