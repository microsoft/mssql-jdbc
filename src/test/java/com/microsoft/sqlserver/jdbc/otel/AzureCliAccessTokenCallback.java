/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.otel;

import java.time.OffsetDateTime;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.AzureCliCredentialBuilder;
import com.microsoft.sqlserver.jdbc.SQLServerAccessTokenCallback;
import com.microsoft.sqlserver.jdbc.SqlAuthenticationToken;

/**
 * Demo {@link SQLServerAccessTokenCallback} that mints an Azure AD JWT with the Azure CLI
 * ({@code az account get-access-token}) via {@link com.azure.identity.AzureCliCredential}.
 *
 * <p>Wire it into the OTel POC with the connection-string property
 * {@code otelAccessTokenCallbackClass=com.microsoft.sqlserver.jdbc.otel.AzureCliAccessTokenCallback}
 * (and {@code otelUseSqlAccessToken=false}); the driver then sends the returned token as the
 * OTLP/HTTP {@code Authorization: Bearer ...} header. This decouples the OTLP bearer from the SQL
 * login, so the SQL connection can keep using SQL authentication.
 *
 * <p>The container must have the {@code az} CLI on its PATH and a logged-in {@code ~/.azure}
 * profile mounted (see docs/otel-poc/docker-compose.yml). The requested scope defaults to
 * {@value #DEFAULT_SCOPE} and can be overridden with the {@code otelAccessTokenScope} system
 * property or the {@code OTEL_ACCESS_TOKEN_SCOPE} environment variable.
 */
public class AzureCliAccessTokenCallback implements SQLServerAccessTokenCallback {

    /** TODO: Replace with T&I 1P App Audience. */
    public static final String DEFAULT_SCOPE = "https://database.windows.net/.default";

    private final TokenCredential credential;

    /** Required public no-argument constructor for {@code otelAccessTokenCallbackClass} reflection. */
    public AzureCliAccessTokenCallback() {
        this.credential = new AzureCliCredentialBuilder().build();
    }

    @Override
    public SqlAuthenticationToken getAccessToken(String spn, String stsurl) {
        String scope = resolveScope();
        AccessToken token = credential.getToken(new TokenRequestContext().addScopes(scope)).block();
        if (token == null) {
            throw new IllegalStateException("AzureCliCredential returned no token for scope " + scope);
        }
        OffsetDateTime expiresAt = token.getExpiresAt();
        long expiresOnMillis = expiresAt == null ? System.currentTimeMillis() + 3_600_000L
                : expiresAt.toInstant().toEpochMilli();
        // Confirmation only - never print the token itself.
        System.out.printf("[AzureCliAccessTokenCallback] minted JWT for scope=%s length=%d expiresAt=%s%n",
                scope, token.getToken().length(), expiresAt);
        return new SqlAuthenticationToken(token.getToken(), expiresOnMillis);
    }

    private static String resolveScope() {
        String scope = System.getProperty("otelAccessTokenScope");
        if (scope == null || scope.trim().isEmpty()) {
            scope = System.getenv("OTEL_ACCESS_TOKEN_SCOPE");
        }
        return (scope == null || scope.trim().isEmpty()) ? DEFAULT_SCOPE : scope.trim();
    }
}
