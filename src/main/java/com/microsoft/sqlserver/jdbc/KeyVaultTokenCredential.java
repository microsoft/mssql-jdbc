/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import com.azure.core.annotation.Immutable;
import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.core.util.logging.ClientLogger;
import com.microsoft.aad.msal4j.ClientCredentialFactory;
import com.microsoft.aad.msal4j.ClientCredentialParameters;
import com.microsoft.aad.msal4j.ConfidentialClientApplication;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import com.microsoft.aad.msal4j.IClientCredential;
import com.microsoft.aad.msal4j.SilentParameters;
import java.net.MalformedURLException;
import java.text.MessageFormat;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import reactor.core.publisher.Mono;

/**
 * An AAD credential that acquires a token with a client secret for an AAD application.
 */
@Immutable
class KeyVaultTokenCredential implements TokenCredential {
    private final ClientLogger logger = new ClientLogger(KeyVaultTokenCredential.class);
    private final String clientId;
    private final String clientSecret;
    private final SQLServerKeyVaultAuthenticationCallback authenticationCallback;
    private String authorization;
    private ConfidentialClientApplication confidentialClientApplication;
    private String resource;
    private String scope;

    /**
     * Creates a KeyVaultCredential with the given identity client options.
     *
     * @param clientId
     *        the client ID of the application
     * @param clientSecret
     *        the secret value of the AAD application
     * @throws SQLServerException
     */
    KeyVaultTokenCredential(String clientId, String clientSecret) throws SQLServerException {
        if (null == clientId || clientId.isEmpty()) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_NullValue"));
            Object[] msgArgs1 = {"Client ID"};
            throw new SQLServerException(form.format(msgArgs1), null);
        }

        if (null == clientSecret || clientSecret.isEmpty()) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_NullValue"));
            Object[] msgArgs1 = {"Client Secret"};
            throw new SQLServerException(form.format(msgArgs1), null);
        }

        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.authenticationCallback = null;
    }

    KeyVaultTokenCredential(SQLServerKeyVaultAuthenticationCallback authenticationCallback) {
        this.authenticationCallback = authenticationCallback;
        this.clientId = null;
        this.clientSecret = null;
    }

    @Override
    public Mono<AccessToken> getToken(TokenRequestContext request) {
        if (null != authenticationCallback) {
            String accessToken = authenticationCallback.getAccessToken(this.authorization, this.resource, this.scope);
            return Mono.just(new AccessToken(accessToken, OffsetDateTime.MIN));
        }

        return authenticateWithConfidentialClientCache(request).onErrorResume(t -> Mono.empty())
                .switchIfEmpty(Mono.defer(() -> authenticateWithConfidentialClient(request)));
    }

    KeyVaultTokenCredential setAuthorization(String authorization) {
        if (null != this.authorization && this.authorization.equals(authorization)) {
            return this;
        }
        this.authorization = authorization;
        confidentialClientApplication = getConfidentialClientApplication();
        return this;
    }

    private ConfidentialClientApplication getConfidentialClientApplication() {
        if (null == clientId) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_NullValue"));
            Object[] msgArgs1 = {"Client ID"};
            throw new IllegalArgumentException(form.format(msgArgs1), null);
        }

        if (null == authorization) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_NullValue"));
            Object[] msgArgs1 = {"Authorization"};
            throw new IllegalArgumentException(form.format(msgArgs1), null);
        }

        if (null == clientSecret) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_NullValue"));
            Object[] msgArgs1 = {"Client Secret"};
            throw new IllegalArgumentException(form.format(msgArgs1), null);
        }

        IClientCredential credential;
        credential = ClientCredentialFactory.createFromSecret(clientSecret);
        ConfidentialClientApplication.Builder applicationBuilder = ConfidentialClientApplication.builder(clientId,
                credential);
        try {
            applicationBuilder = applicationBuilder.authority(authorization);
        } catch (MalformedURLException e) {
            throw logger.logExceptionAsWarning(new IllegalStateException(e));
        }
        return applicationBuilder.build();
    }

    private Mono<AccessToken> authenticateWithConfidentialClientCache(TokenRequestContext request) {
        return Mono.fromFuture(() -> {
            SilentParameters.SilentParametersBuilder parametersBuilder = SilentParameters
                    .builder(new HashSet<>(request.getScopes()));
            try {
                return confidentialClientApplication.acquireTokenSilently(parametersBuilder.build());
            } catch (MalformedURLException e) {
                return getFailedCompletableFuture(logger.logExceptionAsError(new RuntimeException(e)));
            }
        }).map(ar -> new AccessToken(ar.accessToken(),
                OffsetDateTime.ofInstant(ar.expiresOnDate().toInstant(), ZoneOffset.UTC))).filter(t -> !t.isExpired());
    }

    private CompletableFuture<IAuthenticationResult> getFailedCompletableFuture(Exception e) {
        CompletableFuture<IAuthenticationResult> completableFuture = new CompletableFuture<>();
        completableFuture.completeExceptionally(e);
        return completableFuture;
    }

    private Mono<AccessToken> authenticateWithConfidentialClient(TokenRequestContext request) {
        return Mono
                .fromFuture(() -> confidentialClientApplication
                        .acquireToken(ClientCredentialParameters.builder(new HashSet<>(request.getScopes())).build()))
                .map(ar -> new AccessToken(ar.accessToken(),
                        OffsetDateTime.ofInstant(ar.expiresOnDate().toInstant(), ZoneOffset.UTC)));
    }

    void setResource(String resource) {
        this.resource = resource;
    }

    void setScope(String scope) {
        this.scope = scope;
    }
}
