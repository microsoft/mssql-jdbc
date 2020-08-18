// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.sqlserver.jdbc;

import com.azure.core.annotation.Immutable;
import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenRequestContext;
import com.azure.core.util.logging.ClientLogger;
import com.microsoft.aad.msal4j.ClientCredentialFactory;
import com.microsoft.aad.msal4j.ClientCredentialParameters;
import com.microsoft.aad.msal4j.ConfidentialClientApplication;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import com.microsoft.aad.msal4j.IClientCredential;
import com.microsoft.aad.msal4j.SilentParameters;
import java.net.MalformedURLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import reactor.core.publisher.Mono;


/**
 * An AAD credential that acquires a token with a client secret for an AAD application.
 *
 * <p><strong>Sample: Construct a simple KeyVaultCredential</strong></p>
 * {@codesnippet com.azure.identity.credential.clientsecretcredential.construct}
 *
 * <p><strong>Sample: Construct a KeyVaultCredential behind a proxy</strong></p>
 * {@codesnippet com.azure.identity.credential.clientsecretcredential.constructwithproxy}
 */
@Immutable
class KeyVaultCredential {
    private final ClientLogger logger = new ClientLogger(KeyVaultCredential.class);
    private final String clientId;
    private final String clientSecret;
    private String authorization;
    private ConfidentialClientApplication confidentialClientApplication;

    /**
     * Creates a KeyVaultCredential with the given identity client options.
     *
     * @param clientId the client ID of the application
     * @param clientSecret the secret value of the AAD application.
     */
    KeyVaultCredential(String clientId, String clientSecret) {
        Objects.requireNonNull(clientSecret, "'clientSecret' cannot be null.");
        Objects.requireNonNull(clientSecret, "'clientId' cannot be null.");
        this.clientId = clientId;
        this.clientSecret = clientSecret;
    }

    public Mono<AccessToken> getToken(TokenRequestContext request) {
        return authenticateWithConfidentialClientCache(request)
                       .onErrorResume(t -> Mono.empty())
                       .switchIfEmpty(Mono.defer(() -> authenticateWithConfidentialClient(request)));
    }

    public KeyVaultCredential setAuthorization(String authorization) {
            if (this.authorization != null && this.authorization.equals(authorization)) {
                return this;
            }
            this.authorization = authorization;
            confidentialClientApplication = getConfidentialClientApplication();
            return this;
    }

    private ConfidentialClientApplication getConfidentialClientApplication() {
        if (clientId == null) {
            throw logger.logExceptionAsError(new IllegalArgumentException(
                    "A non-null value for client ID must be provided for user authentication."));
        }

        if (authorization == null) {
            throw logger.logExceptionAsError(new IllegalArgumentException(
                    "A non-null value for authorization must be provided for user authentication."));
        }

        IClientCredential credential;
        if (clientSecret != null) {
            credential = ClientCredentialFactory.create(clientSecret);
        } else {
            throw logger.logExceptionAsError(
                    new IllegalArgumentException("Must provide client secret."));
        }
        ConfidentialClientApplication.Builder applicationBuilder =
                ConfidentialClientApplication.builder(clientId, credential);
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
                OffsetDateTime.ofInstant(ar.expiresOnDate().toInstant(), ZoneOffset.UTC)))
        .filter(t -> !t.isExpired());
    }

    private CompletableFuture<IAuthenticationResult> getFailedCompletableFuture(Exception e) {
        CompletableFuture<IAuthenticationResult> completableFuture = new CompletableFuture<>();
        completableFuture.completeExceptionally(e);
        return completableFuture;
    }

    private Mono<AccessToken> authenticateWithConfidentialClient(TokenRequestContext request) {
        return Mono.fromFuture(() -> confidentialClientApplication
                .acquireToken(ClientCredentialParameters.builder(new HashSet<>(request.getScopes())).build()))
                .map(ar -> new AccessToken(ar.accessToken(),
                        OffsetDateTime.ofInstant(ar.expiresOnDate().toInstant(), ZoneOffset.UTC)));
    }
}
