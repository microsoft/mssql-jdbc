/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import com.azure.core.credential.TokenRequestContext;
import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.core.util.CoreUtils;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import reactor.core.publisher.Mono;

/**
 * A policy that authenticates requests with Azure Key Vault service.
 */
class KeyVaultCustomCredentialPolicy implements HttpPipelinePolicy {
    private static final String WWW_AUTHENTICATE = "WWW-Authenticate";
    private static final String BEARER_TOKEN_PREFIX = "Bearer ";
    private static final String AUTHORIZATION = "Authorization";
    private final ScopeTokenCache cache;
    private final KeyVaultCredential keyVaultCredential;

    /**
     * Creates KeyVaultCustomCredentialPolicy.
     *
     * @param credential the token credential to authenticate the request
     */
    public KeyVaultCustomCredentialPolicy(KeyVaultCredential credential) {
        Objects.requireNonNull(credential, "'credential' cannot be null.");
        this.cache = new ScopeTokenCache(credential::getToken);
        this.keyVaultCredential =  credential;
    }

    /**
     * Adds the required header to authenticate a request to Azure Key Vault service.
     *
     * @param context The request context
     * @param next The next HTTP pipeline policy to process the {@code context's} request after this policy completes.
     * @return A {@link Mono} representing the HTTP response that will arrive asynchronously.
     */
    @Override
    public Mono<HttpResponse> process(HttpPipelineCallContext context, HttpPipelineNextPolicy next) {
        if ("http".equals(context.getHttpRequest().getUrl().getProtocol())) {
            return Mono.error(new RuntimeException("Token credentials require a URL using the HTTPS protocol scheme"));
        }
        return next.clone().process()
                       // Ignore body
                       .doOnNext(HttpResponse::close)
                       .map(res -> res.getHeaderValue(WWW_AUTHENTICATE))
                       .map(header -> extractChallenge(header, BEARER_TOKEN_PREFIX))
                       .flatMap(map -> {
                           keyVaultCredential.setAuthorization(map.get("authorization"));
                           cache.setRequest(new TokenRequestContext().addScopes(map.get("resource") + "/.default"));
                           return cache.getToken();
                       })
                       .flatMap(token -> {
                           context.getHttpRequest().setHeader(AUTHORIZATION, BEARER_TOKEN_PREFIX + token.getToken());
                           return next.process();
                       });
    }

    /**
     * Extracts the challenge off the authentication header.
     *
     * @param authenticateHeader The authentication header containing all the challenges.
     * @param authChallengePrefix The authentication challenge name.
     * @return a challenge map.
     */
    private static Map<String, String> extractChallenge(String authenticateHeader, String authChallengePrefix) {
        if (!isValidChallenge(authenticateHeader, authChallengePrefix)) {
            return null;
        }
        authenticateHeader = authenticateHeader.toLowerCase(Locale.ROOT).replace(authChallengePrefix.toLowerCase(Locale.ROOT), "");

        String[] challenges = authenticateHeader.split(", ");
        Map<String, String> challengeMap = new HashMap<>();
        for (String pair : challenges) {
            String[] keyValue = pair.split("=");
            challengeMap.put(keyValue[0].replaceAll("\"", ""), keyValue[1].replaceAll("\"", ""));
        }
        return challengeMap;
    }

    /**
     * Verifies whether a challenge is bearer or not.
     *
     * @param authenticateHeader The authentication header containing all the challenges.
     * @param authChallengePrefix The authentication challenge name.
     * @return A boolean indicating tha challenge is valid or not.
     */
    private static boolean isValidChallenge(String authenticateHeader, String authChallengePrefix) {
        return (!CoreUtils.isNullOrEmpty(authenticateHeader)
                        && authenticateHeader.toLowerCase(Locale.ROOT).startsWith(authChallengePrefix.toLowerCase(Locale.ROOT)));
    }
}
