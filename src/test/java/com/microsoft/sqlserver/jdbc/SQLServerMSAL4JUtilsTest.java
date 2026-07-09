/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.Collections;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

import com.microsoft.aad.msal4j.ClientCredentialParameters;
import com.microsoft.aad.msal4j.ConfidentialClientApplication;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import com.microsoft.aad.msal4j.InteractiveRequestParameters;
import com.microsoft.aad.msal4j.SilentParameters;


/**
 * Unit tests for {@link SQLServerMSAL4JUtils} that do not require a SQL Server
 * connection or an interactive Azure AD sign-in.
 */
public class SQLServerMSAL4JUtilsTest {

    /**
     * Regression guard for the ActiveDirectoryInteractive hardening: the MSAL4J
     * interactive token request must include {@code response_mode=form_post}
     * so the AAD authorization response is delivered as an HTTP POST body to
     * the loopback redirect URI instead of in the URL. This prevents the auth
     * code / state from leaking via browser history, referrers, or
     * redirect-based phishing.
     */
    @Test
    public void testInteractiveRequestUsesFormPostResponseMode() throws Exception {
        String user = "user@contoso.com";
        String spn = "https://database.windows.net";

        InteractiveRequestParameters params = SQLServerMSAL4JUtils.buildInteractiveRequestParameters(
                new URI(SQLServerMSAL4JUtils.REDIRECTURI), user, spn);

        assertNotNull(params.extraQueryParameters(), "extraQueryParameters must not be null");
        assertEquals("form_post", params.extraQueryParameters().get("response_mode"),
                "response_mode must be form_post to avoid leaking the auth code via the redirect URL");
        assertEquals(user, params.loginHint(), "loginHint should be propagated from the connection user");
        assertTrue(params.scopes().contains(spn + SQLServerMSAL4JUtils.SLASH_DEFAULT),
                "scopes must contain the resource SPN with the /.default suffix");
    }

    /**
     * The silent fast-path must return the cached token on a cache hit AND must never fall through
     * to {@code acquireToken} (the real Entra ID call, which is gated by the global semaphore). This
     * is the behavior that lets in-memory cache hits bypass the gate.
     */
    @Test
    public void testAcquireTokenSilentlyReturnsCachedTokenOnHit() throws Exception {
        ConfidentialClientApplication clientApplication = mock(ConfidentialClientApplication.class);
        IAuthenticationResult silentResult = mock(IAuthenticationResult.class);
        String cachedAccessToken = "cached-access-token";
        Date expiry = new Date(System.currentTimeMillis() + 3_600_000L);
        when(silentResult.accessToken()).thenReturn(cachedAccessToken);
        when(silentResult.expiresOnDate()).thenReturn(expiry);
        when(clientApplication.acquireTokenSilently(any(SilentParameters.class)))
                .thenReturn(CompletableFuture.completedFuture(silentResult));

        Set<String> scopes = Collections.singleton("https://database.windows.net/.default");
        SqlAuthenticationToken token = SQLServerMSAL4JUtils.acquireTokenSilentlyOrNull(clientApplication, scopes,
                30000, "principal-id");

        assertNotNull(token, "a cache hit must return a token");
        assertEquals(cachedAccessToken, token.getAccessToken(), "the cached access token must be returned");
        verify(clientApplication, never()).acquireToken(any(ClientCredentialParameters.class));
    }

    /**
     * On a cache miss, MSAL's silent future completes exceptionally; the helper must swallow that and
     * return {@code null} so the caller takes the gated slow path that performs the real AAD call.
     */
    @Test
    public void testAcquireTokenSilentlyReturnsNullOnMiss() throws Exception {
        ConfidentialClientApplication clientApplication = mock(ConfidentialClientApplication.class);
        CompletableFuture<IAuthenticationResult> miss = new CompletableFuture<>();
        miss.completeExceptionally(new RuntimeException("no cached token for these scopes"));
        when(clientApplication.acquireTokenSilently(any(SilentParameters.class))).thenReturn(miss);

        Set<String> scopes = Collections.singleton("https://database.windows.net/.default");
        SqlAuthenticationToken token = SQLServerMSAL4JUtils.acquireTokenSilentlyOrNull(clientApplication, scopes,
                30000, "principal-id");

        assertNull(token, "a cache miss must return null so the caller takes the gated slow path");
    }
}
