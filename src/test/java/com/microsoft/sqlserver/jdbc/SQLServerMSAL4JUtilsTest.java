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
import java.util.concurrent.ExecutionException;

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
     * Fast-path cache hit: when the MSAL client already holds a valid access token,
     * {@link SQLServerMSAL4JUtils#acquireTokenSilentlyOrNull} must return it directly and must NOT
     * fall through to a real Entra ID token acquisition. This is the head-of-line-blocking
     * optimization: cache hits are served without taking the global token semaphore.
     */
    @Test
    public void testAcquireTokenSilentlyReturnsCachedTokenOnHit() throws Exception {
        ConfidentialClientApplication clientApplication = mock(ConfidentialClientApplication.class);
        IAuthenticationResult silentResult = mock(IAuthenticationResult.class);
        Date expiresOn = new Date(System.currentTimeMillis() + 3_600_000L);
        when(silentResult.accessToken()).thenReturn("cached-access-token");
        when(silentResult.expiresOnDate()).thenReturn(expiresOn);
        when(clientApplication.acquireTokenSilently(any(SilentParameters.class)))
                .thenReturn(CompletableFuture.completedFuture(silentResult));

        Set<String> scopes = Collections.singleton("https://database.windows.net/.default");

        SqlAuthenticationToken token = SQLServerMSAL4JUtils.acquireTokenSilentlyOrNull(clientApplication, scopes,
                20000, "test-principal-id");

        assertNotNull(token, "a silent cache hit must return a token");
        assertEquals("cached-access-token", token.getAccessToken(), "the cached access token must be returned");
        assertEquals(expiresOn, token.getExpiresOn(), "the cached token expiry must be preserved");
        verify(clientApplication, never()).acquireToken(any(ClientCredentialParameters.class));
    }

    /**
     * Fast-path cache miss: when no valid token is cached, MSAL's silent future completes with an
     * {@link ExecutionException}. {@link SQLServerMSAL4JUtils#acquireTokenSilentlyOrNull} must
     * translate that into {@code null} so the caller falls through to the gated slow path that
     * performs the real Entra ID token acquisition.
     */
    @Test
    public void testAcquireTokenSilentlyReturnsNullOnMiss() throws Exception {
        ConfidentialClientApplication clientApplication = mock(ConfidentialClientApplication.class);
        CompletableFuture<IAuthenticationResult> missFuture = new CompletableFuture<>();
        missFuture.completeExceptionally(new IllegalStateException("no cached token for scopes"));
        when(clientApplication.acquireTokenSilently(any(SilentParameters.class))).thenReturn(missFuture);

        Set<String> scopes = Collections.singleton("https://database.windows.net/.default");

        SqlAuthenticationToken token = SQLServerMSAL4JUtils.acquireTokenSilentlyOrNull(clientApplication, scopes,
                20000, "test-principal-id");

        assertNull(token, "a silent cache miss must return null so the caller takes the gated slow path");
    }
}
