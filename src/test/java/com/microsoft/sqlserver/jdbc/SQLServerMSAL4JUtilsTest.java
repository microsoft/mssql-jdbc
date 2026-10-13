/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.text.MessageFormat;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.microsoft.aad.msal4j.InteractiveRequestParameters;


/**
 * Unit tests for {@link SQLServerMSAL4JUtils} that do not require a SQL Server
 * connection or an interactive Azure AD sign-in.
 */
public class SQLServerMSAL4JUtilsTest {
    private static final String USER = "user@contoso.com";
    private static final String AUTHENTICATION = "ActiveDirectoryPassword";

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
        String spn = "https://database.windows.net";

        InteractiveRequestParameters params = SQLServerMSAL4JUtils.buildInteractiveRequestParameters(
                new URI(SQLServerMSAL4JUtils.REDIRECTURI), USER, spn);

        assertNotNull(params.extraQueryParameters(), "extraQueryParameters must not be null");
        assertEquals("form_post", params.extraQueryParameters().get("response_mode"),
                "response_mode must be form_post to avoid leaking the auth code via the redirect URL");
        assertEquals(USER, params.loginHint(), "loginHint should be propagated from the connection user");
        assertTrue(params.scopes().contains(spn + SQLServerMSAL4JUtils.SLASH_DEFAULT),
                "scopes must contain the resource SPN with the /.default suffix");
    }

    @Test
    public void testTokenAcquisitionTimeoutIsMapped() {
        TimeoutException timeout = new TimeoutException("token acquisition timed out");
        SQLServerException exception = SQLServerMSAL4JUtils.mapTokenAcquisitionException(
                new ExecutionException(timeout), USER, AUTHENTICATION);

        assertEquals(SQLServerException.getErrString("R_AADTokenAcquisitionTimeout"), exception.getMessage());
        assertSame(timeout, exception.getCause());
    }

    @Test
    public void testDirectTokenAcquisitionInterruptionIsMappedAndRestored() {
        InterruptedException interrupted = new InterruptedException("token acquisition interrupted");

        try {
            SQLServerException exception = SQLServerMSAL4JUtils.mapTokenAcquisitionException(
                    interrupted, USER, AUTHENTICATION);

            assertEquals(SQLServerException.getErrString("R_AADTokenAcquisitionInterrupted"),
                    exception.getMessage());
            assertSame(interrupted, exception.getCause());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void testWorkerTokenAcquisitionInterruptionDoesNotInterruptCaller() {
        InterruptedException interrupted = new InterruptedException("worker token acquisition interrupted");
        SQLServerException exception = SQLServerMSAL4JUtils.mapTokenAcquisitionException(
                new ExecutionException(interrupted), USER, AUTHENTICATION);

        assertEquals(SQLServerException.getErrString("R_AADTokenAcquisitionInterrupted"),
                exception.getMessage());
        assertSame(interrupted, exception.getCause());
        assertFalse(Thread.currentThread().isInterrupted());
    }

    @Test
    public void testDirectRuntimeFailurePreservesCause() {
        RuntimeException failure = new RuntimeException("{\"error\":\"synchronous MSAL failure\"}");
        SQLServerException exception = SQLServerMSAL4JUtils
                .mapTokenAcquisitionException(failure, USER, AUTHENTICATION);

        assertTrue(exception.getMessage().startsWith(MessageFormat.format(
                SQLServerException.getErrString("R_MSALExecution"), USER, AUTHENTICATION)));
        assertTrue(exception.getMessage().contains(failure.getMessage()));
        assertSame(failure, exception.getCause());
    }

    @Test
    public void testWrappedAuthenticationFailurePreservesCauseChain() {
        RuntimeException authenticationFailure = new RuntimeException("authentication failed");
        SQLServerException exception = SQLServerMSAL4JUtils.mapTokenAcquisitionException(
                new ExecutionException(authenticationFailure), USER, AUTHENTICATION);

        assertNotNull(exception.getCause());
        assertSame(authenticationFailure, exception.getCause().getCause().getCause());
    }
}
