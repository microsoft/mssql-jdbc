/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;

import org.junit.jupiter.api.Test;

import com.microsoft.aad.msal4j.InteractiveRequestParameters;


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
}
