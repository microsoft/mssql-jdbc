/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.UUID;

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

    /**
     * The pooled-semaphore gate maps each credential onto a fixed pool slot via its hashed secret.
     * Every mapping must land inside the pool bounds {@code [0, SEM_POOL_SIZE)} -- including hashed
     * secrets whose {@link String#hashCode()} is negative, which is why the mapping masks off the
     * sign bit before the modulo. An out-of-bounds index would throw
     * {@link ArrayIndexOutOfBoundsException} on the token-acquisition path.
     */
    @Test
    public void testPoolIndexIsAlwaysWithinBounds() {
        for (int i = 0; i < 10000; i++) {
            String hashedSecret = UUID.randomUUID().toString();
            int index = SQLServerMSAL4JUtils.poolIndexForHashedSecret(hashedSecret);
            assertTrue(index >= 0 && index < SQLServerMSAL4JUtils.SEM_POOL_SIZE,
                    "pool index " + index + " for '" + hashedSecret + "' must be in [0, SEM_POOL_SIZE)");
        }
    }

    /**
     * Guards the sign-bit masking explicitly: a string whose {@code hashCode()} is negative must
     * still map to a non-negative slot. Without the {@code & 0x7fffffff} mask the modulo would
     * yield a negative index.
     */
    @Test
    public void testPoolIndexHandlesNegativeHashCode() {
        // "polygenelubricants" is a well-known String with hashCode == Integer.MIN_VALUE.
        String negativeHash = "polygenelubricants";
        assertTrue(negativeHash.hashCode() < 0, "precondition: this string must hash to a negative value");

        int index = SQLServerMSAL4JUtils.poolIndexForHashedSecret(negativeHash);
        assertTrue(index >= 0 && index < SQLServerMSAL4JUtils.SEM_POOL_SIZE,
                "a negative hashCode must still map to a slot in [0, SEM_POOL_SIZE)");
    }

    /**
     * The slot mapping must be deterministic: the same credential always resolves to the same pool
     * slot, so concurrent callers for one service principal serialise on the same semaphore.
     */
    @Test
    public void testPoolIndexIsDeterministic() {
        String hashedSecret = UUID.randomUUID().toString();
        int first = SQLServerMSAL4JUtils.poolIndexForHashedSecret(hashedSecret);
        for (int i = 0; i < 100; i++) {
            assertEquals(first, SQLServerMSAL4JUtils.poolIndexForHashedSecret(hashedSecret),
                    "the same hashed secret must always map to the same pool slot");
        }
    }
}
