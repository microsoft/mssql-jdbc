/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.Objects;


/**
 * Key for mapping public client application entries
 *
 */
class PublicClientApplicationKey {
    String authority;
    String redirectUri;
    String applicationClientId;

    /**
     * Creates a public client application key
     * 
     * @param authority
     *        URL of the authenticating authority or security token service (STS) from which MSAL will acquire security
     *        tokens
     * @param redirectUri
     *        Redirect URI where MSAL will listen to for the authorization code returned by Azure AD
     * @param applicationClientId
     *        application client id
     */
    PublicClientApplicationKey(String authority, String redirectUri, String applicationClientId) {
        this.authority = authority;
        this.redirectUri = redirectUri;
        this.applicationClientId = applicationClientId;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (!(o instanceof PublicClientApplicationKey)) {
            return false;
        }

        PublicClientApplicationKey key = (PublicClientApplicationKey) o;
        return authority.equals(key.authority) && redirectUri.equals(redirectUri)
                && applicationClientId.equals(key.applicationClientId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(authority, redirectUri, applicationClientId);
    }
}
