/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * Provides a callback delegate which is to be implemented by the client code
 * 
 */
public interface SQLServerKeyVaultAuthenticationCallback {

    /**
     * Returns the acesss token of the authentication request
     * 
     * @param authority
     *        - Identifier of the authority, a URL.
     * @param resource
     *        - Identifier of the target resource that is the recipient of the requested token, a URL.
     * @param scope
     *        - The scope of the authentication request.
     * @return access token
     */
    String getAccessToken(String authority, String resource, String scope);
}
