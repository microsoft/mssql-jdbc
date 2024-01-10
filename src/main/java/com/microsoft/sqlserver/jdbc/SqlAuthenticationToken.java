/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.Serializable;
import java.util.Date;


/**
 * Provides an implementation of a SqlAuthenticationToken
 */
public class SqlAuthenticationToken implements Serializable {

    /** Always update serialVersionUID when prompted **/
    private static final long serialVersionUID = -1343105491285383937L;

    /** The token expiration date. **/
    private final Date expiresOn;

    /** The access token string. **/
    private final String accessToken;

    /**
     * Contructs a SqlAuthentication token.
     *
     * @param accessToken
     *        The access token string.
     * @param expiresOn
     *        The expiration date in seconds since the unix epoch.
     */
    public SqlAuthenticationToken(String accessToken, long expiresOn) {
        this.accessToken = accessToken;
        this.expiresOn = new Date(expiresOn);
    }

    /**
     * Contructs a SqlAuthentication token.
     *
     * @param accessToken
     *        The access token string.
     * @param expiresOn
     *        The expiration date.
     */
    public SqlAuthenticationToken(String accessToken, Date expiresOn) {
        this.accessToken = accessToken;
        this.expiresOn = expiresOn;
    }

    /**
     * Returns the expiration date of the token.
     *
     * @return The token expiration date.
     */
    public Date getExpiresOn() {
        return expiresOn;
    }

    /**
     * Returns the access token string.
     *
     * @return The access token.
     */
    public String getAccessToken() {
        return accessToken;
    }

    public String toString() {
        return "accessToken hashCode: " + accessToken.hashCode() + " expiresOn: " + expiresOn.toInstant().toString();
    }
}
