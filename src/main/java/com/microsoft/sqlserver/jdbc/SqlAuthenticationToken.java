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

    /**
     * Always update serialVersionUID when prompted
     */
    private static final long serialVersionUID = -1343105491285383937L;

    private final Date expiresOn;
    private final String accessToken;

    public SqlAuthenticationToken(String accessToken, long expiresIn) {
        this.accessToken = accessToken;

        Date now = new Date();
        now.setTime(now.getTime() + (expiresIn * 1000));
        this.expiresOn = now;
    }

    public SqlAuthenticationToken(String accessToken, Date expiresOn) {
        this.accessToken = accessToken;
        this.expiresOn = expiresOn;
    }

    public Date getExpiresOn() {
        return expiresOn;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public String toString() {
        return "accessToken hashCode: " + accessToken.hashCode() + " expiresOn: " + expiresOn.toInstant().toString();
    }
}
