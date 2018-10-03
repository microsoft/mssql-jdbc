/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.Date;


class SqlFedAuthToken {
    Date expiresOn;
    String accessToken;
    final String refreshToken;

    SqlFedAuthToken(final String accessToken, final long expiresIn, final String refreshToken) {
        this.accessToken = accessToken;

        Date now = new Date();
        now.setTime(now.getTime() + (expiresIn * 1000));
        this.expiresOn = now;

        this.refreshToken = refreshToken;
    }

    SqlFedAuthToken(final String accessToken, final Date expiresOn, final String refreshToken) {
        this.accessToken = accessToken;
        this.expiresOn = expiresOn;
        this.refreshToken = refreshToken;
    }

    void updateAccessToken(String accessToken, Date expiresOnDate) {
        this.accessToken = accessToken;
        this.expiresOn = expiresOnDate;

    }
}
