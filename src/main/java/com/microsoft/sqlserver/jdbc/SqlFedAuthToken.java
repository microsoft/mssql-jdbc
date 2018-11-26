/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.Date;


class SqlFedAuthToken {
    final Date expiresOn;
    final String accessToken;

    SqlFedAuthToken(String accessToken, long expiresIn) {
        this.accessToken = accessToken;

        Date now = new Date();
        now.setTime(now.getTime() + (expiresIn * 1000));
        this.expiresOn = now;
    }

    SqlFedAuthToken(String accessToken, Date expiresOn) {
        this.accessToken = accessToken;
        this.expiresOn = expiresOn;
    }
}
