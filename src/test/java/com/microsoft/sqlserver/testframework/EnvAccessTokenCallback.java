/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.testframework;

import com.microsoft.sqlserver.jdbc.SQLServerAccessTokenCallback;
import com.microsoft.sqlserver.jdbc.SqlAuthenticationToken;


/**
 * Access token callback implementation that reads the access token from the ACCESS_TOKEN environment variable.
 * 
 * This class can be used with the accessTokenCallbackClass connection string property:
 * jdbc:sqlserver://server;accessTokenCallbackClass=com.microsoft.sqlserver.testframework.EnvAccessTokenCallback
 * 
 * When the ACCESS_TOKEN environment variable is set, this callback will provide the token to the driver.
 * When it's not set, the callback returns null and the driver will use other authentication methods.
 */
public class EnvAccessTokenCallback implements SQLServerAccessTokenCallback {

    private static final String ACCESS_TOKEN_ENV_VAR = "ACCESS_TOKEN";
    
    // Token validity period in milliseconds (4 hours)
    private static final long TOKEN_VALIDITY_MS = 14400000;

    /**
     * Default constructor required for reflection-based instantiation by the driver.
     */
    public EnvAccessTokenCallback() {
        // Required public no-arg constructor
    }

    /**
     * Returns the access token from the ACCESS_TOKEN environment variable.
     * 
     * @param spn Service Principal Name (not used when reading from env var)
     * @param stsurl Security Token Service URL (not used when reading from env var)
     * @return SqlAuthenticationToken if ACCESS_TOKEN env var is set, null otherwise
     */
    @Override
    public SqlAuthenticationToken getAccessToken(String spn, String stsurl) {
        String accessToken = System.getenv(ACCESS_TOKEN_ENV_VAR);
        
        if (accessToken != null && !accessToken.isEmpty()) {
            // Set expiry to 4 hours from now
            long expiresOn = System.currentTimeMillis() + TOKEN_VALIDITY_MS;
            return new SqlAuthenticationToken(accessToken, expiresOn);
        }
        
        // Return null to let the driver use other authentication methods
        return null;
    }
}
