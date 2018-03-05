/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.Map;

import org.apache.http.Header;
import org.apache.http.message.BasicHeader;

import com.microsoft.azure.keyvault.authentication.KeyVaultCredentials;
import com.microsoft.windowsazure.core.pipeline.filter.ServiceRequestContext;

/**
 * 
 * An implementation of ServiceClientCredentials that supports automatic bearer token refresh.
 *
 */
class KeyVaultCredential extends KeyVaultCredentials {

    // this is the only supported access token type
    // https://msdn.microsoft.com/en-us/library/azure/dn645538.aspx
    private final String accessTokenType = "Bearer";

    SQLServerKeyVaultAuthenticationCallback authenticationCallback = null;
    String clientId = null;
    String clientKey = null;
    String accessToken = null;

    KeyVaultCredential(SQLServerKeyVaultAuthenticationCallback authenticationCallback) {
        this.authenticationCallback = authenticationCallback;
    }

    /**
     * Authenticates the service request
     * 
     * @param request
     *            the ServiceRequestContext
     * @param challenge
     *            used to get the accessToken
     * @return BasicHeader
     */
    @Override
    public Header doAuthenticate(ServiceRequestContext request,
            Map<String, String> challenge) {
        assert null != challenge;

        String authorization = challenge.get("authorization");
        String resource = challenge.get("resource");

        accessToken = authenticationCallback.getAccessToken(authorization, resource, "");
        return new BasicHeader("Authorization", accessTokenType + " " + accessToken);
    }

    void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }

}
