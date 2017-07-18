/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientCredential;
import com.microsoft.azure.keyvault.authentication.KeyVaultCredentials;

/**
 * 
 * An implementation of ServiceClientCredentials that supports automatic bearer token refresh.
 *
 */
class KeyVaultCredential extends KeyVaultCredentials {

    String clientId = null;
    String clientKey = null;

    KeyVaultCredential(String clientId,
            String clientKey) {
        this.clientId = clientId;
        this.clientKey = clientKey;
    }

    public String doAuthenticate(String authorization,
            String resource,
            String scope) {
        AuthenticationResult token = getAccessTokenFromClientCredentials(authorization, resource, clientId, clientKey);
        return token.getAccessToken();
    }

    private static AuthenticationResult getAccessTokenFromClientCredentials(String authorization,
            String resource,
            String clientId,
            String clientKey) {
        AuthenticationContext context = null;
        AuthenticationResult result = null;
        ExecutorService service = null;
        try {
            service = Executors.newFixedThreadPool(1);
            context = new AuthenticationContext(authorization, false, service);
            ClientCredential credentials = new ClientCredential(clientId, clientKey);
            Future<AuthenticationResult> future = context.acquireToken(resource, credentials, null);
            result = future.get();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            service.shutdown();
        }

        if (result == null) {
            throw new RuntimeException("authentication result was null");
        }
        return result;
    }
}
