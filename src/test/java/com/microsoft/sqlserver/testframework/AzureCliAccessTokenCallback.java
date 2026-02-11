/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.testframework;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.microsoft.sqlserver.jdbc.SQLServerAccessTokenCallback;
import com.microsoft.sqlserver.jdbc.SqlAuthenticationToken;

import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Access token callback implementation that uses Azure Identity SDK's DefaultAzureCredential
 * to obtain access tokens for Azure SQL Database authentication.
 * 
 * This class can be used with the accessTokenCallbackClass connection string property:
 * jdbc:sqlserver://server;accessTokenCallbackClass=com.microsoft.sqlserver.testframework.AzureCliAccessTokenCallback
 * 
 * DefaultAzureCredential automatically tries multiple authentication methods:
 * 1. EnvironmentCredential (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID)
 * 2. WorkloadIdentityCredential (for Kubernetes workloads)
 * 3. ManagedIdentityCredential (for Azure-hosted environments)
 * 4. AzureCliCredential (uses 'az login' credentials - works with AzureCLI@2 task)
 * 5. AzurePowerShellCredential (uses 'Connect-AzAccount' credentials)
 * 6. AzureDeveloperCliCredential (uses 'azd auth login' credentials)
 * 
 * When running in Azure DevOps with AzureCLI@2 task using a service connection
 * (backed by managed identity), the AzureCliCredential will automatically pick up
 * the authenticated session.
 * 
 * Token caching:
 * - DefaultAzureCredential handles token caching internally
 * - We add an additional layer of caching to avoid SDK overhead
 * - Tokens are refreshed 5 minutes before expiry
 * - Thread-safe implementation using ReentrantLock
 */
public class AzureCliAccessTokenCallback implements SQLServerAccessTokenCallback {

    private static final Logger LOGGER = Logger.getLogger(AzureCliAccessTokenCallback.class.getName());
    
    // Azure SQL Database scope for token requests
    private static final String AZURE_SQL_SCOPE = "https://database.windows.net/.default";
    
    // Refresh token 5 minutes before expiry
    private static final long TOKEN_REFRESH_BUFFER_MS = 5 * 60 * 1000; // 5 minutes
    
    // Cached token and its expiry time (thread-safe access via lock)
    private static volatile SqlAuthenticationToken cachedToken = null;
    private static volatile long tokenExpiryTime = 0;
    
    // Lock for thread-safe token refresh
    private static final ReentrantLock tokenLock = new ReentrantLock();
    
    // Cached credential instance (thread-safe, lazy initialization)
    private static volatile DefaultAzureCredential credential = null;

    /**
     * Default constructor required for reflection-based instantiation by the driver.
     */
    public AzureCliAccessTokenCallback() {
        // Required public no-arg constructor
    }

    /**
     * Returns the access token for Azure SQL Database authentication.
     * 
     * Uses cached token if available and not expired. Otherwise, fetches a new token
     * using DefaultAzureCredential from the Azure Identity SDK.
     * 
     * @param spn Service Principal Name (not used)
     * @param stsurl Security Token Service URL (not used)
     * @return SqlAuthenticationToken with the access token
     * @throws RuntimeException if token acquisition fails
     */
    @Override
    public SqlAuthenticationToken getAccessToken(String spn, String stsurl) {
        // Check if cached token is still valid (with buffer time)
        if (isTokenValid()) {
            LOGGER.fine("Using cached access token");
            return cachedToken;
        }
        
        // Need to refresh token - acquire lock
        tokenLock.lock();
        try {
            // Double-check after acquiring lock (another thread might have refreshed)
            if (isTokenValid()) {
                LOGGER.fine("Using cached access token (refreshed by another thread)");
                return cachedToken;
            }
            
            // Fetch new token using DefaultAzureCredential
            LOGGER.fine("Fetching new access token using DefaultAzureCredential");
            AccessToken token = fetchTokenFromAzureIdentity();
            
            long expiresOn = token.getExpiresAt().toInstant().toEpochMilli();
            cachedToken = new SqlAuthenticationToken(token.getToken(), expiresOn);
            tokenExpiryTime = expiresOn;
            
            LOGGER.fine("Successfully obtained new access token using DefaultAzureCredential");
            return cachedToken;
            
        } finally {
            tokenLock.unlock();
        }
    }
    
    /**
     * Checks if the cached token is still valid (not expired and has buffer time remaining).
     * 
     * @return true if token is valid and doesn't need refresh
     */
    private static boolean isTokenValid() {
        return cachedToken != null && 
               System.currentTimeMillis() < (tokenExpiryTime - TOKEN_REFRESH_BUFFER_MS);
    }
    
    /**
     * Gets or creates the DefaultAzureCredential instance (lazy singleton).
     * 
     * @return DefaultAzureCredential instance
     */
    private static DefaultAzureCredential getCredential() {
        if (credential == null) {
            synchronized (AzureCliAccessTokenCallback.class) {
                if (credential == null) {
                    credential = new DefaultAzureCredentialBuilder().build();
                }
            }
        }
        return credential;
    }
    
    /**
     * Fetches an access token using DefaultAzureCredential from Azure Identity SDK.
     * 
     * @return AccessToken from Azure Identity SDK
     * @throws RuntimeException if token acquisition fails
     */
    private AccessToken fetchTokenFromAzureIdentity() {
        try {
            TokenRequestContext context = new TokenRequestContext().addScopes(AZURE_SQL_SCOPE);
            AccessToken token = getCredential().getToken(context).block();
            
            if (token == null) {
                throw new RuntimeException("DefaultAzureCredential returned null token");
            }
            
            return token;
            
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to get access token using DefaultAzureCredential", e);
            throw new RuntimeException("Failed to get access token using DefaultAzureCredential: " + e.getMessage(), e);
        }
    }
    
    /**
     * Clears the cached token. Useful for testing or forcing a token refresh.
     */
    public static void clearCachedToken() {
        tokenLock.lock();
        try {
            cachedToken = null;
            tokenExpiryTime = 0;
            LOGGER.fine("Cached token cleared");
        } finally {
            tokenLock.unlock();
        }
    }
}
