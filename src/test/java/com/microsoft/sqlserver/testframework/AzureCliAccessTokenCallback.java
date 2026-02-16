/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.testframework;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.AzureCliCredential;
import com.azure.identity.AzureCliCredentialBuilder;
import com.azure.identity.ChainedTokenCredential;
import com.azure.identity.ChainedTokenCredentialBuilder;
import com.azure.identity.EnvironmentCredential;
import com.azure.identity.EnvironmentCredentialBuilder;
import com.microsoft.sqlserver.jdbc.SQLServerAccessTokenCallback;
import com.microsoft.sqlserver.jdbc.SqlAuthenticationToken;

import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Access token callback implementation that uses Azure Identity SDK's AzureCliCredential
 * to obtain access tokens for Azure SQL Database authentication.
 * 
 * This class can be used with the accessTokenCallbackClass connection string property:
 * jdbc:sqlserver://server;accessTokenCallbackClass=com.microsoft.sqlserver.testframework.AzureCliAccessTokenCallback
 * 
 * This credential provider tries authentication methods in this order:
 * 1. EnvironmentCredential (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID)
 * 2. AzureCliCredential (uses 'az login' credentials - works with AzureCLI@2 task)
 * 
 * NOTE: ManagedIdentityCredential is intentionally excluded to ensure that when running
 * in Azure DevOps with AzureCLI@2 task, the credentials from the service connection
 * are used rather than any managed identity attached to the VM/agent pool.
 * 
 * Token caching:
 * - We cache tokens to avoid repeated SDK calls
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
    private static volatile TokenCredential credential = null;

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
     * using AzureCliCredential from the Azure Identity SDK.
     * 
     * @param spn Service Principal Name (not used - we use the standard Azure SQL scope)
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
            
            // Fetch new token using AzureCliCredential (or EnvironmentCredential if env vars are set)
            LOGGER.fine("Fetching new access token using AzureCliCredential");
            AccessToken token = fetchTokenFromAzureIdentity();
            
            // Debug: Print token identity claims
            printTokenIdentity(token.getToken());
            
            long expiresOn = token.getExpiresAt().toInstant().toEpochMilli();
            cachedToken = new SqlAuthenticationToken(token.getToken(), expiresOn);
            tokenExpiryTime = expiresOn;
            
            LOGGER.fine("Successfully obtained new access token");
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
     * Gets or creates the credential instance (lazy singleton).
     * 
     * Creates a ChainedTokenCredential that tries:
     * 1. EnvironmentCredential - for CI/CD with service principal env vars
     * 2. AzureCliCredential - for AzureCLI@2 task authenticated sessions
     * 
     * ManagedIdentityCredential is intentionally excluded to ensure the
     * AzureCLI@2 service connection identity is used, not any MI on the VM.
     * 
     * @return TokenCredential instance
     */
    public static TokenCredential getCredential() {
        if (credential == null) {
            synchronized (AzureCliAccessTokenCallback.class) {
                if (credential == null) {
                    // Build a credential chain that excludes ManagedIdentityCredential
                    // This ensures AzureCLI@2 task credentials are used, not VM's MI
                    EnvironmentCredential envCredential = new EnvironmentCredentialBuilder().build();
                    AzureCliCredential cliCredential = new AzureCliCredentialBuilder().build();
                    
                    credential = new ChainedTokenCredentialBuilder()
                            .addFirst(envCredential)
                            .addLast(cliCredential)
                            .build();
                }
            }
        }
        return credential;
    }
    
    /**
     * Fetches an access token using the credential chain (Environment or AzureCli).
     * 
     * @return AccessToken from Azure Identity SDK
     * @throws RuntimeException if token acquisition fails
     */
    private AccessToken fetchTokenFromAzureIdentity() {
        try {
            TokenRequestContext context = new TokenRequestContext().addScopes(AZURE_SQL_SCOPE);
            AccessToken token = getCredential().getToken(context).block();
            
            if (token == null) {
                throw new RuntimeException("Credential chain returned null token");
            }
            
            return token;
            
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to get access token", e);
            throw new RuntimeException("Failed to get access token: " + e.getMessage(), e);
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
    
    /**
     * Debug method to print identity information from the JWT token.
     * Extracts and prints key claims: oid (object id), appid (application id), 
     * sub (subject), and upn (user principal name) to identify which identity acquired the token.
     */
    private static void printTokenIdentity(String token) {
        try {
            // JWT format: header.payload.signature
            String[] parts = token.split("\\.");
            if (parts.length >= 2) {
                String payload = new String(java.util.Base64.getUrlDecoder().decode(parts[1]));
                
                System.out.println("=== ACCESS TOKEN IDENTITY (DEBUG) ===");
                
                // Extract specific claims for identity
                String oid = extractClaim(payload, "oid");
                String appid = extractClaim(payload, "appid");
                String sub = extractClaim(payload, "sub");
                String upn = extractClaim(payload, "upn");
                String appidacr = extractClaim(payload, "appidacr");
                String idtyp = extractClaim(payload, "idtyp");
                
                System.out.println("oid (Object ID): " + oid);
                System.out.println("appid (Application/Client ID): " + appid);
                System.out.println("sub (Subject): " + sub);
                System.out.println("upn (User Principal Name): " + upn);
                System.out.println("appidacr (App ID ACR): " + appidacr);
                System.out.println("idtyp (Identity Type): " + idtyp);
                System.out.println("=====================================");
            }
        } catch (Exception e) {
            System.out.println("Could not decode token for identity: " + e.getMessage());
        }
    }
    
    /**
     * Simple JSON claim extractor (avoids adding JSON library dependency).
     */
    private static String extractClaim(String json, String claim) {
        String searchKey = "\"" + claim + "\":\"";
        int start = json.indexOf(searchKey);
        if (start == -1) {
            return "not present";
        }
        start += searchKey.length();
        int end = json.indexOf("\"", start);
        if (end == -1) {
            return "parse error";
        }
        return json.substring(start, end);
    }
}
