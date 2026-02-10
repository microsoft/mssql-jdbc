/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.testframework;

import com.microsoft.sqlserver.jdbc.SQLServerAccessTokenCallback;
import com.microsoft.sqlserver.jdbc.SqlAuthenticationToken;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Access token callback implementation that obtains access tokens via Azure CLI.
 * 
 * This class can be used with the accessTokenCallbackClass connection string property:
 * jdbc:sqlserver://server;accessTokenCallbackClass=com.microsoft.sqlserver.testframework.EnvAccessTokenCallback
 * 
 * Prerequisites:
 * - Azure CLI must be installed and in PATH
 * - User must be logged in via 'az login' (or running in AzureCLI@2 task in Azure DevOps)
 * 
 * Token caching:
 * - Tokens are cached and reused until they are about to expire
 * - A new token is fetched 5 minutes before the cached token expires
 * - Thread-safe implementation using ReentrantLock
 */
public class EnvAccessTokenCallback implements SQLServerAccessTokenCallback {

    private static final Logger LOGGER = Logger.getLogger(EnvAccessTokenCallback.class.getName());
    
    // Token validity period - Azure CLI tokens are valid for ~60-70 minutes
    // We'll set expiry to 55 minutes to be safe
    private static final long TOKEN_VALIDITY_MS = TimeUnit.MINUTES.toMillis(55);
    
    // Refresh token 5 minutes before expiry
    private static final long TOKEN_REFRESH_BUFFER_MS = TimeUnit.MINUTES.toMillis(5);
    
    // Process timeout in seconds
    private static final int PROCESS_TIMEOUT_SECONDS = 30;
    
    // Cached token and its expiry time (thread-safe access via lock)
    private static volatile SqlAuthenticationToken cachedToken = null;
    private static volatile long tokenExpiryTime = 0;
    
    // Lock for thread-safe token refresh
    private static final ReentrantLock tokenLock = new ReentrantLock();

    /**
     * Default constructor required for reflection-based instantiation by the driver.
     */
    public EnvAccessTokenCallback() {
        // Required public no-arg constructor
    }

    /**
     * Returns the access token for Azure SQL Database authentication.
     * 
     * Uses cached token if available and not expired. Otherwise, fetches a new token
     * from Azure CLI using 'az account get-access-token' command.
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
            
            // Fetch new token from Azure CLI
            LOGGER.fine("Fetching new access token from Azure CLI");
            String token = fetchTokenFromAzureCli();
            
            long expiresOn = System.currentTimeMillis() + TOKEN_VALIDITY_MS;
            cachedToken = new SqlAuthenticationToken(token, expiresOn);
            tokenExpiryTime = expiresOn;
            
            LOGGER.fine("Successfully obtained new access token from Azure CLI");
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
     * Fetches an access token from Azure CLI by executing 'az account get-access-token'.
     * 
     * @return the access token string
     * @throws RuntimeException if the Azure CLI command fails
     */
    private String fetchTokenFromAzureCli() {
        try {
            ProcessBuilder pb = new ProcessBuilder(
                "az", "account", "get-access-token",
                "--resource", "https://database.windows.net/",
                "--query", "accessToken",
                "-o", "tsv"
            );
            pb.redirectErrorStream(true);
            
            Process p = pb.start();
            
            StringBuilder output = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line);
                }
            }
            
            boolean completed = p.waitFor(PROCESS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!completed) {
                p.destroyForcibly();
                throw new RuntimeException("Azure CLI command timed out after " + PROCESS_TIMEOUT_SECONDS + " seconds");
            }
            
            int exitCode = p.exitValue();
            String token = output.toString().trim();
            
            if (exitCode != 0 || token.isEmpty()) {
                throw new RuntimeException("Azure CLI failed with exit code " + exitCode + ": " + token);
            }
            
            // Basic validation - JWT tokens have 3 parts separated by dots
            if (!token.contains(".")) {
                throw new RuntimeException("Invalid token format received from Azure CLI: " + 
                    (token.length() > 50 ? token.substring(0, 50) + "..." : token));
            }
            
            return token;
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for Azure CLI", e);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to get access token via Azure CLI", e);
            throw new RuntimeException("Failed to get access token via Azure CLI: " + e.getMessage(), e);
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
