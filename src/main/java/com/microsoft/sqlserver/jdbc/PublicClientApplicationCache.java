/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import com.microsoft.aad.msal4j.PublicClientApplication;


/**
 * 
 * An entry in the map containing the public client application and the associated executor service used by the
 * application to executor requests
 *
 */
class PublicClientApplicationEntry {
    private PublicClientApplication pca;
    private ExecutorService executorService;

    /**
     * Creates a public client application entry
     * 
     * @param pca
     *        public client application
     * @param executorService
     *        executor service for the public client application
     */
    PublicClientApplicationEntry(PublicClientApplication pca, ExecutorService executorService) {
        this.pca = pca;
        this.executorService = executorService;
    }

    /**
     * Gets the public client application in the entry
     * 
     * @return public client application
     */
    PublicClientApplication getPublicClientApplication() {
        return this.pca;
    }

    /**
     * Gets the executor service for the public client application in the entry
     * 
     * @return executor service
     */
    ExecutorService getExecutorService() {
        return this.executorService;
    }
}


/**
 * Cache for public client applications
 * 
 *
 */
public class PublicClientApplicationCache {

    private static ConcurrentHashMap<PublicClientApplicationKey, PublicClientApplicationEntry> cache = new ConcurrentHashMap<PublicClientApplicationKey, PublicClientApplicationEntry>();

    /**
     * Get the entry to which the specified key is mapped
     * 
     * @return entry to which key is mapped, or null if map contains no mapping for the key
     */
    PublicClientApplicationEntry get(PublicClientApplicationKey key) {
        return cache.get(key);
    }

    /**
     * Maps the specified key to the entry
     * 
     * @param key
     *        public client application key
     * @param entry
     *        public client application entry
     */
    void put(PublicClientApplicationKey key, PublicClientApplicationEntry entry) {
        cache.putIfAbsent(key, entry);
    }

    /**
     * Clears cached instances of Public Client Application maintained by driver to access cached tokens from underlying
     * token provider library.
     */
    public static void clearUserTokenCache() {
        if (null != cache && !cache.isEmpty()) {
            for (ConcurrentHashMap.Entry<PublicClientApplicationKey, PublicClientApplicationEntry> entry : cache
                    .entrySet()) {
                ExecutorService executorService = entry.getValue().getExecutorService();
                if (null != executorService) {
                    executorService.shutdown();
                }
            }
            cache.clear();
        }
    }
}
