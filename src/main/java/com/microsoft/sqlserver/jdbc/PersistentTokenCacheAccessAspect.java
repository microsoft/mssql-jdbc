/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import com.microsoft.aad.msal4j.ITokenCacheAccessAspect;
import com.microsoft.aad.msal4j.ITokenCacheAccessContext;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Access aspect for accessing the token cache.
 * 
 * MSAL token cache does not persist beyond lifetime of the application. This class implements the
 * ITokenCacheAccessAspect interface to persist the token cache between application instances so subsequent
 * authentications can use silent authentication if the user account is in the token cache.
 * 
 * @see <a href="https://aka.ms/msal4j-token-cache">https://aka.ms/msal4j-token-cache</a>
 */
public class PersistentTokenCacheAccessAspect implements ITokenCacheAccessAspect {
    private static PersistentTokenCacheAccessAspect instance;
    private final Lock lock = new ReentrantLock();

    static final long TIME_TO_LIVE = 86400000L; // Token cache time to live (24 hrs).
    private long expiryTime;

    /**
     * default constructor
     */
    public PersistentTokenCacheAccessAspect() {
        // default constructor
    }

    static PersistentTokenCacheAccessAspect getInstance() {
        if (instance == null) {
            instance = new PersistentTokenCacheAccessAspect();
        }
        return instance;
    }

    /**
     * Token cache in JSON format
     */
    private String cache = null;

    @Override
    public void beforeCacheAccess(ITokenCacheAccessContext iTokenCacheAccessContext) {
        lock.lock();
        try {
            if (null != cache && null != iTokenCacheAccessContext && null != iTokenCacheAccessContext.tokenCache()) {
                iTokenCacheAccessContext.tokenCache().deserialize(cache);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void afterCacheAccess(ITokenCacheAccessContext iTokenCacheAccessContext) {
        lock.lock();
        try {
            if (null != iTokenCacheAccessContext && iTokenCacheAccessContext.hasCacheChanged()
                    && null != iTokenCacheAccessContext.tokenCache())
                cache = iTokenCacheAccessContext.tokenCache().serialize();
        } finally {
            lock.unlock();
        }

    }

    /**
     * Get expiry time
     * 
     * @return expiry time
     */
    public long getExpiryTime() {
        return this.expiryTime;
    }

    /**
     * Set expiry time
     * 
     * @param expiryTime
     *        expiry time
     */
    public void setExpiryTime(long expiryTime) {
        this.expiryTime = expiryTime;
    }

    /**
     * Clears User token cache. This will clear all account info so interactive login will be required on the next
     * request to acquire an access token.
     */
    static void clearUserTokenCache() {
        if (null != instance.cache && !instance.cache.isEmpty()) {
            instance.cache = null;
        }
    }
}
