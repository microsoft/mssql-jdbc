/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.fedauth;

import com.microsoft.aad.msal4j.ITokenCacheAccessAspect;
import com.microsoft.aad.msal4j.ITokenCacheAccessContext;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.Tag;
import com.microsoft.sqlserver.testframework.Constants;


@Tag(Constants.fedAuth)
public class FedauthTokenCache implements ITokenCacheAccessAspect {
    private static FedauthTokenCache instance = new FedauthTokenCache();
    private final Lock lock = new ReentrantLock();

    private FedauthTokenCache() {}

    static FedauthTokenCache getInstance() {
        return instance;
    }

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

    static void clearUserTokenCache() {
        if (null != instance.cache && !instance.cache.isEmpty()) {
            instance.cache = null;
        }
    }
}
