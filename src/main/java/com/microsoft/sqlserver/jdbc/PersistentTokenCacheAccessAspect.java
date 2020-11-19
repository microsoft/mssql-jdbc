/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import com.microsoft.aad.msal4j.ITokenCacheAccessAspect;
import com.microsoft.aad.msal4j.ITokenCacheAccessContext;


/**
 * Access aspect for accessing the token cache.
 * 
 * MSAL token cache only lasts for the lifetime of the application. This class implements the ITokenCacheAccessAspect
 * interface to persist the token cache between application instances so subsequent authentications will use the silent
 * authentication flow if the user account is in the token cache.
 * 
 * @see <a href="https://aka.ms/msal4j-token-cache">https://aka.ms/msal4j-token-cache</a>
 */
class PersistentTokenCacheAccessAspect implements ITokenCacheAccessAspect {
    private static PersistentTokenCacheAccessAspect instance = null;

    private PersistentTokenCacheAccessAspect() {};

    static PersistentTokenCacheAccessAspect getInstance() {
        if (null == instance) {
            instance = new PersistentTokenCacheAccessAspect();
        }
        return instance;
    }

    /**
     * Token cache in JSON format
     */
    static String cache = null;

    @Override
    public synchronized void beforeCacheAccess(ITokenCacheAccessContext iTokenCacheAccessContext) {
        if (null != cache) {
            iTokenCacheAccessContext.tokenCache().deserialize(cache);
        }
    }

    @Override
    public synchronized void afterCacheAccess(ITokenCacheAccessContext iTokenCacheAccessContext) {
        if (iTokenCacheAccessContext.hasCacheChanged()) {
            cache = iTokenCacheAccessContext.tokenCache().serialize();
        }
    }
}
