/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.text.MessageFormat;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import java.util.Base64;;

class CacheClear implements Runnable {

    private String keylookupValue;
    static private java.util.logging.Logger aeLogger = java.util.logging.Logger.getLogger("com.microsoft.sqlserver.jdbc.CacheClear");

    CacheClear(String keylookupValue) {
        this.keylookupValue = keylookupValue;
    }

    @Override
    public void run() {
        // remove() is a no-op if the key is not in the map.
        // It is a concurrentHashMap, update/remove operations are thread safe.
        synchronized (SQLServerSymmetricKeyCache.lock) {
            SQLServerSymmetricKeyCache instance = SQLServerSymmetricKeyCache.getInstance();
            if (instance.getCache().containsKey(keylookupValue)) {
                instance.getCache().get(keylookupValue).zeroOutKey();
                instance.getCache().remove(keylookupValue);
                if (aeLogger.isLoggable(java.util.logging.Level.FINE)) {
                    aeLogger.fine("Removed encryption key from cache...");
                }
            }
        }
    }
}

/**
 * 
 * Cache for the Symmetric keys
 *
 */
final class SQLServerSymmetricKeyCache {
    static Object lock = new Object();
    private final ConcurrentHashMap<String, SQLServerSymmetricKey> cache;
    private static final SQLServerSymmetricKeyCache instance = new SQLServerSymmetricKeyCache();
    private static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setDaemon(true);
            return t;
        }
    });

    static final private java.util.logging.Logger aeLogger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.SQLServerSymmetricKeyCache");

    private SQLServerSymmetricKeyCache() {
        cache = new ConcurrentHashMap<>();
    }

    static SQLServerSymmetricKeyCache getInstance() {
        return instance;
    }

    ConcurrentHashMap<String, SQLServerSymmetricKey> getCache() {
        return cache;
    }

    /**
     * Retrieves key
     * 
     * @param keyInfo
     *            contains encryption meta data information
     * @param connection
     * @return plain text key
     */
    SQLServerSymmetricKey getKey(EncryptionKeyInfo keyInfo,
            SQLServerConnection connection) throws SQLServerException {
        SQLServerSymmetricKey encryptionKey = null;
        synchronized (lock) {
            String serverName = connection.getTrustedServerNameAE();
            assert null != serverName : "serverName should not be null in getKey.";

            StringBuilder keyLookupValuebuffer = new StringBuilder(serverName);
            String keyLookupValue;
            keyLookupValuebuffer.append(":");

            keyLookupValuebuffer.append(Base64.getEncoder().encodeToString((new String(keyInfo.encryptedKey, UTF_8)).getBytes()));

            keyLookupValuebuffer.append(":");
            keyLookupValuebuffer.append(keyInfo.keyStoreName);
            keyLookupValue = keyLookupValuebuffer.toString();
            keyLookupValuebuffer.setLength(0); // Get rid of the buffer, will be garbage collected.

            if (aeLogger.isLoggable(java.util.logging.Level.FINE)) {
                aeLogger.fine("Checking trusted master key path...");
            }
            Boolean[] hasEntry = new Boolean[1];
            List<String> trustedKeyPaths = SQLServerConnection.getColumnEncryptionTrustedMasterKeyPaths(serverName, hasEntry);
            if (hasEntry[0]) {
                if ((null == trustedKeyPaths) || (0 == trustedKeyPaths.size()) || (!trustedKeyPaths.contains(keyInfo.keyPath))) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_UntrustedKeyPath"));
                    Object[] msgArgs = {keyInfo.keyPath, serverName};
                    throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
                }
            }

            if (aeLogger.isLoggable(java.util.logging.Level.FINE)) {
                aeLogger.fine("Checking Symmetric key cache...");
            }

            // if ColumnEncryptionKeyCacheTtl is 0 no caching at all
            if (!cache.containsKey(keyLookupValue)) {

                // Check for the connection provider first.
                SQLServerColumnEncryptionKeyStoreProvider provider = connection.getSystemColumnEncryptionKeyStoreProvider(keyInfo.keyStoreName);

                // There is no connection provider of this name, check for the global system providers.
                if (null == provider) {
                    provider = SQLServerConnection.getGlobalSystemColumnEncryptionKeyStoreProvider(keyInfo.keyStoreName);
                }

                // There is no global system provider of this name, check for the global custom providers.
                if (null == provider) {
                    provider = SQLServerConnection.getGlobalCustomColumnEncryptionKeyStoreProvider(keyInfo.keyStoreName);
                }

                // No provider was found of this name.
                if (null == provider) {
                    String systemProviders = connection.getAllSystemColumnEncryptionKeyStoreProviders();
                    String customProviders = SQLServerConnection.getAllGlobalCustomSystemColumnEncryptionKeyStoreProviders();
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_UnrecognizedKeyStoreProviderName"));
                    Object[] msgArgs = {keyInfo.keyStoreName, systemProviders, customProviders};
                    throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
                }

                byte[] plaintextKey;
                plaintextKey = provider.decryptColumnEncryptionKey(keyInfo.keyPath, keyInfo.algorithmName, keyInfo.encryptedKey);
                encryptionKey = new SQLServerSymmetricKey(plaintextKey);

                /*
                 * a ColumnEncryptionKeyCacheTtl value of '0' means no caching at all. The expected use case is to have the application set it once.
                 * The application could set it multiple times, in which case a key gets the TTL defined at the time of its entry into the cache.
                 */
                long columnEncryptionKeyCacheTtl = SQLServerConnection.getColumnEncryptionKeyCacheTtl();
                if (0 != columnEncryptionKeyCacheTtl) {
                    cache.putIfAbsent(keyLookupValue, encryptionKey);
                    if (aeLogger.isLoggable(java.util.logging.Level.FINE)) {
                        aeLogger.fine("Adding encryption key to cache...");
                    }
                    scheduler.schedule(new CacheClear(keyLookupValue), columnEncryptionKeyCacheTtl, SECONDS);
                }
            }
            else {
                encryptionKey = cache.get(keyLookupValue);
            }
        }
        return encryptionKey;
    }
}
