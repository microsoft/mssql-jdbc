/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * 
 * Cache for the Symmetric keys
 *
 */
final class SQLServerSymmetricKeyCache {
    static final Lock lock = new ReentrantLock();
    private final SimpleTtlCache<String, SQLServerSymmetricKey> cache;
    private static final SQLServerSymmetricKeyCache instance = new SQLServerSymmetricKeyCache();

    static final private java.util.logging.Logger aeLogger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.SQLServerSymmetricKeyCache");

    private SQLServerSymmetricKeyCache() {
        cache = new SimpleTtlCache<>();
    }

    static SQLServerSymmetricKeyCache getInstance() {
        return instance;
    }

    SimpleTtlCache<String, SQLServerSymmetricKey> getCache() {
        return cache;
    }

    /**
     * Returns key
     * 
     * @param keyInfo
     *        contains encryption meta data information
     * @param connection
     * @return plain text key
     */
    SQLServerSymmetricKey getKey(EncryptionKeyInfo keyInfo, SQLServerConnection connection) throws SQLServerException {
        SQLServerSymmetricKey encryptionKey = null;
        lock.lock();
        try {
            String serverName = connection.getTrustedServerNameAE();
            assert null != serverName : "serverName should not be null in getKey.";

            StringBuilder keyLookupValuebuffer = new StringBuilder(serverName);
            String keyLookupValue;
            keyLookupValuebuffer.append(":");

            keyLookupValuebuffer
                    .append(Base64.getEncoder().encodeToString((new String(keyInfo.encryptedKey, UTF_8)).getBytes()));

            keyLookupValuebuffer.append(":");
            keyLookupValuebuffer.append(keyInfo.keyStoreName);
            keyLookupValue = keyLookupValuebuffer.toString();
            keyLookupValuebuffer.setLength(0); // Get rid of the buffer, will be garbage collected.

            if (aeLogger.isLoggable(java.util.logging.Level.FINE)) {
                aeLogger.fine("Checking trusted master key path...");
            }
            Boolean[] hasEntry = new Boolean[1];
            List<String> trustedKeyPaths = SQLServerConnection.getColumnEncryptionTrustedMasterKeyPaths(serverName,
                    hasEntry);
            if (hasEntry[0] && ((null == trustedKeyPaths) || (trustedKeyPaths.isEmpty())
                    || (!trustedKeyPaths.contains(keyInfo.keyPath)))) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_UntrustedKeyPath"));
                Object[] msgArgs = {keyInfo.keyPath, serverName};
                throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
            }

            if (aeLogger.isLoggable(java.util.logging.Level.FINE)) {
                aeLogger.fine("Checking Symmetric key cache...");
            }

            if (!cache.contains(keyLookupValue)) {

                // search system/global key store providers
                SQLServerColumnEncryptionKeyStoreProvider provider = connection
                        .getSystemOrGlobalColumnEncryptionKeyStoreProvider(keyInfo.keyStoreName);
                assert null != provider : "Provider should not be null.";

                byte[] plaintextKey;

                /*
                 * When provider decrypt Column Encryption Key, it can cache the decrypted key if cacheTTL > 0.
                 * To prevent conflicts between CEK caches, system providers and global providers should not use their own CEK caches.
                 */
                provider.setColumnEncryptionCacheTtl(Duration.ZERO);
                plaintextKey = provider.decryptColumnEncryptionKey(keyInfo.keyPath, keyInfo.algorithmName,
                        keyInfo.encryptedKey);

                encryptionKey = new SQLServerSymmetricKey(plaintextKey);

                /*
                 * a ColumnEncryptionKeyCacheTtl value of '0' means no caching at all. The expected use case is to have
                 * the application set it once. The application could set it multiple times, in which case a key gets
                 * the TTL defined at the time of its entry into the cache.
                 */
                long columnEncryptionKeyCacheTtl = SQLServerConnection.getColumnEncryptionKeyCacheTtl();
                if (0 != columnEncryptionKeyCacheTtl) {
                    cache.setCacheTtl(columnEncryptionKeyCacheTtl);
                    cache.put(keyLookupValue, encryptionKey);
                }
            } else {
                encryptionKey = cache.get(keyLookupValue);
            }
            return encryptionKey;
        } finally {
            lock.unlock();
        }
    }
}
