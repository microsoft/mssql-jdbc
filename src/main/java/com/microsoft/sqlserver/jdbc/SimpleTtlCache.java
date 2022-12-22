/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import static java.util.concurrent.TimeUnit.SECONDS;


/**
 * Internal class implements a cache data structure based on ConcurrentHashMap. The cache uses a scheduler to
 * provide eviction for the cache entry according to the pre-defined time-to-live value.
 * 
 * This class is used as local caches for the column encrypted keys and the results of signature verification of
 * column master key metadata. Refer to SQLServerColumnEncryptionAzureKeyVaultProvider for an example.
 */
final class SimpleTtlCache<K, V> {

    // This class clears cache entry when it is called by scheduler.
    class CacheClear implements Runnable {

        private K keylookupValue;
        final private java.util.logging.Logger logger = java.util.logging.Logger
                .getLogger("com.microsoft.sqlserver.jdbc.SimpleTtlCache.CacheClear");

        CacheClear(K keylookupValue) {
            this.keylookupValue = keylookupValue;
        }

        @Override
        public void run() {
            // remove() is a no-op if the key is not in the map.
            // It is a concurrentHashMap, update/remove operations are thread safe.
            if (cache.containsKey(keylookupValue)) {
                V value = cache.get(keylookupValue);

                if (value instanceof SQLServerSymmetricKey) {
                    ((SQLServerSymmetricKey) value).zeroOutKey();
                }

                cache.remove(keylookupValue);
                if (logger.isLoggable(java.util.logging.Level.FINE)) {
                    logger.fine("Removed key from cache...");
                }
            }
        }
    }

    private static final java.util.logging.Logger simpleCacheLogger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.SimpleTtlCache");
    private static final long DEFAULT_TTL_IN_HOURS = 2;

    private final ConcurrentHashMap<K, V> cache;
    private Duration cacheTtl = Duration.ofHours(DEFAULT_TTL_IN_HOURS); // The default time-to-live is set to 2 hours.

    private static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setDaemon(true);
            return t;
        }
    });

    /**
     * Get the cache size.
     * 
     * @return cache size
     */
    int getCacheSize() {
        return cache.size();
    }

    /**
     * Set time-to-live value.
     * 
     * @param duration
     *        time-to-live value in duration
     */
    void setCacheTtl(Duration duration) {
        cacheTtl = duration;
    }

    /**
     * Set time-to-live value.
     * 
     * @param seconds
     *        time-to-live value in seconds
     */
    void setCacheTtl(long seconds) {
        cacheTtl = Duration.ofSeconds(seconds);
    }

    /**
     * Get the time-to-live value.
     * 
     * @return time-to-live in duration
     */
    Duration getCacheTtl() {
        return cacheTtl;
    }

    /**
     * Constructor used for creating a cache instance.
     * 
     * Use default time-to-live value.
     */
    SimpleTtlCache() {
        cache = new ConcurrentHashMap<>();
    }

    /**
     * Constructor used for creating a cache instance.
     * 
     * @param duration
     *        time-to-live in duration
     */
    SimpleTtlCache(Duration duration) {
        cacheTtl = duration;
        cache = new ConcurrentHashMap<>();
    }

    /**
     * Check if key exists.
     * 
     * @param key
     *        key
     * @return true if existed or false if not.
     */
    boolean contains(Object key) {
        return cache.containsKey(key);
    }

    /**
     * Get value from cache based on the key given.
     * 
     * @param key
     *        key
     * @return value
     */
    V get(Object key) {
        return cache.get(key);
    }

    /**
     * Put (Key, Value) entry into cache.
     * 
     * @param key
     *        key
     * @param value
     *        value
     * @return value
     */
    V put(K key, V value) {
        V previousValue = null;
        long cacheTtlInSeconds = cacheTtl.getSeconds();

        if (0 < cacheTtlInSeconds) {
            previousValue = cache.put(key, value);
            if (simpleCacheLogger.isLoggable(java.util.logging.Level.FINEST)) {
                simpleCacheLogger.fine("Adding encryption key to cache...");
            }
            scheduler.schedule(new CacheClear(key), cacheTtlInSeconds, SECONDS);
        }

        return previousValue;
    }

    /**
     * Put (Key, Value, TTL) entry into cache.
     * 
     * @param key
     *        key
     * @param value
     *        value
     * @param ttl
     *        Time-To-Live for this cache entry
     * @return value
     */
    V put(K key, V value, Duration ttl) {
        V previousValue = null;
        long cacheTtlInSeconds = ttl.getSeconds();

        if (0 < cacheTtlInSeconds) {
            previousValue = cache.put(key, value);
            if (simpleCacheLogger.isLoggable(java.util.logging.Level.FINEST)) {
                simpleCacheLogger.fine("Adding encryption key to cache...");
            }
            scheduler.schedule(new CacheClear(key), cacheTtlInSeconds, SECONDS);
        }

        return previousValue;
    }
}
