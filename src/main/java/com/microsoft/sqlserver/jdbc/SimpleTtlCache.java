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

final class SimpleTtlCache<K,V> {    

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

    static final private java.util.logging.Logger simpleCacheLogger = java.util.logging.Logger.getLogger("com.microsoft.sqlserver.jdbc.SimpleTtlCache");

    private final ConcurrentHashMap<K, V> cache;
    private Duration cacheTtl = Duration.ofHours(2);    

    private static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setDaemon(true);
            return t;
        }
    });

    void setCacheTtl(Duration duration) {
        cacheTtl = duration;
    }

    void setCacheTtl(long seconds) {
        cacheTtl = Duration.ofSeconds(seconds);
    }

    Duration getCacheTtl() {
        return cacheTtl;
    }

    SimpleTtlCache() {
        cache = new ConcurrentHashMap<>();
    }

    SimpleTtlCache(Duration duration) {
        cacheTtl = duration;
        cache = new ConcurrentHashMap<>();
    }

    boolean contains(Object key) {
        return cache.containsKey(key);
    }

    V get(Object key) {
        return cache.get(key);
    }

    V put(K key, V value) {
        V previousValue = null;
        long cacheTtlInSeconds = cacheTtl.getSeconds();
        
        if (0 < cacheTtlInSeconds) {
            previousValue = cache.put(key, value);
            if (simpleCacheLogger.isLoggable(java.util.logging.Level.FINE)) {
                simpleCacheLogger.fine("Adding encryption key to cache...");
            }
            scheduler.schedule(new CacheClear(key), cacheTtlInSeconds, SECONDS);
        }

        return previousValue;
    }

}
