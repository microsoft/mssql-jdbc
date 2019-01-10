/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


class SharedTimer {
    static final String CORE_THREAD_PREFIX = "mssql-jdbc-shared-timer-core-";
    private static final AtomicLong CORE_THREAD_COUNTER = new AtomicLong();
    private static SharedTimer instance;

    private final long id = CORE_THREAD_COUNTER.getAndIncrement();
    private int refCount = 0;
    private ScheduledThreadPoolExecutor executor;

    private SharedTimer() {
        executor = new ScheduledThreadPoolExecutor(1, task -> new Thread(task, CORE_THREAD_PREFIX + id));
        executor.setRemoveOnCancelPolicy(true);
    }

    public long getId() {
        return id;
    }

    static synchronized boolean isRunning() {
        return instance != null;
    }

    public synchronized void removeRef() {
        if (refCount <= 0) {
            throw new IllegalStateException("removeRef() called more than actual references");
        }
        refCount -= 1;
        if (refCount == 0) {
            // Removed last reference so perform cleanup
            executor.shutdownNow();
            executor = null;
            instance = null;
        }
    }

    public static synchronized SharedTimer getTimer() {
        if (instance == null) {
            // No shared object exists so create a new one
            instance = new SharedTimer();
        }
        instance.refCount += 1;
        return instance;
    }

    public ScheduledFuture<?> schedule(TdsTimeoutTask task, long delaySeconds) {
        return schedule(task, delaySeconds, TimeUnit.SECONDS);
    }

    public ScheduledFuture<?> schedule(TdsTimeoutTask task, long delay, TimeUnit unit) {
        if (executor == null) {
            throw new IllegalStateException("Cannot schedule tasks after shutdown");
        }
        return executor.schedule(task, delay, unit);
    }
}
