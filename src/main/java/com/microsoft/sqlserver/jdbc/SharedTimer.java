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

    /**
     * Unique ID of this SharedTimer
     */
    private final long id = CORE_THREAD_COUNTER.getAndIncrement();
    /**
     * Number of outstanding references to this SharedTimer
     */
    private int refCount = 0;
    private ScheduledThreadPoolExecutor executor;

    private SharedTimer() {
        executor = new ScheduledThreadPoolExecutor(1, task -> new Thread(task, CORE_THREAD_PREFIX + id));
        executor.setRemoveOnCancelPolicy(true);
    }

    public long getId() {
        return id;
    }

    /**
     * @return Whether there is an instance of the SharedTimer currently allocated.
     */
    static synchronized boolean isRunning() {
        return instance != null;
    }

    /**
     * Remove a reference to this SharedTimer.
     *
     * If the reference count reaches zero then the underlying executor will be shutdown so that its thread stops.
     */
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

    /**
     * Retrieve a reference to existing SharedTimer or create a new one.
     *
     * The SharedTimer's reference count will be incremented to account for the new reference.
     *
     * When the caller is finished with the SharedTimer it must be released via {@link#removeRef}
     */
    public static synchronized SharedTimer getTimer() {
        if (instance == null) {
            // No shared object exists so create a new one
            instance = new SharedTimer();
        }
        instance.refCount += 1;
        return instance;
    }

    /**
     * Schedule a task to execute in the future using this SharedTimer's internal executor.
     */
    public ScheduledFuture<?> schedule(TDSTimeoutTask task, long delaySeconds) {
        return schedule(task, delaySeconds, TimeUnit.SECONDS);
    }

    /**
     * Schedule a task to execute in the future using this SharedTimer's internal executor.
     */
    public ScheduledFuture<?> schedule(TDSTimeoutTask task, long delay, TimeUnit unit) {
        if (executor == null) {
            throw new IllegalStateException("Cannot schedule tasks after shutdown");
        }
        return executor.schedule(task, delay, unit);
    }
}
