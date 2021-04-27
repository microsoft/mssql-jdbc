/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import java.io.Serializable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Provides timeout handling for basic and bulk TDS commands to use a shared timer class. SharedTimer provides a static
 * method for fetching an existing static object or creating one on demand. Usage is tracked through reference counting
 * and callers are required to call removeRef() when they will no longer be using the SharedTimer. If the SharedTimer
 * does not have any more references then its internal ScheduledThreadPoolExecutor will be shutdown.
 * 
 * The SharedTimer is cached at the Connection level so that repeated invocations do not create new timers. Connections
 * only create timers on first use so if no actions involve a timeout then no timer is fetched or created. If a
 * Connection does create a timer then it will be released when the Connection closed.
 * 
 * Properly written JDBC applications that always close their Connection objects when they are finished using them
 * should not have any extra threads running after they are all closed. Applications that do not use query timeouts will
 * not have any extra threads created as they are only done on demand. Applications that use timeouts and use a JDBC
 * connection pool will have a single shared object across all JDBC connections as long as there are some open
 * connections in the pool with timeouts enabled.
 * 
 * Interrupt actions to handle a timeout are executed in their own thread. A handler thread is created when the timeout
 * occurs with the thread name matching the connection id of the client connection that created the timeout. If the
 * timeout is canceled prior to the interrupt action being executed, say because the command finished, then no handler
 * thread is created.
 * 
 * Note that the sharing of the timers happens across all Connections, not just Connections with the same JDBC URL and
 * properties.
 * 
 */
class SharedTimer implements Serializable {
    /**
     * Always update serialVersionUID when prompted
     */
    private static final long serialVersionUID = -4069361613863955760L;

    static final String CORE_THREAD_PREFIX = "mssql-jdbc-shared-timer-core-";

    private static final AtomicLong CORE_THREAD_COUNTER = new AtomicLong();
    private static final Object lock = new Object();
    /**
     * Unique ID of this SharedTimer
     */
    private final long id = CORE_THREAD_COUNTER.getAndIncrement();
    /**
     * Number of outstanding references to this SharedTimer
     */
    private final AtomicInteger refCount = new AtomicInteger();

    private static volatile SharedTimer instance;
    private ScheduledThreadPoolExecutor executor;

    private SharedTimer() {
        executor = new ScheduledThreadPoolExecutor(1, task -> {
            Thread t = new Thread(task, CORE_THREAD_PREFIX + id);
            t.setDaemon(true);
            return t;
        });
        executor.setRemoveOnCancelPolicy(true);
    }

    public long getId() {
        return id;
    }

    /**
     * @return Whether there is an instance of the SharedTimer currently allocated.
     */
    static boolean isRunning() {
        return instance != null;
    }

    /**
     * Remove a reference to this SharedTimer.
     *
     * If the reference count reaches zero then the underlying executor will be shutdown so that its thread stops.
     */
    public void removeRef() {
        synchronized (lock) {
            if (refCount.get() <= 0) {
                throw new IllegalStateException("removeRef() called more than actual references");
            }
            if (refCount.decrementAndGet() == 0) {
                // Removed last reference so perform cleanup
                executor.shutdownNow();
                executor = null;
                instance = null;
            }
        }
    }

    /**
     * Retrieve a reference to existing SharedTimer or create a new one.
     *
     * The SharedTimer's reference count will be incremented to account for the new reference.
     *
     * When the caller is finished with the SharedTimer it must be released via {@link#removeRef}
     */
    public static SharedTimer getTimer() {
        synchronized (lock) {
            if (instance == null) {
                // No shared object exists so create a new one
                instance = new SharedTimer();
            }
            instance.refCount.getAndIncrement();
            return instance;
        }
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
