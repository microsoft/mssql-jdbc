/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Object that handles management of timeout tasks across all sql server connections
 */
final class SQLServerTimeoutManager {
    private static ScheduledExecutorService scheduledTimeoutTasks = Executors.newScheduledThreadPool(1,
            createThreadFactory("com.microsoft.sqlserver.jdbc.SQLServerTimeoutManager"));
    private static ExecutorService timeoutTaskWorker = Executors.newSingleThreadExecutor(
            createThreadFactory("com.microsoft.sqlserver.jdbc.SQLServerTimeoutManager.TimeoutTaskWorker"));
    private final static Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.SQLServerTimeoutManager");

    static void startTimeoutCommand(Callable<?> timeoutCommand, int timeout) {
        if (scheduledTimeoutTasks.isShutdown()) {
            scheduledTimeoutTasks = Executors.newScheduledThreadPool(1,
                    createThreadFactory("com.microsoft.sqlserver.jdbc.SQLServerTimeoutManager"));
            timeoutTaskWorker = Executors.newSingleThreadExecutor(
                    createThreadFactory("com.microsoft.sqlserver.jdbc.SQLServerTimeoutManager.TimeoutTaskWorker"));
        }

        Runnable timeoutTaskRunnable = () -> {
            Future<?> timeoutTask = timeoutTaskWorker.submit(timeoutCommand);
            try {
                // if the timeout command takes too long to interrupt, cancel it and release
                timeoutTask.get(10, TimeUnit.SECONDS);
            } catch (TimeoutException te) {
                logger.log(Level.WARNING, "Timeout task too long, aborting", te);
            } catch (InterruptedException | ExecutionException e) {
                logger.log(Level.FINE, "Unexpected Exception occured in timeout thread.", e);
            } finally {
                if (!(timeoutTask.isCancelled() || timeoutTask.isDone())) {
                    timeoutTask.cancel(true);
                }
            }
        };

        scheduledTimeoutTasks.schedule(timeoutTaskRunnable, timeout, TimeUnit.SECONDS);
    }

    static void releaseAll() {
        scheduledTimeoutTasks.shutdownNow();
        timeoutTaskWorker.shutdownNow();
    }

    private static ThreadFactory createThreadFactory(String name) {
        return new ThreadFactory() {

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, name);
            }
        };
    }

    private SQLServerTimeoutManager() {}
}
