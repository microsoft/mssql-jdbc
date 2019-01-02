/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
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
    private static Map<UUID, List<TimeoutCommand<?>>> timeoutCommands = new HashMap<>();
    private final static Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.SQLServerTimeoutManager");

    static void startTimeoutCommand(TimeoutCommand<?> timeoutCommand) {
        if (scheduledTimeoutTasks.isShutdown()) {
            // reset id counter for timeout commands
            TimeoutCommand.uniqueId.set(0);
            scheduledTimeoutTasks = Executors.newScheduledThreadPool(1);
            timeoutTaskWorker = Executors.newSingleThreadExecutor();
        }

        timeoutCommand.setTimeoutTask(scheduledTimeoutTasks.schedule(new Runnable() {

            @Override
            public void run() {
                Future<?> timeoutTask = timeoutTaskWorker.submit(new Runnable() {
                    @Override
                    public void run() {
                        timeoutCommand.interrupt();
                    }
                });
                try {
                    // if the timeout command takes too long to interrupt, cancel it and release
                    timeoutTask.get(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                    logger.log(Level.WARNING, "Timeout task too long, aborting", e);
                } finally {
                    releaseTimeoutCommand(timeoutCommand);
                }
            }

        }, timeoutCommand.getTimeout(), TimeUnit.SECONDS));
        addTimeoutCommand(timeoutCommand);
    }

    static void releaseTimeoutCommand(TimeoutCommand<?> timeoutCommand) {
        removeTimeoutCommand(timeoutCommand);
        try {
            if (!timeoutCommand.isTimeoutTaskComplete()) {
                timeoutCommand.cancelTimeoutTask();
            }
            if (!areTimeoutCommandsAvailable()) {
                scheduledTimeoutTasks.shutdownNow();
                timeoutTaskWorker.shutdownNow();
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "Could not cancel timeout task", e);
        }

    }

    static void releaseTimeoutCommands(UUID connectionId) {
        synchronized (timeoutCommands) {
            List<TimeoutCommand<?>> timeouts = timeoutCommands.get(connectionId);
            if (timeouts != null && !timeouts.isEmpty()) {
                for (TimeoutCommand<?> timeoutCommand : timeouts) {
                    releaseTimeoutCommand(timeoutCommand);
                }
            }
        }
    }

    private static void addTimeoutCommand(TimeoutCommand<?> timeoutCommand) {
        synchronized (timeoutCommands) {
            UUID connectionId = timeoutCommand.getSqlServerConnection().getClientConIdInternal();
            List<TimeoutCommand<?>> timeouts = timeoutCommands.get(connectionId);
            if (timeouts == null) {
                timeouts = new ArrayList<>();
                timeoutCommands.put(connectionId, timeouts);
            }
            timeouts.add(timeoutCommand);
        }
    }

    private static void removeTimeoutCommand(TimeoutCommand<?> timeoutCommand) {
        synchronized (timeoutCommands) {
            UUID connectionId = timeoutCommand.getSqlServerConnection().getClientConIdInternal();
            List<TimeoutCommand<?>> timeouts = timeoutCommands.get(connectionId);
            if (timeouts != null) {
                if (!timeouts.isEmpty()) {
                    timeouts.remove(timeoutCommand);
                }

                if (timeouts.isEmpty()) {
                    // if there are no more timeouts, remove the connection from cache
                    timeoutCommands.remove(connectionId);
                }
            }
        }
    }

    private static boolean areTimeoutCommandsAvailable() {
        synchronized (timeoutCommands) {
            return !timeoutCommands.isEmpty();
        }
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
