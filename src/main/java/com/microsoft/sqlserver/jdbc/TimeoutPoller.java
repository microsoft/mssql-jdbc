/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Thread that runs in the background while the mssql driver is used that can timeout TDSCommands Checks all registered
 * commands every second to see if they can be interrupted
 */
final class TimeoutPoller implements Runnable {
    private List<TimeoutCommand<TDSCommand>> timeoutCommands = new ArrayList<>();
    final static Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.TimeoutPoller");
    private static volatile TimeoutPoller timeoutPoller = null;

    static TimeoutPoller getTimeoutPoller() {
        if (timeoutPoller == null) {
            synchronized (TimeoutPoller.class) {
                if (timeoutPoller == null) {
                    // initialize the timeout poller thread once
                    timeoutPoller = new TimeoutPoller();
                    // start the timeout polling thread
                    new Thread(timeoutPoller, "mssql-jdbc-TimeoutPoller").start();
                }
            }
        }
        return timeoutPoller;
    }

    void addTimeoutCommand(TimeoutCommand<TDSCommand> timeoutCommand) {
        synchronized (timeoutCommands) {
            timeoutCommands.add(timeoutCommand);
        }
    }

    void remove(TimeoutCommand<TDSCommand> timeoutCommand) {
        synchronized (timeoutCommands) {
            timeoutCommands.remove(timeoutCommand);
        }
    }

    private TimeoutPoller() {}

    public void run() {
        try {
            // Poll every second checking for commands that have timed out and need
            // interruption
            while (true) {
                synchronized (timeoutCommands) {
                    Iterator<TimeoutCommand<TDSCommand>> timeoutCommandIterator = timeoutCommands.iterator();
                    while (timeoutCommandIterator.hasNext()) {
                        TimeoutCommand<TDSCommand> timeoutCommand = timeoutCommandIterator.next();
                        try {
                            if (timeoutCommand.canTimeout()) {
                                try {
                                    timeoutCommand.interrupt();
                                } finally {
                                    timeoutCommandIterator.remove();
                                }
                            }
                        } catch (Exception e) {
                            logger.log(Level.WARNING, "Could not timeout command", e);
                        }
                    }
                }
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error processing timeout commands", e);
        }
    }
}
