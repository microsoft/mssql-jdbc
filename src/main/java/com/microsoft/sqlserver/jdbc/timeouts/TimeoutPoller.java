package com.microsoft.sqlserver.jdbc.timeouts;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Thread that runs in the background while the mssql driver is used that can timeout TDSCommands
 * Checks all registered commands every second to see if they can be interrupted
 */
public final class TimeoutPoller implements Runnable {
    private List<TimeoutCommand> timeoutCommands = new ArrayList<>();
    final static Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.TDS.Command");

    private static volatile TimeoutPoller timeoutPoller = null;

    public static TimeoutPoller getTimeoutPoller() {
        if (timeoutPoller == null) {
            synchronized (TimeoutPoller.class) {
                if (timeoutPoller == null) {
                    //initialize the timeout poller thread once
                    timeoutPoller = new TimeoutPoller();
                    //start the timeout polling thread
                    new Thread(timeoutPoller, "mssql-jdbc-TimeoutPoller").start();
                }
            }
        }
        return timeoutPoller;
    }

    public void addTimeoutCommand(TimeoutCommand timeoutCommand) {
        synchronized (timeoutCommands) {
            timeoutCommands.add(timeoutCommand);
        }
    }

    public void remove(TimeoutCommand timeoutCommand) {
        synchronized (timeoutCommands) {
            timeoutCommands.remove(timeoutCommand);
        }
    }

    private TimeoutPoller() {}

    public void run() {
        try {
            // Poll every second checking for commands that have timed out and need interruption
            while (true) {
                synchronized (timeoutCommands) {
                    Iterator<TimeoutCommand> timeoutCommandIterator = timeoutCommands.iterator();
                    while (timeoutCommandIterator.hasNext()) {
                        TimeoutCommand timeoutCommand = timeoutCommandIterator.next();
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