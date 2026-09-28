/*
 * Microsoft JDBC Driver for SQL Server
 * Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;


/**
 * Captures INSERT BULK commands for one escaped destination table without changing parent handlers.
 * Tests using this helper should share the {@link #LOGGER_NAME} JUnit resource lock.
 */
public final class BulkCopyCommandCapture extends Handler implements AutoCloseable {
    public static final String LOGGER_NAME = "com.microsoft.sqlserver.jdbc.SQLServerBulkCopy";

    private final Logger logger = Logger.getLogger(LOGGER_NAME);
    private final Level previousLevel = logger.getLevel();
    private final String commandPrefix;
    private final List<String> commands = new ArrayList<>();
    private boolean closed;

    /**
     * @param escapedTableName
     *        the uniquely named destination table, already escaped as it appears in INSERT BULK
     */
    public BulkCopyCommandCapture(String escapedTableName) {
        commandPrefix = "INSERT BULK " + escapedTableName + " (";
        setLevel(Level.FINER);
        logger.addHandler(this);
        logger.setLevel(Level.FINER);
    }

    @Override
    public synchronized void publish(LogRecord record) {
        String message = record.getMessage();
        if (!closed && null != message) {
            int commandStart = message.indexOf("TDSCommand: " + commandPrefix);
            if (commandStart >= 0) {
                commands.add(message.substring(commandStart + "TDSCommand: ".length()));
            }
        }
    }

    /**
     * @return an independent snapshot of the captured INSERT BULK commands
     */
    public synchronized List<String> getCommands() {
        return new ArrayList<>(commands);
    }

    @Override
    public void flush() {}

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            logger.removeHandler(this);
            logger.setLevel(previousLevel);
        }
    }
}
