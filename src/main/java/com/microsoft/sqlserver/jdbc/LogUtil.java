/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.text.MessageFormat;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;



/**
 * Provides utility methods to log messages with variable arguments
 */
public class LogUtil {

    private static void log(Logger logger, Level level, String format, Object... args) {
        if (!logger.isLoggable(level)) {
            return;
        }

        String msg = MessageFormat.format(format, args);
        LogRecord record = new LogRecord(level, msg);
        
        // set source to calling class and method
        record.setSourceClassName(Thread.currentThread().getStackTrace()[3].getClassName());
        record.setSourceMethodName(Thread.currentThread().getStackTrace()[3].getMethodName());
        record.setLoggerName(logger.getName());

        logger.log(record);
    }

    /**
     * Calls the corresponding logging methods in Logger
     * 
     * @param logger
     *        logger to log the
     * @param format
     *        format pattern string for the log message
     * @param args
     *        argument(s) to the format pattern
     */
    public static void info(Logger logger, String format, Object... args) {
        log(logger, Level.INFO, format, args);
    }

    public static void warning(Logger logger, String format, Object... args) {
        log(logger, Level.WARNING, format, args);
    }

    public static void fine(Logger logger, String format, Object... args) {
        log(logger, Level.FINE, format, args);
    }

    public static void finer(Logger logger, String format, Object... args) {
        log(logger, Level.FINER, format, args);
    }

    public static void finest(Logger logger, String format, Object... args) {
        log(logger, Level.FINEST, format, args);
    }

    public static void severe(Logger logger, String format, Object... args) {
        log(logger, Level.SEVERE, format, args);
    }

    public static void config(Logger logger, String format, Object... args) {
        log(logger, Level.CONFIG, format, args);
    }

    public static void entering(Logger logger, String sourceClass, String sourceMethod, Object... args) {
        if (!logger.isLoggable(Level.FINER)) {
            return;
        }

        Object[] params = new Object[args.length];
        for (int i = 0; i < params.length; i++) {
            params[i] = args[i];
        }
        logger.entering(sourceClass, sourceMethod, params);
    }
}
