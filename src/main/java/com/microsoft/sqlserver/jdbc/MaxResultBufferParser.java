/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.lang.management.ManagementFactory;
import java.text.MessageFormat;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Parser created to parse String value from Connection String to equivalent number of bytes for JDBC Driver to work on.
 */
public class MaxResultBufferParser {

    private static final Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.MaxResultBufferParser");
    private static final String[] PERCENT_PHRASES = {"percent", "pct", "p"};
    private static final String ERROR_MESSAGE = "MaxResultBuffer property is badly formatted: {0}.";

    private MaxResultBufferParser() {}

    /**
     *
     * Returns number of bytes for maxResultBuffer property
     *
     * @param input
     *        String value for maxResultProperty provided in Connection String
     * @return 'maxResultBuffer' property as number of bytes
     * @throws SQLServerException
     *         Is Thrown when maxResultProperty's syntax is wrong
     */
    public static long validateMaxResultBuffer(String input) throws SQLServerException {
        String numberString;
        long number = -1;

        // check for null values and empty String "", if so return -1 (default value)
        if (StringUtils.isEmpty(input) || input.equals("-1")) {
            return number;
        }

        // check if input is number
        if (!StringUtils.isEmpty(input) && input.matches("-?\\d+(\\.\\d+)?")) {
            try {
                number = Long.parseLong(input);
            } catch (NumberFormatException e) {
                logger.log(Level.INFO, ERROR_MESSAGE, new Object[] {input});
                throwNewInvalidMaxResultBufferParameterException(e, input);
            }
            return adjustMemory(number, 1);
        } else {
            // check PERCENT_PHRASES
            for (String percentPhrase : PERCENT_PHRASES) {
                if (input.endsWith(percentPhrase)) {
                    numberString = input.substring(0, input.length() - percentPhrase.length());
                    try {
                        number = Long.parseLong(numberString);
                    } catch (NumberFormatException e) {
                        logger.log(Level.INFO, ERROR_MESSAGE, new Object[] {input});
                        throwNewInvalidMaxResultBufferParameterException(e, numberString);
                    }
                    return adjustMemoryPercentage(number);
                }
            }

            // check if prefix was supplied
            long multiplier = getMultiplier(input);
            numberString = input.substring(0, input.length() - 1);

            try {
                number = Long.parseLong(numberString);
            } catch (NumberFormatException e) {
                logger.log(Level.INFO, ERROR_MESSAGE, new Object[] {input});
                throwNewInvalidMaxResultBufferParameterException(e, numberString);
            }
            return adjustMemory(number, multiplier);
        }
    }

    private static void checkForNegativeValue(long value) throws SQLServerException {
        if (value <= 0) {
            Object[] objectToThrow = new Object[] {value};
            MessageFormat form = new MessageFormat(
                    SQLServerException.getErrString("R_maxResultBufferNegativeParameterValue"));
            logger.log(Level.INFO, SQLServerException.getErrString("R_maxResultBufferNegativeParameterValue"),
                    objectToThrow);
            throw new SQLServerException(form.format(objectToThrow), new Throwable());
        }
    }

    private static long getMultiplier(String input) throws SQLServerException {
        long multiplier = 1;
        switch (Character.toUpperCase(input.charAt(input.length() - 1))) {
            case 'K':
                multiplier = 1_000L;
                break;
            case 'M':
                multiplier = 1_000_000L;
                break;
            case 'G':
                multiplier = 1_000_000_000L;
                break;
            case 'T':
                multiplier = 1_000_000_000_000L;
                break;
            default:
                logger.log(Level.INFO, ERROR_MESSAGE, new Object[] {input});
                throwNewInvalidMaxResultBufferParameterException(null, input);
        }
        return multiplier;
    }

    private static long adjustMemoryPercentage(long percentage) throws SQLServerException {
        checkForNegativeValue(percentage);
        if (percentage > 90)
            return (long) (0.9 * getMaxMemory());
        else
            return (long) ((percentage) / 100.0 * getMaxMemory());
    }

    private static long adjustMemory(long size, long multiplier) throws SQLServerException {
        checkForNegativeValue(size);
        if (size * multiplier > 0.9 * getMaxMemory())
            return (long) (0.9 * getMaxMemory());
        else
            return size * multiplier;
    }

    private static long getMaxMemory() {
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
    }

    private static void throwNewInvalidMaxResultBufferParameterException(Throwable cause,
            Object... arguments) throws SQLServerException {
        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_maxResultBufferInvalidSyntax"));
        throw new SQLServerException(form.format(arguments), cause);
    }

}
