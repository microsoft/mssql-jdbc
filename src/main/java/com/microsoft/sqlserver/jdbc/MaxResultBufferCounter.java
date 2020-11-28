/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.text.MessageFormat;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Implementation of ICounter for 'maxResultBuffer' property.
 */
public class MaxResultBufferCounter implements ICounter {

    private final Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.MaxResultBufferCounter");

    private long counter = 0;
    private final long maxResultBuffer;

    public MaxResultBufferCounter(long maxResultBuffer) {
        this.maxResultBuffer = maxResultBuffer;
    }

    public void increaseCounter(long bytes) throws SQLServerException {
        if (maxResultBuffer > 0) {
            counter += bytes;
            checkForMaxResultBufferOverflow(counter);
        }
    }

    public void resetCounter() {
        counter = 0;
    }

    private void checkForMaxResultBufferOverflow(long number) throws SQLServerException {
        if (number > maxResultBuffer) {
            if (logger.isLoggable(Level.SEVERE)) {
                logger.log(Level.SEVERE, SQLServerException.getErrString("R_maxResultBufferPropertyExceeded"),
                        new Object[] {number, maxResultBuffer});
            }
            throwExceededMaxResultBufferException(counter, maxResultBuffer);
        }
    }

    private void throwExceededMaxResultBufferException(Object... arguments) throws SQLServerException {
        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_maxResultBufferPropertyExceeded"));
        throw new SQLServerException(form.format(arguments), null);
    }
}
