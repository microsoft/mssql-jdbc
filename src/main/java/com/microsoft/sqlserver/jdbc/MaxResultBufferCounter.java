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
            if (counter > maxResultBuffer) {
                logger.log(Level.WARNING, "MaxResultBuffer exceeded: {0} .Property was set to {1}",
                        new Object[] {counter, maxResultBuffer});
                throwExceededMaxResultBufferException(maxResultBuffer, counter);
            }
        }
    }

    public void resetCounter() {
        counter = 0;
    }

    private void throwExceededMaxResultBufferException(Object... arguments) throws SQLServerException {
        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_maxResultBufferPropertyDescription"));
        Object[] msgArgs = {arguments};
        throw new SQLServerException(form.format(msgArgs), null);
    }
}
