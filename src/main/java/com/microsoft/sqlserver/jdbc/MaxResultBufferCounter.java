package com.microsoft.sqlserver.jdbc;

import java.text.MessageFormat;

public class MaxResultBufferCounter implements Counter {

    private long counter = 0;
    private final long maxResultBuffer;

    public MaxResultBufferCounter(long maxResultBuffer) {
        this.maxResultBuffer = maxResultBuffer;
    }

    @Override
    public void increaseCounter(long bytes) throws SQLServerException {
        if (maxResultBuffer != 0) {
            counter += bytes;
            if (counter > maxResultBuffer) {
                resetCounter();
                throwExceededMaxResultBufferException(maxResultBuffer, counter);
            }
        }
    }

    @Override
    public void resetCounter() {
        counter = 0;
    }

    private static void throwExceededMaxResultBufferException(Object... arguments) throws SQLServerException {
        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_maxResultBufferPropertyDescription"));
        Object[] msgArgs = {arguments};
        throw new SQLServerException(form.format(msgArgs), null);
    }
}
