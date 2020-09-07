package com.microsoft.sqlserver.jdbc;

import java.lang.management.ManagementFactory;
import java.text.MessageFormat;

public class MaxResultBufferParser {

    private static final String[] PERCENT_PHRASES = {"percent", "pct", "p"};

    private MaxResultBufferParser() {}

    public static long validateMaxResultBuffer(String input) throws SQLServerException {
        String numberString;
        long number = 0;

        //check PERCENT_PHRASES
        for (String percentPhrase : PERCENT_PHRASES) {
            if (input.contains(percentPhrase)) {
                numberString = input.substring(0, input.length() - percentPhrase.length());
                try {
                    number = Long.parseLong(numberString);
                } catch (NumberFormatException e) {
                    throwNewInvalidMaxResultBufferParameterException(e, numberString);
                }
                return adjustMemory(number);
            }
        }
        //check if not only number was supplied
        long multiplier = 1;
        if (input.matches("\\d+")) {
            number = Long.parseLong(input);
            return adjustMemory(number, multiplier);
        }
        //check if prefix was supplied
        switch (input.substring(input.length() - 1).toUpperCase()) {
            case "K":
                multiplier = 1000L;
                break;
            case "M":
                multiplier = 1000000L;
                break;
            case "G":
                multiplier = 1000000000L;
                break;
            case "T":
                multiplier = 1000000000000L;
                break;
            default:
                throwNewInvalidMaxResultBufferParameterException(null, input);
        }

        numberString = input.substring(0, input.length() - 1);

        try {
            number = Long.parseLong(numberString);
        } catch (NumberFormatException e) {
            throwNewInvalidMaxResultBufferParameterException(e, numberString);
        }
        return adjustMemory(number, multiplier);
    }

    private static long adjustMemory(long percentage) {
        if (percentage > 90)
            return (long) (0.9 * getMaxMemory());
        else
            return (long) ((percentage) / 100.0 * getMaxMemory());
    }

    private static long adjustMemory(long size, long multiplier) {
        if (size * multiplier > 0.9 * getMaxMemory())
            return (long) (0.9 * getMaxMemory());
        else
            return size * multiplier;
    }

    private static long getMaxMemory() {
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
    }

    private static void throwNewInvalidMaxResultBufferParameterException(Throwable cause, Object... arguments) throws SQLServerException {
        MessageFormat form = new MessageFormat("Invalid syntax: {0} in maxResultBuffer parameter");
        Object[] msgArgs = {arguments};
        throw new SQLServerException(form.format(msgArgs), cause);
    }

}
