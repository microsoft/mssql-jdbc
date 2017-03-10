/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.testframework.sqlType;

import java.sql.JDBCType;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.concurrent.ThreadLocalRandom;

import microsoft.sql.DateTimeOffset;

public class SqlDateTimeOffset extends SqlDateTime {
    public static boolean returnMinMax = (0 == ThreadLocalRandom.current().nextInt(5)); // 20% chance of return Min/Max value
    private static String numberCharSet2 = "123456789";
    DateTimeOffset maxDTS;
    DateTimeOffset minDTS;
    long max;
    long min;

    // TODO: datetiemoffset can extend SqlDateTime2
    // timezone is not supported in Timestamp so its useless to initialize
    // min/max with offset
    public SqlDateTimeOffset() {
        super("datetimeoffset", JDBCType.TIMESTAMP /* microsoft.sql.Types.DATETIMEOFFSET */, null, null);
        type = microsoft.sql.DateTimeOffset.class;
        minvalue = Timestamp.valueOf((String) SqlTypeValue.DATETIMEOFFSET.minValue);
        maxvalue = Timestamp.valueOf((String) SqlTypeValue.DATETIMEOFFSET.maxValue);
        this.precision = 7;
        this.variableLengthType = VariableLengthType.Precision;
        generatePrecision();
        maxDTS = calculateDateTimeOffsetMinMax("max", precision, (String) SqlTypeValue.DATETIMEOFFSET.maxValue);
        minDTS = calculateDateTimeOffsetMinMax("min", precision, (String) SqlTypeValue.DATETIMEOFFSET.minValue);

        max = maxDTS.getTimestamp().getTime();
        min = minDTS.getTimestamp().getTime();
    }

    /**
     * create data
     */
    public Object createdata() {
        return generateDatetimeoffset(this.precision);
    }

    /**
     * 
     * @param precision
     * @return
     */
    public Object generateDatetimeoffset(Integer precision) {
        if (null == precision) {
            precision = 7;
        }

        Timestamp ts = generateTimestamp(max, min);

        if (null == ts) {
            return null;
        }

        if (returnMinMax) {
            if (ThreadLocalRandom.current().nextBoolean()) {
                return maxDTS;
            }
            else {
                return minDTS;
            }
        }

        int precisionDigits = buildPrecision(precision, numberCharSet2);
        ts.setNanos(precisionDigits);

        int randomTimeZoneInMinutes = ThreadLocalRandom.current().nextInt(1681) - 840;

        return microsoft.sql.DateTimeOffset.valueOf(ts, randomTimeZoneInMinutes);
    }

    private static DateTimeOffset calculateDateTimeOffsetMinMax(String maxOrMin,
            Integer precision,
            String tsMinMax) {
        int providedTimeZoneInMinutes;
        if (maxOrMin.toLowerCase().equals("max")) {
            providedTimeZoneInMinutes = 840;
        }
        else {
            providedTimeZoneInMinutes = -840;
        }

        Timestamp tsMax = Timestamp.valueOf(tsMinMax);

        Calendar cal = Calendar.getInstance();
        long offset = cal.get(Calendar.ZONE_OFFSET); // in milliseconds

        // max Timestamp + difference of current time zone and GMT - provided time zone in milliseconds
        tsMax = new Timestamp(tsMax.getTime() + offset - (providedTimeZoneInMinutes * 60 * 1000));

        if (maxOrMin.toLowerCase().equals("max")) {
            int precisionDigits = buildPrecision(precision, "9");
            tsMax.setNanos(precisionDigits);
        }

        return microsoft.sql.DateTimeOffset.valueOf(tsMax, providedTimeZoneInMinutes);
    }

    private static int buildPrecision(int precision,
            String charSet) {
        String stringValue = calculatePrecisionDigits(precision, charSet);
        return Integer.parseInt(stringValue);
    }

    // setNanos(999999900) gives 00:00:00.9999999
    // so, this value has to be 9 digits
    private static String calculatePrecisionDigits(int precision,
            String charSet) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < precision; i++) {
            char c = pickRandomChar(charSet);
            sb.append(c);
        }

        for (int i = sb.length(); i < 9; i++) {
            sb.append("0");
        }

        return sb.toString();
    }

    private static Timestamp generateTimestamp(long max,
            long min) {

        if (returnMinMax) {
            if (ThreadLocalRandom.current().nextBoolean()) {
                return new Timestamp(max);
            }
            else {
                return new Timestamp(min);
            }
        }

        while (true) {
            long longValue = ThreadLocalRandom.current().nextLong();

            if (longValue >= min && longValue <= max) {
                return new Timestamp(longValue);
            }
        }
    }

    private static char pickRandomChar(String charSet) {
        int charSetLength = charSet.length();
        int randomIndex = ThreadLocalRandom.current().nextInt(charSetLength);
        return charSet.charAt(randomIndex);
    }
}