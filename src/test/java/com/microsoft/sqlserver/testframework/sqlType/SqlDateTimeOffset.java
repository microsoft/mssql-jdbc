// ---------------------------------------------------------------------------------------------------------------------------------
// File: SqlDateTimeOffset.java
//
//
// Microsoft JDBC Driver for SQL Server
// Copyright(c) Microsoft Corporation
// All rights reserved.
// MIT License
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files(the "Software"),
// to deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense,
// and / or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions :
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.
// ---------------------------------------------------------------------------------------------------------------------------------

package com.microsoft.sqlserver.testframework.sqlType;

import java.sql.JDBCType;
import java.sql.Timestamp;
import java.util.Calendar;

import microsoft.sql.DateTimeOffset;

public class SqlDateTimeOffset extends SqlDateTime {
    public static boolean returnMinMax = (0 == r.nextInt(5)); // 20% chance of return Min/Max value
    private static String numberCharSet2 = "123456789";

    // TODO: datetiemoffset can extend SqlDateTime2
    // timezone is not supported in Timestamp so its useless to initialize
    // min/max with offset
    public SqlDateTimeOffset() {
        super("datetimeoffset", JDBCType.TIMESTAMP /* microsoft.sql.Types.DATETIMEOFFSET */, null, null);
        minvalue = Timestamp.valueOf((String) SqlTypeValue.DATETIMEOFFSET.minValue);
        maxvalue = Timestamp.valueOf((String) SqlTypeValue.DATETIMEOFFSET.maxValue);
        this.precision = 7;
        this.variableLengthType = VariableLengthType.Precision;
        generatePrecision();

    }

    public Object createdata() {
        return generateDatetimeoffset(this.precision);
    }

    public Object generateDatetimeoffset(Integer precision) {
        if (null == precision) {
            precision = 7;
        }

        DateTimeOffset maxDTS = calculateDateTimeOffsetMinMax("max", precision, "9999-12-31 23:59:59");
        DateTimeOffset minDTS = calculateDateTimeOffsetMinMax("min", precision, "0001-01-01 00:00:00");

        long max = maxDTS.getTimestamp().getTime();
        long min = minDTS.getTimestamp().getTime();

        Timestamp ts = generateTimestamp(max, min);

        if (null == ts) {
            return null;
        }

        if (returnMinMax) {
            if (r.nextBoolean()) {
                return maxDTS;
            }
            else {
                // return minDTS;
                return calculateDateTimeOffsetMinMax("min", precision, "0001-01-01 00:00:00.0000000");
            }
        }

        int precisionDigits = buildPrecision(precision, numberCharSet2);
        ts.setNanos(precisionDigits);

        int randomTimeZoneInMinutes = r.nextInt(1681) - 840;

        return microsoft.sql.DateTimeOffset.valueOf(ts, randomTimeZoneInMinutes);
    }

    private static DateTimeOffset calculateDateTimeOffsetMinMax(String maxOrMin, Integer precision, String tsMinMax) {
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

    private static int buildPrecision(int precision, String charSet) {
        String stringValue = calculatePrecisionDigits(precision, charSet);
        return Integer.parseInt(stringValue);
    }

    // setNanos(999999900) gives 00:00:00.9999999
    // so, this value has to be 9 digits
    private static String calculatePrecisionDigits(int precision, String charSet) {
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

    private static Timestamp generateTimestamp(long max, long min) {

        if (returnMinMax) {
            if (r.nextBoolean()) {
                return new Timestamp(max);
            }
            else {
                return new Timestamp(min);
            }
        }

        while (true) {
            long longValue = r.nextLong();

            if (longValue >= min && longValue <= max) {
                return new Timestamp(longValue);
            }
        }
    }

    private static char pickRandomChar(String charSet) {
        int charSetLength = charSet.length();
        int randomIndex = r.nextInt(charSetLength);
        return charSet.charAt(randomIndex);
    }
}