package com.microsoft.sqlserver.testframework.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Random;

import microsoft.sql.DateTimeOffset;

/**
 * Utility class for generating random data for testing
 */
public class RandomData {

    private static Random r = new Random();

    public static boolean returnNull = (0 == r.nextInt(5)); // 20% chance of return null
    public static boolean returnFullLength = (0 == r.nextInt(2)); // 50% chance of return full length for char/nchar and binary types
    public static boolean returnMinMax = (0 == r.nextInt(5)); // 20% chance of return Min/Max value
    public static boolean returnZero = (0 == r.nextInt(10)); // 10% chance of return zero

    private static String specicalCharSet = "ÀÂÃÄËßîðÐ";
    private static String normalCharSet = "1234567890-=!@#$%^&*()_+qwertyuiop[]\\asdfghjkl;'zxcvbnm,./QWERTYUIOP{}|ASDFGHJKL:\"ZXCVBNM<>?";

    private static String unicodeCharSet = "♠♣♥♦林花謝了春紅太匆匆無奈朝我附件为放假哇额外放我放问역사적으로본래한민족의영역은만주와연해주의일부를포함하였으나会和太空特工我來寒雨晚來風胭脂淚留人醉幾時重自是人生長恨水長東ྱོགས་སུ་འཁོར་བའི་ས་ཟླུུམ་ཞིག་ལ་ངོས་འཛིན་དགོས་ཏེ།ངག་ཕྱོαβγδεζηθικλμνξοπρστυφχψ太陽系の年齢もまた隕石の年代測定に依拠するので";

    private static String numberCharSet = "1234567890";
    private static String numberCharSet2 = "123456789";

    /**
     * Utility method for generating a random boolean.
     * 
     * @param nullable
     * @return
     */
    public static Boolean generateBoolean(boolean nullable) {
        if (nullable) {
            if (returnNull) {
                return null;
            }
        }

        return r.nextBoolean();
    }

    /**
     * Utility method for generating a random int.
     * 
     * @param nullable
     * @return
     */
    public static Integer generateInt(boolean nullable) {
        if (nullable) {
            if (returnNull) {
                return null;
            }
        }

        if (returnZero) {
            return 0;
        }

        if (returnMinMax) {
            if (r.nextBoolean()) {
                return 2147483647;
            }
            else {
                return -2147483648;
            }
        }

        // can be either negative or positive
        return r.nextInt();
    }

    /**
     * Utility method for generating a random long.
     * 
     * @param nullable
     * @return
     */
    public static Long generateLong(boolean nullable) {
        if (nullable) {
            if (returnNull) {
                return null;
            }
        }

        if (returnZero) {
            return 0L;
        }

        if (returnMinMax) {
            if (r.nextBoolean()) {
                return 9223372036854775807L;
            }
            else {
                return -9223372036854775808L;
            }
        }

        // can be either negative or positive
        return r.nextLong();
    }

    /**
     * Utility method for generating a random tinyint.
     * 
     * @param nullable
     * @return
     */
    public static Short generateTinyint(boolean nullable) {
        Integer value = pickInt(nullable, 255, 0);

        if (null != value) {
            return value.shortValue();
        }
        else {
            return null;
        }
    }

    /**
     * Utility method for generating a random short.
     * 
     * @param nullable
     * @return
     */
    public static Short generateSmallint(boolean nullable) {
        Integer value = pickInt(nullable, 32767, -32768);

        if (null != value) {
            return value.shortValue();
        }
        else {
            return null;
        }
    }

    /**
     * Utility method for generating a random BigDecimal.
     * 
     * @param precision
     * @param scale
     * @param nullable
     * @return
     */
    public static BigDecimal generateDecimalNumeric(int precision,
            int scale,
            boolean nullable) {

        if (nullable) {
            if (returnNull) {
                return null;
            }
        }

        if (returnZero) {
            return BigDecimal.ZERO.setScale(scale);

        }

        if (returnMinMax) {
            BigInteger n;
            if (r.nextBoolean()) {
                n = BigInteger.TEN.pow(precision);
                if (scale > 0)
                    return new BigDecimal(n, scale).subtract(new BigDecimal("" + Math.pow(10, -scale)).setScale(scale, BigDecimal.ROUND_HALF_UP))
                            .negate();
                else
                    return new BigDecimal(n, scale).subtract(new BigDecimal("1")).negate();
            }
            else {
                n = BigInteger.TEN.pow(precision);
                if (scale > 0)
                    return new BigDecimal(n, scale).subtract(new BigDecimal("" + Math.pow(10, -scale)).setScale(scale, BigDecimal.ROUND_HALF_UP))
                            .negate();
                else
                    return new BigDecimal(n, scale).subtract(new BigDecimal("1")).negate();

            }

        }
        BigInteger n = BigInteger.TEN.pow(precision);
        if (r.nextBoolean()) {
            return new BigDecimal(newRandomBigInteger(n, r, precision), scale);
        }
        return (new BigDecimal(newRandomBigInteger(n, r, precision), scale).negate());

    }

    /**
     * Utility method for generating a random float.
     * 
     * @param nullable
     * @return
     */
    public static Float generateReal(boolean nullable) {
        Double doubleValue = generateFloat(24, nullable);

        if (null != doubleValue) {
            return doubleValue.floatValue();
        }
        else {
            return null;
        }
    }

    /**
     * Utility method for generating a random double.
     * 
     * @param n
     *            integer
     * @param nullable
     * @return
     */
    public static Double generateFloat(Integer n,
            boolean nullable) {
        if (nullable) {
            if (returnNull) {
                return null;
            }
        }

        if (returnZero) {
            return new Double(0);
        }

        // only 2 options: 24 or 53
        // The default value of n is 53. If 1<=n<=24, n is treated as 24. If 25<=n<=53, n is treated as 53.
        // https://msdn.microsoft.com/en-us/library/ms173773.aspx
        if (null == n) {
            n = 53;
        }
        else if (25 <= n && 53 >= n) {
            n = 53;
        }
        else {
            n = 24;
        }

        if (returnMinMax) {
            if (53 == n) {
                if (r.nextBoolean()) {
                    if (r.nextBoolean()) {
                        return Double.valueOf("1.79E+308");
                    }
                    else {
                        return Double.valueOf("2.23E-308");
                    }
                }
                else {
                    if (r.nextBoolean()) {
                        return Double.valueOf("-2.23E-308");
                    }
                    else {
                        return Double.valueOf("-1.79E+308");
                    }
                }
            }
            else {
                if (r.nextBoolean()) {
                    if (r.nextBoolean()) {
                        return Double.valueOf("3.40E+38");
                    }
                    else {
                        return Double.valueOf("1.18E-38");
                    }
                }
                else {
                    if (r.nextBoolean()) {
                        return Double.valueOf("-1.18E-38");
                    }
                    else {
                        return Double.valueOf("-3.40E+38");
                    }
                }
            }
        }

        String intPart = "" + r.nextInt(10);

        // generate n bits of binary data and convert to long, then use the long as decimal part
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < n; i++) {
            sb.append(r.nextInt(2));
        }
        long longValue = Long.parseLong(sb.toString(), 2);
        String stringValue = intPart + "." + longValue;

        return Double.valueOf(stringValue);
    }

    /**
     * Utility method for generating a random Money.
     * 
     * @param nullable
     * @return
     */
    public static BigDecimal generateMoney(boolean nullable) {
        String charSet = numberCharSet;
        BigDecimal max = new BigDecimal("922337203685477.5807");
        BigDecimal min = new BigDecimal("-922337203685477.5808");
        float multiplier = 10000;
        return generateMoneyOrSmallMoney(nullable, max, min, multiplier, charSet);
    }

    /**
     * Utility method for generating a random SmallMoney.
     * 
     * @param nullable
     * @return
     */
    public static BigDecimal generateSmallMoney(boolean nullable) {
        String charSet = numberCharSet;
        BigDecimal max = new BigDecimal("214748.3647");
        BigDecimal min = new BigDecimal("-214748.3648");
        float multiplier = (float) (1.0 / 10000.0);
        return generateMoneyOrSmallMoney(nullable, max, min, multiplier, charSet);
    }

    /**
     * Utility method for generating a random char or Nchar.
     * 
     * @param columnLength
     * @param nullable
     * @param encrypted
     * @return
     */
    public static String generateCharTypes(String columnLength,
            boolean nullable,
            boolean encrypted) {
        String charSet = normalCharSet;

        return buildCharOrNChar(columnLength, nullable, encrypted, charSet, 8001);
    }

    public static String generateNCharTypes(String columnLength,
            boolean nullable,
            boolean encrypted) {
        String charSet = specicalCharSet + normalCharSet + unicodeCharSet;

        return buildCharOrNChar(columnLength, nullable, encrypted, charSet, 4001);
    }

    /**
     * Utility method for generating a random binary.
     * 
     * @param columnLength
     * @param nullable
     * @param encrypted
     * @return
     */
    public static byte[] generateBinaryTypes(String columnLength,
            boolean nullable,
            boolean encrypted) {
        int maxBound = 8001;

        if (nullable) {
            if (returnNull) {
                return null;
            }
        }

        // if column is encrypted, string value cannot be "", not supported.
        int minimumLength = 0;
        if (encrypted) {
            minimumLength = 1;
        }

        int length;
        if (columnLength.toLowerCase().equals("max")) {
            // 50% chance of return value longer than 8000/4000
            if (r.nextBoolean()) {
                length = r.nextInt(100000) + maxBound;
                byte[] bytes = new byte[length];
                r.nextBytes(bytes);
                return bytes;
            }
            else {
                length = r.nextInt(maxBound - minimumLength) + minimumLength;
                byte[] bytes = new byte[length];
                r.nextBytes(bytes);
                return bytes;
            }
        }
        else {
            int columnLengthInt = Integer.parseInt(columnLength);
            if (returnFullLength) {
                length = columnLengthInt;
                byte[] bytes = new byte[length];
                r.nextBytes(bytes);
                return bytes;
            }
            else {
                length = r.nextInt(columnLengthInt - minimumLength) + minimumLength;
                byte[] bytes = new byte[length];
                r.nextBytes(bytes);
                return bytes;
            }
        }
    }

    /**
     * Utility method for generating a random date.
     * 
     * @param nullable
     * @return
     */
    public static Date generateDate(boolean nullable) {
        if (nullable) {
            if (returnNull) {
                return null;
            }
        }

        long max = Timestamp.valueOf("9999-12-31 00:00:00.000").getTime();
        long min = Timestamp.valueOf("0001-01-01 00:00:00.000").getTime();

        if (returnMinMax) {
            if (r.nextBoolean()) {
                return new Date(max);
            }
            else {
                return new Date(min);
            }
        }

        while (true) {
            long longValue = r.nextLong();

            if (longValue >= min && longValue <= max) {
                return new Date(longValue);
            }
        }
    }

    /**
     * Utility method for generating a random timestamp.
     * 
     * @param nullable
     * @return
     */
    public static Timestamp generateDatetime(boolean nullable) {
        long max = Timestamp.valueOf("9999-12-31 23:59:59.997").getTime();
        long min = Timestamp.valueOf("1753-01-01 00:00:00.000").getTime();

        return generateTimestamp(nullable, max, min);
    }

    /**
     * Utility method for generating a random datetimeoffset.
     * 
     * @param nullable
     * @return
     */
    public static DateTimeOffset generateDatetimeoffset(Integer precision,
            boolean nullable) {
        if (null == precision) {
            precision = 7;
        }

        DateTimeOffset maxDTS = calculateDateTimeOffsetMinMax("max", precision, "9999-12-31 23:59:59");
        DateTimeOffset minDTS = calculateDateTimeOffsetMinMax("min", precision, "0001-01-01 00:00:00");

        long max = maxDTS.getTimestamp().getTime();
        long min = minDTS.getTimestamp().getTime();

        Timestamp ts = generateTimestamp(nullable, max, min);

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

    /**
     * Utility method for generating a random small datetime.
     * 
     * @param nullable
     * @return
     */
    public static Timestamp generateSmalldatetime(boolean nullable) {
        long max = Timestamp.valueOf("2079-06-06 23:59:00").getTime();
        long min = Timestamp.valueOf("1900-01-01 00:00:00").getTime();

        return generateTimestamp(nullable, max, min);
    }

    /**
     * Utility method for generating a random datetime.
     * 
     * @param precision
     * @param nullable
     * @return
     */
    public static Timestamp generateDatetime2(Integer precision,
            boolean nullable) {
        if (null == precision) {
            precision = 7;
        }

        long max = Timestamp.valueOf("9999-12-31 23:59:59").getTime();
        long min = Timestamp.valueOf("0001-01-01 00:00:00").getTime();

        Timestamp ts = generateTimestamp(nullable, max, min);

        if (null == ts) {
            return ts;
        }

        if (returnMinMax) {
            if (ts.getTime() == max) {
                int precisionDigits = buildPrecision(precision, "9");
                ts.setNanos(precisionDigits);
                return ts;
            }
            else {
                ts.setNanos(0);
                return ts;
            }
        }

        int precisionDigits = buildPrecision(precision, numberCharSet2); // not to use 0 in the random data for now. E.g creates 9040330 and when set
                                                                         // it is 904033.
        ts.setNanos(precisionDigits);
        return ts;
    }

    /**
     * Utility method for generating a random time.
     * 
     * @param precision
     * @param nullable
     * @return
     */
    public static Time generateTime(Integer precision,
            boolean nullable) {
        if (null == precision) {
            precision = 7;
        }

        long max = Timestamp.valueOf("9999-12-31 23:59:59").getTime();
        long min = Timestamp.valueOf("0001-01-01 00:00:00").getTime();

        Timestamp ts = generateTimestamp(nullable, max, min);

        if (null == ts) {
            return null;
        }

        if (returnMinMax) {
            if (ts.getTime() == max) {
                int precisionDigits = buildPrecision(precision, "9");
                ts.setNanos(precisionDigits);
                return new Time(ts.getTime());
            }
            else {
                ts.setNanos(0);
                return new Time(ts.getTime());
            }
        }

        int precisionDigits = buildPrecision(precision, numberCharSet);
        ts.setNanos(precisionDigits);
        return new Time(ts.getTime());
    }

    private static int buildPrecision(int precision,
            String charSet) {
        String stringValue = calculatePrecisionDigits(precision, charSet);
        return Integer.parseInt(stringValue);
    }

    private static String calculatePrecisionDigits(int precision,
            String charSet) {
        // setNanos(999999900) gives 00:00:00.9999999
        // so, this value has to be 9 digits
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

    private static Timestamp generateTimestamp(boolean nullable,
            long max,
            long min) {
        if (nullable) {
            if (returnNull) {
                return null;
            }
        }

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

    private static BigDecimal generateMoneyOrSmallMoney(boolean nullable,
            BigDecimal max,
            BigDecimal min,
            float multiplier,
            String charSet) {
        if (nullable) {
            if (returnNull) {
                return null;
            }
        }

        if (returnZero) {
            return BigDecimal.ZERO.setScale(4);
        }

        if (returnMinMax) {
            if (r.nextBoolean()) {
                return max;
            }
            else {
                return min;
            }
        }

        long intPart = (long) (r.nextInt() * multiplier);

        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < 4; i++) {
            char c = pickRandomChar(charSet);
            sb.append(c);
        }

        return new BigDecimal(intPart + "." + sb.toString());
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

    private static Integer pickInt(boolean nullable,
            int max,
            int min) {
        if (nullable) {
            if (returnNull) {
                return null;
            }
        }

        if (returnZero) {
            return 0;
        }

        if (returnMinMax) {
            if (r.nextBoolean()) {
                return max;
            }
            else {
                return min;
            }
        }

        return (int) r.nextInt(max - min) + min;
    }

    private static String buildCharOrNChar(String columnLength,
            boolean nullable,
            boolean encrypted,
            String charSet,
            int maxBound) {

        if (nullable) {
            if (returnNull) {
                return null;
            }
        }

        // if column is encrypted, string value cannot be "", not supported.
        int minimumLength = 0;
        if (encrypted) {
            minimumLength = 1;
        }

        int length;
        if (columnLength.toLowerCase().equals("max")) {
            // 50% chance of return value longer than 8000/4000
            if (r.nextBoolean()) {
                length = r.nextInt(100000) + maxBound;
                return buildRandomString(length, charSet);
            }
            else {
                length = r.nextInt(maxBound - minimumLength) + minimumLength;
                return buildRandomString(length, charSet);
            }
        }
        else {
            int columnLengthInt = Integer.parseInt(columnLength);
            if (returnFullLength) {
                length = columnLengthInt;
                return buildRandomString(length, charSet);
            }
            else {
                length = r.nextInt(columnLengthInt - minimumLength) + minimumLength;
                return buildRandomString(length, charSet);
            }
        }
    }

    private static String buildRandomString(int length,
            String charSet) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            char c = pickRandomChar(charSet);
            sb.append(c);
        }

        return sb.toString();
    }

    private static char pickRandomChar(String charSet) {
        int charSetLength = charSet.length();

        int randomIndex = r.nextInt(charSetLength);
        return charSet.charAt(randomIndex);
    }

    private static BigInteger newRandomBigInteger(BigInteger n,
            Random rnd,
            int precision) {
        BigInteger r;
        do {
            r = new BigInteger(n.bitLength(), rnd);
        }
        while (r.toString().length() != precision);

        return r;
    }
}
