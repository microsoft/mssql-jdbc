/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static java.nio.charset.StandardCharsets.US_ASCII;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.text.MessageFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Utility class for all Data Dependant Conversions (DDC).
 */

final class DDC {

    /**
     * Convert an Integer object to desired target user type.
     * 
     * @param intvalue
     *            the value to convert.
     * @param valueLength
     *            the value to convert.
     * @param jdbcType
     *            the jdbc type required.
     * @param streamType
     *            the type of stream required.
     * @return the required object.
     */
    static final Object convertIntegerToObject(int intValue,
            int valueLength,
            JDBCType jdbcType,
            StreamType streamType) {
        switch (jdbcType) {
            case INTEGER:
                return intValue;
            case SMALLINT: // 2.21 small and tinyint returned as short
            case TINYINT:
                return (short) intValue;
            case BIT:
            case BOOLEAN:
                return 0 != intValue;
            case BIGINT:
                return (long) intValue;
            case DECIMAL:
            case NUMERIC:
            case MONEY:
            case SMALLMONEY:
                return new BigDecimal(Integer.toString(intValue));
            case FLOAT:
            case DOUBLE:
                return (double) intValue;
            case REAL:
                return (float) intValue;
            case BINARY:
                return convertIntToBytes(intValue, valueLength);
            default:
                return Integer.toString(intValue);
        }
    }

    /**
     * Convert a Long object to desired target user type.
     * 
     * @param longVal
     *            the value to convert.
     * @param jdbcType
     *            the jdbc type required.
     * @param baseSSType
     *            the base SQLServer type.
     * @param streamType
     *            the stream type.
     * @return the required object.
     */
    static final Object convertLongToObject(long longVal,
            JDBCType jdbcType,
            SSType baseSSType,
            StreamType streamType) {
        switch (jdbcType) {
            case BIGINT:
                return longVal;
            case INTEGER:
                return (int) longVal;
            case SMALLINT: // small and tinyint returned as short
            case TINYINT:
                return (short) longVal;
            case BIT:
            case BOOLEAN:
                return 0 != longVal;
            case DECIMAL:
            case NUMERIC:
            case MONEY:
            case SMALLMONEY:
                return new BigDecimal(Long.toString(longVal));
            case FLOAT:
            case DOUBLE:
                return (double) longVal;
            case REAL:
                return (float) longVal;
            case BINARY:
                byte[] convertedBytes = convertLongToBytes(longVal);
                int bytesToReturnLength;
                byte[] bytesToReturn;

                switch (baseSSType) {
                    case BIT:
                    case TINYINT:
                        bytesToReturnLength = 1;
                        bytesToReturn = new byte[bytesToReturnLength];
                        System.arraycopy(convertedBytes, convertedBytes.length - bytesToReturnLength, bytesToReturn, 0, bytesToReturnLength);
                        return bytesToReturn;
                    case SMALLINT:
                        bytesToReturnLength = 2;
                        bytesToReturn = new byte[bytesToReturnLength];
                        System.arraycopy(convertedBytes, convertedBytes.length - bytesToReturnLength, bytesToReturn, 0, bytesToReturnLength);
                        return bytesToReturn;
                    case INTEGER:
                        bytesToReturnLength = 4;
                        bytesToReturn = new byte[bytesToReturnLength];
                        System.arraycopy(convertedBytes, convertedBytes.length - bytesToReturnLength, bytesToReturn, 0, bytesToReturnLength);
                        return bytesToReturn;
                    case BIGINT:
                        bytesToReturnLength = 8;
                        bytesToReturn = new byte[bytesToReturnLength];
                        System.arraycopy(convertedBytes, convertedBytes.length - bytesToReturnLength, bytesToReturn, 0, bytesToReturnLength);
                        return bytesToReturn;
                    default:
                        return convertedBytes;
                }

            case VARBINARY:
                switch (baseSSType) {
                    case BIGINT:
                        return longVal;
                    case INTEGER:
                        return (int) longVal;
                    case SMALLINT: // small and tinyint returned as short
                    case TINYINT:
                        return (short) longVal;
                    case BIT:
                        return 0 != longVal;
                    case DECIMAL:
                    case NUMERIC:
                    case MONEY:
                    case SMALLMONEY:
                        return new BigDecimal(Long.toString(longVal));
                    case FLOAT:
                        return (double) longVal;
                    case REAL:
                        return (float) longVal;
                    case BINARY:
                        return convertLongToBytes(longVal);
                    default:
                        return Long.toString(longVal);
                }
            default:
                return Long.toString(longVal);
        }
    }

    /**
     * Encodes an integer value to a byte array in big-endian order.
     * 
     * @param intValue
     *            the integer value to encode.
     * @param valueLength
     *            the number of bytes to encode.
     * @return the byte array containing the big-endian encoded value.
     */
    static final byte[] convertIntToBytes(int intValue,
            int valueLength) {
        byte bytes[] = new byte[valueLength];
        for (int i = valueLength; i-- > 0;) {
            bytes[i] = (byte) (intValue & 0xFF);
            intValue >>= 8;
        }
        return bytes;
    }

    /**
     * Convert a Float object to desired target user type.
     * 
     * @param floatVal
     *            the value to convert.
     * @param jdbcType
     *            the jdbc type required.
     * @param streamType
     *            the stream type.
     * @return the required object.
     */
    static final Object convertFloatToObject(float floatVal,
            JDBCType jdbcType,
            StreamType streamType) {
        switch (jdbcType) {
            case REAL:
                return floatVal;
            case INTEGER:
                return (int) floatVal;
            case SMALLINT: // small and tinyint returned as short
            case TINYINT:
                return (short) floatVal;
            case BIT:
            case BOOLEAN:
                return 0 != Float.compare(0.0f, floatVal);
            case BIGINT:
                return (long) floatVal;
            case DECIMAL:
            case NUMERIC:
            case MONEY:
            case SMALLMONEY:
                return new BigDecimal(Float.toString(floatVal));
            case FLOAT:
            case DOUBLE:
                return (Float.valueOf(floatVal)).doubleValue();
            case BINARY:
                return convertIntToBytes(Float.floatToRawIntBits(floatVal), 4);
            default:
                return Float.toString(floatVal);
        }
    }

    /**
     * Encodes a long value to a byte array in big-endian order.
     * 
     * @param longValue
     *            the long value to encode.
     * @return the byte array containing the big-endian encoded value.
     */
    static final byte[] convertLongToBytes(long longValue) {
        byte bytes[] = new byte[8];
        for (int i = 8; i-- > 0;) {
            bytes[i] = (byte) (longValue & 0xFF);
            longValue >>= 8;
        }
        return bytes;
    }

    /**
     * Convert a Double object to desired target user type.
     * 
     * @param doubleVal
     *            the value to convert.
     * @param jdbcType
     *            the jdbc type required.
     * @param streamType
     *            the stream type.
     * @return the required object.
     */
    static final Object convertDoubleToObject(double doubleVal,
            JDBCType jdbcType,
            StreamType streamType) {
        switch (jdbcType) {
            case FLOAT:
            case DOUBLE:
                return doubleVal;
            case REAL:
                return (Double.valueOf(doubleVal)).floatValue();
            case INTEGER:
                return (int) doubleVal;
            case SMALLINT: // small and tinyint returned as short
            case TINYINT:
                return (short) doubleVal;
            case BIT:
            case BOOLEAN:
                return 0 != Double.compare(0.0d, doubleVal);
            case BIGINT:
                return (long) doubleVal;
            case DECIMAL:
            case NUMERIC:
            case MONEY:
            case SMALLMONEY:
                return new BigDecimal(Double.toString(doubleVal));
            case BINARY:
                return convertLongToBytes(Double.doubleToRawLongBits(doubleVal));
            default:
                return Double.toString(doubleVal);
        }
    }

    static final byte[] convertBigDecimalToBytes(BigDecimal bigDecimalVal,
            int scale) {
        byte[] valueBytes;

        if (bigDecimalVal == null) {
            valueBytes = new byte[2];
            valueBytes[0] = (byte) scale;
            valueBytes[1] = 0; // data length
        }
        else {
            boolean isNegative = (bigDecimalVal.signum() < 0);

            // NOTE: Handle negative scale as a special case for JDK 1.5 and later VMs.
            if (bigDecimalVal.scale() < 0)
                bigDecimalVal = bigDecimalVal.setScale(0);

            BigInteger bi = bigDecimalVal.unscaledValue();

            if (isNegative)
                bi = bi.negate();

            byte[] unscaledBytes = bi.toByteArray();

            valueBytes = new byte[unscaledBytes.length + 3];
            int j = 0;
            valueBytes[j++] = (byte) bigDecimalVal.scale();
            valueBytes[j++] = (byte) (unscaledBytes.length + 1); // data length + sign
            valueBytes[j++] = (byte) (isNegative ? 0 : 1); // 1 = +ve, 0 = -ve
            for (int i = unscaledBytes.length - 1; i >= 0; i--)
                valueBytes[j++] = unscaledBytes[i];
        }

        return valueBytes;
    }

    /**
     * Convert a BigDecimal object to desired target user type.
     * 
     * @param bigDecimalVal
     *            the value to convert.
     * @param jdbcType
     *            the jdbc type required.
     * @param streamType
     *            the stream type.
     * @return the required object.
     */
    static final Object convertBigDecimalToObject(BigDecimal bigDecimalVal,
            JDBCType jdbcType,
            StreamType streamType) {
        switch (jdbcType) {
            case DECIMAL:
            case NUMERIC:
            case MONEY:
            case SMALLMONEY:
                return bigDecimalVal;
            case FLOAT:
            case DOUBLE:
                return bigDecimalVal.doubleValue();
            case REAL:
                return bigDecimalVal.floatValue();
            case INTEGER:
                return bigDecimalVal.intValue();
            case SMALLINT: // small and tinyint returned as short
            case TINYINT:
                return bigDecimalVal.shortValue();
            case BIT:
            case BOOLEAN:
                return 0 != bigDecimalVal.compareTo(BigDecimal.valueOf(0));
            case BIGINT:
                return bigDecimalVal.longValue();
            case BINARY:
                return convertBigDecimalToBytes(bigDecimalVal, bigDecimalVal.scale());
            default:
                return bigDecimalVal.toString();
        }
    }

    /**
     * Convert a Money object to desired target user type.
     * 
     * @param bigDecimalVal
     *            the value to convert.
     * @param jdbcType
     *            the jdbc type required.
     * @param streamType
     *            the stream type.
     * @param numberOfBytes
     *            the number of bytes to convert
     * @return the required object.
     */
    static final Object convertMoneyToObject(BigDecimal bigDecimalVal,
            JDBCType jdbcType,
            StreamType streamType,
            int numberOfBytes) {
        switch (jdbcType) {
            case DECIMAL:
            case NUMERIC:
            case MONEY:
            case SMALLMONEY:
                return bigDecimalVal;
            case FLOAT:
            case DOUBLE:
                return bigDecimalVal.doubleValue();
            case REAL:
                return bigDecimalVal.floatValue();
            case INTEGER:
                return bigDecimalVal.intValue();
            case SMALLINT: // small and tinyint returned as short
            case TINYINT:
                return bigDecimalVal.shortValue();
            case BIT:
            case BOOLEAN:
                return 0 != bigDecimalVal.compareTo(BigDecimal.valueOf(0));
            case BIGINT:
                return bigDecimalVal.longValue();
            case BINARY:
                return convertToBytes(bigDecimalVal, bigDecimalVal.scale(), numberOfBytes);
            default:
                return bigDecimalVal.toString();
        }
    }

    // converts big decimal to money and smallmoney
    private static byte[] convertToBytes(BigDecimal value,
            int scale,
            int numBytes) {
        boolean isNeg = value.signum() < 0;

        value = value.setScale(scale);

        BigInteger bigInt = value.unscaledValue();

        byte[] unscaledBytes = bigInt.toByteArray();

        byte[] ret = new byte[numBytes];
        if (unscaledBytes.length < numBytes) {
            for (int i = 0; i < numBytes - unscaledBytes.length; ++i) {
                ret[i] = (byte) (isNeg ? -1 : 0);
            }
        }
        int offset = numBytes - unscaledBytes.length;
        System.arraycopy(unscaledBytes, 0, ret, offset, numBytes - offset);
        return ret;
    }

    /**
     * Convert a byte array to desired target user type.
     * 
     * @param bytesValue
     *            the value to convert.
     * @param jdbcType
     *            the jdbc type required.
     * @param baseTypeInfo
     *            the type information associated with bytesValue.
     * @return the required object.
     * @throws SQLServerException
     *             when an error occurs.
     */
    static final Object convertBytesToObject(byte[] bytesValue,
            JDBCType jdbcType,
            TypeInfo baseTypeInfo) throws SQLServerException {
        switch (jdbcType) {
            case CHAR:
                String str = Util.bytesToHexString(bytesValue, bytesValue.length);

                if ((SSType.BINARY == baseTypeInfo.getSSType()) && (str.length() < (baseTypeInfo.getPrecision() * 2))) {

                    StringBuilder strbuf = new StringBuilder(str);

                    while (strbuf.length() < (baseTypeInfo.getPrecision() * 2)) {
                        strbuf.append('0');
                    }
                    return strbuf.toString();
                }
                return str;

            case BINARY:
            case VARBINARY:
            case LONGVARBINARY:
                if ((SSType.BINARY == baseTypeInfo.getSSType()) && (bytesValue.length < baseTypeInfo.getPrecision())) {

                    byte[] newBytes = new byte[baseTypeInfo.getPrecision()];
                    System.arraycopy(bytesValue, 0, newBytes, 0, bytesValue.length);
                    return newBytes;
                }

                return bytesValue;

            default:
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_unsupportedConversionFromTo"));
                throw new SQLServerException(form.format(new Object[] {baseTypeInfo.getSSType().name(), jdbcType}), null, 0, null);
        }
    }

    /**
     * Convert a String object to desired target user type.
     * 
     * @param stringVal
     *            the value to convert.
     * @param charset
     *            the character set.
     * @param jdbcType
     *            the jdbc type required.
     * @return the required object.
     */
    static final Object convertStringToObject(String stringVal,
            Charset charset,
            JDBCType jdbcType,
            StreamType streamType) throws UnsupportedEncodingException, IllegalArgumentException {
        switch (jdbcType) {
            // Convert String to Numeric types.
            case DECIMAL:
            case NUMERIC:
            case MONEY:
            case SMALLMONEY:
                return new BigDecimal(stringVal.trim());
            case FLOAT:
            case DOUBLE:
                return Double.valueOf(stringVal.trim());
            case REAL:
                return Float.valueOf(stringVal.trim());
            case INTEGER:
                return Integer.valueOf(stringVal.trim());
            case SMALLINT: // small and tinyint returned as short
            case TINYINT:
                return Short.valueOf(stringVal.trim());
            case BIT:
            case BOOLEAN:
                String trimmedString = stringVal.trim();
                return (1 == trimmedString.length()) ? Boolean.valueOf('1' == trimmedString.charAt(0)) : Boolean.valueOf(trimmedString);
            case BIGINT:
                return Long.valueOf(stringVal.trim());

            // Convert String to Temporal types.
            case TIMESTAMP:
                return java.sql.Timestamp.valueOf(stringVal.trim());
            case DATE:
                return java.sql.Date.valueOf(getDatePart(stringVal.trim()));
            case TIME: {
                // Accepted character formats for conversion to java.sql.Time are:
                // hh:mm:ss[.nnnnnnnnn]
                // YYYY-MM-DD hh:mm:ss[.nnnnnnnnn]
                //
                // To handle either of these formats:
                // 1) Normalize and parse as a Timestamp
                // 2) Round fractional seconds up to the nearest millisecond (max resolution of java.sql.Time)
                // 3) Renormalize (as rounding may have changed the date) to a java.sql.Time
                java.sql.Timestamp ts = java.sql.Timestamp.valueOf(TDS.BASE_DATE_1970 + " " + getTimePart(stringVal.trim()));
                GregorianCalendar cal = new GregorianCalendar(Locale.US);
                cal.clear();
                cal.setTimeInMillis(ts.getTime());
                if (ts.getNanos() % Nanos.PER_MILLISECOND >= Nanos.PER_MILLISECOND / 2)
                    cal.add(Calendar.MILLISECOND, 1);
                cal.set(TDS.BASE_YEAR_1970, Calendar.JANUARY, 1);
                return new java.sql.Time(cal.getTimeInMillis());
            }

            case BINARY:
                return stringVal.getBytes(charset);

            default:
                // For everything else, just return either a string or appropriate stream.
                switch (streamType) {
                    case CHARACTER:
                        return new StringReader(stringVal);
                    case ASCII:
                        return new ByteArrayInputStream(stringVal.getBytes(US_ASCII));
                    case BINARY:
                        return new ByteArrayInputStream(stringVal.getBytes());

                    default:
                        return stringVal;
                }
        }
    }

    static final Object convertStreamToObject(BaseInputStream stream,
            TypeInfo typeInfo,
            JDBCType jdbcType,
            InputStreamGetterArgs getterArgs) throws SQLServerException {
        // Need to handle the simple case of a null value here, as it is not done
        // outside this function.
        if (null == stream)
            return null;

        assert null != typeInfo;
        assert null != getterArgs;

        SSType ssType = typeInfo.getSSType();

        try {
            switch (jdbcType) {
                case CHAR:
                case VARCHAR:
                case LONGVARCHAR:
                case NCHAR:
                case NVARCHAR:
                case LONGNVARCHAR:
                default:

                    // Binary streams to character types:
                    // - Direct conversion to ASCII stream
                    // - Convert as hexized value to other character types
                    if (SSType.BINARY == ssType || SSType.VARBINARY == ssType || SSType.VARBINARYMAX == ssType || SSType.TIMESTAMP == ssType
                            || SSType.IMAGE == ssType || SSType.UDT == ssType) {
                        if (StreamType.ASCII == getterArgs.streamType) {
                            return stream;
                        }
                        else {
                            assert StreamType.CHARACTER == getterArgs.streamType || StreamType.NONE == getterArgs.streamType;

                            byte[] byteValue = stream.getBytes();
                            if (JDBCType.GUID == jdbcType) {
                                return Util.readGUID(byteValue);
                            } else if (JDBCType.GEOMETRY == jdbcType) {
                                return Geometry.STGeomFromWKB(byteValue);
                            } else if (JDBCType.GEOGRAPHY == jdbcType) {
                                return Geography.STGeomFromWKB(byteValue);
                            }
                            else {
                                String hexString = Util.bytesToHexString(byteValue, byteValue.length);

                                if (StreamType.NONE == getterArgs.streamType)
                                    return hexString;

                                return new StringReader(hexString);
                            }
                        }
                    }

                    // Handle streams converting to ASCII
                    if (StreamType.ASCII == getterArgs.streamType) {
                        // Fast path for SBCS data that converts directly/easily to ASCII
                        if (typeInfo.supportsFastAsciiConversion())
                            return new AsciiFilteredInputStream(stream);

                        // Slightly less fast path for MBCS data that converts directly/easily to ASCII
                        if (getterArgs.isAdaptive) {
                            return AsciiFilteredUnicodeInputStream.MakeAsciiFilteredUnicodeInputStream(stream,
                                    new BufferedReader(new InputStreamReader(stream, typeInfo.getCharset())));
                        }
                        else {
                            return new ByteArrayInputStream((new String(stream.getBytes(), typeInfo.getCharset())).getBytes(US_ASCII));
                        }
                    }
                    else if (StreamType.CHARACTER == getterArgs.streamType || StreamType.NCHARACTER == getterArgs.streamType) {
                        if (getterArgs.isAdaptive)
                            return new BufferedReader(new InputStreamReader(stream, typeInfo.getCharset()));
                        else
                            return new StringReader(new String(stream.getBytes(), typeInfo.getCharset()));
                    }

                    // None of the special/fast textual conversion cases applied. Just go the normal route of converting via String.
                    return convertStringToObject(new String(stream.getBytes(), typeInfo.getCharset()), typeInfo.getCharset(), jdbcType,
                            getterArgs.streamType);

                case CLOB:
                    return new SQLServerClob(stream, typeInfo);

                case NCLOB:
                    return new SQLServerNClob(stream, typeInfo);
                case SQLXML:
                    return new SQLServerSQLXML(stream, getterArgs, typeInfo);

                case BINARY:
                case VARBINARY:
                case LONGVARBINARY:
                case BLOB:

                    // Where allowed, streams convert directly to binary representation

                    if (StreamType.BINARY == getterArgs.streamType)
                        return stream;

                    if (JDBCType.BLOB == jdbcType)
                        return new SQLServerBlob(stream);

                    return stream.getBytes();
            }
        }

        // Conversion can throw either of these exceptions:
        //
        // UnsupportedEncodingException (binary conversions)
        // IllegalArgumentException (any conversion - note: numerics throw NumberFormatException subclass)
        //
        // Catch them and translate them to a SQLException so that we don't propagate an unexpected exception
        // type all the way up to the app, which may not catch it either...
        catch (IllegalArgumentException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_errorConvertingValue"));
            throw new SQLServerException(form.format(new Object[] {typeInfo.getSSType(), jdbcType}), null, 0, e);
        }
        catch (UnsupportedEncodingException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_errorConvertingValue"));
            throw new SQLServerException(form.format(new Object[] {typeInfo.getSSType(), jdbcType}), null, 0, e);
        }
    }

    // Returns date portion of string.
    // Expects one of "<date>" or "<date><space><time>".
    private static String getDatePart(String s) {
        int sp = s.indexOf(' ');
        if (-1 == sp)
            return s;
        return s.substring(0, sp);
    }

    // Returns time portion of string.
    // Expects one of "<time>" or "<date><space><time>".
    private static String getTimePart(String s) {
        int sp = s.indexOf(' ');
        if (-1 == sp)
            return s;
        return s.substring(sp + 1);
    }

    // Formats nanoseconds as a String of the form ".nnnnnnn...." where the number
    // of digits is equal to the scale. Returns the empty string for scale = 0;
    private static String fractionalSecondsString(long subSecondNanos,
            int scale) {
        assert 0 <= subSecondNanos && subSecondNanos < Nanos.PER_SECOND;
        assert 0 <= scale && scale <= TDS.MAX_FRACTIONAL_SECONDS_SCALE;

        // Fast path for 0 scale (avoids creation of two BigDecimal objects and
        // two Strings when the answer is going to be "" anyway...)
        if (0 == scale)
            return "";

        return java.math.BigDecimal.valueOf(subSecondNanos % Nanos.PER_SECOND, 9).setScale(scale).toPlainString().substring(1);
    }

    /**
     * Convert a SQL Server temporal value to the desired Java object type.
     *
     * Accepted SQL server data types:
     *
     * DATETIME SMALLDATETIME DATE TIME DATETIME2 DATETIMEOFFSET
     *
     * Converts to Java types (determined by JDBC type):
     *
     * java.sql.Date java.sql.Time java.sql.Timestamp java.lang.String
     *
     * @param jdbcType
     *            the JDBC type indicating the desired conversion
     *
     * @param ssType
     *            the SQL Server data type of the value being converted
     *
     * @param timeZoneCalendar
     *            (optional) a Calendar representing the time zone to associate with the resulting converted value. For DATETIMEOFFSET, this parameter
     *            represents the time zone associated with the value. Null means to use the default VM time zone.
     *
     * @param daysSinceBaseDate
     *            The date part of the value, expressed as a number of days since the base date for the specified SQL Server data type. For DATETIME
     *            and SMALLDATETIME, the base date is 1/1/1900. For other types, the base date is 1/1/0001. The number of days assumes Gregorian leap
     *            year behavior over the entire supported range of values. For TIME values, this parameter must be the number of days between 1/1/0001
     *            and 1/1/1900 when converting to java.sql.Timestamp.
     * 
     * @param ticksSinceMidnight
     *            The time part of the value, expressed as a number of time units (ticks) since midnight. For DATETIME and SMALLDATETIME SQL Server
     *            data types, time units are in milliseconds. For other types, time units are in nanoseconds. For DATE values, this parameter must be
     *            0.
     *
     * @param fractionalSecondsScale
     *            the desired fractional seconds scale to use when formatting the value as a String. Ignored for conversions to Java types other than
     *            String.
     *
     * @return a Java object of the desired type.
     */
    static final Object convertTemporalToObject(JDBCType jdbcType,
            SSType ssType,
            Calendar timeZoneCalendar,
            int daysSinceBaseDate,
            long ticksSinceMidnight,
            int fractionalSecondsScale) {
        // Determine the local time zone to associate with the value. Use the default VM
        // time zone if no time zone is otherwise specified.
        TimeZone localTimeZone = (null != timeZoneCalendar) ? timeZoneCalendar.getTimeZone() : TimeZone.getDefault();

        // Assumed time zone associated with the date and time parts of the value.
        //
        // For DATETIMEOFFSET, the date and time parts are assumed to be relative to UTC.
        // For other data types, the date and time parts are assumed to be relative to the local time zone.
        TimeZone componentTimeZone = (SSType.DATETIMEOFFSET == ssType) ? UTC.timeZone : localTimeZone;

        int subSecondNanos;

        // The date and time parts assume a Gregorian calendar with Gregorian leap year behavior
        // over the entire supported range of values. Create and initialize such a calendar to
        // use to interpret the date and time parts in their associated time zone.
        GregorianCalendar cal = new GregorianCalendar(componentTimeZone, Locale.US);

        // Allow overflow in "smaller" fields (such as MILLISECOND and DAY_OF_YEAR) to update
        // "larger" fields (such as HOUR, MINUTE, SECOND, and YEAR, MONTH, DATE).
        cal.setLenient(true);

        // Clear old state from the calendar. Newly created calendars are always initialized to the
        // current date and time.
        cal.clear();

        // Set the calendar value according to the specified local time zone and constituent
        // date (days since base date) and time (ticks since midnight) parts.
        switch (ssType) {
            case TIME: {
                // Set the calendar to the specified value. Lenient calendar behavior will update
                // individual fields according to standard Gregorian leap year rules, which are sufficient
                // for all TIME values.
                //
                // When initializing the value, set the date component to 1/1/1900 to facilitate conversion
                // to String and java.sql.Timestamp. Note that conversion to java.sql.Time, which is
                // the expected majority conversion, resets the date to 1/1/1970. It is not measurably
                // faster to conditionalize the date on the target data type to avoid resetting it.
                //
                // Ticks are in nanoseconds.
                cal.set(TDS.BASE_YEAR_1900, Calendar.JANUARY, 1, 0, 0, 0);
                cal.set(Calendar.MILLISECOND, (int) (ticksSinceMidnight / Nanos.PER_MILLISECOND));

                subSecondNanos = (int) (ticksSinceMidnight % Nanos.PER_SECOND);
                break;
            }

            case DATE:
            case DATETIME2:
            case DATETIMEOFFSET: {
                // For dates after the standard Julian-Gregorian calendar change date,
                // the calendar value can be accurately set using a straightforward
                // (and measurably better performing) assignment.
                //
                // This optimized path is not functionally correct for dates earlier
                // than the standard Gregorian change date.
                if (daysSinceBaseDate >= GregorianChange.DAYS_SINCE_BASE_DATE_HINT) {
                    // Set the calendar to the specified value. Lenient calendar behavior will update
                    // individual fields according to pure Gregorian calendar rules.
                    //
                    // Ticks are in nanoseconds.

                    cal.set(1, Calendar.JANUARY, 1 + daysSinceBaseDate + GregorianChange.EXTRA_DAYS_TO_BE_ADDED, 0, 0, 0);
                    cal.set(Calendar.MILLISECOND, (int) (ticksSinceMidnight / Nanos.PER_MILLISECOND));
                }

                // For dates before the standard change date, it is necessary to rationalize
                // the difference between SQL Server (pure Gregorian) calendar behavior and
                // Java (standard Gregorian) calendar behavior. Rationalization ensures that
                // the "wall calendar" representation of the value on both server and client
                // are the same, taking into account the difference in the respective calendars'
                // leap year rules.
                //
                // This code path is functionally correct, but less performant, than the
                // optimized path above for dates after the standard Gregorian change date.
                else {
                    cal.setGregorianChange(GregorianChange.PURE_CHANGE_DATE);

                    // Set the calendar to the specified value. Lenient calendar behavior will update
                    // individual fields according to pure Gregorian calendar rules.
                    //
                    // Ticks are in nanoseconds.
                    cal.set(1, Calendar.JANUARY, 1 + daysSinceBaseDate, 0, 0, 0);
                    cal.set(Calendar.MILLISECOND, (int) (ticksSinceMidnight / Nanos.PER_MILLISECOND));

                    // Recompute the calendar's internal UTC milliseconds value according to the historically
                    // standard Gregorian cutover date, which is needed for constructing java.sql.Time,
                    // java.sql.Date, and java.sql.Timestamp values from UTC milliseconds.
                    int year = cal.get(Calendar.YEAR);
                    int month = cal.get(Calendar.MONTH);
                    int date = cal.get(Calendar.DATE);
                    int hour = cal.get(Calendar.HOUR_OF_DAY);
                    int minute = cal.get(Calendar.MINUTE);
                    int second = cal.get(Calendar.SECOND);
                    int millis = cal.get(Calendar.MILLISECOND);

                    cal.setGregorianChange(GregorianChange.STANDARD_CHANGE_DATE);
                    cal.set(year, month, date, hour, minute, second);
                    cal.set(Calendar.MILLISECOND, millis);
                }

                // For DATETIMEOFFSET values, recompute the calendar's UTC milliseconds value according
                // to the specified local time zone (the time zone associated with the offset part
                // of the DATETIMEOFFSET value).
                //
                // Optimization: Skip this step if there is no time zone difference
                // (i.e. the time zone of the DATETIMEOFFSET value is UTC).
                if (SSType.DATETIMEOFFSET == ssType && !componentTimeZone.hasSameRules(localTimeZone)) {
                    GregorianCalendar localCalendar = new GregorianCalendar(localTimeZone, Locale.US);
                    localCalendar.clear();
                    localCalendar.setTimeInMillis(cal.getTimeInMillis());
                    cal = localCalendar;
                }

                subSecondNanos = (int) (ticksSinceMidnight % Nanos.PER_SECOND);
                break;
            }

            case DATETIME: // and SMALLDATETIME
            {
                // For Yukon (and earlier) data types DATETIME and SMALLDATETIME, there is no need to
                // change the Gregorian cutover because the earliest representable value (1/1/1753)
                // is after the historically standard cutover date (10/15/1582).

                // Set the calendar to the specified value. Lenient calendar behavior will update
                // individual fields according to standard Gregorian leap year rules, which are sufficient
                // for all values in the supported DATETIME range.
                //
                // Ticks are in milliseconds.
                cal.set(TDS.BASE_YEAR_1900, Calendar.JANUARY, 1 + daysSinceBaseDate, 0, 0, 0);
                cal.set(Calendar.MILLISECOND, (int) ticksSinceMidnight);

                subSecondNanos = (int) ((ticksSinceMidnight * Nanos.PER_MILLISECOND) % Nanos.PER_SECOND);
                break;
            }

            default:
                throw new AssertionError("Unexpected SSType: " + ssType);
        }
        int localMillisOffset;
        if (null == timeZoneCalendar) {
            TimeZone tz = TimeZone.getDefault();
            GregorianCalendar _cal = new GregorianCalendar(componentTimeZone, Locale.US);
            _cal.setLenient(true);
            _cal.clear();
            localMillisOffset = tz.getOffset(_cal.getTimeInMillis());
        }
        else {
            localMillisOffset = timeZoneCalendar.get(Calendar.ZONE_OFFSET);
        }
        // Convert the calendar value (in local time) to the desired Java object type.
        switch (jdbcType.category) {
            case BINARY:
            case SQL_VARIANT: {
                switch (ssType) {
                    case DATE: {
                        // Per JDBC spec, the time part of java.sql.Date values is initialized to midnight
                        // in the specified local time zone.
                        cal.set(Calendar.HOUR_OF_DAY, 0);
                        cal.set(Calendar.MINUTE, 0);
                        cal.set(Calendar.SECOND, 0);
                        cal.set(Calendar.MILLISECOND, 0);
                        return new java.sql.Date(cal.getTimeInMillis());
                    }

                    case DATETIME:
                    case DATETIME2: {
                        java.sql.Timestamp ts = new java.sql.Timestamp(cal.getTimeInMillis());
                        ts.setNanos(subSecondNanos);
                        return ts;
                    }

                    case DATETIMEOFFSET: {
                        // Per driver spec, conversion to DateTimeOffset is only supported from
                        // DATETIMEOFFSET SQL Server values.
                        assert SSType.DATETIMEOFFSET == ssType;

                        // For DATETIMEOFFSET SQL Server values, the time zone offset is in minutes.
                        // The offset from Java TimeZone objects is in milliseconds. Because we
                        // are only dealing with DATETIMEOFFSET SQL Server values here, we can assume
                        // that the offset is precise only to the minute and that rescaling from
                        // milliseconds precision results in no loss of precision.
                        assert 0 == localMillisOffset % (60 * 1000);

                        java.sql.Timestamp ts = new java.sql.Timestamp(cal.getTimeInMillis());
                        ts.setNanos(subSecondNanos);
                        return microsoft.sql.DateTimeOffset.valueOf(ts, localMillisOffset / (60 * 1000));
                    }

                    case TIME: {
                        // Per driver spec, values of sql server data types types (including TIME) which have greater
                        // than millisecond precision are rounded, not truncated, to the nearest millisecond when
                        // converting to java.sql.Time. Since the milliseconds value in the calendar is truncated,
                        // round it now.
                        if (subSecondNanos % Nanos.PER_MILLISECOND >= Nanos.PER_MILLISECOND / 2)
                            cal.add(Calendar.MILLISECOND, 1);

                        // Per JDBC spec, the date part of java.sql.Time values is initialized to 1/1/1970
                        // in the specified local time zone. This must be done after rounding (above) to
                        // prevent rounding values within nanoseconds of the next day from ending up normalized
                        // to 1/2/1970 instead...
                        cal.set(TDS.BASE_YEAR_1970, Calendar.JANUARY, 1);

                        return new java.sql.Time(cal.getTimeInMillis());
                    }

                    default:
                        throw new AssertionError("Unexpected SSType: " + ssType);
                }
            }

            case DATE: {
                // Per JDBC spec, the time part of java.sql.Date values is initialized to midnight
                // in the specified local time zone.
                cal.set(Calendar.HOUR_OF_DAY, 0);
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.SECOND, 0);
                cal.set(Calendar.MILLISECOND, 0);
                return new java.sql.Date(cal.getTimeInMillis());
            }

            case TIME: {
                // Per driver spec, values of sql server data types types (including TIME) which have greater
                // than millisecond precision are rounded, not truncated, to the nearest millisecond when
                // converting to java.sql.Time. Since the milliseconds value in the calendar is truncated,
                // round it now.
                if (subSecondNanos % Nanos.PER_MILLISECOND >= Nanos.PER_MILLISECOND / 2)
                    cal.add(Calendar.MILLISECOND, 1);

                // Per JDBC spec, the date part of java.sql.Time values is initialized to 1/1/1970
                // in the specified local time zone. This must be done after rounding (above) to
                // prevent rounding values within nanoseconds of the next day from ending up normalized
                // to 1/2/1970 instead...
                cal.set(TDS.BASE_YEAR_1970, Calendar.JANUARY, 1);

                return new java.sql.Time(cal.getTimeInMillis());
            }

            case TIMESTAMP: {
                java.sql.Timestamp ts = new java.sql.Timestamp(cal.getTimeInMillis());
                ts.setNanos(subSecondNanos);
                return ts;
            }

            case DATETIMEOFFSET: {
                // Per driver spec, conversion to DateTimeOffset is only supported from
                // DATETIMEOFFSET SQL Server values.
                assert SSType.DATETIMEOFFSET == ssType;

                // For DATETIMEOFFSET SQL Server values, the time zone offset is in minutes.
                // The offset from Java TimeZone objects is in milliseconds. Because we
                // are only dealing with DATETIMEOFFSET SQL Server values here, we can assume
                // that the offset is precise only to the minute and that rescaling from
                // milliseconds precision results in no loss of precision.
                assert 0 == localMillisOffset % (60 * 1000);

                java.sql.Timestamp ts = new java.sql.Timestamp(cal.getTimeInMillis());
                ts.setNanos(subSecondNanos);
                return microsoft.sql.DateTimeOffset.valueOf(ts, localMillisOffset / (60 * 1000));
            }

            case CHARACTER: {
                switch (ssType) {
                    case DATE: {
                        return String.format(Locale.US, "%1$tF", // yyyy-mm-dd
                                cal);
                    }

                    case TIME: {
                        return String.format(Locale.US, "%1$tT%2$s", // hh:mm:ss[.nnnnnnn]
                                cal, fractionalSecondsString(subSecondNanos, fractionalSecondsScale));
                    }

                    case DATETIME2: {
                        return String.format(Locale.US, "%1$tF %1$tT%2$s", // yyyy-mm-dd hh:mm:ss[.nnnnnnn]
                                cal, fractionalSecondsString(subSecondNanos, fractionalSecondsScale));
                    }

                    case DATETIMEOFFSET: {
                        // The offset part of a DATETIMEOFFSET value is precise only to the minute,
                        // but TimeZone returns the raw offset as precise to the millisecond.
                        assert 0 == localMillisOffset % (60 * 1000);

                        int unsignedMinutesOffset = Math.abs(localMillisOffset / (60 * 1000));
                        return String.format(Locale.US, "%1$tF %1$tT%2$s %3$c%4$02d:%5$02d", // yyyy-mm-dd hh:mm:ss[.nnnnnnn] [+|-]hh:mm
                                cal, fractionalSecondsString(subSecondNanos, fractionalSecondsScale), (localMillisOffset >= 0) ? '+' : '-',
                                unsignedMinutesOffset / 60, unsignedMinutesOffset % 60);
                    }

                    case DATETIME: // and SMALLDATETIME
                    {
                        return (new java.sql.Timestamp(cal.getTimeInMillis())).toString();
                    }

                    default:
                        throw new AssertionError("Unexpected SSType: " + ssType);
                }
            }

            default:
                throw new AssertionError("Unexpected JDBCType: " + jdbcType);
        }
    }

    /**
     * Returns the number of days elapsed from January 1 of the specified baseYear (Gregorian) to the specified dayOfYear in the specified year,
     * assuming pure Gregorian calendar rules (no Julian to Gregorian cutover).
     */
    static int daysSinceBaseDate(int year,
            int dayOfYear,
            int baseYear) {
        assert year >= 1;
        assert baseYear >= 1;
        assert dayOfYear >= 1;

        return (dayOfYear - 1) +                        // Days into the current year
                (year - baseYear) * TDS.DAYS_PER_YEAR +  // plus whole years (in days) ...
                leapDaysBeforeYear(year) -               // ... plus leap days
                leapDaysBeforeYear(baseYear);
    }

    /**
     * Returns the number of leap days that have occurred between January 1, 1AD and January 1 of the specified year, assuming a Proleptic Gregorian
     * Calendar
     */
    private static int leapDaysBeforeYear(int year) {
        assert year >= 1;

        // On leap years, the US Naval Observatory says:
        // "According to the Gregorian calendar, which is the civil calendar
        // in use today, years evenly divisible by 4 are leap years, with
        // the exception of centurial years that are not evenly divisible
        // by 400. Therefore, the years 1700, 1800, 1900 and 2100 are not
        // leap years, but 1600, 2000, and 2400 are leap years."
        //
        // So, using year 1AD as a base, we can compute the number of leap
        // days between 1AD and the specified year as follows:
        return (year - 1) / 4 - (year - 1) / 100 + (year - 1) / 400;
    }

    // Maximum allowed RPC decimal value (raw integer value with scale removed).
    // This limits the value to 38 digits of precision for SQL.
    private final static BigInteger maxRPCDecimalValue = new BigInteger("99999999999999999999999999999999999999");

    // Returns true if input bigDecimalValue exceeds allowable
    // TDS wire format precision or scale for DECIMAL TDS token.
    static final boolean exceedsMaxRPCDecimalPrecisionOrScale(BigDecimal bigDecimalValue) {
        if (null == bigDecimalValue)
            return false;

        // Maximum scale allowed is same as maximum precision allowed.
        if (bigDecimalValue.scale() > SQLServerConnection.maxDecimalPrecision)
            return true;

        // Convert to unscaled integer value, then compare with maxRPCDecimalValue.
        // NOTE: Handle negative scale as a special case for JDK 1.5 and later VMs.
        BigInteger bi = (bigDecimalValue.scale() < 0) ? bigDecimalValue.setScale(0).unscaledValue() : bigDecimalValue.unscaledValue();
        if (bigDecimalValue.signum() < 0)
            bi = bi.negate();
        return (bi.compareTo(maxRPCDecimalValue) > 0);
    }

    // Converts a Reader to a String.
    static String convertReaderToString(Reader reader,
            int readerLength) throws SQLServerException {
        assert DataTypes.UNKNOWN_STREAM_LENGTH == readerLength || readerLength >= 0;

        // Optimize simple cases.
        if (null == reader)
            return null;
        if (0 == readerLength)
            return "";

        try {
            // Set up a StringBuilder big enough to hold the Reader value. If we weren't told the size of
            // the value then start with a "reasonable" guess StringBuilder size. If necessary, the StringBuilder
            // will grow automatically to accomodate arbitrary amounts of data.
            StringBuilder sb = new StringBuilder((DataTypes.UNKNOWN_STREAM_LENGTH != readerLength) ? readerLength : 4000);

            // Set up the buffer into which blocks of characters are read from the Reader. This buffer
            // should be no larger than the Reader value's size (if known). For known very large values,
            // limit the buffer's size to reduce this function's memory requirements.
            char charArray[] = new char[(DataTypes.UNKNOWN_STREAM_LENGTH != readerLength && readerLength < 4000) ? readerLength : 4000];

            // Loop and read characters, chunk into StringBuilder until EOS.
            int readChars;

            while ((readChars = reader.read(charArray, 0, charArray.length)) > 0) {
                // Check for invalid bytesRead returned from InputStream.read
                if (readChars > charArray.length) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_errorReadingStream"));
                    Object[] msgArgs = {SQLServerException.getErrString("R_streamReadReturnedInvalidValue")};
                    SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), "", true);
                }

                sb.append(charArray, 0, readChars);
            }

            return sb.toString();
        }
        catch (IOException ioEx) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_errorReadingStream"));
            Object[] msgArgs = {ioEx.toString()};
            SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), "", true);
        }

        // Unreachable code, but needed for compiler.
        return null;
    }
}

/**
 * InputStream implementation that wraps a contained InputStream, filtering it for 7-bit ASCII characters.
 *
 * The wrapped input stream must supply byte values from a SBCS character set whose first 128 entries match the 7-bit US-ASCII character set. Values
 * that lie outside of the 7-bit US-ASCII range are translated to the '?' character.
 */
final class AsciiFilteredInputStream extends InputStream {
    private final InputStream containedStream;
    private final static byte[] ASCII_FILTER;

    static {
        ASCII_FILTER = new byte[256];

        // First 128 entries map ASCII values in to ASCII values out
        for (int i = 0; i < 128; i++)
            ASCII_FILTER[i] = (byte) i;

        // Remaining 128 filter entries map other values to '?'
        for (int i = 128; i < 256; i++)
            ASCII_FILTER[i] = (byte) '?';
    }

    AsciiFilteredInputStream(BaseInputStream containedStream) throws SQLServerException {
        if (BaseInputStream.logger.isLoggable(java.util.logging.Level.FINER))
            BaseInputStream.logger.finer(containedStream.toString() + " wrapping in AsciiFilteredInputStream");
        this.containedStream = containedStream;
    }

    public void close() throws IOException {
        containedStream.close();
    }

    public long skip(long n) throws IOException {
        return containedStream.skip(n);
    }

    public int available() throws IOException {
        return containedStream.available();
    }

    public int read() throws IOException {
        int value = containedStream.read();
        if (value >= 0 && value <= 255)
            return ASCII_FILTER[value];
        return value;
    }

    public int read(byte[] b) throws IOException {
        int bytesRead = containedStream.read(b);
        if (bytesRead > 0) {
            assert bytesRead <= b.length;
            for (int i = 0; i < bytesRead; i++)
                b[i] = ASCII_FILTER[b[i] & 0xFF];
        }
        return bytesRead;
    }

    public int read(byte b[],
            int offset,
            int maxBytes) throws IOException {
        int bytesRead = containedStream.read(b, offset, maxBytes);
        if (bytesRead > 0) {
            assert offset + bytesRead <= b.length;
            for (int i = 0; i < bytesRead; i++)
                b[offset + i] = ASCII_FILTER[b[offset + i] & 0xFF];
        }
        return bytesRead;
    }

    public boolean markSupported() {
        return containedStream.markSupported();
    }

    public void mark(int readLimit) {
        containedStream.mark(readLimit);
    }

    public void reset() throws IOException {
        containedStream.reset();
    }
}

/**
 * InputStream implementation that wraps a contained InputStream, filtering it for 7-bit ASCII characters from UNICODE.
 *
 * The wrapped input stream must supply byte values from a UNICODE character set.
 * 
 */
final class AsciiFilteredUnicodeInputStream extends InputStream {
    private final Reader containedReader;
    private final Charset asciiCharSet;

    static AsciiFilteredUnicodeInputStream MakeAsciiFilteredUnicodeInputStream(BaseInputStream strm,
            Reader rd) throws SQLServerException {
        if (BaseInputStream.logger.isLoggable(java.util.logging.Level.FINER))
            BaseInputStream.logger.finer(strm.toString() + " wrapping in AsciiFilteredInputStream");
        return new AsciiFilteredUnicodeInputStream(rd);
    }

    // Note the Reader provided should support mark, reset
    private AsciiFilteredUnicodeInputStream(Reader rd) throws SQLServerException {
        containedReader = rd;
        asciiCharSet = US_ASCII;
    }

    public void close() throws IOException {
        containedReader.close();
    }

    public long skip(long n) throws IOException {
        return containedReader.skip(n);
    }

    public int available() throws IOException {
        // from the JDBC spec
        // Note: A stream may return 0 when the method InputStream.available is called whether there is data available or not.
        // Reader does not give us available data.
        return 0;
    }

    private final byte[] bSingleByte = new byte[1];

    public int read() throws IOException {
        int bytesRead = read(bSingleByte);
        return (-1 == bytesRead) ? -1 : (bSingleByte[0] & 0xFF);
    }

    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    public int read(byte b[],
            int offset,
            int maxBytes) throws IOException {
        char tempBufferToHoldCharDataForConversion[] = new char[maxBytes];
        int charsRead = containedReader.read(tempBufferToHoldCharDataForConversion);

        if (charsRead > 0) {
            if (charsRead < maxBytes)
                maxBytes = charsRead;
            ByteBuffer encodedBuff = asciiCharSet.encode(CharBuffer.wrap(tempBufferToHoldCharDataForConversion));
            encodedBuff.get(b, offset, maxBytes);
        }
        return charsRead;
    }

    public boolean markSupported() {
        return containedReader.markSupported();
    }

    public void mark(int readLimit) {
        try {
            containedReader.mark(readLimit);
        }
        catch (IOException e) {
            // unfortunately inputstream mark does not throw an exception so we have to eat any exception from the reader here
            // likely to be a bug in the original InputStream spec.
        }
    }

    public void reset() throws IOException {
        containedReader.reset();
    }
}

// Helper class to hold + pass around stream/reader setter arguments.
final class StreamSetterArgs {
    private long length;

    final long getLength() {
        return length;
    }

    final void setLength(long newLength) {
        // We only expect length to be changed from an initial unknown value (-1)
        // to an actual length (+ve or 0).
        assert DataTypes.UNKNOWN_STREAM_LENGTH == length;
        assert newLength >= 0;
        length = newLength;
    }

    final StreamType streamType;

    StreamSetterArgs(StreamType streamType,
            long length) {
        this.streamType = streamType;
        this.length = length;
    }
}

// Helper class to hold + pass around InputStream getter arguments.
final class InputStreamGetterArgs {
    final StreamType streamType;
    final boolean isAdaptive;
    final boolean isStreaming;
    final String logContext;

    static final InputStreamGetterArgs defaultArgs = new InputStreamGetterArgs(StreamType.NONE, false, false, "");

    static final InputStreamGetterArgs getDefaultArgs() {
        return defaultArgs;
    }

    InputStreamGetterArgs(StreamType streamType,
            boolean isAdaptive,
            boolean isStreaming,
            String logContext) {
        this.streamType = streamType;
        this.isAdaptive = isAdaptive;
        this.isStreaming = isStreaming;
        this.logContext = logContext;
    }
}
