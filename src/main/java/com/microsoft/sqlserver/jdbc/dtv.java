/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static java.nio.charset.StandardCharsets.UTF_16LE;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.Calendar;
import java.util.EnumMap;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.Map;
import java.util.SimpleTimeZone;
import java.util.TimeZone;
import java.util.UUID;

import com.microsoft.sqlserver.jdbc.JavaType.SetterConversionAE;

/**
 * Defines an abstraction for execution of type-specific operations on DTV values.
 *
 * This abstract design keeps the logic of determining how to handle particular DTV value and jdbcType combinations in a single place
 * (DTV.executeOp()) and forces new operations to be able to handle *all* the required combinations.
 *
 * Current operations and their locations are:
 *
 * Parameter.GetTypeDefinitionOp Determines the specific parameter type definition according to the current value.
 *
 * DTV.SendByRPCOp (below) Marshals the value onto the binary (RPC) buffer for sending to the server.
 */
abstract class DTVExecuteOp {
    abstract void execute(DTV dtv,
            String strValue) throws SQLServerException;

    abstract void execute(DTV dtv,
            Clob clobValue) throws SQLServerException;

    abstract void execute(DTV dtv,
            Byte byteValue) throws SQLServerException;

    abstract void execute(DTV dtv,
            Integer intValue) throws SQLServerException;

    abstract void execute(DTV dtv,
            java.sql.Time timeValue) throws SQLServerException;

    abstract void execute(DTV dtv,
            java.sql.Date dateValue) throws SQLServerException;

    abstract void execute(DTV dtv,
            java.sql.Timestamp timestampValue) throws SQLServerException;

    abstract void execute(DTV dtv,
            java.util.Date utilDateValue) throws SQLServerException;

    abstract void execute(DTV dtv,
            java.util.Calendar calendarValue) throws SQLServerException;

    abstract void execute(DTV dtv,
            LocalDate localDateValue) throws SQLServerException;

    abstract void execute(DTV dtv,
            LocalTime localTimeValue) throws SQLServerException;

    abstract void execute(DTV dtv,
            LocalDateTime localDateTimeValue) throws SQLServerException;

    abstract void execute(DTV dtv,
            OffsetTime offsetTimeValue) throws SQLServerException;

    abstract void execute(DTV dtv,
            OffsetDateTime offsetDateTimeValue) throws SQLServerException;

    abstract void execute(DTV dtv,
            microsoft.sql.DateTimeOffset dtoValue) throws SQLServerException;

    abstract void execute(DTV dtv,
            Float floatValue) throws SQLServerException;

    abstract void execute(DTV dtv,
            Double doubleValue) throws SQLServerException;

    abstract void execute(DTV dtv,
            BigDecimal bigDecimalValue) throws SQLServerException;

    abstract void execute(DTV dtv,
            Long longValue) throws SQLServerException;

    abstract void execute(DTV dtv,
            BigInteger bigIntegerValue) throws SQLServerException;

    abstract void execute(DTV dtv,
            Short shortValue) throws SQLServerException;

    abstract void execute(DTV dtv,
            Boolean booleanValue) throws SQLServerException;

    abstract void execute(DTV dtv,
            byte[] byteArrayValue) throws SQLServerException;

    abstract void execute(DTV dtv,
            Blob blobValue) throws SQLServerException;

    abstract void execute(DTV dtv,
            InputStream inputStreamValue) throws SQLServerException;

    abstract void execute(DTV dtv,
            Reader readerValue) throws SQLServerException;

    abstract void execute(DTV dtv,
            SQLServerSQLXML xmlValue) throws SQLServerException;

    abstract void execute(DTV dtv,
            TVP tvpValue) throws SQLServerException;

    abstract void execute(DTV dtv,
            SqlVariant sqlVariantValue) throws SQLServerException;
}

/**
 * Outer level DTV class.
 *
 * All DTV manipulation is done through this class.
 */
final class DTV {
    static final private java.util.logging.Logger aeLogger = java.util.logging.Logger.getLogger("com.microsoft.sqlserver.jdbc.DTV");

    /** The source (app or server) providing the data for this value. */
    private DTVImpl impl;

    CryptoMetadata cryptoMeta = null;
    JDBCType jdbcTypeSetByUser = null;
    int valueLength = 0;
    boolean sendStringParametersAsUnicode = true;

    /**
     * Sets a DTV value from a Java object.
     *
     * The new value replaces any value (column or parameter return value) that was previously set via the TDS response. Doing this may change the
     * DTV's internal implementation to an AppDTVImpl.
     */
    void setValue(SQLCollation collation,
            JDBCType jdbcType,
            Object value,
            JavaType javaType,
            StreamSetterArgs streamSetterArgs,
            Calendar calendar,
            Integer scale,
            SQLServerConnection con,
            boolean forceEncrypt) throws SQLServerException {
        if (null == impl)
            impl = new AppDTVImpl();

        impl.setValue(this, collation, jdbcType, value, javaType, streamSetterArgs, calendar, scale, con, forceEncrypt);
    }

    final void setValue(Object value,
            JavaType javaType) {
        impl.setValue(value, javaType);
    }

    final void clear() {
        impl = null;
    }

    final void skipValue(TypeInfo type,
            TDSReader tdsReader,
            boolean isDiscard) throws SQLServerException {
        if (null == impl)
            impl = new ServerDTVImpl();

        impl.skipValue(type, tdsReader, isDiscard);
    }

    final void initFromCompressedNull() {
        if (null == impl)
            impl = new ServerDTVImpl();

        impl.initFromCompressedNull();
    }

    final void setStreamSetterArgs(StreamSetterArgs streamSetterArgs) {
        impl.setStreamSetterArgs(streamSetterArgs);
    }

    final void setCalendar(Calendar calendar) {
        impl.setCalendar(calendar);
    }

    final void setScale(Integer scale) {
        impl.setScale(scale);
    }

    final void setForceEncrypt(boolean forceEncrypt) {
        impl.setForceEncrypt(forceEncrypt);
    }

    StreamSetterArgs getStreamSetterArgs() {
        return impl.getStreamSetterArgs();
    }

    Calendar getCalendar() {
        return impl.getCalendar();
    }

    Integer getScale() {
        return impl.getScale();
    }

    /**
     * Returns whether the DTV's current value is null.
     */
    boolean isNull() {
        return null == impl || impl.isNull();
    }

    /**
     * @return true if impl is not null
     */
    final boolean isInitialized() {
        return (null != impl);
    }

    final void setJdbcType(JDBCType jdbcType) {
        if (null == impl)
            impl = new AppDTVImpl();

        impl.setJdbcType(jdbcType);
    }

    /**
     * Returns the DTV's current JDBC type
     */
    final JDBCType getJdbcType() {
        assert null != impl;
        return impl.getJdbcType();
    }

    /**
     * Returns the DTV's current JDBC type
     */
    final JavaType getJavaType() {
        assert null != impl;
        return impl.getJavaType();
    }

    /**
     * Returns the DTV's current value as the specified type.
     *
     * This variant of getValue() takes an extra parameter to handle the few cases where extra arguments are needed to determine the value (e.g. a
     * Calendar object for time-valued DTV values).
     */
    Object getValue(JDBCType jdbcType,
            int scale,
            InputStreamGetterArgs streamGetterArgs,
            Calendar cal,
            TypeInfo typeInfo,
            CryptoMetadata cryptoMetadata,
            TDSReader tdsReader) throws SQLServerException {
        if (null == impl)
            impl = new ServerDTVImpl();
        return impl.getValue(this, jdbcType, scale, streamGetterArgs, cal, typeInfo, cryptoMetadata, tdsReader);
    }

    Object getSetterValue() {
        return impl.getSetterValue();
    }

    SqlVariant getInternalVariant() {
        return impl.getInternalVariant();
    }
    
    /**
     * Called by DTV implementation instances to change to a different DTV implementation.
     */
    void setImpl(DTVImpl impl) {
        this.impl = impl;
    }

    final class SendByRPCOp extends DTVExecuteOp {
        private final String name;
        private final TypeInfo typeInfo;
        private final SQLCollation collation;
        private final int precision;
        private final int outScale;
        private final boolean isOutParam;
        private final TDSWriter tdsWriter;
        private final SQLServerConnection conn;

        SendByRPCOp(String name,
                TypeInfo typeInfo,
                SQLCollation collation,
                int precision,
                int outScale,
                boolean isOutParam,
                TDSWriter tdsWriter,
                SQLServerConnection conn) {
            this.name = name;
            this.typeInfo = typeInfo;
            this.collation = collation;
            this.precision = precision;
            this.outScale = outScale;
            this.isOutParam = isOutParam;
            this.tdsWriter = tdsWriter;
            this.conn = conn;
        }

        void execute(DTV dtv,
                String strValue) throws SQLServerException {
            tdsWriter.writeRPCStringUnicode(name, strValue, isOutParam, collation);
        }

        void execute(DTV dtv,
                Clob clobValue) throws SQLServerException {
            // executeOp should have handled null Clob as a String
            assert null != clobValue;

            long clobLength = 0;
            Reader clobReader = null;

            try {
                clobLength = DataTypes.getCheckedLength(conn, dtv.getJdbcType(), clobValue.length(), false);
                clobReader = clobValue.getCharacterStream();
            }
            catch (SQLException e) {
                SQLServerException.makeFromDriverError(conn, null, e.getMessage(), null, false);
            }

            // If the Clob value is to be sent as MBCS, then convert the value to an MBCS InputStream
            JDBCType jdbcType = dtv.getJdbcType();
            if (null != collation
                    && (JDBCType.CHAR == jdbcType || JDBCType.VARCHAR == jdbcType || JDBCType.LONGVARCHAR == jdbcType || JDBCType.CLOB == jdbcType)) {
                if (null == clobReader) {
                    tdsWriter.writeRPCByteArray(name, null, isOutParam, jdbcType, collation);
                }
                else {
                    ReaderInputStream clobStream = new ReaderInputStream(clobReader, collation.getCharset(), clobLength);

                    tdsWriter.writeRPCInputStream(name, clobStream, DataTypes.UNKNOWN_STREAM_LENGTH, isOutParam, jdbcType, collation);
                }
            }
            else // Send CLOB value as Unicode
            {
                if (null == clobReader) {
                    tdsWriter.writeRPCStringUnicode(name, null, isOutParam, collation);
                }
                else {
                    tdsWriter.writeRPCReaderUnicode(name, clobReader, clobLength, isOutParam, collation);
                }
            }
        }

        void execute(DTV dtv,
                Byte byteValue) throws SQLServerException {
            tdsWriter.writeRPCByte(name, byteValue, isOutParam);
        }

        void execute(DTV dtv,
                Integer intValue) throws SQLServerException {
            tdsWriter.writeRPCInt(name, intValue, isOutParam);
        }

        void execute(DTV dtv,
                java.sql.Time timeValue) throws SQLServerException {
            sendTemporal(dtv, JavaType.TIME, timeValue);
        }

        void execute(DTV dtv,
                java.sql.Date dateValue) throws SQLServerException {
            sendTemporal(dtv, JavaType.DATE, dateValue);
        }

        void execute(DTV dtv,
                java.sql.Timestamp timestampValue) throws SQLServerException {
            sendTemporal(dtv, JavaType.TIMESTAMP, timestampValue);
        }

        void execute(DTV dtv,
                java.util.Date utilDateValue) throws SQLServerException {
            sendTemporal(dtv, JavaType.UTILDATE, utilDateValue);
        }

        void execute(DTV dtv,
                java.util.Calendar calendarValue) throws SQLServerException {
            sendTemporal(dtv, JavaType.CALENDAR, calendarValue);
        }

        void execute(DTV dtv,
                LocalDate localDateValue) throws SQLServerException {
            sendTemporal(dtv, JavaType.LOCALDATE, localDateValue);
        }

        void execute(DTV dtv,
                LocalTime localTimeValue) throws SQLServerException {
            sendTemporal(dtv, JavaType.LOCALTIME, localTimeValue);
        }

        void execute(DTV dtv,
                LocalDateTime localDateTimeValue) throws SQLServerException {
            sendTemporal(dtv, JavaType.LOCALDATETIME, localDateTimeValue);
        }

        void execute(DTV dtv,
                OffsetTime offsetTimeValue) throws SQLServerException {
            sendTemporal(dtv, JavaType.OFFSETTIME, offsetTimeValue);
        }

        void execute(DTV dtv,
                OffsetDateTime offsetDateTimeValue) throws SQLServerException {
            sendTemporal(dtv, JavaType.OFFSETDATETIME, offsetDateTimeValue);
        }

        void execute(DTV dtv,
                microsoft.sql.DateTimeOffset dtoValue) throws SQLServerException {
            sendTemporal(dtv, JavaType.DATETIMEOFFSET, dtoValue);
        }

        void execute(DTV dtv,
                TVP tvpValue) throws SQLServerException {
            // shouldn't be an output parameter
            tdsWriter.writeTVP(tvpValue);
        }

        /**
         * Clears the calendar and then sets only the fields passed in. Rest of the fields will have default values.
         */
        private void clearSetCalendar(Calendar cal,
                boolean lenient,
                Integer year,
                Integer month,
                Integer day_of_month,
                Integer hour_of_day,
                Integer minute,
                Integer second) {
            cal.clear();
            cal.setLenient(lenient);
            if (null != year) {
                cal.set(Calendar.YEAR, year);
            }
            if (null != month) {
                cal.set(Calendar.MONTH, month);
            }
            if (null != day_of_month) {
                cal.set(Calendar.DAY_OF_MONTH, day_of_month);
            }
            if (null != hour_of_day) {
                cal.set(Calendar.HOUR_OF_DAY, hour_of_day);
            }
            if (null != minute) {
                cal.set(Calendar.MINUTE, minute);
            }
            if (null != second) {
                cal.set(Calendar.SECOND, second);
            }
        }

        /**
         * Sends the specified temporal type value to the server as the appropriate SQL Server type.
         *
         * To send the value to the server, this method does the following: 1) Converts its given temporal value argument into a common form
         * encapsulated by a pure, lenient GregorianCalendar instance. 2) Normalizes that common form according to the target data type. 3) Sends the
         * value to the server using the appropriate target data type method.
         *
         * @param dtv
         *            DTV with type info, etc.
         * @param javaType
         *            the Java type of the Object that follows
         * @param value
         *            the temporal value to send to the server. May be null.
         */
        private void sendTemporal(DTV dtv,
                JavaType javaType,
                Object value) throws SQLServerException {
            JDBCType jdbcType = dtv.getJdbcType();
            GregorianCalendar calendar = null;
            int subSecondNanos = 0;
            int minutesOffset = 0;

            /*
             * Some precisions to consider: java.sql.Time is millisecond precision java.sql.Timestamp is nanosecond precision java.util.Date is
             * millisecond precision java.util.Calendar is millisecond precision java.time.LocalTime is nanosecond precision java.time.LocalDateTime
             * is nanosecond precision java.time.OffsetTime is nanosecond precision, with a zone offset java.time.OffsetDateTime is nanosecond
             * precision, with a zone offset SQL Server Types: datetime2 is 7 digit precision, i.e 100 ns (default and max) datetime is 3 digit
             * precision, i.e ms precision (default and max) time is 7 digit precision, i.e 100 ns (default and max) Note: sendTimeAsDatetime is true
             * by default and it actually sends the time value as datetime, not datetime2 which looses precision values as datetime is only MS
             * precision (1/300 of a second to be precise).
             * 
             * Null values pass right on through to the typed writers below.
             *
             * For non-null values, load the value from its original Java object (java.sql.Time, java.sql.Date, java.sql.Timestamp, java.util.Date,
             * java.time.LocalDate, java.time.LocalTime, java.time.LocalDateTime or microsoft.sql.DateTimeOffset) into a Gregorian calendar. Don't use
             * the DTV's calendar directly, as it may not be Gregorian...
             */
            if (null != value) {
                TimeZone timeZone = TimeZone.getDefault(); // Time zone to associate with the value in the Gregorian calendar
                long utcMillis = 0;    // Value to which the calendar is to be set (in milliseconds 1/1/1970 00:00:00 GMT)

                // Figure out the value components according to the type of the Java object passed in...
                switch (javaType) {
                    case TIME: {
                        // Set the time zone from the calendar supplied by the app or use the JVM default
                        timeZone = (null != dtv.getCalendar()) ? dtv.getCalendar().getTimeZone() : TimeZone.getDefault();

                        utcMillis = ((java.sql.Time) value).getTime();
                        subSecondNanos = Nanos.PER_MILLISECOND * (int) (utcMillis % 1000);

                        // The utcMillis value may be negative for morning times in time zones east of GMT.
                        // Since the date part of the java.sql.Time object is normalized to 1/1/1970
                        // in the local time zone, the date may still be 12/31/1969 UTC.
                        //
                        // If that is the case then adjust the sub-second nanos to the correct non-negative
                        // "wall clock" value. For example: -1 nanos (one nanosecond before midnight) becomes
                        // 999999999 nanos (999,999,999 nanoseconds after 11:59:59).
                        if (subSecondNanos < 0)
                            subSecondNanos += Nanos.PER_SECOND;

                        break;
                    }

                    case DATE: {
                        // Set the time zone from the calendar supplied by the app or use the JVM default
                        timeZone = (null != dtv.getCalendar()) ? dtv.getCalendar().getTimeZone() : TimeZone.getDefault();

                        utcMillis = ((java.sql.Date) value).getTime();
                        break;
                    }

                    case TIMESTAMP: {
                        // Set the time zone from the calendar supplied by the app or use the JVM default
                        timeZone = (null != dtv.getCalendar()) ? dtv.getCalendar().getTimeZone() : TimeZone.getDefault();

                        java.sql.Timestamp timestampValue = (java.sql.Timestamp) value;
                        utcMillis = timestampValue.getTime();
                        subSecondNanos = timestampValue.getNanos();
                        break;
                    }

                    case UTILDATE: {
                        // java.util.Date is mapped to JDBC type TIMESTAMP
                        // java.util.Date and java.sql.Date are both millisecond precision
                        // Set the time zone from the calendar supplied by the app or use the JVM default
                        timeZone = (null != dtv.getCalendar()) ? dtv.getCalendar().getTimeZone() : TimeZone.getDefault();

                        utcMillis = ((java.util.Date) value).getTime();

                        // Need to use the subsecondnanoes part in UTILDATE besause it is mapped to JDBC TIMESTAMP. This is not
                        // needed in DATE because DATE is mapped to JDBC DATE (with time part normalized to midnight)
                        subSecondNanos = Nanos.PER_MILLISECOND * (int) (utcMillis % 1000);

                        // The utcMillis value may be negative for morning times in time zones east of GMT.
                        // Since the date part of the java.sql.Time object is normalized to 1/1/1970
                        // in the local time zone, the date may still be 12/31/1969 UTC.
                        //
                        // If that is the case then adjust the sub-second nanos to the correct non-negative
                        // "wall clock" value. For example: -1 nanos (one nanosecond before midnight) becomes
                        // 999999999 nanos (999,999,999 nanoseconds after 11:59:59).
                        if (subSecondNanos < 0)
                            subSecondNanos += Nanos.PER_SECOND;
                        break;
                    }

                    case CALENDAR: {
                        // java.util.Calendar is mapped to JDBC type TIMESTAMP
                        // java.util.Calendar is millisecond precision
                        // Set the time zone from the calendar supplied by the app or use the JVM default
                        timeZone = (null != dtv.getCalendar()) ? dtv.getCalendar().getTimeZone() : TimeZone.getDefault();

                        utcMillis = ((java.util.Calendar) value).getTimeInMillis();

                        // Need to use the subsecondnanoes part in CALENDAR besause it is mapped to JDBC TIMESTAMP. This is not
                        // needed in DATE because DATE is mapped to JDBC DATE (with time part normalized to midnight)
                        subSecondNanos = Nanos.PER_MILLISECOND * (int) (utcMillis % 1000);

                        // The utcMillis value may be negative for morning times in time zones east of GMT.
                        // Since the date part of the java.sql.Time object is normalized to 1/1/1970
                        // in the local time zone, the date may still be 12/31/1969 UTC.
                        //
                        // If that is the case then adjust the sub-second nanos to the correct non-negative
                        // "wall clock" value. For example: -1 nanos (one nanosecond before midnight) becomes
                        // 999999999 nanos (999,999,999 nanoseconds after 11:59:59).
                        if (subSecondNanos < 0)
                            subSecondNanos += Nanos.PER_SECOND;
                        break;
                    }

                    case LOCALDATE:
                        // Mapped to JDBC type DATE
                        calendar = new GregorianCalendar(UTC.timeZone, Locale.US);

                        // All time fields are set to default
                        clearSetCalendar(calendar, true, ((LocalDate) value).getYear(), ((LocalDate) value).getMonthValue() - 1, // Calendar 'month'
                                                                                                                                 // is 0-based, but
                                                                                                                                 // LocalDate 'month'
                                                                                                                                 // is 1-based
                                ((LocalDate) value).getDayOfMonth(), null, null, null);
                        break;

                    case LOCALTIME:
                        // Nanoseconds precision, mapped to JDBC type TIME
                        calendar = new GregorianCalendar(UTC.timeZone, Locale.US);

                        // All date fields are set to default
                        LocalTime LocalTimeValue = ((LocalTime) value);
                        clearSetCalendar(calendar, true, conn.baseYear(), 1, 1, LocalTimeValue.getHour(), // Gets hour_of_day field
                                LocalTimeValue.getMinute(), LocalTimeValue.getSecond());
                        subSecondNanos = LocalTimeValue.getNano();

                        // Do not need to adjust subSecondNanos as in the case for TIME
                        // because LOCALTIME does not have time zone and is not using utcMillis
                        break;

                    case LOCALDATETIME:
                        // Nanoseconds precision, mapped to JDBC type TIMESTAMP

                        calendar = new GregorianCalendar(UTC.timeZone, Locale.US);
                        // Calendar 'month' is 0-based, but LocalDateTime 'month' is 1-based
                        LocalDateTime localDateTimeValue = (LocalDateTime) value;
                        clearSetCalendar(calendar, true, localDateTimeValue.getYear(), localDateTimeValue.getMonthValue() - 1,
                                localDateTimeValue.getDayOfMonth(), localDateTimeValue.getHour(), // Gets hour_of_day field
                                localDateTimeValue.getMinute(), localDateTimeValue.getSecond());
                        subSecondNanos = localDateTimeValue.getNano();

                        // Do not need to adjust subSecondNanos as in the case for TIME
                        // because LOCALDATETIME does not have time zone and is not using utcMillis
                        break;

                    case OFFSETTIME:
                        OffsetTime offsetTimeValue = (OffsetTime) value;
                        try {
                            // offsetTimeValue.getOffset() returns a ZoneOffset object which has only hours and minutes
                            // components. So the result of the division will be an integer always. SQL Server also supports
                            // offsets in minutes precision.
                            minutesOffset = offsetTimeValue.getOffset().getTotalSeconds() / 60;
                        }
                        catch (Exception e) {
                            throw new SQLServerException(SQLServerException.getErrString("R_zoneOffsetError"), null, // SQLState is null as this error
                                                                                                                     // is generated in the driver
                                    0, // Use 0 instead of DriverError.NOT_SET to use the correct constructor
                                    e);
                        }
                        subSecondNanos = offsetTimeValue.getNano();

                        // If the target data type is TIME_WITH_TIMEZONE, then use UTC for the calendar that
                        // will hold the value, since writeRPCDateTimeOffset expects a UTC calendar.
                        // Otherwise, when converting from DATETIMEOFFSET to other temporal data types,
                        // use a local time zone determined by the minutes offset of the value, since
                        // the writers for those types expect local calendars.
                        timeZone = (JDBCType.TIME_WITH_TIMEZONE == jdbcType && (null == typeInfo || SSType.DATETIMEOFFSET == typeInfo.getSSType())) ?

                                UTC.timeZone : new SimpleTimeZone(minutesOffset * 60 * 1000, "");

                        // The behavior is similar to microsoft.sql.DateTimeOffset
                        // In Timestamp format, leading zeros for the fields can be omitted.
                        String offsetTimeStr = conn.baseYear() + "-01-01" + ' ' + offsetTimeValue.getHour() + ':' + offsetTimeValue.getMinute() + ':'
                                + offsetTimeValue.getSecond();
                        utcMillis = Timestamp.valueOf(offsetTimeStr).getTime();
                        break;

                    case OFFSETDATETIME:
                        OffsetDateTime offsetDateTimeValue = (OffsetDateTime) value;
                        try {
                            // offsetTimeValue.getOffset() returns a ZoneOffset object which has only hours and minutes
                            // components. So the result of the division will be an integer always. SQL Server also supports
                            // offsets in minutes precision.
                            minutesOffset = offsetDateTimeValue.getOffset().getTotalSeconds() / 60;
                        }
                        catch (Exception e) {
                            throw new SQLServerException(SQLServerException.getErrString("R_zoneOffsetError"), null, // SQLState is null as this error
                                                                                                                     // is generated in the driver
                                    0, // Use 0 instead of DriverError.NOT_SET to use the correct constructor
                                    e);
                        }

                        subSecondNanos = offsetDateTimeValue.getNano();

                        // If the target data type is TIME_WITH_TIMEZONE or TIMESTAMP_WITH_TIMEZONE, then use UTC for the calendar that
                        // will hold the value, since writeRPCDateTimeOffset expects a UTC calendar.
                        // Otherwise, when converting from DATETIMEOFFSET to other temporal data types,
                        // use a local time zone determined by the minutes offset of the value, since
                        // the writers for those types expect local calendars.
                        timeZone = ((JDBCType.TIMESTAMP_WITH_TIMEZONE == jdbcType || JDBCType.TIME_WITH_TIMEZONE == jdbcType)
                                && (null == typeInfo || SSType.DATETIMEOFFSET == typeInfo.getSSType())) ? UTC.timeZone
                                        : new SimpleTimeZone(minutesOffset * 60 * 1000, "");

                        // The behavior is similar to microsoft.sql.DateTimeOffset
                        // In Timestamp format, only YEAR needs to have 4 digits. The leading zeros for the rest of the fields can be omitted.
                        String offDateTimeStr = String.format("%04d", offsetDateTimeValue.getYear()) + '-' + offsetDateTimeValue.getMonthValue() + '-'
                                + offsetDateTimeValue.getDayOfMonth() + ' ' + offsetDateTimeValue.getHour() + ':' + offsetDateTimeValue.getMinute()
                                + ':' + offsetDateTimeValue.getSecond();
                        utcMillis = Timestamp.valueOf(offDateTimeStr).getTime();
                        break;

                    case DATETIMEOFFSET: {
                        microsoft.sql.DateTimeOffset dtoValue = (microsoft.sql.DateTimeOffset) value;
                        utcMillis = dtoValue.getTimestamp().getTime();
                        subSecondNanos = dtoValue.getTimestamp().getNanos();
                        minutesOffset = dtoValue.getMinutesOffset();

                        // microsoft.sql.DateTimeOffset values have a time zone offset that is internal
                        // to the value, so there should not be any DTV calendar for DateTimeOffset values.
                        assert null == dtv.getCalendar();

                        // If the target data type is DATETIMEOFFSET, then use UTC for the calendar that
                        // will hold the value, since writeRPCDateTimeOffset expects a UTC calendar.
                        // Otherwise, when converting from DATETIMEOFFSET to other temporal data types,
                        // use a local time zone determined by the minutes offset of the value, since
                        // the writers for those types expect local calendars.
                        timeZone = (JDBCType.DATETIMEOFFSET == jdbcType && (null == typeInfo || SSType.DATETIMEOFFSET == typeInfo.getSSType()
                                || SSType.VARBINARY == typeInfo.getSSType() || SSType.VARBINARYMAX == typeInfo.getSSType())) ?

                                        UTC.timeZone : new SimpleTimeZone(minutesOffset * 60 * 1000, "");

                        break;
                    }

                    default:
                        throw new AssertionError("Unexpected JavaType: " + javaType);
                }

                // For the LocalDate, LocalTime and LocalDateTime values, calendar should be set by now.
                if (null == calendar) {
                    // Create the calendar that will hold the value. For DateTimeOffset values, the calendar's
                    // time zone is UTC. For other values, the calendar's time zone is a local time zone.
                    calendar = new GregorianCalendar(timeZone, Locale.US);

                    // Set the calendar lenient to allow setting the DAY_OF_YEAR and MILLISECOND fields
                    // to roll other fields to their correct values.
                    calendar.setLenient(true);

                    // Clear the calendar of any existing state. The state of a new Calendar object always
                    // reflects the current date, time, DST offset, etc.
                    calendar.clear();

                    // Load the calendar with the desired value
                    calendar.setTimeInMillis(utcMillis);
                }

            }

            // With the value now stored in a Calendar object, determine the backend data type and
            // write out the value to the server, after any necessarily normalization.
            // typeInfo is null when called from PreparedStatement->Parameter->SendByRPC
            if (null != typeInfo) // updater
            {
                switch (typeInfo.getSSType()) {
                    case DATETIME2:
                        // Default and max fractional precision is 7 digits (100ns)
                        tdsWriter.writeRPCDateTime2(name, timestampNormalizedCalendar(calendar, javaType, conn.baseYear()), subSecondNanos,
                                typeInfo.getScale(), isOutParam);

                        break;

                    case DATE:
                        tdsWriter.writeRPCDate(name, calendar, isOutParam);

                        break;

                    case TIME:
                        // Default and max fractional precision is 7 digits (100ns)
                        tdsWriter.writeRPCTime(name, calendar, subSecondNanos, typeInfo.getScale(), isOutParam);

                        break;

                    case DATETIMEOFFSET:
                        // When converting from any other temporal Java type to DATETIMEOFFSET,
                        // deliberately interpret the "wall calendar" representation as expressing
                        // a date/time in UTC rather than the local time zone.
                        if (JavaType.DATETIMEOFFSET != javaType) {
                            calendar = timestampNormalizedCalendar(localCalendarAsUTC(calendar), javaType, conn.baseYear());

                            minutesOffset = 0; // UTC
                        }

                        tdsWriter.writeRPCDateTimeOffset(name, calendar, minutesOffset, subSecondNanos, typeInfo.getScale(), isOutParam);

                        break;

                    case DATETIME:
                    case SMALLDATETIME:
                        tdsWriter.writeRPCDateTime(name, timestampNormalizedCalendar(calendar, javaType, conn.baseYear()), subSecondNanos,
                                isOutParam);
                        break;

                    case VARBINARY:
                    case VARBINARYMAX:
                        switch (jdbcType) {
                            case DATETIME:
                            case SMALLDATETIME:
                                tdsWriter.writeEncryptedRPCDateTime(name, timestampNormalizedCalendar(calendar, javaType, conn.baseYear()),
                                        subSecondNanos, isOutParam, jdbcType);
                                break;

                            case TIMESTAMP:
                                assert null != cryptoMeta;
                                tdsWriter.writeEncryptedRPCDateTime2(name, timestampNormalizedCalendar(calendar, javaType, conn.baseYear()),
                                        subSecondNanos, valueLength, isOutParam);
                                break;

                            case TIME:
                                // when colum is encrypted, always send time as time, ignore sendTimeAsDatetime setting
                                assert null != cryptoMeta;
                                tdsWriter.writeEncryptedRPCTime(name, calendar, subSecondNanos, valueLength, isOutParam);
                                break;

                            case DATE:
                                assert null != cryptoMeta;
                                tdsWriter.writeEncryptedRPCDate(name, calendar, isOutParam);
                                break;

                            case TIMESTAMP_WITH_TIMEZONE:
                            case DATETIMEOFFSET:
                                // When converting from any other temporal Java type to DATETIMEOFFSET/TIMESTAMP_WITH_TIMEZONE,
                                // deliberately reinterpret the value as local to UTC. This is to match
                                // SQL Server behavior for such conversions.
                                if ((JavaType.DATETIMEOFFSET != javaType) && (JavaType.OFFSETDATETIME != javaType)) {
                                    calendar = timestampNormalizedCalendar(localCalendarAsUTC(calendar), javaType, conn.baseYear());

                                    minutesOffset = 0; // UTC
                                }

                                assert null != cryptoMeta;
                                tdsWriter.writeEncryptedRPCDateTimeOffset(name, calendar, minutesOffset, subSecondNanos, valueLength, isOutParam);
                                break;

                            default:
                                assert false : "Unexpected JDBCType: " + jdbcType;
                        }
                        break;

                    default:
                        assert false : "Unexpected SSType: " + typeInfo.getSSType();
                }
            }
            else // setter
            {
                // Katmai and later
                // ----------------
                //
                // When sending as...
                // - java.sql.Types.TIMESTAMP, use DATETIME2 SQL Server data type
                // - java.sql.Types.TIME, use TIME or DATETIME SQL Server data type
                // as determined by sendTimeAsDatetime setting
                // - java.sql.Types.DATE, use DATE SQL Server data type
                // - microsoft.sql.Types.DATETIMEOFFSET, use DATETIMEOFFSET SQL Server data type
                if (conn.isKatmaiOrLater()) {
                    if (aeLogger.isLoggable(java.util.logging.Level.FINE) && (null != cryptoMeta)) {
                        aeLogger.fine("Encrypting temporal data type.");
                    }

                    switch (jdbcType) {
                        case DATETIME:
                        case SMALLDATETIME:
                        case TIMESTAMP:
                            if (null != cryptoMeta) {
                                if ((JDBCType.DATETIME == jdbcType) || (JDBCType.SMALLDATETIME == jdbcType)) {
                                    tdsWriter.writeEncryptedRPCDateTime(name, timestampNormalizedCalendar(calendar, javaType, conn.baseYear()),
                                            subSecondNanos, isOutParam, jdbcType);
                                }
                                else if (0 == valueLength) {
                                    tdsWriter.writeEncryptedRPCDateTime2(name, timestampNormalizedCalendar(calendar, javaType, conn.baseYear()),
                                            subSecondNanos, outScale, isOutParam);
                                }
                                else {
                                    tdsWriter.writeEncryptedRPCDateTime2(name, timestampNormalizedCalendar(calendar, javaType, conn.baseYear()),
                                            subSecondNanos, (valueLength), isOutParam);
                                }
                            }
                            else
                                tdsWriter.writeRPCDateTime2(name, timestampNormalizedCalendar(calendar, javaType, conn.baseYear()), subSecondNanos,
                                        TDS.MAX_FRACTIONAL_SECONDS_SCALE, isOutParam);

                            break;

                        case TIME:
                            // if column is encrypted, always send as TIME
                            if (null != cryptoMeta) {
                                if (0 == valueLength) {
                                    tdsWriter.writeEncryptedRPCTime(name, calendar, subSecondNanos, outScale, isOutParam);
                                }
                                else {
                                    tdsWriter.writeEncryptedRPCTime(name, calendar, subSecondNanos, valueLength, isOutParam);
                                }
                            }
                            else {
                                // Send the java.sql.Types.TIME value as TIME or DATETIME SQL Server
                                // data type, based on sendTimeAsDatetime setting.
                                if (conn.getSendTimeAsDatetime()) {
                                    tdsWriter.writeRPCDateTime(name, timestampNormalizedCalendar(calendar, JavaType.TIME, TDS.BASE_YEAR_1970),
                                            subSecondNanos, isOutParam);
                                }
                                else {
                                    tdsWriter.writeRPCTime(name, calendar, subSecondNanos, TDS.MAX_FRACTIONAL_SECONDS_SCALE, isOutParam);
                                }
                            }

                            break;

                        case DATE:
                            if (null != cryptoMeta)
                                tdsWriter.writeEncryptedRPCDate(name, calendar, isOutParam);
                            else
                                tdsWriter.writeRPCDate(name, calendar, isOutParam);

                            break;

                        case TIME_WITH_TIMEZONE:
                            // When converting from any other temporal Java type to TIME_WITH_TIMEZONE,
                            // deliberately reinterpret the value as local to UTC. This is to match
                            // SQL Server behavior for such conversions.
                            if ((JavaType.OFFSETDATETIME != javaType) && (JavaType.OFFSETTIME != javaType)) {
                                calendar = timestampNormalizedCalendar(localCalendarAsUTC(calendar), javaType, conn.baseYear());

                                minutesOffset = 0; // UTC
                            }

                            tdsWriter.writeRPCDateTimeOffset(name, calendar, minutesOffset, subSecondNanos, TDS.MAX_FRACTIONAL_SECONDS_SCALE,
                                    isOutParam);

                            break;

                        case TIMESTAMP_WITH_TIMEZONE:
                        case DATETIMEOFFSET:
                            // When converting from any other temporal Java type to DATETIMEOFFSET/TIMESTAMP_WITH_TIMEZONE,
                            // deliberately reinterpret the value as local to UTC. This is to match
                            // SQL Server behavior for such conversions.
                            if ((JavaType.DATETIMEOFFSET != javaType) && (JavaType.OFFSETDATETIME != javaType)) {
                                calendar = timestampNormalizedCalendar(localCalendarAsUTC(calendar), javaType, conn.baseYear());

                                minutesOffset = 0; // UTC
                            }

                            if (null != cryptoMeta) {
                                if (0 == valueLength) {
                                    tdsWriter.writeEncryptedRPCDateTimeOffset(name, calendar, minutesOffset, subSecondNanos, outScale, isOutParam);
                                }
                                else {
                                    tdsWriter.writeEncryptedRPCDateTimeOffset(name, calendar, minutesOffset, subSecondNanos,
                                            (0 == valueLength ? TDS.MAX_FRACTIONAL_SECONDS_SCALE : valueLength), isOutParam);
                                }
                            }
                            else
                                tdsWriter.writeRPCDateTimeOffset(name, calendar, minutesOffset, subSecondNanos, TDS.MAX_FRACTIONAL_SECONDS_SCALE,
                                        isOutParam);

                            break;

                        default:
                            assert false : "Unexpected JDBCType: " + jdbcType;

                    }
                }

                // Yukon and earlier
                // -----------------
                //
                // When sending as...
                // - java.sql.Types.TIMESTAMP, use DATETIME SQL Server data type (all components)
                // - java.sql.Types.TIME, use DATETIME SQL Server data type (with date = 1/1/1970)
                // - java.sql.Types.DATE, use DATETIME SQL Server data type (with time = midnight)
                // - microsoft.sql.Types.DATETIMEOFFSET (not supported - exception should have been thrown earlier)
                else {
                    assert JDBCType.TIME == jdbcType || JDBCType.DATE == jdbcType || JDBCType.TIMESTAMP == jdbcType : "Unexpected JDBCType: "
                            + jdbcType;

                    tdsWriter.writeRPCDateTime(name, timestampNormalizedCalendar(calendar, javaType, TDS.BASE_YEAR_1970), subSecondNanos, isOutParam);
                }
            } // setters
        }

        /**
         * Normalizes a GregorianCalendar value appropriately for a DATETIME, SMALLDATETIME, DATETIME2, or DATETIMEOFFSET SQL Server data type.
         *
         * For DATE values, the time must be normalized to midnight. For TIME values, the date must be normalized to January of the specified base
         * year (1970 or 1900) For other temporal types (DATETIME, SMALLDATETIME, DATETIME2, DATETIMEOFFSET), no normalization is needed - both date
         * and time contribute to the final value.
         *
         * @param calendar
         *            the value to normalize. May be null.
         * @param javaType
         *            the Java type that the calendar value represents.
         * @param baseYear
         *            the base year (1970 or 1900) for use in normalizing TIME values.
         */
        private GregorianCalendar timestampNormalizedCalendar(GregorianCalendar calendar,
                JavaType javaType,
                int baseYear) {
            if (null != calendar) {
                switch (javaType) {
                    case LOCALDATE:
                    case DATE:
                        // Note: Although UTILDATE is a Date object (java.util.Date) it cannot have normalized time values.
                        // This is because java.util.Date is mapped to JDBC TIMESTAMP according to the JDBC spec.
                        // java.util.Calendar is also mapped to JDBC TIMESTAMP and hence should have both date and time parts.
                        calendar.set(Calendar.HOUR_OF_DAY, 0);
                        calendar.set(Calendar.MINUTE, 0);
                        calendar.set(Calendar.SECOND, 0);
                        calendar.set(Calendar.MILLISECOND, 0);
                        break;

                    case OFFSETTIME:
                    case LOCALTIME:
                    case TIME:
                        assert TDS.BASE_YEAR_1970 == baseYear || TDS.BASE_YEAR_1900 == baseYear;
                        calendar.set(baseYear, Calendar.JANUARY, 1);
                        break;

                    default:
                        break;
                }
            }

            return calendar;
        }

        // Conversion from Date/Time/Timestamp to DATETIMEOFFSET reinterprets (changes)
        // the "wall clock" value to be local to UTC rather than the local to the
        // local Calendar's time zone. This behavior (ignoring the local time zone
        // when converting to DATETIMEOFFSET, which is time zone-aware) may seem
        // counterintuitive, but is necessary per the data types spec to match SQL
        // Server's conversion behavior for both setters and updaters.
        private GregorianCalendar localCalendarAsUTC(GregorianCalendar cal) {
            if (null == cal)
                return null;

            // Interpret "wall clock" value of the local calendar as a date/time/timestamp in UTC
            int year = cal.get(Calendar.YEAR);
            int month = cal.get(Calendar.MONTH);
            int date = cal.get(Calendar.DATE);
            int hour = cal.get(Calendar.HOUR_OF_DAY);
            int minute = cal.get(Calendar.MINUTE);
            int second = cal.get(Calendar.SECOND);
            int millis = cal.get(Calendar.MILLISECOND);

            cal.setTimeZone(UTC.timeZone);
            cal.set(year, month, date, hour, minute, second);
            cal.set(Calendar.MILLISECOND, millis);
            return cal;
        }

        void execute(DTV dtv,
                Float floatValue) throws SQLServerException {
            if (JDBCType.REAL == dtv.getJdbcType()) {
                tdsWriter.writeRPCReal(name, floatValue, isOutParam);
            }
            else // all other jdbcTypes (not just JDBCType.FLOAT!)
            {
                /*
                 * 2.26 floating point data needs to go to the DB as 8 bytes or else rounding errors occur at the DB. The ODBC driver sends 4 byte
                 * floats as 8 bytes also. So tried assigning the float to a double with double d = float f but d now also shows rounding errors (from
                 * simply the assignment in the Java runtime). So the only way found is to convert the float to a string and init the double with that
                 * string
                 */
                Double doubleValue = (null == floatValue) ? null : (double) floatValue;
                tdsWriter.writeRPCDouble(name, doubleValue, isOutParam);
            }
        }

        void execute(DTV dtv,
                Double doubleValue) throws SQLServerException {
            tdsWriter.writeRPCDouble(name, doubleValue, isOutParam);
        }

        void execute(DTV dtv,
                BigDecimal bigDecimalValue) throws SQLServerException {
            if (DDC.exceedsMaxRPCDecimalPrecisionOrScale(bigDecimalValue)) {
                String strValue = bigDecimalValue.toString();
                tdsWriter.writeRPCStringUnicode(name, strValue, isOutParam, collation);
            }
            else {
                tdsWriter.writeRPCBigDecimal(name, bigDecimalValue, outScale, isOutParam);
            }
        }

        void execute(DTV dtv,
                Long longValue) throws SQLServerException {
            tdsWriter.writeRPCLong(name, longValue, isOutParam);
        }

        void execute(DTV dtv,
                BigInteger bigIntegerValue) throws SQLServerException {
            tdsWriter.writeRPCLong(name, bigIntegerValue.longValue(), isOutParam);
        }

        void execute(DTV dtv,
                Short shortValue) throws SQLServerException {
            tdsWriter.writeRPCShort(name, shortValue, isOutParam);
        }

        void execute(DTV dtv,
                Boolean booleanValue) throws SQLServerException {
            tdsWriter.writeRPCBit(name, booleanValue, isOutParam);
        }

        void execute(DTV dtv,
                byte[] byteArrayValue) throws SQLServerException {
            if (null != cryptoMeta) {
                tdsWriter.writeRPCNameValType(name, isOutParam, TDSType.BIGVARBINARY);
                if (null != byteArrayValue) {
                    byteArrayValue = SQLServerSecurityUtility.encryptWithKey(byteArrayValue, cryptoMeta, conn);
                    tdsWriter.writeEncryptedRPCByteArray(byteArrayValue);
                    writeEncryptData(dtv, false);
                }
                else {
                    // long and 8000/4000 types, and output parameter without input
                    // if there is no setter is called, javaType is null
                    if ((JDBCType.LONGVARCHAR == jdbcTypeSetByUser || JDBCType.LONGNVARCHAR == jdbcTypeSetByUser
                            || JDBCType.LONGVARBINARY == jdbcTypeSetByUser
                            || (DataTypes.SHORT_VARTYPE_MAX_BYTES == precision && JDBCType.VARCHAR == jdbcTypeSetByUser)
                            || (DataTypes.SHORT_VARTYPE_MAX_CHARS == precision && JDBCType.NVARCHAR == jdbcTypeSetByUser)
                            || (DataTypes.SHORT_VARTYPE_MAX_BYTES == precision && JDBCType.VARBINARY == jdbcTypeSetByUser))
                            && null == dtv.getJavaType() && isOutParam) {
                        tdsWriter.writeEncryptedRPCPLP();
                    }
                    else {
                        tdsWriter.writeEncryptedRPCByteArray(byteArrayValue);
                    }

                    writeEncryptData(dtv, true);
                }

            }
            else
                tdsWriter.writeRPCByteArray(name, byteArrayValue, isOutParam, dtv.getJdbcType(), collation);

        }

        void writeEncryptData(DTV dtv,
                boolean isNull) throws SQLServerException {
            JDBCType destType = (null == jdbcTypeSetByUser) ? dtv.getJdbcType() : jdbcTypeSetByUser;

            switch (destType.getIntValue()) {
                case java.sql.Types.INTEGER: // 0x38
                    tdsWriter.writeByte(TDSType.INTN.byteValue());
                    tdsWriter.writeByte((byte) 0x04);
                    break;

                case java.sql.Types.BIGINT: // 0x7f
                    tdsWriter.writeByte(TDSType.INTN.byteValue());
                    tdsWriter.writeByte((byte) 0x08);
                    break;

                case java.sql.Types.BIT: // 0x32
                    tdsWriter.writeByte(TDSType.BITN.byteValue());
                    tdsWriter.writeByte((byte) 0x01);
                    break;

                case java.sql.Types.SMALLINT: // 0x34
                    tdsWriter.writeByte(TDSType.INTN.byteValue());
                    tdsWriter.writeByte((byte) 0x02);
                    break;

                case java.sql.Types.TINYINT: // 0x30
                    tdsWriter.writeByte(TDSType.INTN.byteValue());
                    tdsWriter.writeByte((byte) 0x01);
                    break;

                case java.sql.Types.DOUBLE: // (FLT8TYPE) 0x3E
                    tdsWriter.writeByte(TDSType.FLOATN.byteValue());
                    tdsWriter.writeByte((byte) 0x08);
                    break;

                case java.sql.Types.REAL: // (FLT4TYPE) 0x3B
                    tdsWriter.writeByte(TDSType.FLOATN.byteValue());
                    tdsWriter.writeByte((byte) 0x04);
                    break;

                case microsoft.sql.Types.MONEY:
                case microsoft.sql.Types.SMALLMONEY:
                case java.sql.Types.NUMERIC:
                case java.sql.Types.DECIMAL:
                    // money/smallmoney is mapped to JDBC types java.sql.Types.Decimal
                    if ((JDBCType.MONEY == destType) || (JDBCType.SMALLMONEY == destType)) {
                        tdsWriter.writeByte(TDSType.MONEYN.byteValue()); // 0x6E
                        tdsWriter.writeByte((byte) ((JDBCType.MONEY == destType) ? 8 : 4));
                    }
                    else {
                        tdsWriter.writeByte(TDSType.NUMERICN.byteValue()); // 0x6C
                        if (isNull) {
                            tdsWriter.writeByte((byte) 0x11); // maximum length

                            if (null != cryptoMeta && null != cryptoMeta.getBaseTypeInfo()) {
                                tdsWriter.writeByte((byte) ((0 != valueLength) ? valueLength : cryptoMeta.getBaseTypeInfo().getPrecision()));
                            }
                            else {
                                tdsWriter.writeByte((byte) ((0 != valueLength) ? valueLength : 0x12)); // default length, 0x12 equals to 18, which is
                                                                                                       // the default length for decimal value
                            }

                            tdsWriter.writeByte((byte) (outScale));	// send scale
                        }
                        else {
                            tdsWriter.writeByte((byte) 0x11); // maximum length

                            if (null != cryptoMeta && null != cryptoMeta.getBaseTypeInfo()) {
                                tdsWriter.writeByte((byte) cryptoMeta.getBaseTypeInfo().getPrecision());
                            }
                            else {
                                tdsWriter.writeByte((byte) ((0 != valueLength) ? valueLength : 0x12)); // default length, 0x12 equals to 18, which is
                                                                                                       // the default length for decimal value
                            }

                            if (null != cryptoMeta && null != cryptoMeta.getBaseTypeInfo()) {
                                tdsWriter.writeByte((byte) cryptoMeta.getBaseTypeInfo().getScale());
                            }
                            else {
                                tdsWriter.writeByte((byte) ((null != dtv.getScale()) ? dtv.getScale() : 0));	// send scale
                            }
                        }
                    }
                    break;

                case microsoft.sql.Types.GUID:
                    tdsWriter.writeByte(TDSType.GUID.byteValue());
                    if (isNull)
                        tdsWriter.writeByte((byte) ((0 != valueLength) ? valueLength : 1));
                    else
                        tdsWriter.writeByte((byte) 0x10);
                    break;

                case java.sql.Types.CHAR: // 0xAF
                    // BIGCHARTYPE
                    tdsWriter.writeByte(TDSType.BIGCHAR.byteValue());

                    if (isNull)
                        tdsWriter.writeShort((short) ((0 != valueLength) ? valueLength : 1));
                    else
                        tdsWriter.writeShort((short) (valueLength));

                    if (null != collation)
                        collation.writeCollation(tdsWriter);
                    else
                        conn.getDatabaseCollation().writeCollation(tdsWriter);
                    break;

                case java.sql.Types.NCHAR: // 0xEF
                    tdsWriter.writeByte(TDSType.NCHAR.byteValue());
                    if (isNull)
                        tdsWriter.writeShort((short) ((0 != valueLength) ? (valueLength * 2) : 1));
                    else {
                        if (isOutParam) {
                            tdsWriter.writeShort((short) (valueLength * 2));
                        }
                        else {
                            if (valueLength > DataTypes.SHORT_VARTYPE_MAX_BYTES) {
                                tdsWriter.writeShort((short) DataTypes.MAX_VARTYPE_MAX_CHARS);
                            }
                            else {
                                tdsWriter.writeShort((short) valueLength);
                            }
                        }
                    }
                    if (null != collation)
                        collation.writeCollation(tdsWriter);
                    else
                        conn.getDatabaseCollation().writeCollation(tdsWriter);
                    break;

                case java.sql.Types.LONGVARCHAR:
                case java.sql.Types.VARCHAR: // 0xA7
                    // BIGVARCHARTYPE
                    tdsWriter.writeByte(TDSType.BIGVARCHAR.byteValue());
                    if (isNull) {
                        if (dtv.jdbcTypeSetByUser.getIntValue() == java.sql.Types.LONGVARCHAR) {
                            tdsWriter.writeShort((short) DataTypes.MAX_VARTYPE_MAX_CHARS);
                        }
                        else {
                            tdsWriter.writeShort((short) ((0 != valueLength) ? valueLength : 1));
                        }
                    }
                    else {
                        if (dtv.jdbcTypeSetByUser.getIntValue() == java.sql.Types.LONGVARCHAR) {
                            tdsWriter.writeShort((short) DataTypes.MAX_VARTYPE_MAX_CHARS);
                        }
                        else if ((dtv.getJdbcType().getIntValue() == java.sql.Types.LONGVARCHAR)
                                || (dtv.getJdbcType().getIntValue() == java.sql.Types.LONGNVARCHAR)) {
                            tdsWriter.writeShort((short) 1);
                        }
                        else {
                            if (valueLength > DataTypes.SHORT_VARTYPE_MAX_BYTES) {
                                tdsWriter.writeShort((short) DataTypes.MAX_VARTYPE_MAX_CHARS);
                            }
                            else {
                                tdsWriter.writeShort((short) valueLength);
                            }
                        }
                    }

                    if (null != collation)
                        collation.writeCollation(tdsWriter);
                    else
                        conn.getDatabaseCollation().writeCollation(tdsWriter);
                    break;

                case java.sql.Types.LONGNVARCHAR:
                case java.sql.Types.NVARCHAR: // 0xE7
                    tdsWriter.writeByte(TDSType.NVARCHAR.byteValue());
                    if (isNull) {
                        if (dtv.jdbcTypeSetByUser.getIntValue() == java.sql.Types.LONGNVARCHAR) {
                            tdsWriter.writeShort((short) DataTypes.MAX_VARTYPE_MAX_CHARS);
                        }
                        else {
                            tdsWriter.writeShort((short) ((0 != valueLength) ? (valueLength * 2) : 1));
                        }
                    }
                    else {
                        if (isOutParam) {
                            // for stored procedure output parameter, we need to
                            // double the length that is sent to SQL Server,
                            // Otherwise it gives Operand Clash exception.
                            if (dtv.jdbcTypeSetByUser.getIntValue() == java.sql.Types.LONGNVARCHAR) {
                                tdsWriter.writeShort((short) DataTypes.MAX_VARTYPE_MAX_CHARS);
                            }
                            else {
                                tdsWriter.writeShort((short) (valueLength * 2));
                            }
                        }
                        else {
                            if (valueLength > DataTypes.SHORT_VARTYPE_MAX_BYTES) {
                                tdsWriter.writeShort((short) DataTypes.MAX_VARTYPE_MAX_CHARS);
                            }
                            else {
                                tdsWriter.writeShort((short) valueLength);
                            }
                        }
                    }

                    if (null != collation)
                        collation.writeCollation(tdsWriter);
                    else
                        conn.getDatabaseCollation().writeCollation(tdsWriter);
                    break;

                case java.sql.Types.BINARY: // 0xAD
                    tdsWriter.writeByte(TDSType.BIGBINARY.byteValue());
                    if (isNull)
                        tdsWriter.writeShort((short) ((0 != valueLength) ? valueLength : 1));
                    else
                        tdsWriter.writeShort((short) (valueLength));
                    break;

                case java.sql.Types.LONGVARBINARY:
                case java.sql.Types.VARBINARY: // 0xA5
                    // BIGVARBINARY
                    tdsWriter.writeByte(TDSType.BIGVARBINARY.byteValue());
                    if (isNull) {
                        if (dtv.jdbcTypeSetByUser.getIntValue() == java.sql.Types.LONGVARBINARY) {
                            tdsWriter.writeShort((short) DataTypes.MAX_VARTYPE_MAX_BYTES);
                        }
                        else {
                            tdsWriter.writeShort((short) ((0 != valueLength) ? valueLength : 1));
                        }
                    }
                    else {
                        if (dtv.jdbcTypeSetByUser.getIntValue() == java.sql.Types.LONGVARBINARY) {
                            tdsWriter.writeShort((short) DataTypes.MAX_VARTYPE_MAX_BYTES);
                        }
                        else {
                            tdsWriter.writeShort((short) (valueLength));
                        }
                    }
                    break;
                default:
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_UnsupportedDataTypeAE"));
                    throw new SQLServerException(form.format(new Object[] {destType}), null, 0, null);
            }

            tdsWriter.writeCryptoMetaData();
        }

        void execute(DTV dtv,
                Blob blobValue) throws SQLServerException {
            assert null != blobValue;

            long blobLength = 0;
            InputStream blobStream = null;

            try {
                blobLength = DataTypes.getCheckedLength(conn, dtv.getJdbcType(), blobValue.length(), false);
                blobStream = blobValue.getBinaryStream();
            }
            catch (SQLException e) {
                SQLServerException.makeFromDriverError(conn, null, e.getMessage(), null, false);
            }

            if (null == blobStream) {
                tdsWriter.writeRPCByteArray(name, null, isOutParam, dtv.getJdbcType(), collation);
            }
            else {
                tdsWriter.writeRPCInputStream(name, blobStream, blobLength, isOutParam, dtv.getJdbcType(), collation);
            }
        }

        void execute(DTV dtv,
                SQLServerSQLXML xmlValue) throws SQLServerException {
            InputStream o = (null == xmlValue) ? null : xmlValue.getValue();
            tdsWriter.writeRPCXML(name, o, null == o ? 0 : dtv.getStreamSetterArgs().getLength(), isOutParam);
        }

        void execute(DTV dtv,
                InputStream inputStreamValue) throws SQLServerException {
            tdsWriter.writeRPCInputStream(name, inputStreamValue, null == inputStreamValue ? 0 : dtv.getStreamSetterArgs().getLength(), isOutParam,
                    dtv.getJdbcType(), collation);
        }

        void execute(DTV dtv,
                Reader readerValue) throws SQLServerException {
            JDBCType jdbcType = dtv.getJdbcType();

            // executeOp should have handled null Reader as a null String.
            assert null != readerValue;

            // Non-unicode JDBCType should have been handled before now
            assert (JDBCType.NCHAR == jdbcType || JDBCType.NVARCHAR == jdbcType || JDBCType.LONGNVARCHAR == jdbcType
                    || JDBCType.NCLOB == jdbcType) : "SendByRPCOp(Reader): Unexpected JDBC type " + jdbcType;

            // Write the reader value as a stream of Unicode characters
            tdsWriter.writeRPCReaderUnicode(name, readerValue, dtv.getStreamSetterArgs().getLength(), isOutParam, collation);
        }

        /*
         * (non-Javadoc)
         * 
         * @see com.microsoft.sqlserver.jdbc.DTVExecuteOp#execute(com.microsoft.sqlserver.jdbc.DTV, microsoft.sql.SqlVariant)
         */
        @Override
        void execute(DTV dtv,
                SqlVariant sqlVariantValue) throws SQLServerException {
            tdsWriter.writeRPCSqlVariant(name, sqlVariantValue, isOutParam);

        }
    }

    /**
     * Execute a caller-defined, object type-specific operation on a DTV.
     *
     * See DTVExecuteOp
     */
    final void executeOp(DTVExecuteOp op) throws SQLServerException {
        JDBCType jdbcType = getJdbcType();
        Object value = getSetterValue();
        JavaType javaType = getJavaType();
        boolean unsupportedConversion = false;
        byte[] byteValue = null;

        if (null != cryptoMeta && !SetterConversionAE.converts(javaType, jdbcType, sendStringParametersAsUnicode)) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_unsupportedConversionAE"));
            Object[] msgArgs = {javaType.toString().toLowerCase(Locale.ENGLISH), jdbcType.toString().toLowerCase(Locale.ENGLISH)};
            throw new SQLServerException(form.format(msgArgs), null);
        }

        if (null == value) {

            switch (jdbcType) {
                case NCHAR:
                case NVARCHAR:
                case LONGNVARCHAR:
                case NCLOB:
                    if (null != cryptoMeta)
                        op.execute(this, (byte[]) null);
                    else
                        op.execute(this, (String) null);
                    break;

                case INTEGER:
                    if (null != cryptoMeta)
                        op.execute(this, (byte[]) null);
                    else
                        op.execute(this, (Integer) null);
                    break;

                case DATE:
                    op.execute(this, (java.sql.Date) null);
                    break;

                case TIME:
                    op.execute(this, (java.sql.Time) null);
                    break;

                case DATETIME:
                case SMALLDATETIME:
                case TIMESTAMP:
                    op.execute(this, (java.sql.Timestamp) null);
                    break;

                case TIME_WITH_TIMEZONE:
                case TIMESTAMP_WITH_TIMEZONE:
                case DATETIMEOFFSET:
                    op.execute(this, (microsoft.sql.DateTimeOffset) null);
                    break;

                case FLOAT:
                case REAL:
                    if (null != cryptoMeta)
                        op.execute(this, (byte[]) null);
                    else
                        op.execute(this, (Float) null);
                    break;

                case NUMERIC:
                case DECIMAL:
                case MONEY:
                case SMALLMONEY:
                    if (null != cryptoMeta)
                        op.execute(this, (byte[]) null);
                    else
                        op.execute(this, (BigDecimal) null);
                    break;

                case BINARY:
                case VARBINARY:
                case LONGVARBINARY:
                case BLOB:
                case CHAR:
                case VARCHAR:
                case LONGVARCHAR:
                case CLOB:
                case GUID:
                    op.execute(this, (byte[]) null);
                    break;

                case TINYINT:
                    if (null != cryptoMeta)
                        op.execute(this, (byte[]) null);
                    else
                        op.execute(this, (Byte) null);
                    break;

                case BIGINT:
                    if (null != cryptoMeta)
                        op.execute(this, (byte[]) null);
                    else
                        op.execute(this, (Long) null);
                    break;

                case DOUBLE:
                    if (null != cryptoMeta)
                        op.execute(this, (byte[]) null);
                    else
                        op.execute(this, (Double) null);
                    break;

                case SMALLINT:
                    if (null != cryptoMeta)
                        op.execute(this, (byte[]) null);
                    else
                        op.execute(this, (Short) null);
                    break;

                case BIT:
                case BOOLEAN:
                    if (null != cryptoMeta)
                        op.execute(this, (byte[]) null);
                    else
                        op.execute(this, (Boolean) null);
                    break;

                case SQLXML:
                    op.execute(this, (SQLServerSQLXML) null);
                    break;

                case ARRAY:
                case DATALINK:
                case DISTINCT:
                case JAVA_OBJECT:
                case NULL:
                case OTHER:
                case REF:
                case ROWID:
                case STRUCT:
                    unsupportedConversion = true;
                    break;
                    
                case SQL_VARIANT:
                    op.execute(this, (SqlVariant) null);
                    break;

                case UNKNOWN:
                default:
                    assert false : "Unexpected JDBCType: " + jdbcType;
                    unsupportedConversion = true;
                    break;
            }
        }
        else // null != value
        {
            if (aeLogger.isLoggable(java.util.logging.Level.FINE) && (null != cryptoMeta)) {
                aeLogger.fine("Encrypting java data type: " + javaType);
            }

            switch (javaType) {
                case STRING:
                    if (JDBCType.GUID == jdbcType) {
                        if (null != cryptoMeta) {
                            if (value instanceof String) {
                                value = UUID.fromString((String) value);
                            }
                            byte[] bArray = Util.asGuidByteArray((UUID) value);
                            op.execute(this, bArray);
                        }
                        else {
                            op.execute(this, String.valueOf(value));
                        }
                    }
                    else if (JDBCType.SQL_VARIANT == jdbcType) {
                        op.execute(this, String.valueOf(value));
                    }
                    else if (JDBCType.GEOMETRY == jdbcType) {
                        op.execute(this, ((Geometry) value).serialize());
                    }
                    else if (JDBCType.GEOGRAPHY == jdbcType) {
                        op.execute(this, ((Geography) value).serialize());
                    }
                    else {
                        if (null != cryptoMeta) {
                            // if streaming types check for allowed data length in AE
                            // jdbcType is set to LONGNVARCHAR if input data length is > DataTypes.SHORT_VARTYPE_MAX_CHARS for string
                            if ((jdbcType == JDBCType.LONGNVARCHAR) && (JDBCType.VARCHAR == jdbcTypeSetByUser)
                                    && (DataTypes.MAX_VARTYPE_MAX_BYTES < valueLength)) {
                                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_StreamingDataTypeAE"));
                                Object[] msgArgs = {DataTypes.MAX_VARTYPE_MAX_BYTES, JDBCType.LONGVARCHAR};
                                throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
                            }
                            else if ((JDBCType.NVARCHAR == jdbcTypeSetByUser) && (DataTypes.MAX_VARTYPE_MAX_CHARS < valueLength)) {
                                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_StreamingDataTypeAE"));
                                Object[] msgArgs = {DataTypes.MAX_VARTYPE_MAX_CHARS, JDBCType.LONGNVARCHAR};
                                throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
                            }
                            // Each character is represented using 2 bytes in NVARCHAR
                            else if ((JDBCType.NVARCHAR == jdbcTypeSetByUser) || (JDBCType.NCHAR == jdbcTypeSetByUser)
                                    || (JDBCType.LONGNVARCHAR == jdbcTypeSetByUser)) {
                                byteValue = ((String) value).getBytes(UTF_16LE);
                            }
                            // Each character is represented using 1 bytes in VARCHAR
                            else if ((JDBCType.VARCHAR == jdbcTypeSetByUser) || (JDBCType.CHAR == jdbcTypeSetByUser)
                                    || (JDBCType.LONGVARCHAR == jdbcTypeSetByUser)) {
                                byteValue = ((String) value).getBytes();
                            }

                            op.execute(this, byteValue);
                        }
                        else
                            op.execute(this, (String) value);
                    }
                    break;

                case INTEGER:
                    if (null != cryptoMeta) {
                        byteValue = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).order(ByteOrder.LITTLE_ENDIAN).putLong(((Integer) value).longValue())
                                .array();
                        op.execute(this, byteValue);
                    }
                    else
                        op.execute(this, (Integer) value);
                    break;

                case DATE:
                    op.execute(this, (java.sql.Date) value);
                    break;

                case TIME:
                    op.execute(this, (java.sql.Time) value);
                    break;

                case TIMESTAMP:
                    op.execute(this, (java.sql.Timestamp) value);
                    break;

                case TVP:
                    op.execute(this, (TVP) value);
                    break;

                case UTILDATE:
                    op.execute(this, (java.util.Date) value);
                    break;

                case CALENDAR:
                    op.execute(this, (java.util.Calendar) value);
                    break;

                case LOCALDATE:
                    op.execute(this, (LocalDate) value);
                    break;

                case LOCALTIME:
                    op.execute(this, (LocalTime) value);
                    break;

                case LOCALDATETIME:
                    op.execute(this, (LocalDateTime) value);
                    break;

                case OFFSETTIME:
                    op.execute(this, (OffsetTime) value);
                    break;

                case OFFSETDATETIME:
                    op.execute(this, (OffsetDateTime) value);
                    break;

                case DATETIMEOFFSET:
                    op.execute(this, (microsoft.sql.DateTimeOffset) value);
                    break;

                case FLOAT:
                    if (null != cryptoMeta) {
                        if (Float.isInfinite((Float) value)) {
                            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_valueOutOfRange"));
                            throw new SQLServerException(form.format(new Object[] {jdbcType}), null, 0, null);
                        }

                        byteValue = ByteBuffer.allocate((Float.SIZE / Byte.SIZE)).order(ByteOrder.LITTLE_ENDIAN).putFloat((Float) value).array();
                        op.execute(this, byteValue);
                    }
                    else
                        op.execute(this, (Float) value);
                    break;

                case BIGDECIMAL:
                    if (null != cryptoMeta) {
                        // For AE, we need to use the setMoney/setSmallMoney methods to send encrypted data
                        // to money types. Also we need to use the TDS MONEYN rule for these.
                        // For these methods, the Java type is still BigDecimal, but the JDBCType
                        // would be JDBCType.MONEY or JDBCType.SMALLMONEY.
                        if ((JDBCType.MONEY == jdbcType) || (JDBCType.SMALLMONEY == jdbcType)) {
                            // For TDS we need to send the money value multiplied by 10^4 - this gives us the
                            // money value as integer. 4 is the default and only scale available with money.
                            // smallmoney is noralized to money.
                            BigDecimal bdValue = (BigDecimal) value;
                            // Need to validate range in the client side as we are converting BigDecimal to integers.
                            Util.validateMoneyRange(bdValue, jdbcType);

                            // Get the total number of digits after the multiplication. Scale is hardcoded to 4. This is needed to get the proper
                            // rounding.
                            // BigDecimal calculates precision a bit differently. For 0.000001, the precision is 1, but scale is 6 which makes the
                            // digit count -ve.
                            int digitCount = Math.max((bdValue.precision() - bdValue.scale()), 0) + 4;

                            long moneyVal = ((BigDecimal) value).multiply(new BigDecimal(10000), new MathContext(digitCount, RoundingMode.HALF_UP))
                                    .longValue();
                            ByteBuffer bbuf = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
                            bbuf.putInt((int) (moneyVal >> 32)).array();
                            bbuf.putInt((int) moneyVal).array();
                            op.execute(this, bbuf.array());
                        }
                        else {
                            BigDecimal bigDecimalVal = (BigDecimal) value;
                            byte[] decimalToByte = DDC.convertBigDecimalToBytes(bigDecimalVal, bigDecimalVal.scale());
                            byteValue = new byte[16];
                            // removing the precision and scale information from the decimalToByte array
                            System.arraycopy(decimalToByte, 2, byteValue, 0, decimalToByte.length - 2);
                            this.setScale(bigDecimalVal.scale());

                            if (null != cryptoMeta.getBaseTypeInfo()) {
                                // if the precision of the column is smaller than the precision of the actual value,
                                // the driver throws exception
                                if (cryptoMeta.getBaseTypeInfo().getPrecision() < Util.getValueLengthBaseOnJavaType(bigDecimalVal, javaType, null,
                                        null, jdbcType)) {
                                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_valueOutOfRange"));
                                    Object[] msgArgs = {cryptoMeta.getBaseTypeInfo().getSSTypeName()};
                                    throw new SQLServerException(form.format(msgArgs), SQLState.DATA_EXCEPTION_DATETIME_FIELD_OVERFLOW,
                                            DriverError.NOT_SET, null);
                                }
                            }
                            else {
                                // if the precision that user provides is smaller than the precision of the actual value,
                                // the driver assumes the precision that user provides is the correct precision, and throws
                                // exception
                                if (valueLength < Util.getValueLengthBaseOnJavaType(bigDecimalVal, javaType, null, null, jdbcType)) {
                                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_valueOutOfRange"));
                                    Object[] msgArgs = {SSType.DECIMAL};
                                    throw new SQLServerException(form.format(msgArgs), SQLState.DATA_EXCEPTION_DATETIME_FIELD_OVERFLOW,
                                            DriverError.NOT_SET, null);
                                }
                            }

                            op.execute(this, byteValue);
                        }
                    }
                    else
                        op.execute(this, (BigDecimal) value);
                    break;

                case BYTEARRAY:
                    if ((null != cryptoMeta) && (DataTypes.MAX_VARTYPE_MAX_BYTES < valueLength)) {
                        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_StreamingDataTypeAE"));
                        Object[] msgArgs = {DataTypes.MAX_VARTYPE_MAX_BYTES, JDBCType.BINARY};
                        throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
                    }
                    else
                        op.execute(this, (byte[]) value);
                    break;

                case BYTE:
                    // for tinyint
                    if (null != cryptoMeta) {
                        byteValue = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).order(ByteOrder.LITTLE_ENDIAN).putLong((byte) value & 0xFF).array();
                        op.execute(this, byteValue);
                    }
                    else
                        op.execute(this, (Byte) value);
                    break;

                case LONG:
                    if (null != cryptoMeta) {
                        byteValue = ByteBuffer.allocate((Long.SIZE / Byte.SIZE)).order(ByteOrder.LITTLE_ENDIAN).putLong((Long) value).array();
                        op.execute(this, byteValue);
                    }
                    else
                        op.execute(this, (Long) value);
                    break;

                case BIGINTEGER:
                    op.execute(this, (java.math.BigInteger) value);
                    break;

                case DOUBLE:
                    if (null != cryptoMeta) {
                        if (Double.isInfinite((Double) value)) {
                            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_valueOutOfRange"));
                            throw new SQLServerException(form.format(new Object[] {jdbcType}), null, 0, null);
                        }
                        byteValue = ByteBuffer.allocate((Double.SIZE / Byte.SIZE)).order(ByteOrder.LITTLE_ENDIAN).putDouble((Double) value).array();
                        op.execute(this, byteValue);
                    }
                    else
                        op.execute(this, (Double) value);
                    break;

                case SHORT:
                    if (null != cryptoMeta) {
                        byteValue = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).order(ByteOrder.LITTLE_ENDIAN).putLong((short) value).array();
                        op.execute(this, byteValue);
                    }
                    else
                        op.execute(this, (Short) value);
                    break;

                case BOOLEAN:
                    if (null != cryptoMeta) {
                        byteValue = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).order(ByteOrder.LITTLE_ENDIAN).putLong((Boolean) value ? 1 : 0)
                                .array();
                        op.execute(this, byteValue);
                    }
                    else
                        op.execute(this, (Boolean) value);
                    break;

                case BLOB:
                    op.execute(this, (Blob) value);
                    break;

                case CLOB:
                case NCLOB:
                    op.execute(this, (Clob) value);
                    break;

                case INPUTSTREAM:
                    op.execute(this, (InputStream) value);
                    break;

                case READER:
                    op.execute(this, (Reader) value);
                    break;

                case SQLXML:
                    op.execute(this, (SQLServerSQLXML) value);
                    break;

                default:
                    assert false : "Unexpected JavaType: " + javaType;
                    unsupportedConversion = true;
                    break;
            }
        }

        if (unsupportedConversion) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_unsupportedConversionFromTo"));
            Object[] msgArgs = {javaType, jdbcType};
            throw new SQLServerException(form.format(msgArgs), SQLState.DATA_EXCEPTION_NOT_SPECIFIC, DriverError.NOT_SET, null);
        }
    }

    void sendCryptoMetaData(CryptoMetadata cryptoMeta,
            TDSWriter tdsWriter) {
        this.cryptoMeta = cryptoMeta;
        tdsWriter.setCryptoMetaData(cryptoMeta);
    }

    void jdbcTypeSetByUser(JDBCType jdbcTypeSetByUser,
            int valueLength) {
        this.jdbcTypeSetByUser = jdbcTypeSetByUser;
        this.valueLength = valueLength;
    }

    /**
     * Serializes a value as the specified type for RPC.
     */
    void sendByRPC(String name,
            TypeInfo typeInfo,
            SQLCollation collation,
            int precision,
            int outScale,
            boolean isOutParam,
            TDSWriter tdsWriter,
            SQLServerConnection conn) throws SQLServerException {
        // typeInfo is null when called from PreparedStatement->Parameter->SendByRPC
        executeOp(new SendByRPCOp(name, typeInfo, collation, precision, outScale, isOutParam, tdsWriter, conn));
    }
}

/**
 * DTV implementation class interface.
 *
 * Currently there are two implementations: one for values which originate from Java values set by the app (AppDTVImpl) and one for values which
 * originate from byte in the TDS response buffer (ServerDTVImpl).
 */
abstract class DTVImpl {
    abstract void setValue(DTV dtv,
            SQLCollation collation,
            JDBCType jdbcType,
            Object value,
            JavaType javaType,
            StreamSetterArgs streamSetterArgs,
            Calendar cal,
            Integer scale,
            SQLServerConnection con,
            boolean forceEncrypt) throws SQLServerException;

    abstract void setValue(Object value,
            JavaType javaType);

    abstract void setStreamSetterArgs(StreamSetterArgs streamSetterArgs);

    abstract void setCalendar(Calendar cal);

    abstract void setScale(Integer scale);

    abstract void setForceEncrypt(boolean forceEncrypt);

    abstract StreamSetterArgs getStreamSetterArgs();

    abstract Calendar getCalendar();

    abstract Integer getScale();

    abstract boolean isNull();

    abstract void setJdbcType(JDBCType jdbcType);

    abstract JDBCType getJdbcType();

    abstract JavaType getJavaType();

    abstract Object getValue(DTV dtv,
            JDBCType jdbcType,
            int scale,
            InputStreamGetterArgs streamGetterArgs,
            Calendar cal,
            TypeInfo type,
            CryptoMetadata cryptoMetadata,
            TDSReader tdsReader) throws SQLServerException;

    abstract Object getSetterValue();

    abstract void skipValue(TypeInfo typeInfo,
            TDSReader tdsReader,
            boolean isDiscard) throws SQLServerException;

    abstract void initFromCompressedNull();
    
    abstract SqlVariant getInternalVariant();
}

/**
 * DTV implementation for values set by the app from Java types.
 */
final class AppDTVImpl extends DTVImpl {
    private JDBCType jdbcType = JDBCType.UNKNOWN;
    private Object value;
    private JavaType javaType;
    private StreamSetterArgs streamSetterArgs;
    private Calendar cal;
    private Integer scale;
    private boolean forceEncrypt;
    private SqlVariant internalVariant;
    
    final void skipValue(TypeInfo typeInfo,
            TDSReader tdsReader,
            boolean isDiscard) throws SQLServerException {
        assert false;
    }

    final void initFromCompressedNull() {
        assert false;
    }

    final class SetValueOp extends DTVExecuteOp {
        private final SQLCollation collation;
        private final SQLServerConnection con;

        SetValueOp(SQLCollation collation,
                SQLServerConnection con) {
            this.collation = collation;
            this.con = con;
        }

        void execute(DTV dtv,
                String strValue) throws SQLServerException {
            JDBCType jdbcType = dtv.getJdbcType();

            // Normally we let the server convert the string to whatever backend
            // type it is going into. However, if the app says that the string
            // is a numeric/decimal value then convert the value to a BigDecimal
            // now so that GetTypeDefinitionOp can generate a type definition
            // with the correct scale.

            if ((JDBCType.DECIMAL == jdbcType) || (JDBCType.NUMERIC == jdbcType) || (JDBCType.MONEY == jdbcType)
                    || (JDBCType.SMALLMONEY == jdbcType)) {
                assert null != strValue;

                try {
                    dtv.setValue(new BigDecimal(strValue), JavaType.BIGDECIMAL);
                }
                catch (NumberFormatException e) {
                    DataTypes.throwConversionError("String", jdbcType.toString());
                }
            }

            // If the backend column is a binary type, or we don't know the backend
            // type, but we are being told that it is binary, then we need to convert
            // the hexized text value to binary here because the server doesn't do
            // this conversion for us.
            else if (jdbcType.isBinary()) {
                assert null != strValue;
                dtv.setValue(ParameterUtils.HexToBin(strValue), JavaType.BYTEARRAY);
            }

            // If the (Unicode) string value is to be sent to the server as MBCS,
            // then do the conversion now so that the decision to use a "short" or "long"
            // SSType (i.e. VARCHAR vs. TEXT/VARCHAR(max)) is based on the exact length of
            // the MBCS value (in bytes).
            else if (null != collation
                    && (JDBCType.CHAR == jdbcType || JDBCType.VARCHAR == jdbcType || JDBCType.LONGVARCHAR == jdbcType || JDBCType.CLOB == jdbcType)) {
                byte[] nativeEncoding = null;

                if (null != strValue) {
                    nativeEncoding = strValue.getBytes(collation.getCharset());
                }

                dtv.setValue(nativeEncoding, JavaType.BYTEARRAY);
            }
        }

        void execute(DTV dtv,
                Clob clobValue) throws SQLServerException {
            // executeOp should have handled null Clob as a String
            assert null != clobValue;

            // Fail fast if the Clob's advertised length is not within bounds.
            //
            // The Clob's length or contents may still change before SendByRPCOp
            // materializes the Clob at execution time if the app fails to treat
            // the parameter as immutable once set, as is recommended by the JDBC spec.
            try {
                DataTypes.getCheckedLength(con, dtv.getJdbcType(), clobValue.length(), false);
            }
            catch (SQLException e) {
                SQLServerException.makeFromDriverError(con, null, e.getMessage(), null, false);
            }
        }

        void execute(DTV dtv,
                SQLServerSQLXML xmlValue) throws SQLServerException {
        }

        void execute(DTV dtv,
                Byte byteValue) throws SQLServerException {
        }

        void execute(DTV dtv,
                Integer intValue) throws SQLServerException {
        }

        void execute(DTV dtv,
                java.sql.Time timeValue) throws SQLServerException {
            if (dtv.getJdbcType().isTextual()) {
                assert timeValue != null : "value is null";
                dtv.setValue(timeValue.toString(), JavaType.STRING);
            }
        }

        void execute(DTV dtv,
                java.sql.Date dateValue) throws SQLServerException {
            if (dtv.getJdbcType().isTextual()) {
                assert dateValue != null : "value is null";
                dtv.setValue(dateValue.toString(), JavaType.STRING);
            }
        }

        void execute(DTV dtv,
                java.sql.Timestamp timestampValue) throws SQLServerException {
            if (dtv.getJdbcType().isTextual()) {
                assert timestampValue != null : "value is null";
                dtv.setValue(timestampValue.toString(), JavaType.STRING);
            }
        }

        void execute(DTV dtv,
                java.util.Date utilDateValue) throws SQLServerException {
            if (dtv.getJdbcType().isTextual()) {
                assert utilDateValue != null : "value is null";
                dtv.setValue(utilDateValue.toString(), JavaType.STRING);
            }
        }

        void execute(DTV dtv,
                LocalDate localDateValue) throws SQLServerException {
            if (dtv.getJdbcType().isTextual()) {
                assert localDateValue != null : "value is null";
                dtv.setValue(localDateValue.toString(), JavaType.STRING);
            }
        }

        void execute(DTV dtv,
                LocalTime localTimeValue) throws SQLServerException {
            if (dtv.getJdbcType().isTextual()) {
                assert localTimeValue != null : "value is null";
                dtv.setValue(localTimeValue.toString(), JavaType.STRING);
            }
        }

        void execute(DTV dtv,
                LocalDateTime localDateTimeValue) throws SQLServerException {
            if (dtv.getJdbcType().isTextual()) {
                assert localDateTimeValue != null : "value is null";
                dtv.setValue(localDateTimeValue.toString(), JavaType.STRING);
            }
        }

        void execute(DTV dtv,
                OffsetTime offsetTimeValue) throws SQLServerException {
            if (dtv.getJdbcType().isTextual()) {
                assert offsetTimeValue != null : "value is null";
                dtv.setValue(offsetTimeValue.toString(), JavaType.STRING);
            }
        }

        void execute(DTV dtv,
                OffsetDateTime offsetDateTimeValue) throws SQLServerException {
            if (dtv.getJdbcType().isTextual()) {
                assert offsetDateTimeValue != null : "value is null";
                dtv.setValue(offsetDateTimeValue.toString(), JavaType.STRING);
            }
        }

        void execute(DTV dtv,
                java.util.Calendar calendarValue) throws SQLServerException {
            if (dtv.getJdbcType().isTextual()) {
                assert calendarValue != null : "value is null";
                dtv.setValue(calendarValue.toString(), JavaType.STRING);
            }
        }

        void execute(DTV dtv,
                microsoft.sql.DateTimeOffset dtoValue) throws SQLServerException {
            if (dtv.getJdbcType().isTextual()) {
                assert dtoValue != null : "value is null";
                dtv.setValue(dtoValue.toString(), JavaType.STRING);
            }
        }

        void execute(DTV dtv,
                TVP tvpValue) throws SQLServerException {
        }

        void execute(DTV dtv,
                Float floatValue) throws SQLServerException {
        }

        void execute(DTV dtv,
                Double doubleValue) throws SQLServerException {
        }

        void execute(DTV dtv,
                BigDecimal bigDecimalValue) throws SQLServerException {
            // Rescale the value if necessary
            if (null != bigDecimalValue) {
                Integer inScale = dtv.getScale();
                if (null != inScale && inScale != bigDecimalValue.scale())
                    bigDecimalValue = bigDecimalValue.setScale(inScale, RoundingMode.DOWN);
            }

            dtv.setValue(bigDecimalValue, JavaType.BIGDECIMAL);
        }

        void execute(DTV dtv,
                Long longValue) throws SQLServerException {
        }

        void execute(DTV dtv,
                BigInteger bigIntegerValue) throws SQLServerException {
        }

        void execute(DTV dtv,
                Short shortValue) throws SQLServerException {
        }

        void execute(DTV dtv,
                Boolean booleanValue) throws SQLServerException {
        }

        void execute(DTV dtv,
                byte[] byteArrayValue) throws SQLServerException {
        }

        void execute(DTV dtv,
                Blob blobValue) throws SQLServerException {
            assert null != blobValue;

            // Fail fast if the Blob's advertised length is not within bounds.
            //
            // The Blob's length or contents may still change before SendByRPCOp
            // materializes the Blob at execution time if the app fails to treat
            // the parameter as immutable once set, as is recommended by the JDBC spec.
            try {
                DataTypes.getCheckedLength(con, dtv.getJdbcType(), blobValue.length(), false);
            }
            catch (SQLException e) {
                SQLServerException.makeFromDriverError(con, null, e.getMessage(), null, false);
            }
        }

        void execute(DTV dtv,
                InputStream inputStreamValue) throws SQLServerException {
            DataTypes.getCheckedLength(con, dtv.getJdbcType(), dtv.getStreamSetterArgs().getLength(), true);

            // If the stream is to be sent as Unicode, then assume it's an ASCII stream
            if (JDBCType.NCHAR == jdbcType || JDBCType.NVARCHAR == jdbcType || JDBCType.LONGNVARCHAR == jdbcType) {
                Reader readerValue = null;
                try {
                    readerValue = new InputStreamReader(inputStreamValue, "US-ASCII");
                }
                catch (UnsupportedEncodingException ex) {
                    throw new SQLServerException(ex.getMessage(), null, 0, ex);
                }

                dtv.setValue(readerValue, JavaType.READER);

                // No need to change SetterArgs since, for ASCII streams, the
                // Reader length, in characters, is the same as the InputStream
                // length, in bytes.

                // Re-execute with the stream value to perform any further conversions
                execute(dtv, readerValue);
            }
        }

        void execute(DTV dtv,
                Reader readerValue) throws SQLServerException {
            // executeOp should have handled null Reader as a null String.
            assert null != readerValue;

            JDBCType jdbcType = dtv.getJdbcType();
            long readerLength = DataTypes.getCheckedLength(con, dtv.getJdbcType(), dtv.getStreamSetterArgs().getLength(), true);

            if (
            // If the backend column is a binary type, or we don't know the backend
            // type, but we are being told that it is binary, then we need to convert
            // the hexized text value to binary here because the server doesn't do
            // this conversion for us.
            jdbcType.isBinary()) {
                String stringValue = DDC.convertReaderToString(readerValue, (int) readerLength);

                // If we were given an input stream length that we had to match and
                // the actual stream length did not match then cancel the request.
                if (DataTypes.UNKNOWN_STREAM_LENGTH != readerLength && stringValue.length() != readerLength) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_mismatchedStreamLength"));
                    Object[] msgArgs = {readerLength, stringValue.length()};
                    SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), "", true);
                }

                dtv.setValue(stringValue, JavaType.STRING);
                execute(dtv, stringValue);
            }

            // If the reader value is to be sent as MBCS, then convert the value to an MBCS InputStream
            else if (null != collation
                    && (JDBCType.CHAR == jdbcType || JDBCType.VARCHAR == jdbcType || JDBCType.LONGVARCHAR == jdbcType || JDBCType.CLOB == jdbcType)) {
                ReaderInputStream streamValue = new ReaderInputStream(readerValue, collation.getCharset(), readerLength);

                dtv.setValue(streamValue, JavaType.INPUTSTREAM);

                // Even if we knew the length of the Reader, we do not know the length of the converted MBCS stream up front.
                dtv.setStreamSetterArgs(new StreamSetterArgs(StreamType.CHARACTER, DataTypes.UNKNOWN_STREAM_LENGTH));
                execute(dtv, streamValue);
            }
        }

        /*
         * (non-Javadoc)
         * 
         * @see com.microsoft.sqlserver.jdbc.DTVExecuteOp#execute(com.microsoft.sqlserver.jdbc.DTV, microsoft.sql.SqlVariant)
         */
        @Override
        void execute(DTV dtv,
                SqlVariant SqlVariantValue) throws SQLServerException {
        }

    }

    void setValue(DTV dtv,
            SQLCollation collation,
            JDBCType jdbcType,
            Object value,
            JavaType javaType,
            StreamSetterArgs streamSetterArgs,
            Calendar cal,
            Integer scale,
            SQLServerConnection con,
            boolean forceEncrypt) throws SQLServerException {
        // Set the value according to its Java object type, nullness, and specified JDBC type
        dtv.setValue(value, javaType);
        dtv.setJdbcType(jdbcType);
        dtv.setStreamSetterArgs(streamSetterArgs);
        dtv.setCalendar(cal);
        dtv.setScale(scale);
        dtv.setForceEncrypt(forceEncrypt);
        dtv.executeOp(new SetValueOp(collation, con));
    }

    void setValue(Object value,
            JavaType javaType) {
        this.value = value;
        this.javaType = javaType;
    }

    void setStreamSetterArgs(StreamSetterArgs streamSetterArgs) {
        this.streamSetterArgs = streamSetterArgs;
    }

    void setCalendar(Calendar cal) {
        this.cal = cal;
    }

    void setScale(Integer scale) {
        this.scale = scale;
    }

    void setForceEncrypt(boolean forceEncrypt) {
        this.forceEncrypt = forceEncrypt;
    }

    StreamSetterArgs getStreamSetterArgs() {
        return streamSetterArgs;
    }

    Calendar getCalendar() {
        return cal;
    }

    Integer getScale() {
        return scale;
    }

    boolean isNull() {
        return null == value;
    }

    void setJdbcType(JDBCType jdbcType) {
        this.jdbcType = jdbcType;
    }

    JDBCType getJdbcType() {
        return jdbcType;
    }

    JavaType getJavaType() {
        return javaType;
    }

    Object getValue(DTV dtv,
            JDBCType jdbcType,
            int scale,
            InputStreamGetterArgs streamGetterArgs,
            Calendar cal,
            TypeInfo typeInfo,
            CryptoMetadata cryptoMetadata,
            TDSReader tdsReader) throws SQLServerException {
        // Client side type conversion is not supported
        if (this.jdbcType != jdbcType)
            DataTypes.throwConversionError(this.jdbcType.toString(), jdbcType.toString());

        return value;
    }

    Object getSetterValue() {
        return value;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.microsoft.sqlserver.jdbc.DTVImpl#getInternalVariant()
     */
    @Override
    SqlVariant getInternalVariant() {
        return this.internalVariant;
    }

    /**
     * Sets the internal datatype of variant type
     * 
     * @param type
     *            sql_variant internal type
     */
    void setInternalVariant(SqlVariant type) {
        this.internalVariant = type;
    }
}

/**
 * Encapsulation of type information associated with values returned in the TDS response stream (TYPE_INFO).
 *
 * ResultSet rows have type info for each column returned in a COLMETADATA response stream.
 *
 * CallableStatement output parameters have their type info returned in a RETURNVALUE response stream.
 */
final class TypeInfo {
    private int maxLength; // Max length of data
    private SSLenType ssLenType; // Length type (FIXEDLENTYPE, PARTLENTYPE, etc.)
    private int precision;
    private int displaySize;// size is in characters. display size assumes a formatted hexa decimal representation for binaries.
    private int scale;
    private short flags;
    private SSType ssType;
    private int userType;
    private String udtTypeName;

    // Collation (will be null for non-textual types).
    private SQLCollation collation;
    private Charset charset;

    SSType getSSType() {
        return ssType;
    }
    
    void setSSType(SSType ssType) {
        this.ssType = ssType;
    }

    SSLenType getSSLenType() {
        return ssLenType;
    }
    
    void setSSLenType(SSLenType ssLenType){
        this.ssLenType = ssLenType;
    }

    String getSSTypeName() {
        return (SSType.UDT == ssType) ? udtTypeName : ssType.toString();
    }

    int getMaxLength() {
        return maxLength;
    }
    
    void setMaxLength(int maxLength) {
        this.maxLength = maxLength;
    }
    int getPrecision() {
        return precision;
    }
    
    void setPrecision(int precision) {
        this.precision = precision;
    }

    int getDisplaySize() {
        return displaySize;
    }
    
    void setDisplaySize(int displaySize){
        this.displaySize = displaySize;
    }

    int getScale() {
        return scale;
    }

    SQLCollation getSQLCollation() {
        return collation;
    }

    void setSQLCollation(SQLCollation collation) {
        this.collation = collation;
    }

    Charset getCharset() {
        return charset;
    }
    
    void setCharset(Charset charset){
        this.charset = charset;
    }

    boolean isNullable() {
        return 0x0001 == (flags & 0x0001);
    }

    boolean isCaseSensitive() {
        return 0x0002 == (flags & 0x0002);
    }

    boolean isSparseColumnSet() {
        return 0x0400 == (flags & 0x0400);
    }

    boolean isEncrypted() {
        return 0x0800 == (flags & 0x0800);
    }

    static int UPDATABLE_READ_ONLY = 0;
    static int UPDATABLE_READ_WRITE = 1;
    static int UPDATABLE_UNKNOWN = 2;

    int getUpdatability() {
        return (flags >> 2) & 0x0003;
    }

    boolean isIdentity() {
        return 0x0010 == (flags & 0x0010);
    }

    byte[] getFlags() {
        byte[] f = new byte[2];
        f[0] = (byte) (flags & 0xFF);
        f[1] = (byte) ((flags >> 8) & 0xFF);
        return f;
    }

    short getFlagsAsShort() {
        return flags;
    }

    void setFlags(Short flags) {
        this.flags = flags;
    }
    
    void setScale(int scale){
        this.scale = scale;
    }

	//TypeInfo Builder enum defines a set of builders used to construct TypeInfo instances 
	//for the various data types. Each builder builds a TypeInfo instance using a builder Strategy. 
	//Some strategies are used for multiple types (for example: FixedLenStrategy)
	enum Builder
	{
		BIT (TDSType.BIT1, new FixedLenStrategy(
				SSType.BIT,
				1, // TDS length (bytes)
				1, // precision (max numeric precision, in decimal digits)
				"1".length(), // column display size
				0) // scale
				),

		BIGINT (TDSType.INT8, new FixedLenStrategy(
				SSType.BIGINT,
				8,  // TDS length (bytes)
				Long.toString(Long.MAX_VALUE).length(), //precision (max numeric precision, in decimal digits)
				("-" + Long.toString(Long.MAX_VALUE)).length(), // column display size (includes sign)
				0)  // scale
				),

		INTEGER (TDSType.INT4, new FixedLenStrategy(
				SSType.INTEGER,
				4,  // TDS length (bytes)
				Integer.toString(Integer.MAX_VALUE).length(), // precision (max numeric precision, in decimal digits)
				("-" + Integer.toString(Integer.MAX_VALUE)).length(), // column display size (includes sign)
				0)  // scale
				),

		SMALLINT (TDSType.INT2, new FixedLenStrategy(
				SSType.SMALLINT,
				2, // TDS length (bytes)
				Short.toString(Short.MAX_VALUE).length(), // precision (max numeric precision, in decimal digits)
				("-" + Short.toString(Short.MAX_VALUE)).length(), // column display size (includes sign)
				0) // scale
				),

		TINYINT (TDSType.INT1, new FixedLenStrategy(
				SSType.TINYINT,
				1, // TDS length (bytes)
				Byte.toString(Byte.MAX_VALUE).length(), // precision (max numeric precision, in decimal digits)
				Byte.toString(Byte.MAX_VALUE).length(), // column display size (no sign - TINYINT is unsigned)
				0) // scale
				),

		REAL (TDSType.FLOAT4, new FixedLenStrategy(
				SSType.REAL,
				4,  // TDS length (bytes)
				7,  // precision (max numeric precision, in bits)
				13, // column display size
				0)  // scale
				),

		FLOAT (TDSType.FLOAT8, new FixedLenStrategy(
				SSType.FLOAT,
				8,  // TDS length (bytes)
				15, // precision (max numeric precision, in bits)
				22, // column display size
				0)  // scale
				),

		SMALLDATETIME (TDSType.DATETIME4, new FixedLenStrategy(
				SSType.SMALLDATETIME,
				4,  // TDS length (bytes)
				"yyyy-mm-dd hh:mm".length(), // precision (formatted length, in characters, assuming max fractional seconds precision (0))
				"yyyy-mm-dd hh:mm".length(), // column display size
				0)  // scale
				),

		DATETIME (TDSType.DATETIME8, new FixedLenStrategy(
				SSType.DATETIME,
				8,  // TDS length (bytes)
				"yyyy-mm-dd hh:mm:ss.fff".length(), // precision (formatted length, in characters, assuming max fractional seconds precision)
				"yyyy-mm-dd hh:mm:ss.fff".length(), // column display size
				3)  // scale
				),

		SMALLMONEY (TDSType.MONEY4, new FixedLenStrategy(
				SSType.SMALLMONEY,
				4,  // TDS length (bytes)
				Integer.toString(Integer.MAX_VALUE).length(), // precision (max unscaled numeric precision, in decimal digits)
				("-" + "." + Integer.toString(Integer.MAX_VALUE)).length(), // column display size (includes sign and decimal for scale)
				4)  // scale
				),

		MONEY (TDSType.MONEY8, new FixedLenStrategy(
				SSType.MONEY,
				8,  // TDS length (bytes)
				Long.toString(Long.MAX_VALUE).length(), // precision (max unscaled numeric precision, in decimal digits)
				("-" + "." + Long.toString(Long.MAX_VALUE)).length(), // column display size (includes sign and decimal for scale)
				4)  // scale
				),

		BITN (TDSType.BITN, new Strategy()
		{
            /**
             * Sets the fields of typeInfo to the correct values
             * 
             * @param typeInfo
             *            the TypeInfo whos values are being corrected
             * @param tdsReader
             *            the TDSReader used to set the fields of typeInfo to the correct values
             * @throws SQLServerException
             *             when an error occurs
             */
            public void apply(TypeInfo typeInfo,
                    TDSReader tdsReader) throws SQLServerException {
                if (1 != tdsReader.readUnsignedByte())
                    tdsReader.throwInvalidTDS();

                BIT.build(typeInfo, tdsReader);
                typeInfo.ssLenType = SSLenType.BYTELENTYPE;
            }
		}),

		INTN (TDSType.INTN, new Strategy()
		{
            /**
             * Sets the fields of typeInfo to the correct values
             * 
             * @param typeInfo
             *            the TypeInfo whos values are being corrected
             * @param tdsReader
             *            the TDSReader used to set the fields of typeInfo to the correct values
             * @throws SQLServerException
             *             when an error occurs
             */
            public void apply(TypeInfo typeInfo,
                    TDSReader tdsReader) throws SQLServerException {
                switch (tdsReader.readUnsignedByte()) {
                    case 8:
                        BIGINT.build(typeInfo, tdsReader);
                        break;
                    case 4:
                        INTEGER.build(typeInfo, tdsReader);
                        break;
                    case 2:
                        SMALLINT.build(typeInfo, tdsReader);
                        break;
                    case 1:
                        TINYINT.build(typeInfo, tdsReader);
                        break;
                    default:
                        tdsReader.throwInvalidTDS();
                        break;
                }

                typeInfo.ssLenType = SSLenType.BYTELENTYPE;
            }
		}),

		DECIMAL (TDSType.DECIMALN, new DecimalNumericStrategy(SSType.DECIMAL)),
		NUMERIC (TDSType.NUMERICN, new DecimalNumericStrategy(SSType.NUMERIC)),
		FLOATN (TDSType.FLOATN, new BigOrSmallByteLenStrategy(FLOAT, REAL)),
		MONEYN (TDSType.MONEYN, new BigOrSmallByteLenStrategy(MONEY, SMALLMONEY)),
		DATETIMEN (TDSType.DATETIMEN, new BigOrSmallByteLenStrategy(DATETIME, SMALLDATETIME)),

		TIME (TDSType.TIMEN, new KatmaiScaledTemporalStrategy(SSType.TIME)),
		DATETIME2 (TDSType.DATETIME2N, new KatmaiScaledTemporalStrategy(SSType.DATETIME2)),
		DATETIMEOFFSET (TDSType.DATETIMEOFFSETN, new KatmaiScaledTemporalStrategy(SSType.DATETIMEOFFSET)),

		DATE (TDSType.DATEN, new Strategy()
		{
            /**
             * Sets the fields of typeInfo to the correct values
             * 
             * @param typeInfo
             *            the TypeInfo whos values are being corrected
             * @param tdsReader
             *            the TDSReader used to set the fields of typeInfo to the correct values
             * @throws SQLServerException
             *             when an error occurs
             */
            public void apply(TypeInfo typeInfo,
                    TDSReader tdsReader) throws SQLServerException {
                typeInfo.ssType = SSType.DATE;
                typeInfo.ssLenType = SSLenType.BYTELENTYPE;
                typeInfo.maxLength = 3;
                typeInfo.displaySize = typeInfo.precision = "yyyy-mm-dd".length();
            }
		}),

		BIGBINARY (TDSType.BIGBINARY, new Strategy()
		{
            /**
             * Sets the fields of typeInfo to the correct values
             * 
             * @param typeInfo
             *            the TypeInfo whos values are being corrected
             * @param tdsReader
             *            the TDSReader used to set the fields of typeInfo to the correct values
             * @throws SQLServerException
             *             when an error occurs
             */
            public void apply(TypeInfo typeInfo,
                    TDSReader tdsReader) throws SQLServerException {
                typeInfo.ssLenType = SSLenType.USHORTLENTYPE;
                typeInfo.maxLength = tdsReader.readUnsignedShort();
                if (typeInfo.maxLength > DataTypes.SHORT_VARTYPE_MAX_BYTES)
                    tdsReader.throwInvalidTDS();
                typeInfo.precision = typeInfo.maxLength;
                typeInfo.displaySize = 2 * typeInfo.maxLength;
                typeInfo.ssType = (UserTypes.TIMESTAMP == typeInfo.userType) ? SSType.TIMESTAMP : SSType.BINARY;
            }
		}),

		BIGVARBINARY (TDSType.BIGVARBINARY, new Strategy()
		{
            /**
             * Sets the fields of typeInfo to the correct values
             * 
             * @param typeInfo
             *            the TypeInfo whos values are being corrected
             * @param tdsReader
             *            the TDSReader used to set the fields of typeInfo to the correct values
             * @throws SQLServerException
             *             when an error occurs
             */
            public void apply(TypeInfo typeInfo,
                    TDSReader tdsReader) throws SQLServerException {
                typeInfo.maxLength = tdsReader.readUnsignedShort();
                if (DataTypes.MAXTYPE_LENGTH == typeInfo.maxLength)// for PLP types
                {
                    typeInfo.ssLenType = SSLenType.PARTLENTYPE;
                    typeInfo.ssType = SSType.VARBINARYMAX;
                    typeInfo.displaySize = typeInfo.precision = DataTypes.MAX_VARTYPE_MAX_BYTES;
                }
                else if (typeInfo.maxLength <= DataTypes.SHORT_VARTYPE_MAX_BYTES)// for non-PLP types
                {
                    typeInfo.ssLenType = SSLenType.USHORTLENTYPE;
                    typeInfo.ssType = SSType.VARBINARY;
                    typeInfo.precision = typeInfo.maxLength;
                    typeInfo.displaySize = 2 * typeInfo.maxLength;
                }
                else {
                    tdsReader.throwInvalidTDS();
                }
            }
		}),

		IMAGE (TDSType.IMAGE, new Strategy()
		{
            /**
             * Sets the fields of typeInfo to the correct values
             * 
             * @param typeInfo
             *            the TypeInfo whos values are being corrected
             * @param tdsReader
             *            the TDSReader used to set the fields of typeInfo to the correct values
             * @throws SQLServerException
             *             when an error occurs
             */
            public void apply(TypeInfo typeInfo,
                    TDSReader tdsReader) throws SQLServerException {
                typeInfo.ssLenType = SSLenType.LONGLENTYPE;
                typeInfo.maxLength = tdsReader.readInt();
                if (typeInfo.maxLength < 0)
                    tdsReader.throwInvalidTDS();
                typeInfo.ssType = SSType.IMAGE;
                typeInfo.displaySize = typeInfo.precision = Integer.MAX_VALUE;
            }
		}),

		BIGCHAR (TDSType.BIGCHAR, new Strategy()
		{
            /**
             * Sets the fields of typeInfo to the correct values
             * 
             * @param typeInfo
             *            the TypeInfo whos values are being corrected
             * @param tdsReader
             *            the TDSReader used to set the fields of typeInfo to the correct values
             * @throws SQLServerException
             *             when an error occurs
             */
            public void apply(TypeInfo typeInfo,
                    TDSReader tdsReader) throws SQLServerException {
                typeInfo.ssLenType = SSLenType.USHORTLENTYPE;
                typeInfo.maxLength = tdsReader.readUnsignedShort();
                if (typeInfo.maxLength > DataTypes.SHORT_VARTYPE_MAX_BYTES)
                    tdsReader.throwInvalidTDS();
                typeInfo.displaySize = typeInfo.precision = typeInfo.maxLength;
                typeInfo.ssType = SSType.CHAR;
                typeInfo.collation = tdsReader.readCollation();
                typeInfo.charset = typeInfo.collation.getCharset();
            }
		}),

		BIGVARCHAR (TDSType.BIGVARCHAR, new Strategy()
		{
            /**
             * Sets the fields of typeInfo to the correct values
             * 
             * @param typeInfo
             *            the TypeInfo whos values are being corrected
             * @param tdsReader
             *            the TDSReader used to set the fields of typeInfo to the correct values
             * @throws SQLServerException
             *             when an error occurs
             */
            public void apply(TypeInfo typeInfo,
                    TDSReader tdsReader) throws SQLServerException {
                typeInfo.maxLength = tdsReader.readUnsignedShort();
                if (DataTypes.MAXTYPE_LENGTH == typeInfo.maxLength)// for PLP types
                {
                    typeInfo.ssLenType = SSLenType.PARTLENTYPE;
                    typeInfo.ssType = SSType.VARCHARMAX;
                    typeInfo.displaySize = typeInfo.precision = DataTypes.MAX_VARTYPE_MAX_BYTES;
                }
                else if (typeInfo.maxLength <= DataTypes.SHORT_VARTYPE_MAX_BYTES)// for non-PLP types
                {
                    typeInfo.ssLenType = SSLenType.USHORTLENTYPE;
                    typeInfo.ssType = SSType.VARCHAR;
                    typeInfo.displaySize = typeInfo.precision = typeInfo.maxLength;
                }
                else {
                    tdsReader.throwInvalidTDS();
                }

                typeInfo.collation = tdsReader.readCollation();
                typeInfo.charset = typeInfo.collation.getCharset();
            }
		}),

		TEXT (TDSType.TEXT, new Strategy()
		{
            /**
             * Sets the fields of typeInfo to the correct values
             * 
             * @param typeInfo
             *            the TypeInfo whos values are being corrected
             * @param tdsReader
             *            the TDSReader used to set the fields of typeInfo to the correct values
             * @throws SQLServerException
             *             when an error occurs
             */
            public void apply(TypeInfo typeInfo,
                    TDSReader tdsReader) throws SQLServerException {
                typeInfo.ssLenType = SSLenType.LONGLENTYPE;
                typeInfo.maxLength = tdsReader.readInt();
                if (typeInfo.maxLength < 0)
                    tdsReader.throwInvalidTDS();
                typeInfo.ssType = SSType.TEXT;
                typeInfo.displaySize = typeInfo.precision = Integer.MAX_VALUE;
                typeInfo.collation = tdsReader.readCollation();
                typeInfo.charset = typeInfo.collation.getCharset();
            }
		}),

		NCHAR (TDSType.NCHAR, new Strategy()
		{
            /**
             * Sets the fields of typeInfo to the correct values
             * 
             * @param typeInfo
             *            the TypeInfo whos values are being corrected
             * @param tdsReader
             *            the TDSReader used to set the fields of typeInfo to the correct values
             * @throws SQLServerException
             *             when an error occurs
             */
            public void apply(TypeInfo typeInfo,
                    TDSReader tdsReader) throws SQLServerException {
                typeInfo.ssLenType = SSLenType.USHORTLENTYPE;
                typeInfo.maxLength = tdsReader.readUnsignedShort();
                if (typeInfo.maxLength > DataTypes.SHORT_VARTYPE_MAX_BYTES || 0 != typeInfo.maxLength % 2)
                    tdsReader.throwInvalidTDS();
                typeInfo.displaySize = typeInfo.precision = typeInfo.maxLength / 2;
                typeInfo.ssType = SSType.NCHAR;
                typeInfo.collation = tdsReader.readCollation();
                typeInfo.charset = Encoding.UNICODE.charset();
            }
		}),

		NVARCHAR (TDSType.NVARCHAR, new Strategy()
		{
            /**
             * Sets the fields of typeInfo to the correct values
             * 
             * @param typeInfo
             *            the TypeInfo whos values are being corrected
             * @param tdsReader
             *            the TDSReader used to set the fields of typeInfo to the correct values
             * @throws SQLServerException
             *             when an error occurs
             */
            public void apply(TypeInfo typeInfo,
                    TDSReader tdsReader) throws SQLServerException {
                typeInfo.maxLength = tdsReader.readUnsignedShort();
                if (DataTypes.MAXTYPE_LENGTH == typeInfo.maxLength)// for PLP types
                {
                    typeInfo.ssLenType = SSLenType.PARTLENTYPE;
                    typeInfo.ssType = SSType.NVARCHARMAX;
                    typeInfo.displaySize = typeInfo.precision = DataTypes.MAX_VARTYPE_MAX_CHARS;
                }
                else if (typeInfo.maxLength <= DataTypes.SHORT_VARTYPE_MAX_BYTES && 0 == typeInfo.maxLength % 2)// for non-PLP types
                {
                    typeInfo.ssLenType = SSLenType.USHORTLENTYPE;
                    typeInfo.ssType = SSType.NVARCHAR;
                    typeInfo.displaySize = typeInfo.precision = typeInfo.maxLength / 2;
                }
                else {
                    tdsReader.throwInvalidTDS();
                }
                typeInfo.collation = tdsReader.readCollation();
                typeInfo.charset = Encoding.UNICODE.charset();
            }
		}),

		NTEXT (TDSType.NTEXT, new Strategy()
		{
            /**
             * Sets the fields of typeInfo to the correct values
             * 
             * @param typeInfo
             *            the TypeInfo whos values are being corrected
             * @param tdsReader
             *            the TDSReader used to set the fields of typeInfo to the correct values
             * @throws SQLServerException
             *             when an error occurs
             */
            public void apply(TypeInfo typeInfo,
                    TDSReader tdsReader) throws SQLServerException {
                typeInfo.ssLenType = SSLenType.LONGLENTYPE;
                typeInfo.maxLength = tdsReader.readInt();
                if (typeInfo.maxLength < 0)
                    tdsReader.throwInvalidTDS();
                typeInfo.ssType = SSType.NTEXT;
                typeInfo.displaySize = typeInfo.precision = Integer.MAX_VALUE / 2;
                typeInfo.collation = tdsReader.readCollation();
                typeInfo.charset = Encoding.UNICODE.charset();
            }
		}),

		GUID (TDSType.GUID, new Strategy()
		{
            /**
             * Sets the fields of typeInfo to the correct values
             * 
             * @param typeInfo
             *            the TypeInfo whos values are being corrected
             * @param tdsReader
             *            the TDSReader used to set the fields of typeInfo to the correct values
             * @throws SQLServerException
             *             when an error occurs
             */
            public void apply(TypeInfo typeInfo,
                    TDSReader tdsReader) throws SQLServerException {
                int maxLength = tdsReader.readUnsignedByte();
                if (maxLength != 16 && maxLength != 0)
                    tdsReader.throwInvalidTDS();

                typeInfo.ssLenType = SSLenType.BYTELENTYPE;
                typeInfo.ssType = SSType.GUID;
                typeInfo.maxLength = maxLength;
                typeInfo.displaySize = typeInfo.precision = "NNNNNNNN-NNNN-NNNN-NNNN-NNNNNNNNNNNN".length();
            }
		}),

		UDT (TDSType.UDT, new Strategy()
		{
            /**
             * Sets the fields of typeInfo to the correct values
             * 
             * @param typeInfo
             *            the TypeInfo whos values are being corrected
             * @param tdsReader
             *            the TDSReader used to set the fields of typeInfo to the correct values
             * @throws SQLServerException
             *             when an error occurs
             */
            public void apply(TypeInfo typeInfo,
                    TDSReader tdsReader) throws SQLServerException {
                UDTTDSHeader udtTDSHeader = new UDTTDSHeader(tdsReader);
                typeInfo.maxLength = udtTDSHeader.getMaxLen();
                if (DataTypes.MAXTYPE_LENGTH == typeInfo.maxLength) {
                    typeInfo.precision = DataTypes.MAX_VARTYPE_MAX_BYTES;
                    typeInfo.displaySize = DataTypes.MAX_VARTYPE_MAX_BYTES;
                }
                else if (typeInfo.maxLength <= DataTypes.SHORT_VARTYPE_MAX_BYTES) {
                    typeInfo.precision = typeInfo.maxLength;
                    typeInfo.displaySize = 2 * typeInfo.maxLength;
                }
                else {
                    tdsReader.throwInvalidTDS();
                }

                typeInfo.ssLenType = SSLenType.PARTLENTYPE;
                typeInfo.ssType = SSType.UDT;

                // Every UDT type has an additional type name embedded in the TDS COLMETADATA
                // header. Per meta-data spec for UDT types we return this type name
                // instead of "UDT".
                typeInfo.udtTypeName = udtTDSHeader.getTypeName();
            }
		}),

		XML (TDSType.XML, new Strategy()
		{
            /**
             * Sets the fields of typeInfo to the correct values
             * 
             * @param typeInfo
             *            the TypeInfo whos values are being corrected
             * @param tdsReader
             *            the TDSReader used to set the fields of typeInfo to the correct values
             * @throws SQLServerException
             *             when an error occurs
             */
            public void apply(TypeInfo typeInfo,
                    TDSReader tdsReader) throws SQLServerException {
                XMLTDSHeader xmlTDSHeader = new XMLTDSHeader(tdsReader);
                typeInfo.ssLenType = SSLenType.PARTLENTYPE;
                typeInfo.ssType = SSType.XML;
                typeInfo.displaySize = typeInfo.precision = Integer.MAX_VALUE / 2;
                typeInfo.charset = Encoding.UNICODE.charset();
            }
		}),

		SQL_VARIANT(TDSType.SQL_VARIANT, new Strategy()
		{
            /**
             * Sets the fields of typeInfo to the correct values
             * 
             * @param typeInfo
             *            the TypeInfo whos values are being corrected
             * @param tdsReader
             *            the TDSReader used to set the fields of typeInfo to the correct values
             * @throws SQLServerException
             *             when an error occurs
             */
            public void apply(TypeInfo typeInfo,
                    TDSReader tdsReader) throws SQLServerException {
                typeInfo.ssLenType = SSLenType.LONGLENTYPE; //sql_variant type should be LONGLENTYPE length.
                typeInfo.maxLength = tdsReader.readInt();
                typeInfo.ssType = SSType.SQL_VARIANT;
                }
		});

        private final TDSType tdsType;
        private final Strategy strategy;

        private interface Strategy {
            /**
             * Sets the fields of typeInfo to the correct values
             * 
             * @param typeInfo
             *            the TypeInfo whos values are being corrected
             * @param tdsReader
             *            the TDSReader used to set the fields of typeInfo to the correct values
             * @throws SQLServerException
             *             when an error occurs
             */
            public void apply(TypeInfo typeInfo,
                    TDSReader tdsReader) throws SQLServerException;
        }

        private static final class FixedLenStrategy implements Strategy {
            private final SSType ssType;
            private final int maxLength;
            private final int precision;
            private final int displaySize;
            private final int scale;

            FixedLenStrategy(SSType ssType,
                    int maxLength,
                    int precision,
                    int displaySize,
                    int scale) {
                this.ssType = ssType;
                this.maxLength = maxLength;
                this.precision = precision;
                this.displaySize = displaySize;
                this.scale = scale;
            }

            /**
             * Sets the fields of typeInfo to the correct values
             * 
             * @param typeInfo
             *            the TypeInfo whos values are being corrected
             * @param tdsReader
             *            the TDSReader used to set the fields of typeInfo to the correct values
             */
            public void apply(TypeInfo typeInfo,
                    TDSReader tdsReader) {
                typeInfo.ssLenType = SSLenType.FIXEDLENTYPE;
                typeInfo.ssType = ssType;
                typeInfo.maxLength = maxLength;
                typeInfo.precision = precision;
                typeInfo.displaySize = displaySize;
                typeInfo.scale = scale;
            }
        }

        private static final class DecimalNumericStrategy implements Strategy {
            private final SSType ssType;

            DecimalNumericStrategy(SSType ssType) {
                this.ssType = ssType;
            }

            /**
             * Sets the fields of typeInfo to the correct values
             * 
             * @param typeInfo
             *            the TypeInfo whos values are being corrected
             * @param tdsReader
             *            the TDSReader used to set the fields of typeInfo to the correct values
             * @throws SQLServerException
             *             when an error occurs
             */
            public void apply(TypeInfo typeInfo,
                    TDSReader tdsReader) throws SQLServerException {
                int maxLength = tdsReader.readUnsignedByte();
                int precision = tdsReader.readUnsignedByte();
                int scale = tdsReader.readUnsignedByte();

                if (maxLength > 17)
                    tdsReader.throwInvalidTDS();

                typeInfo.ssLenType = SSLenType.BYTELENTYPE;
                typeInfo.ssType = ssType;
                typeInfo.maxLength = maxLength;
                typeInfo.precision = precision;
                typeInfo.displaySize = precision + 2;
                typeInfo.scale = scale;
            }
        }

        private static final class BigOrSmallByteLenStrategy implements Strategy {
            private final Builder bigBuilder;
            private final Builder smallBuilder;

            BigOrSmallByteLenStrategy(Builder bigBuilder,
                    Builder smallBuilder) {
                this.bigBuilder = bigBuilder;
                this.smallBuilder = smallBuilder;
            }

            /**
             * Sets the fields of typeInfo to the correct values
             * 
             * @param typeInfo
             *            the TypeInfo whos values are being corrected
             * @param tdsReader
             *            the TDSReader used to set the fields of typeInfo to the correct values
             * @throws SQLServerException
             *             when an error occurs
             */
            public void apply(TypeInfo typeInfo,
                    TDSReader tdsReader) throws SQLServerException {
                switch (tdsReader.readUnsignedByte()) // maxLength
                {
                    case 8:
                        bigBuilder.build(typeInfo, tdsReader);
                        break;
                    case 4:
                        smallBuilder.build(typeInfo, tdsReader);
                        break;
                    default:
                        tdsReader.throwInvalidTDS();
                        break;
                }

                typeInfo.ssLenType = SSLenType.BYTELENTYPE;
            }
        }

        private static final class KatmaiScaledTemporalStrategy implements Strategy {
            private final SSType ssType;

            KatmaiScaledTemporalStrategy(SSType ssType) {
                this.ssType = ssType;
            }

            private int getPrecision(String baseFormat,
                    int scale) {
                // For 0-scale temporal, there is no '.' after the seconds component because there are no sub-seconds.
                // Example: 12:34:56.12134 includes a '.', but 12:34:56 doesn't
                return baseFormat.length() + ((scale > 0) ? (1 + scale) : 0);
            }

            /**
             * Sets the fields of typeInfo to the correct values
             * 
             * @param typeInfo
             *            the TypeInfo whos values are being corrected
             * @param tdsReader
             *            the TDSReader used to set the fields of typeInfo to the correct values
             * @throws SQLServerException
             *             when an error occurs
             */
            public void apply(TypeInfo typeInfo,
                    TDSReader tdsReader) throws SQLServerException {
                typeInfo.scale = tdsReader.readUnsignedByte();
                if (typeInfo.scale > TDS.MAX_FRACTIONAL_SECONDS_SCALE)
                    tdsReader.throwInvalidTDS();

                switch (ssType) {
                    case TIME:
                        typeInfo.precision = getPrecision("hh:mm:ss", typeInfo.scale);
                        typeInfo.maxLength = TDS.timeValueLength(typeInfo.scale);
                        break;

                    case DATETIME2:
                        typeInfo.precision = getPrecision("yyyy-mm-dd hh:mm:ss", typeInfo.scale);
                        typeInfo.maxLength = TDS.datetime2ValueLength(typeInfo.scale);
                        break;

                    case DATETIMEOFFSET:
                        typeInfo.precision = getPrecision("yyyy-mm-dd hh:mm:ss +HH:MM", typeInfo.scale);
                        typeInfo.maxLength = TDS.datetimeoffsetValueLength(typeInfo.scale);
                        break;

                    default:
                        assert false : "Unexpected SSType: " + ssType;
                }

                typeInfo.ssLenType = SSLenType.BYTELENTYPE;
                typeInfo.ssType = ssType;
                typeInfo.displaySize = typeInfo.precision;
            }
        }

        private Builder(TDSType tdsType,
                Strategy strategy) {
            this.tdsType = tdsType;
            this.strategy = strategy;
        }

        final TDSType getTDSType() {
            return tdsType;
        }

        final TypeInfo build(TypeInfo typeInfo,
                TDSReader tdsReader) throws SQLServerException {
            strategy.apply(typeInfo, tdsReader);

            // Postcondition: SSType and SSLenType are initialized
            assert null != typeInfo.ssType;
            assert null != typeInfo.ssLenType;

            return typeInfo;
        }
    }

    /**
     * Returns true if this type is a textual type with a single-byte character set that is compatible with the 7-bit US-ASCII character set.
     */
    boolean supportsFastAsciiConversion() {
        switch (ssType) {
            case CHAR:
            case VARCHAR:
            case VARCHARMAX:
            case TEXT:
                return collation.hasAsciiCompatibleSBCS();

            default:
                return false;
        }
    }

    private static final Map<TDSType, Builder> builderMap = new EnumMap<>(TDSType.class);

    static {
        for (Builder builder : Builder.values())
            builderMap.put(builder.getTDSType(), builder);
    }

    private TypeInfo() {
    }

    static TypeInfo getInstance(TDSReader tdsReader,
            boolean readFlags) throws SQLServerException {
        TypeInfo typeInfo = new TypeInfo();

        // UserType is USHORT in TDS 7.1 and earlier; ULONG in TDS 7.2 and later.
        typeInfo.userType = tdsReader.readInt();

        if (readFlags) {
            // Flags (2 bytes)
            typeInfo.flags = tdsReader.readShort();
        }

        TDSType tdsType = null;

        try {
            tdsType = TDSType.valueOf(tdsReader.readUnsignedByte());
        }
        catch (IllegalArgumentException e) {
            tdsReader.getConnection().terminate(SQLServerException.DRIVER_ERROR_INVALID_TDS, e.getMessage(), e);
            // not reached
        }

        assert null != builderMap.get(tdsType) : "Missing TypeInfo builder for TDSType " + tdsType;
        return builderMap.get(tdsType).build(typeInfo, tdsReader);
    }
}

/**
 * DTV implementation for values set from the TDS response stream.
 */
final class ServerDTVImpl extends DTVImpl {
    private int valueLength;
    private TDSReaderMark valueMark;
    private boolean isNull;
    private SqlVariant internalVariant;
    /**
     * Sets the value of the DTV to an app-specified Java type.
     *
     * Generally, the value cannot be stored back into the TDS byte stream (although this could be done for fixed-length types). So this
     * implementation sets the new value in a new AppDTVImpl instance.
     */
    void setValue(DTV dtv,
            SQLCollation collation,
            JDBCType jdbcType,
            Object value,
            JavaType javaType,
            StreamSetterArgs streamSetterArgs,
            Calendar cal,
            Integer scale,
            SQLServerConnection con,
            boolean forceEncrypt) throws SQLServerException {
        dtv.setImpl(new AppDTVImpl());
        dtv.setValue(collation, jdbcType, value, javaType, streamSetterArgs, cal, scale, con, forceEncrypt);
    }

    void setValue(Object value,
            JavaType javaType) {
        // This function is never called, but must be implemented; it's abstract in DTVImpl.
        assert false;
    }

    private final static int STREAMCONSUMED = -2;

    // This function is used by Adaptive stream objects to denote that the
    // whole value of the stream has been consumed.
    // Note this only to be used by the streams returned to the user.
    void setPositionAfterStreamed(TDSReader tdsReader) {
        valueMark = tdsReader.mark();
        valueLength = STREAMCONSUMED;
    }

    void setStreamSetterArgs(StreamSetterArgs streamSetterArgs) {
        // This function is never called, but must be implemented; it's abstract in DTVImpl.
        assert false;
    }

    void setCalendar(Calendar calendar) {
        // This function is never called, but must be implemented; it's abstract in DTVImpl.
        assert false;
    }

    void setScale(Integer scale) {
        // This function is never called, but must be implemented; it's abstract in DTVImpl.
        assert false;
    }

    void setForceEncrypt(boolean forceEncrypt) {
        // This function is never called, but must be implemented; it's abstract in DTVImpl.
        assert false;
    }

    StreamSetterArgs getStreamSetterArgs() {
        // This function is never called, but must be implemented; it's abstract in DTVImpl.
        assert false;
        return null;
    }

    Calendar getCalendar() {
        // This function is never called, but must be implemented; it's abstract in DTVImpl.
        assert false;
        return null;
    }

    Integer getScale() {
        // This function is never called, but must be implemented; it's abstract in DTVImpl.
        assert false;
        return null;
    }

    boolean isNull() {
        return isNull;
    }

    void setJdbcType(JDBCType jdbcType) {
        // This function is never called, but must be implemented; it's abstract in DTVImpl.
        assert false;
    }

    JDBCType getJdbcType() {
        // This function is never called, but must be implemented; it's abstract in DTVImpl.
        assert false;
        return JDBCType.UNKNOWN;
    }

    JavaType getJavaType() {
        // This function is never called, but must be implemented; it's abstract in DTVImpl.
        assert false;
        return JavaType.OBJECT;
    }

    // used to set null value
    // for the DTV when a null value is
    // received from NBCROW for a particular column
    final void initFromCompressedNull() {
        assert valueMark == null;
        isNull = true;
    }

    final void skipValue(TypeInfo type,
            TDSReader tdsReader,
            boolean isDiscard) throws SQLServerException {
        // indicates that this value was obtained from NBCROW
        // So, there is nothing else to read from the wire
        if (null == valueMark && isNull) {
            return;
        }

        if (null == valueMark)
            getValuePrep(type, tdsReader);
        tdsReader.reset(valueMark);
        // value length zero means that the stream has been already skipped to the end - adaptive case
        if (valueLength != STREAMCONSUMED) {
            if (valueLength == DataTypes.UNKNOWN_STREAM_LENGTH) {
                assert SSLenType.PARTLENTYPE == type.getSSLenType();
                // create a plp type and close it so the value can be skipped.
                // We buffer even when adaptive if the user skips this item.
                PLPInputStream tempPLP = PLPInputStream.makeTempStream(tdsReader, isDiscard, this);
                try {
                    if (null != tempPLP)
                        tempPLP.close();
                }
                catch (IOException e) {
                    tdsReader.getConnection().terminate(SQLServerException.DRIVER_ERROR_IO_FAILED, e.getMessage());
                }
            }
            else {
                assert valueLength >= 0;
                tdsReader.skip(valueLength); // jump over the value data
            }
        }

    }

    static final private java.util.logging.Logger aeLogger = java.util.logging.Logger.getLogger("com.microsoft.sqlserver.jdbc.DTV");

    private void getValuePrep(TypeInfo typeInfo,
            TDSReader tdsReader) throws SQLServerException {
        // If we've already seen this value before, then we shouldn't be here.
        assert null == valueMark;

        // Otherwise, mark the value's location, figure out its length, and determine whether it was NULL.
        switch (typeInfo.getSSLenType()) {
            case PARTLENTYPE:
                valueLength = DataTypes.UNKNOWN_STREAM_LENGTH;
                isNull = PLPInputStream.isNull(tdsReader);
                break;

            case FIXEDLENTYPE:
                valueLength = typeInfo.getMaxLength();
                isNull = (0 == valueLength);
                break;

            case BYTELENTYPE:
                valueLength = tdsReader.readUnsignedByte();
                isNull = (0 == valueLength);
                break;

            case USHORTLENTYPE:
                valueLength = tdsReader.readUnsignedShort();
                isNull = (65535 == valueLength);
                if (isNull)
                    valueLength = 0;
                break;

            case LONGLENTYPE:
                if (SSType.TEXT == typeInfo.getSSType() || SSType.IMAGE == typeInfo.getSSType() || SSType.NTEXT == typeInfo.getSSType()) {
                    isNull = (0 == tdsReader.readUnsignedByte());
                    if (isNull) {
                        valueLength = 0;
                    }
                    else {
                        // skip(24) is to skip the textptr and timestamp fields (Section 2.2.7.17 of TDS 7.3 spec)
                        tdsReader.skip(24);
                        valueLength = tdsReader.readInt();
                    }
                }

                else if (SSType.SQL_VARIANT == typeInfo.getSSType()) {
                    valueLength = tdsReader.readInt();
                    isNull = (0 == valueLength);
                    typeInfo.setSSType(SSType.SQL_VARIANT);
                }
                break;
        }

        if (valueLength > typeInfo.getMaxLength())
            tdsReader.throwInvalidTDS();

        valueMark = tdsReader.mark();
    }

    Object denormalizedValue(byte[] decryptedValue,
            JDBCType jdbcType,
            TypeInfo baseTypeInfo,
            SQLServerConnection con,
            InputStreamGetterArgs streamGetterArgs,
            byte normalizeRuleVersion,
            Calendar cal) throws SQLServerException {
        if (0x01 != normalizeRuleVersion) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_UnsupportedNormalizationVersionAE"));
            throw new SQLServerException(form.format(new Object[] {normalizeRuleVersion, 1}), null, 0, null);
        }

        if (aeLogger.isLoggable(java.util.logging.Level.FINE)) {
            aeLogger.fine(
                    "Denormalizing decrypted data based on its SQL Server type(" + baseTypeInfo.getSSType() + ") and JDBC type(" + jdbcType + ").");
        }

        SSType baseSSType = baseTypeInfo.getSSType();
        switch (baseSSType) {
            case CHAR:
            case VARCHAR:
            case NCHAR:
            case NVARCHAR:
            case VARCHARMAX:
            case NVARCHARMAX: {
                try {
                    String strVal = new String(decryptedValue, 0, decryptedValue.length,
                            (null == baseTypeInfo.getCharset()) ? con.getDatabaseCollation().getCharset() : baseTypeInfo.getCharset());
                    if ((SSType.CHAR == baseSSType) || (SSType.NCHAR == baseSSType)) {
                        // Right pad the string for CHAR types.
                        StringBuilder sb = new StringBuilder(strVal);
                        int padLength = baseTypeInfo.getPrecision() - strVal.length();
                        for (int i = 0; i < padLength; i++) {
                            sb.append(' ');
                        }
                        strVal = sb.toString();
                    }
                    return DDC.convertStringToObject(strVal, baseTypeInfo.getCharset(), jdbcType, streamGetterArgs.streamType);
                }
                catch (IllegalArgumentException e) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_errorConvertingValue"));
                    throw new SQLServerException(form.format(new Object[] {baseSSType, jdbcType}), null, 0, e);
                }
                catch (UnsupportedEncodingException e) {
                    // Important: we should not pass the exception here as it displays the data.
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_unsupportedEncoding"));
                    throw new SQLServerException(form.format(new Object[] {baseTypeInfo.getCharset()}), null, 0, e);
                }
            }

            case BIT:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT: {
                // If data is encrypted, then these types are normalized to BIGINT. Need to denormalize here.
                if (8 != decryptedValue.length) {
                    // Integer datatypes are normalized to bigint for AE.
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_NormalizationErrorAE"));
                    throw new SQLServerException(form.format(new Object[] {baseSSType}), null, 0, null);
                }
                return DDC.convertLongToObject(Util.readLong(decryptedValue, 0), jdbcType, baseSSType, streamGetterArgs.streamType);

            }

            case REAL:
            case FLOAT: {
                // JDBC driver does not normalize real to float.
                if (8 == decryptedValue.length) {
                    return DDC.convertDoubleToObject(ByteBuffer.wrap(decryptedValue).order(ByteOrder.LITTLE_ENDIAN).getDouble(),
                            JDBCType.VARBINARY == jdbcType ? baseSSType.getJDBCType() : jdbcType, // use jdbc type from baseTypeInfo if using
                                                                                                  // getObject()
                            streamGetterArgs.streamType);
                }
                else if (4 == decryptedValue.length) {
                    return DDC.convertFloatToObject(ByteBuffer.wrap(decryptedValue).order(ByteOrder.LITTLE_ENDIAN).getFloat(),
                            JDBCType.VARBINARY == jdbcType ? baseSSType.getJDBCType() : jdbcType, // use jdbc type from baseTypeInfo if using
                                                                                                  // getObject()
                            streamGetterArgs.streamType);
                }
                else {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_NormalizationErrorAE"));
                    throw new SQLServerException(form.format(new Object[] {baseSSType}), null, 0, null);
                }
            }
            case SMALLMONEY: {
                return DDC.convertMoneyToObject(new BigDecimal(BigInteger.valueOf(Util.readInt(decryptedValue, 4)), 4),
                        JDBCType.VARBINARY == jdbcType ? baseSSType.getJDBCType() : jdbcType, // use jdbc type from baseTypeInfo if using getObject()
                        streamGetterArgs.streamType, 4);
            }

            case MONEY: {
                BigInteger bi = BigInteger.valueOf(((long) Util.readInt(decryptedValue, 0) << 32) | (Util.readInt(decryptedValue, 4) & 0xFFFFFFFFL));

                return DDC.convertMoneyToObject(new BigDecimal(bi, 4), JDBCType.VARBINARY == jdbcType ? baseSSType.getJDBCType() : jdbcType, // use
                                                                                                                                             // jdbc
                                                                                                                                             // type
                                                                                                                                             // from
                                                                                                                                             // baseTypeInfo
                                                                                                                                             // if
                                                                                                                                             // using
                                                                                                                                             // getObject()
                        streamGetterArgs.streamType, 8);
            }

            case NUMERIC:
            case DECIMAL: {
                return DDC.convertBigDecimalToObject(Util.readBigDecimal(decryptedValue, decryptedValue.length, baseTypeInfo.getScale()),
                        JDBCType.VARBINARY == jdbcType ? baseSSType.getJDBCType() : jdbcType, // use jdbc type from baseTypeInfo if using getObject()
                        streamGetterArgs.streamType);
            }

            case BINARY:
            case VARBINARY:
            case VARBINARYMAX: {
                return DDC.convertBytesToObject(decryptedValue, jdbcType, baseTypeInfo);
            }

            case DATE: {
                // get the number of days !! Size should be 3
                if (3 != decryptedValue.length) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_NormalizationErrorAE"));
                    throw new SQLServerException(form.format(new Object[] {baseSSType}), null, 0, null);
                }

                // Getting number of days
                // Following Lines of code copied from IOBuffer.readDaysIntoCE as we
                // cannot reuse method
                int daysIntoCE = getDaysIntoCE(decryptedValue, baseSSType);

                return DDC.convertTemporalToObject(jdbcType, baseSSType, cal, daysIntoCE, 0, 0);

            }

            case TIME: {
                long localNanosSinceMidnight = readNanosSinceMidnightAE(decryptedValue, baseTypeInfo.getScale(), baseSSType);

                return DDC.convertTemporalToObject(jdbcType, SSType.TIME, cal, 0, localNanosSinceMidnight, baseTypeInfo.getScale());
            }

            case DATETIME2: {
                if (8 != decryptedValue.length) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_NormalizationErrorAE"));
                    throw new SQLServerException(form.format(new Object[] {baseSSType}), null, 0, null);
                }

                // Last three bytes are for date and remaining for time
                int dateOffset = decryptedValue.length - 3;
                byte[] timePortion = new byte[dateOffset];
                byte[] datePortion = new byte[3];
                System.arraycopy(decryptedValue, 0, timePortion, 0, dateOffset);
                System.arraycopy(decryptedValue, dateOffset, datePortion, 0, 3);
                long localNanosSinceMidnight = readNanosSinceMidnightAE(timePortion, baseTypeInfo.getScale(), baseSSType);

                int daysIntoCE = getDaysIntoCE(datePortion, baseSSType);

                // Convert the DATETIME2 value to the desired Java type.
                return DDC.convertTemporalToObject(jdbcType, SSType.DATETIME2, cal, daysIntoCE, localNanosSinceMidnight, baseTypeInfo.getScale());

            }

            case SMALLDATETIME: {
                if (4 != decryptedValue.length) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_NormalizationErrorAE"));
                    throw new SQLServerException(form.format(new Object[] {baseSSType}), null, 0, null);
                }

                // SQL smalldatetime has less precision. It stores 2 bytes
                // for the days since SQL Base Date and 2 bytes for minutes
                // after midnight.
                return DDC.convertTemporalToObject(jdbcType, SSType.DATETIME, cal, Util.readUnsignedShort(decryptedValue, 0),
                        Util.readUnsignedShort(decryptedValue, 2) * 60L * 1000L, 0);
            }

            case DATETIME: {
                if (8 != decryptedValue.length) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_NormalizationErrorAE"));
                    throw new SQLServerException(form.format(new Object[] {baseSSType}), null, 0, null);
                }

                // SQL datetime is 4 bytes for days since SQL Base Date
                // (January 1, 1900 00:00:00 GMT) and 4 bytes for
                // the number of three hundredths (1/300) of a second since midnight.
                return DDC.convertTemporalToObject(jdbcType, SSType.DATETIME, cal, Util.readInt(decryptedValue, 0),
                        (Util.readInt(decryptedValue, 4) * 10 + 1) / 3, 0);
            }

            case DATETIMEOFFSET: {
                // Last 5 bytes are for date and offset
                int dateOffset = decryptedValue.length - 5;
                byte[] timePortion = new byte[dateOffset];
                byte[] datePortion = new byte[3];
                byte[] offsetPortion = new byte[2];
                System.arraycopy(decryptedValue, 0, timePortion, 0, dateOffset);
                System.arraycopy(decryptedValue, dateOffset, datePortion, 0, 3);
                System.arraycopy(decryptedValue, dateOffset + 3, offsetPortion, 0, 2);
                long localNanosSinceMidnight = readNanosSinceMidnightAE(timePortion, baseTypeInfo.getScale(), baseSSType);

                int daysIntoCE = getDaysIntoCE(datePortion, baseSSType);

                int localMinutesOffset = ByteBuffer.wrap(offsetPortion).order(ByteOrder.LITTLE_ENDIAN).getShort();

                return DDC.convertTemporalToObject(jdbcType, SSType.DATETIMEOFFSET,
                        new GregorianCalendar(new SimpleTimeZone(localMinutesOffset * 60 * 1000, ""), Locale.US), daysIntoCE, localNanosSinceMidnight,
                        baseTypeInfo.getScale());

            }

            case GUID: {
                return Util.readGUID(decryptedValue);
            }

            default:
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_UnsupportedDataTypeAE"));
                throw new SQLServerException(form.format(new Object[] {baseSSType}), null, 0, null);
        }
    }

    Object getValue(DTV dtv,
            JDBCType jdbcType,
            int scale,
            InputStreamGetterArgs streamGetterArgs,
            Calendar cal,
            TypeInfo typeInfo,
            CryptoMetadata cryptoMetadata,
            TDSReader tdsReader) throws SQLServerException {
        SQLServerConnection con = tdsReader.getConnection();
        Object convertedValue = null;
        byte[] decryptedValue;
        boolean encrypted = false;
        SSType baseSSType = typeInfo.getSSType();

        // If column encryption is not enabled on connection or on statement, cryptoMeta will be null.
        if (null != cryptoMetadata) {
            assert (SSType.VARBINARY == typeInfo.getSSType()) || (SSType.VARBINARYMAX == typeInfo.getSSType());
            baseSSType = cryptoMetadata.baseTypeInfo.getSSType();
            encrypted = true;

            if (aeLogger.isLoggable(java.util.logging.Level.FINE)) {
                aeLogger.fine("Data is encrypted, SQL Server Data Type: " + baseSSType + ", Encryption Type: " + cryptoMetadata.getEncryptionType());
            }
        }

        // Note that the value should be prepped
        // only for columns whose values can be read of the wire.
        // If valueMark == null and isNull, it implies that
        // the column is null according to NBCROW and that
        // there is nothing to be read from the wire.
        if (null == valueMark && (!isNull))
            getValuePrep(typeInfo, tdsReader);

        // either there should be a valueMark
        // or valueMark should be null and isNull should be set to true(NBCROW case)
        assert ((valueMark != null) || (valueMark == null && isNull));

        if (null != streamGetterArgs) {
            if (!streamGetterArgs.streamType.convertsFrom(typeInfo))
                DataTypes.throwConversionError(typeInfo.getSSType().toString(), streamGetterArgs.streamType.toString());
        }
        else {
            if (!baseSSType.convertsTo(jdbcType) && !isNull) {
                // if the baseSSType is Character or NCharacter and jdbcType is Longvarbinary,
                // does not throw type conversion error, which allows getObject() on Long Character types.
                if (encrypted) {
                    if (!Util.isBinaryType(jdbcType.getIntValue())) {
                        DataTypes.throwConversionError(baseSSType.toString(), jdbcType.toString());
                    }
                }
                else {
                    DataTypes.throwConversionError(baseSSType.toString(), jdbcType.toString());
                }
            }

            streamGetterArgs = InputStreamGetterArgs.getDefaultArgs();
        }

        if (STREAMCONSUMED == valueLength) {
            throw new SQLServerException(null, SQLServerException.getErrString("R_dataAlreadyAccessed"), null, 0, false);
        }

        if (!isNull) {
            tdsReader.reset(valueMark);

            if (encrypted) {
                if (DataTypes.UNKNOWN_STREAM_LENGTH == valueLength) {
                    convertedValue = DDC.convertStreamToObject(PLPInputStream.makeStream(tdsReader, streamGetterArgs, this), typeInfo,
                            JDBCType.VARBINARY, streamGetterArgs);
                }
                else {
                    convertedValue = DDC.convertStreamToObject(new SimpleInputStream(tdsReader, valueLength, streamGetterArgs, this), typeInfo,
                            JDBCType.VARBINARY, streamGetterArgs);
                }

                if (aeLogger.isLoggable(java.util.logging.Level.FINE)) {
                    aeLogger.fine("Encrypted data is retrieved.");
                }

                // AE does not support streaming types
                if ((convertedValue instanceof SimpleInputStream) || (convertedValue instanceof PLPInputStream)) {
                    throw new SQLServerException(SQLServerException.getErrString("R_notSupported"), null);
                }

                decryptedValue = SQLServerSecurityUtility.decryptWithKey((byte[]) convertedValue, cryptoMetadata, con);
                return denormalizedValue(decryptedValue, jdbcType, cryptoMetadata.baseTypeInfo, con, streamGetterArgs,
                        cryptoMetadata.normalizationRuleVersion, cal);
            }

            switch (baseSSType) {
                // Process all PLP types here.
                case VARBINARYMAX:
                case VARCHARMAX:
                case NVARCHARMAX:
                case UDT: {
                    convertedValue = DDC.convertStreamToObject(PLPInputStream.makeStream(tdsReader, streamGetterArgs, this), typeInfo, jdbcType,
                            streamGetterArgs);
                    break;
                }

                case XML: {
                    convertedValue = DDC.convertStreamToObject(
                            ((jdbcType.isBinary() || jdbcType == JDBCType.SQLXML) ? PLPXMLInputStream.makeXMLStream(tdsReader, streamGetterArgs, this)
                                    : PLPInputStream.makeStream(tdsReader, streamGetterArgs, this)),
                            typeInfo, jdbcType, streamGetterArgs);
                    break;
                }

                // Convert other variable length native types
                // (CHAR/VARCHAR/TEXT/NCHAR/NVARCHAR/NTEXT/BINARY/VARBINARY/IMAGE) -> ANY jdbcType.
                case CHAR:
                case VARCHAR:
                case TEXT:
                case NCHAR:
                case NVARCHAR:
                case NTEXT:
                case IMAGE:
                case BINARY:
                case VARBINARY:
                case TIMESTAMP: // A special BINARY(8)
                {
                    convertedValue = DDC.convertStreamToObject(new SimpleInputStream(tdsReader, valueLength, streamGetterArgs, this), typeInfo,
                            jdbcType, streamGetterArgs);
                    break;
                }

                // Convert BIT/TINYINT/SMALLINT/INTEGER/BIGINT native type -> ANY jdbcType.
                case BIT:
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT: {
                    switch (valueLength) {
                        case 8:
                            convertedValue = DDC.convertLongToObject(tdsReader.readLong(), jdbcType, baseSSType, streamGetterArgs.streamType);
                            break;

                        case 4:
                            convertedValue = DDC.convertIntegerToObject(tdsReader.readInt(), valueLength, jdbcType, streamGetterArgs.streamType);
                            break;

                        case 2:
                            convertedValue = DDC.convertIntegerToObject(tdsReader.readShort(), valueLength, jdbcType, streamGetterArgs.streamType);
                            break;

                        case 1:
                            convertedValue = DDC.convertIntegerToObject(tdsReader.readUnsignedByte(), valueLength, jdbcType,
                                    streamGetterArgs.streamType);
                            break;

                        default:
                            assert false : "Unexpected valueLength" + valueLength;
                            break;
                    }
                    break;
                }

                // Convert DECIMAL|NUMERIC native types -> ANY jdbcType.
                case DECIMAL:
                case NUMERIC:
                    convertedValue = tdsReader.readDecimal(valueLength, typeInfo, jdbcType, streamGetterArgs.streamType);
                    break;

                // Convert MONEY|SMALLMONEY native types -> ANY jdbcType.
                case MONEY:
                case SMALLMONEY:
                    convertedValue = tdsReader.readMoney(valueLength, jdbcType, streamGetterArgs.streamType);
                    break;

                // Convert FLOAT native type -> ANY jdbcType.
                case FLOAT:
                    convertedValue = tdsReader.readFloat(valueLength, jdbcType, streamGetterArgs.streamType);
                    break;

                // Convert REAL native type -> ANY jdbcType.
                case REAL:
                    convertedValue = tdsReader.readReal(valueLength, jdbcType, streamGetterArgs.streamType);
                    break;

                // Convert DATETIME|SMALLDATETIME native types -> ANY jdbcType.
                case DATETIME:
                case SMALLDATETIME:
                    convertedValue = tdsReader.readDateTime(valueLength, cal, jdbcType, streamGetterArgs.streamType);
                    break;

                // Convert DATE native type -> ANY jdbcType.
                case DATE:
                    convertedValue = tdsReader.readDate(valueLength, cal, jdbcType);
                    break;

                // Convert TIME native type -> ANY jdbcType.
                case TIME:
                    convertedValue = tdsReader.readTime(valueLength, typeInfo, cal, jdbcType);
                    break;

                // Convert DATETIME2 native type -> ANY jdbcType.
                case DATETIME2:
                    convertedValue = tdsReader.readDateTime2(valueLength, typeInfo, cal, jdbcType);
                    break;

                // Convert DATETIMEOFFSET native type -> ANY jdbcType.
                case DATETIMEOFFSET:
                    convertedValue = tdsReader.readDateTimeOffset(valueLength, typeInfo, jdbcType);
                    break;

                // Convert GUID native type -> ANY jdbcType.
                case GUID:
                    convertedValue = tdsReader.readGUID(valueLength, jdbcType, streamGetterArgs.streamType);
                    break;
                    
                case SQL_VARIANT:   
                    /**
                     * SQL_Variant has the following structure:
                     * 1- basetype: the underlying type
                     * 2- probByte: holds count of property bytes expected for a sql_variant structure
                     * 3- properties: For example VARCHAR type has 5 byte collation and 2 byte max length 
                     * 4- dataValue: the data value
                     */
                    int baseType = tdsReader.readUnsignedByte();

                    int cbPropsActual = tdsReader.readUnsignedByte();
                    // don't create new one, if we have already created an internalVariant object. For example, in bulkcopy
                    // when we are reading time column, we update the same internalvarianttype's JDBC to be timestamp
                    if (null == internalVariant) {
                        internalVariant = new SqlVariant(baseType);
                    }
                    convertedValue = readSqlVariant(baseType, cbPropsActual, valueLength, tdsReader, baseSSType, typeInfo, jdbcType, streamGetterArgs,
                            cal);
                    break;
                // Unknown SSType should have already been rejected by TypeInfo.setFromTDS()
                default:
                    assert false : "Unexpected SSType " + typeInfo.getSSType();
                    break;
            }
        } // !isNull

        // Postcondition: returned object is null only if value was null.
        assert isNull || null != convertedValue;
        return convertedValue;
    }
        
    SqlVariant getInternalVariant() {
        return internalVariant;
    }

    /**
     * Read the value inside sqlVariant. The reading differs based on what the internal baseType is.
     * 
     * @return sql_variant value
     * @since 6.3.0
     * @throws SQLServerException
     */
    private Object readSqlVariant(int intbaseType,
            int cbPropsActual,
            int valueLength,
            TDSReader tdsReader,
            SSType baseSSType,
            TypeInfo typeInfo,
            JDBCType jdbcType,
            InputStreamGetterArgs streamGetterArgs,
            Calendar cal) throws SQLServerException {
        Object convertedValue = null;
        int lengthConsumed = 2 + cbPropsActual; // We have already read 2bytes for baseType earlier.
        int expectedValueLength = valueLength - lengthConsumed;
        SQLCollation collation = null;
        int precision;
        int scale;
        int maxLength;
        TDSType baseType = TDSType.valueOf(intbaseType);
        switch (baseType) {
            case INT8:
                jdbcType = JDBCType.BIGINT;
                convertedValue = DDC.convertLongToObject(tdsReader.readLong(), jdbcType, baseSSType, streamGetterArgs.streamType);
                break;
                
            case INT4:
                jdbcType = JDBCType.INTEGER;
                convertedValue = DDC.convertIntegerToObject(tdsReader.readInt(), valueLength, jdbcType, streamGetterArgs.streamType);
                break;
                
            case INT2:
                jdbcType = JDBCType.SMALLINT;
                convertedValue = DDC.convertIntegerToObject(tdsReader.readShort(), valueLength, jdbcType, streamGetterArgs.streamType);
                break;
                
            case INT1:
                jdbcType = JDBCType.TINYINT;
                convertedValue = DDC.convertIntegerToObject(tdsReader.readUnsignedByte(), valueLength, jdbcType, streamGetterArgs.streamType);
                break;
                
            case DECIMALN:
            case NUMERICN:
                if (TDSType.DECIMALN == baseType)
                    jdbcType = JDBCType.DECIMAL;
                else if (TDSType.NUMERICN == baseType)
                    jdbcType = JDBCType.NUMERIC;
                if (cbPropsActual != sqlVariantProbBytes.DECIMALN.getIntValue()) {   // Numeric and decimal have the same probbytes value
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidProbbytes"));
                    throw new SQLServerException(form.format(new Object[] {baseType}), null, 0, null);
                }
                jdbcType = JDBCType.DECIMAL;
                precision = tdsReader.readUnsignedByte();
                scale = tdsReader.readUnsignedByte();
                typeInfo.setScale(scale);  // typeInfo needs to be updated. typeInfo is usually set when reading columnMetaData, but for sql_variant
                // type the actual columnMetaData is is set when reading the data rows.
                internalVariant.setPrecision(precision);
                internalVariant.setScale(scale);
                convertedValue = tdsReader.readDecimal(expectedValueLength, typeInfo, jdbcType, streamGetterArgs.streamType);
                break;
                
            case FLOAT4:
                jdbcType = JDBCType.REAL;
                convertedValue = tdsReader.readReal(expectedValueLength, jdbcType, streamGetterArgs.streamType);
                break;
                
            case FLOAT8:
                jdbcType = JDBCType.FLOAT;
                convertedValue = tdsReader.readFloat(expectedValueLength, jdbcType, streamGetterArgs.streamType);
                break;
                
            case MONEY4:
                jdbcType = JDBCType.SMALLMONEY;
                precision = Long.toString(Long.MAX_VALUE).length();
                typeInfo.setPrecision(precision);
                scale = 4;
                typeInfo.setDisplaySize(("-" + "." + Integer.toString(Integer.MAX_VALUE)).length());
                typeInfo.setScale(scale);
                internalVariant.setPrecision(precision);
                internalVariant.setScale(scale);
                convertedValue = tdsReader.readMoney(expectedValueLength, jdbcType, streamGetterArgs.streamType);
                break;
                
            case MONEY8:
                jdbcType = JDBCType.MONEY;
                precision = Long.toString(Long.MAX_VALUE).length();
                scale = 4;
                typeInfo.setPrecision(precision);
                typeInfo.setDisplaySize(("-" + "." + Integer.toString(Integer.MAX_VALUE)).length());
                typeInfo.setScale(scale);
                internalVariant.setPrecision(precision);
                internalVariant.setScale(scale);
                convertedValue = tdsReader.readMoney(expectedValueLength, jdbcType, streamGetterArgs.streamType);
                break;
                
            case BIT1:
            case BITN:
                jdbcType = JDBCType.BIT;
                switch (expectedValueLength) {
                    case 8:
                        convertedValue = DDC.convertLongToObject(tdsReader.readLong(), jdbcType, baseSSType, streamGetterArgs.streamType);
                        break;

                    case 4:
                        convertedValue = DDC.convertIntegerToObject(tdsReader.readInt(), expectedValueLength, jdbcType, streamGetterArgs.streamType);
                        break;

                    case 2:
                        convertedValue = DDC.convertIntegerToObject(tdsReader.readShort(), expectedValueLength, jdbcType,
                                streamGetterArgs.streamType);
                        break;

                    case 1:
                        convertedValue = DDC.convertIntegerToObject(tdsReader.readUnsignedByte(), expectedValueLength, jdbcType,
                                streamGetterArgs.streamType);
                        break;

                    default:
                        assert false : "Unexpected valueLength" + expectedValueLength;
                        break;
                }
                break;
                
            case BIGVARCHAR:   
            case BIGCHAR:
                if (cbPropsActual != sqlVariantProbBytes.BIGCHAR.getIntValue()) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidProbbytes"));
                    throw new SQLServerException(form.format(new Object[] {baseType}), null, 0, null);
                }
                if (TDSType.BIGVARCHAR == baseType)
                    jdbcType = JDBCType.VARCHAR;
                else if (TDSType.BIGCHAR == baseType)
                    jdbcType = JDBCType.CHAR;
                collation = tdsReader.readCollation();
                typeInfo.setSQLCollation(collation);
                maxLength = tdsReader.readUnsignedShort();
                if (maxLength > DataTypes.SHORT_VARTYPE_MAX_BYTES)
                    tdsReader.throwInvalidTDS();
                typeInfo.setDisplaySize(maxLength);
                typeInfo.setPrecision(maxLength);
                internalVariant.setPrecision(maxLength);
                internalVariant.setCollation(collation);
                typeInfo.setCharset(collation.getCharset());
                convertedValue = DDC.convertStreamToObject(new SimpleInputStream(tdsReader, expectedValueLength, streamGetterArgs, this), typeInfo,
                        jdbcType, streamGetterArgs);
                break;
                
            case NCHAR:
            case NVARCHAR:
                if (cbPropsActual != sqlVariantProbBytes.NCHAR.getIntValue()) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidProbbytes"));
                    throw new SQLServerException(form.format(new Object[] {baseType}), null, 0, null);
                }
                if (TDSType.NCHAR == baseType)
                    jdbcType = JDBCType.NCHAR;
                else if (TDSType.NVARCHAR == baseType)
                    jdbcType = JDBCType.NVARCHAR;
                collation = tdsReader.readCollation();
                typeInfo.setSQLCollation(collation);
                maxLength = tdsReader.readUnsignedShort();
                if (maxLength > DataTypes.SHORT_VARTYPE_MAX_BYTES || 0 != maxLength % 2)
                    tdsReader.throwInvalidTDS();
                typeInfo.setDisplaySize(maxLength / 2);
                typeInfo.setPrecision(maxLength / 2);
                internalVariant.setPrecision(maxLength / 2);
                internalVariant.setCollation(collation);
                typeInfo.setCharset(Encoding.UNICODE.charset());
                convertedValue = DDC.convertStreamToObject(new SimpleInputStream(tdsReader, expectedValueLength, streamGetterArgs, this), typeInfo,
                        jdbcType, streamGetterArgs);
                break;
                
            case DATETIME8:
                jdbcType = JDBCType.DATETIME;
                convertedValue = tdsReader.readDateTime(expectedValueLength, cal, jdbcType, streamGetterArgs.streamType);
                break;
                
            case DATETIME4:
                jdbcType = JDBCType.SMALLDATETIME;
                convertedValue = tdsReader.readDateTime(expectedValueLength, cal, jdbcType, streamGetterArgs.streamType);
                break;
                
            case DATEN:
                jdbcType = JDBCType.DATE;
                convertedValue = tdsReader.readDate(expectedValueLength, cal, jdbcType);
                break;
                
            case TIMEN:
                if (cbPropsActual != sqlVariantProbBytes.TIMEN.getIntValue()) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidProbbytes"));
                    throw new SQLServerException(form.format(new Object[] {baseType}), null, 0, null);
                }
                if (internalVariant.isBaseTypeTimeValue()) {
                    jdbcType = JDBCType.TIMESTAMP;
                }
                scale = tdsReader.readUnsignedByte();
                typeInfo.setScale(scale);
                internalVariant.setScale(scale);
                convertedValue = tdsReader.readTime(expectedValueLength, typeInfo, cal, jdbcType);
                break;
                
            case DATETIME2N:
                if (cbPropsActual != sqlVariantProbBytes.DATETIME2N.getIntValue()) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidProbbytes"));
                    throw new SQLServerException(form.format(new Object[] {baseType}), null, 0, null);
                }
                jdbcType = JDBCType.TIMESTAMP;
                scale = tdsReader.readUnsignedByte();
                typeInfo.setScale(scale);
                internalVariant.setScale(scale);
                convertedValue = tdsReader.readDateTime2(expectedValueLength, typeInfo, cal, jdbcType);
                break;
                
            case BIGBINARY:   // e.g binary20, binary 512, binary 8000 -> reads as bigbinary
            case BIGVARBINARY:
                if (cbPropsActual != sqlVariantProbBytes.BIGBINARY.getIntValue()) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidProbbytes"));
                    throw new SQLServerException(form.format(new Object[] {baseType}), null, 0, null);
                }
                if (TDSType.BIGBINARY == baseType)
                    jdbcType = JDBCType.BINARY;// LONGVARCHAR;
                else if (TDSType.BIGVARBINARY == baseType)
                    jdbcType = JDBCType.VARBINARY;
                maxLength = tdsReader.readUnsignedShort();
                internalVariant.setMaxLength(maxLength);
                if (maxLength > DataTypes.SHORT_VARTYPE_MAX_BYTES)
                    tdsReader.throwInvalidTDS();
                typeInfo.setDisplaySize(2 * maxLength);
                typeInfo.setPrecision(maxLength);
                convertedValue = DDC.convertStreamToObject(new SimpleInputStream(tdsReader, expectedValueLength, streamGetterArgs, this), typeInfo,
                        jdbcType, streamGetterArgs);
                break;
                
            case GUID:
                jdbcType = JDBCType.GUID;
                internalVariant.setBaseType(intbaseType);
                internalVariant.setBaseJDBCType(jdbcType);
                typeInfo.setDisplaySize("NNNNNNNN-NNNN-NNNN-NNNN-NNNNNNNNNNNN".length());
                lengthConsumed = 2 + cbPropsActual;
                convertedValue = tdsReader.readGUID(expectedValueLength, jdbcType, streamGetterArgs.streamType);
                break;
                
            // Unsupported TdsType should throw error message
            default: {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidDataTypeSupportForSQLVariant"));
                throw new SQLServerException(form.format(new Object[] {baseType}), null, 0, null);
            }

        }
        return convertedValue;
    }

    Object getSetterValue() {
        // This function is never called, but must be implemented; it's abstract in DTVImpl.
        assert false;
        return null;
    }

    private long readNanosSinceMidnightAE(byte[] value,
            int scale,
            SSType baseSSType) throws SQLServerException {
        long hundredNanosSinceMidnight = 0;
        for (int i = 0; i < value.length; i++)
            hundredNanosSinceMidnight |= (value[i] & 0xFFL) << (8 * i);

        if (!(0 <= hundredNanosSinceMidnight && hundredNanosSinceMidnight < Nanos.PER_DAY / 100)) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_NormalizationErrorAE"));
            throw new SQLServerException(form.format(new Object[] {baseSSType}), null, 0, null);
        }

        return 100 * hundredNanosSinceMidnight;
    }

    private int getDaysIntoCE(byte[] datePortion,
            SSType baseSSType) throws SQLServerException {
        int daysIntoCE = 0;
        for (int i = 0; i < datePortion.length; i++) {
            daysIntoCE |= ((datePortion[i] & 0xFF) << (8 * i));
        }

        if (daysIntoCE < 0) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_NormalizationErrorAE"));
            throw new SQLServerException(form.format(new Object[] {baseSSType}), null, 0, null);
        }

        return daysIntoCE;
    }
}
