//---------------------------------------------------------------------------------------------------------------------------------
// File: SQLServerBulkCopy42Helper.java
//
//
// Microsoft JDBC Driver for SQL Server
// Copyright(c) Microsoft Corporation
// All rights reserved.
// MIT License
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the ""Software""), 
//  to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
//  and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS 
//  IN THE SOFTWARE.
//---------------------------------------------------------------------------------------------------------------------------------


package com.microsoft.sqlserver.jdbc;

import java.sql.Timestamp;
import java.text.MessageFormat;
import java.time.DateTimeException;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;

import microsoft.sql.DateTimeOffset;

/**
 * This class is separated from SQLServerBulkCopy class to resolve run-time error of missing Java 8 types
 * when running with Java 7
 */
class SQLServerBulkCopy42Helper {
    static Object getTemporalObjectFromCSVWithFormatter(String valueStrUntrimmed, int srcJdbcType, int srcColOrdinal, DateTimeFormatter dateTimeFormatter, SQLServerConnection connection, SQLServerBulkCopy sqlServerBC) throws SQLServerException {
        DriverJDBCVersion.checkSupportsJDBC42();

        try {
            TemporalAccessor ta = dateTimeFormatter.parse(valueStrUntrimmed);

            int taHour, taMin, taSec, taYear, taMonth, taDay, taNano, taOffsetSec;
            taHour = taMin = taSec = taYear = taMonth = taDay = taNano = taOffsetSec = 0;
            if (ta.isSupported(ChronoField.NANO_OF_SECOND))
                taNano = ta.get(ChronoField.NANO_OF_SECOND);
            if (ta.isSupported(ChronoField.OFFSET_SECONDS))
                taOffsetSec = ta.get(ChronoField.OFFSET_SECONDS);
            if (ta.isSupported(ChronoField.HOUR_OF_DAY))
                taHour = ta.get(ChronoField.HOUR_OF_DAY);
            if (ta.isSupported(ChronoField.MINUTE_OF_HOUR))
                taMin = ta.get(ChronoField.MINUTE_OF_HOUR);
            if (ta.isSupported(ChronoField.SECOND_OF_MINUTE))
                taSec = ta.get(ChronoField.SECOND_OF_MINUTE);
            if (ta.isSupported(ChronoField.DAY_OF_MONTH))
                taDay = ta.get(ChronoField.DAY_OF_MONTH);
            if (ta.isSupported(ChronoField.MONTH_OF_YEAR))
                taMonth = ta.get(ChronoField.MONTH_OF_YEAR);
            if (ta.isSupported(ChronoField.YEAR))
                taYear = ta.get(ChronoField.YEAR);

            Calendar cal = null;
            cal = new GregorianCalendar(new SimpleTimeZone(taOffsetSec * 1000, ""));
            cal.clear();
            cal.set(Calendar.HOUR_OF_DAY, taHour);
            cal.set(Calendar.MINUTE, taMin);
            cal.set(Calendar.SECOND, taSec);
            cal.set(Calendar.DATE, taDay);
            cal.set(Calendar.MONTH, taMonth - 1);
            cal.set(Calendar.YEAR, taYear);
            int fractionalSecondsLength = Integer.toString(taNano).length();
            for (int i = 0; i < (9 - fractionalSecondsLength); i++)
                taNano *= 10;
            Timestamp ts = new Timestamp(cal.getTimeInMillis());
            ts.setNanos(taNano);

            switch (srcJdbcType) {
                case java.sql.Types.TIMESTAMP:
                    return ts;
                case java.sql.Types.TIME:
                    // Time is returned as Timestamp to preserve nano seconds.
                    cal.set(connection.baseYear(), 00, 01);
                    ts = new java.sql.Timestamp(cal.getTimeInMillis());
                    ts.setNanos(taNano);
                    return new java.sql.Timestamp(ts.getTime());
                case java.sql.Types.DATE:
                    return new java.sql.Date(ts.getTime());
                case microsoft.sql.Types.DATETIMEOFFSET:
                    return DateTimeOffset.valueOf(ts, taOffsetSec / 60);
            }
        } catch (DateTimeException | ArithmeticException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_ParsingError"));
            Object[] msgArgs = {JDBCType.of(srcJdbcType)};
            throw new SQLServerException(sqlServerBC, form.format(msgArgs), null, 0, false);
        }
        // unreachable code. Need to do to compile from Eclipse.
        return valueStrUntrimmed;
    }
}