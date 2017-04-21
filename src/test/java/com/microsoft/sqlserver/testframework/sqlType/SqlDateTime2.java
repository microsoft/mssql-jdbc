/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework.sqlType;

import static org.junit.jupiter.api.Assertions.fail;

import java.sql.JDBCType;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.lang3.RandomStringUtils;

public class SqlDateTime2 extends SqlDateTime {

    static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd'T'HH:mm:ss.SSSSSSS");
    static String basePattern = "yyyy-MM-dd HH:mm:ss";
    static DateTimeFormatter formatter;

    public SqlDateTime2() {
        super("datetime2", JDBCType.TIMESTAMP, null, null);
        try {
            minvalue = new Timestamp(dateFormat.parse((String) SqlTypeValue.DATETIME2.minValue).getTime());
            maxvalue = new Timestamp(dateFormat.parse((String) SqlTypeValue.DATETIME2.maxValue).getTime());
        }
        catch (ParseException ex) {
            fail(ex.getMessage());
        }
        this.precision = 7;
        this.variableLengthType = VariableLengthType.Precision;
        generatePrecision();
        formatter = new DateTimeFormatterBuilder().appendPattern(basePattern).appendFraction(ChronoField.NANO_OF_SECOND, 0, this.precision, true)
                .toFormatter();
        formatter = formatter.withResolverStyle(ResolverStyle.STRICT);
    }

    public Object createdata() {
        Timestamp temp = new Timestamp(ThreadLocalRandom.current().nextLong(((Timestamp) minvalue).getTime(), ((Timestamp) maxvalue).getTime()));
        temp.setNanos(0);
        String timeNano = temp.toString().substring(0, temp.toString().length() - 1) + RandomStringUtils.randomNumeric(this.precision);
        return timeNano;
        // can pass string rather than converting to LocalDateTime, but leaving
        // it unchanged for now to handle prepared statements
//        return LocalDateTime.parse(timeNano, formatter);
    }
}