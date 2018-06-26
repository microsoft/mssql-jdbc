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
import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class SqlTime extends SqlDateTime {

    static SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSSSSSS");
    static String basePattern = "HH:mm:ss";
    static DateTimeFormatter formatter;

    public SqlTime() {
        super("time", JDBCType.TIME, null, null);
        type = java.sql.Time.class;
        try {
            minvalue = new Time(dateFormat.parse((String) SqlTypeValue.TIME.minValue).getTime());
            maxvalue = new Time(dateFormat.parse((String) SqlTypeValue.TIME.maxValue).getTime());
        }
        catch (ParseException ex) {
            fail(ex.getMessage());
        }
        this.scale = 7;
        this.variableLengthType = VariableLengthType.ScaleOnly;
        generateScale();
        
        formatter = new DateTimeFormatterBuilder().appendPattern(basePattern).appendFraction(ChronoField.NANO_OF_SECOND, 0, this.scale, true)
                .toFormatter();
        formatter = formatter.withResolverStyle(ResolverStyle.STRICT);

    }

    public Object createdata() {
        Time temp = new Time(ThreadLocalRandom.current().nextLong(((Time) minvalue).getTime(), ((Time) maxvalue).getTime()));
        Random rnd = new Random();
        // this will generate a number that is this.scale number of digits.
        int randomNumeric;
        if (this.scale <= 0) {
            randomNumeric = 0;
        } else if (this.scale == 1) {
            randomNumeric = rnd.nextInt(10);
        } else {
            randomNumeric = (int) Math.pow(10, this.scale - 1) + rnd.nextInt((int) (9 * Math.pow(10, this.scale - 1)));
        }
        
        String timeNano = temp.toString() + "." + randomNumeric;
        return timeNano;

        // can pass String rather than converting to loacTime, but leaving it
        // unchanged for now to handle prepared statements
        /*
         * converting string '20:53:44.9' to LocalTime results in 20:53:44.900, this extra scale causes failure
         */
//        return LocalTime.parse(timeNano, formatter);
    }
}