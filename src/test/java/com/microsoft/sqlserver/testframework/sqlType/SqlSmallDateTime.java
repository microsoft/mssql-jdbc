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
import java.util.concurrent.ThreadLocalRandom;

public class SqlSmallDateTime extends SqlDateTime {
    static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd'T'HH:mm:ss");

    public SqlSmallDateTime() {
        super("smalldatetime", JDBCType.TIMESTAMP, null, null);
        try {
            minvalue = new Timestamp(dateFormat.parse((String) SqlTypeValue.SMALLDATETIME.minValue).getTime());
            maxvalue = new Timestamp(dateFormat.parse((String) SqlTypeValue.SMALLDATETIME.maxValue).getTime());
        }
        catch (ParseException ex) {
            fail(ex.getMessage());
        }
    }

    public Object createdata() {
        Timestamp smallDateTime = new Timestamp(
                ThreadLocalRandom.current().nextLong(((Timestamp) minvalue).getTime(), ((Timestamp) maxvalue).getTime()));
        // remove the random nanosecond value if any
        smallDateTime.setNanos(0);
        return smallDateTime.toString().substring(0,19);// ignore the nano second portion
//        return smallDateTime;
    }
}