/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework.sqlType;

import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Date;
import java.sql.JDBCType;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.ThreadLocalRandom;

public class SqlDate extends SqlDateTime {

    static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

    public SqlDate() {
        super("date", JDBCType.DATE, null, null);
        type = java.sql.Date.class;
        try {
            minvalue = new Date(dateFormat.parse((String) SqlTypeValue.DATE.minValue).getTime());
            maxvalue = new Date(dateFormat.parse((String) SqlTypeValue.DATE.maxValue).getTime());
        }
        catch (ParseException ex) {
            fail(ex.getMessage());
        }
    }

    public Object createdata() {
        return new Date(ThreadLocalRandom.current().nextLong(((Date) minvalue).getTime(), ((Date) maxvalue).getTime()));
    }
}