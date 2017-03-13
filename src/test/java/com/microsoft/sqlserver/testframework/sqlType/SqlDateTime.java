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

public class SqlDateTime extends SqlType {

    static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd'T'HH:mm:ss.SSS");

    public SqlDateTime() {
        this("datetime", JDBCType.TIMESTAMP, null, null);
        try {
            minvalue = new Timestamp(dateFormat.parse((String) SqlTypeValue.DATETIME.minValue).getTime());
            maxvalue = new Timestamp(dateFormat.parse((String) SqlTypeValue.DATETIME.maxValue).getTime());
        }
        catch (ParseException ex) {
            fail(ex.getMessage());
        }
    }

    SqlDateTime(String name,
            JDBCType jdbctype,
            Object min,
            Object max) {
        super(name, jdbctype, 0, 0, min, max, SqlTypeValue.DATETIME.nullValue, VariableLengthType.Fixed, java.sql.Timestamp.class);
    }

    public Object createdata() {
        return new Timestamp(ThreadLocalRandom.current().nextLong(((Timestamp) minvalue).getTime(), ((Timestamp) maxvalue).getTime()));
    }
}