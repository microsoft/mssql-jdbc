/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework.sqlType;

import static org.junit.jupiter.api.Assertions.fail;

import java.sql.JDBCType;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;


public class SqlDateTime extends SqlType {

    static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd'T'HH:mm:ss.SSS");

    public SqlDateTime() {
        this("datetime", JDBCType.TIMESTAMP, null, null);
        try {
            minvalue = new Timestamp(dateFormat.parse((String) SqlTypeValue.DATETIME.minValue).getTime());
            maxvalue = new Timestamp(dateFormat.parse((String) SqlTypeValue.DATETIME.maxValue).getTime());
        } catch (ParseException ex) {
            fail(ex.getMessage());
        }
    }

    SqlDateTime(String name, JDBCType jdbctype, Object min, Object max) {
        super(name, jdbctype, 0, 0, min, max, SqlTypeValue.DATETIME.nullValue, VariableLengthType.Fixed,
                java.sql.Timestamp.class);
    }

    public Object createdata() {
        return new Timestamp(ThreadLocalRandom.current().nextLong(((Timestamp) minvalue).getTime(),
                ((Timestamp) maxvalue).getTime()));
    }

    protected int generateRandomInt(int length) {
        Random rnd = new Random();
        int power = (int) Math.pow(10, length - 1);

        // Example of how this works:
        // if length is 3, then we add 100 + random number between 0~899, so that we get a random number between
        // 100~999.
        int randomNumeric;
        if (length <= 0) {
            randomNumeric = 0;
        } else if (length == 1) {
            randomNumeric = rnd.nextInt(10);
        } else {
            randomNumeric = power + rnd.nextInt((int) (9 * power));
        }

        return randomNumeric;
    }
}
