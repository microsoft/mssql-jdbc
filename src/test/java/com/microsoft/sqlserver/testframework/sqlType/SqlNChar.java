/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework.sqlType;

import java.sql.JDBCType;
import java.util.concurrent.ThreadLocalRandom;

public class SqlNChar extends SqlChar {
    private static String normalCharSet = "1234567890-=!@#$%^&*()_+qwertyuiop[]\\asdfghjkl;zxcvbnm,./QWERTYUIOP{}|ASDFGHJKL:\"ZXCVBNM<>?";

    SqlNChar(String name,
            JDBCType jdbctype,
            int precision) {
        super(name, jdbctype, precision);
    }

    public SqlNChar() {
        this("nchar", JDBCType.NCHAR, 1000);
    }

    public Object createdata() {
        int dataLength = ThreadLocalRandom.current().nextInt(precision);
        return generateCharTypes(dataLength);
    }

    private static String generateCharTypes(int columnLength) {
        String charSet = normalCharSet;
        return buildCharOrNChar(columnLength, charSet);
    }
}