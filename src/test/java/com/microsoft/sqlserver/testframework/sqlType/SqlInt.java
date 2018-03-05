/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework.sqlType;

import java.sql.JDBCType;
import java.util.concurrent.ThreadLocalRandom;;

public class SqlInt extends SqlNumber {

    public SqlInt() {
        super("int", JDBCType.INTEGER, 10, 0, SqlTypeValue.INTEGER.minValue, SqlTypeValue.INTEGER.maxValue, SqlTypeValue.INTEGER.nullValue,
                VariableLengthType.Fixed, Integer.class);

    }

    public Object createdata() {
        // TODO: include max value
        return ThreadLocalRandom.current().nextInt(Integer.MIN_VALUE, Integer.MAX_VALUE);
    }
}