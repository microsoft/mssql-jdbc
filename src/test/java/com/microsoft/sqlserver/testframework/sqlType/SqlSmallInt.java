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

public class SqlSmallInt extends SqlNumber {

    public SqlSmallInt() {
        super("smallint", JDBCType.SMALLINT, 5, 0, SqlTypeValue.SMALLINT.minValue, SqlTypeValue.SMALLINT.maxValue, SqlTypeValue.SMALLINT.nullValue,
                VariableLengthType.Fixed, Short.class);

    }

    public Object createdata() {
        // TODO: include max value
        return (short) ThreadLocalRandom.current().nextInt(Short.MIN_VALUE, Short.MAX_VALUE);
    }
}