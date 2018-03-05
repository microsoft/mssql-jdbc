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

public class SqlReal extends SqlFloat {

    public SqlReal() {
        super("real", JDBCType.REAL, 24, SqlTypeValue.REAL.minValue, SqlTypeValue.REAL.maxValue, SqlTypeValue.REAL.nullValue,
                VariableLengthType.Fixed, Float.class);
    }

    @Override
    public Object createdata() {
        return (float) ThreadLocalRandom.current().nextDouble((Float) minvalue, (Float) maxvalue);
    }
}