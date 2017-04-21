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

public class SqlFloat extends SqlType {

    // called from real
    SqlFloat(String name,
            JDBCType jdbctype,
            int precision,
            Object min,
            Object max,
            Object nullvalue,
            VariableLengthType variableLengthType,
            Class type) {
        super(name, jdbctype, precision, 0, min, max, nullvalue, variableLengthType, type);
        generatePrecision();
    }

    public SqlFloat() {
        super("float", JDBCType.DOUBLE, 53, 0, SqlTypeValue.FLOAT.minValue, SqlTypeValue.FLOAT.maxValue, SqlTypeValue.FLOAT.nullValue,
                VariableLengthType.Precision, Double.class);
        generatePrecision();
    }

    public Object createdata() {
        // for float in SQL Server, any precision <=24 is considered as real so the value must be within SqlTypeValue.REAL.minValue/maxValue
        if (precision > 24)
            return Double.longBitsToDouble(ThreadLocalRandom.current().nextLong(((Double) minvalue).longValue(), ((Double) maxvalue).longValue()));
        else {
            return ThreadLocalRandom.current().nextDouble((Float) SqlTypeValue.REAL.minValue, (Float) SqlTypeValue.REAL.maxValue);
        }
    }
}