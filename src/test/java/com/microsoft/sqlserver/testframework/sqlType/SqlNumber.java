/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework.sqlType;

import java.sql.JDBCType;

public abstract class SqlNumber extends SqlType {
    SqlNumber(String name,
            JDBCType jdbctype,
            int precision,
            int scale,
            Object min,
            Object max,
            Object nullvalue,
            VariableLengthType variableLengthType,
            Class type) {
        super(name, jdbctype, precision, scale, min, max, nullvalue, VariableLengthType.Fixed, type);
    }
}