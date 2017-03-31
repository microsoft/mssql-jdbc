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

public class SqlBit extends SqlType {

    public SqlBit() {
        super("bit", JDBCType.BIT, 1, 0, SqlTypeValue.BIT.minValue, SqlTypeValue.BIT.maxValue, SqlTypeValue.BIT.nullValue, VariableLengthType.Fixed,
                Boolean.class);
    }

    public Object createdata() {
        return ((0 == ThreadLocalRandom.current().nextInt(2)) ? minvalue : maxvalue);
    }
}