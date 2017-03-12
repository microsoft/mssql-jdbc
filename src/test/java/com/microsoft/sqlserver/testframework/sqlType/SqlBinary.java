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

/**
 * Contains name, jdbctype, precision, scale for binary data type
 */
public class SqlBinary extends SqlType {

    /**
     * set JDBCType and precision for SqlBinary
     */
    public SqlBinary() {
        this("binary", JDBCType.BINARY, 2000);
    }

    /**
     * 
     * @param name
     *            binary or varbinary
     * @param jdbctype
     * @param precision
     */
    SqlBinary(String name,
            JDBCType jdbctype,
            int precision) {
        super(name, jdbctype, precision, 0, SqlTypeValue.BINARY.minValue, SqlTypeValue.BINARY.maxValue, SqlTypeValue.BINARY.nullValue,
                VariableLengthType.Precision, byte[].class);
        flags.set(FIXED);
        generatePrecision();
    }

    /**
     * create random data for binary and varbinary column
     */
    public Object createdata() {
        int dataLength = ThreadLocalRandom.current().nextInt(precision);
        byte[] bytes = new byte[dataLength];
        ThreadLocalRandom.current().nextBytes(bytes);
        return bytes;
    }
}