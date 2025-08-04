/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework.sqlType;

public class SqlJson extends SqlType {

    public SqlJson() {
        super("json", microsoft.sql.Types.JSON, 0, 0, SqlTypeValue.JSON.minValue, SqlTypeValue.JSON.maxValue,
                SqlTypeValue.JSON.nullValue, VariableLengthType.Fixed, String.class);
    }

    @Override
    public Object createdata() {
        return "{}";
    }
}