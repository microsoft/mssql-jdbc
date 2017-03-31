/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework.sqlType;

public class SqlMoney extends SqlDecimal {

    public SqlMoney() {
        super("money", 19, 4, SqlTypeValue.MONEY.minValue, SqlTypeValue.MONEY.maxValue, VariableLengthType.Fixed);
    }
}