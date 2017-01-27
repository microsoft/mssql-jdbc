/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework.sqlType;

public class SqlSmallMoney extends SqlDecimal {

    public SqlSmallMoney() {
        super("smallmoney", 10, 4, SqlTypeValue.SMALLMONEY.minValue, SqlTypeValue.SMALLMONEY.maxValue, VariableLengthType.Fixed);
    }
}