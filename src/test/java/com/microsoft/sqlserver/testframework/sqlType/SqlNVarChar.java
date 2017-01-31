/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework.sqlType;

import java.sql.JDBCType;

public class SqlNVarChar extends SqlNChar {

    public SqlNVarChar() {
        super("nvarchar", JDBCType.NVARCHAR, 2000);
    }
}