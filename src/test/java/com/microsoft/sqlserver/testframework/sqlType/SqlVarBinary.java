/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework.sqlType;

import java.sql.JDBCType;

/**
 * Contains name, jdbctype, precision, scale for varbinary data type
 */
public class SqlVarBinary extends SqlBinary {

    /**
     * set JDBCType and precision for SqlVarBinary
     */
    public SqlVarBinary() {
        super("varbinary", JDBCType.VARBINARY, 4000);
    }
}
