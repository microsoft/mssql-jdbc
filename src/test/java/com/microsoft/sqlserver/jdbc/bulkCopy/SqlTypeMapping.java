/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import com.microsoft.sqlserver.testframework.sqlType.SqlBigInt;
import com.microsoft.sqlserver.testframework.sqlType.SqlBinary;
import com.microsoft.sqlserver.testframework.sqlType.SqlBit;
import com.microsoft.sqlserver.testframework.sqlType.SqlChar;
import com.microsoft.sqlserver.testframework.sqlType.SqlDate;
import com.microsoft.sqlserver.testframework.sqlType.SqlDateTime;
import com.microsoft.sqlserver.testframework.sqlType.SqlDateTime2;
import com.microsoft.sqlserver.testframework.sqlType.SqlDateTimeOffset;
import com.microsoft.sqlserver.testframework.sqlType.SqlDecimal;
import com.microsoft.sqlserver.testframework.sqlType.SqlFloat;
import com.microsoft.sqlserver.testframework.sqlType.SqlInt;
import com.microsoft.sqlserver.testframework.sqlType.SqlMoney;
import com.microsoft.sqlserver.testframework.sqlType.SqlNChar;
import com.microsoft.sqlserver.testframework.sqlType.SqlNVarChar;
import com.microsoft.sqlserver.testframework.sqlType.SqlNumeric;
import com.microsoft.sqlserver.testframework.sqlType.SqlReal;
import com.microsoft.sqlserver.testframework.sqlType.SqlSmallDateTime;
import com.microsoft.sqlserver.testframework.sqlType.SqlSmallInt;
import com.microsoft.sqlserver.testframework.sqlType.SqlSmallMoney;
import com.microsoft.sqlserver.testframework.sqlType.SqlTime;
import com.microsoft.sqlserver.testframework.sqlType.SqlTinyInt;
import com.microsoft.sqlserver.testframework.sqlType.SqlType;
import com.microsoft.sqlserver.testframework.sqlType.SqlVarBinary;
import com.microsoft.sqlserver.testframework.sqlType.SqlVarChar;

/**
 * enum that returns object of desired datatype based for JDBCType type
 */
public enum SqlTypeMapping {
    
    BIGINT          (new SqlBigInt()),
    INT             (new SqlInt()),
    SMALLINT        (new SqlSmallInt()),
    TINYINT         (new SqlTinyInt()),
    BIT             (new SqlBit()),
    DECIMAL         (new SqlDecimal()),
    NUMERIC         (new SqlNumeric()),
    MONEY           (new SqlMoney()),
    SMALLMONEY      (new SqlSmallMoney()),
    // Appx Numeric
    FLOAT           (new SqlFloat()),
    REAL            (new SqlReal()),
    // Character
    CHAR            (new SqlChar()),
    VARCHAR         (new SqlVarChar()),
    // Unicode
    NCHAR           (new SqlNChar()),
    NVARCHAR        (new SqlNVarChar()),
    // Temporal
    DATETIME        (new SqlDateTime()),
    DATE            (new SqlDate()),
    TIME            (new SqlTime()),
    SMALLDATETIME   (new SqlSmallDateTime()),
    DATETIME2       (new SqlDateTime2()),
    DATETIMEOFFSET  (new SqlDateTimeOffset()),
    //Binary
    BINARY          (new SqlBinary()),
    VARBINARY       (new SqlVarBinary()),
    ;

    public SqlType sqlType;

    /**
     * 
     * @param type
     */
    SqlTypeMapping(SqlType type) {
        this.sqlType = type;
    }
}
