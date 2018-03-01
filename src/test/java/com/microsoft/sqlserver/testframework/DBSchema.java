/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
 * Collection of SqlType used to create table in {@link DBTable}
 * 
 * @author Microsoft
 *
 */
public class DBSchema {

    private List<SqlType> sqlTypes;

    /**
     * 
     * @param autoGenerateSchema
     */
    DBSchema(boolean autoGenerateSchema) {
        sqlTypes = new ArrayList<>();
        if (autoGenerateSchema) {
            // Exact Numeric
            sqlTypes.add(new SqlBigInt());
            sqlTypes.add(new SqlInt());
            sqlTypes.add(new SqlSmallInt());
            sqlTypes.add(new SqlTinyInt());
            sqlTypes.add(new SqlBit());
            sqlTypes.add(new SqlDecimal());
            sqlTypes.add(new SqlNumeric());
            sqlTypes.add(new SqlMoney());
            sqlTypes.add(new SqlSmallMoney());
            // Appx Numeric
            sqlTypes.add(new SqlFloat());
            sqlTypes.add(new SqlReal());
            // Character
            sqlTypes.add(new SqlChar());
            sqlTypes.add(new SqlVarChar());
            // Unicode
            sqlTypes.add(new SqlNChar());
            sqlTypes.add(new SqlNVarChar());
            // Temporal
            sqlTypes.add(new SqlDateTime());
            sqlTypes.add(new SqlDate());
            sqlTypes.add(new SqlTime());
            sqlTypes.add(new SqlSmallDateTime());
            sqlTypes.add(new SqlDateTime2());
            sqlTypes.add(new SqlDateTimeOffset());
            // Binary
            sqlTypes.add(new SqlBinary());
            sqlTypes.add(new SqlVarBinary());

            // TODO:
            // Other types
        }
    }

    /**
     * 
     * @param autoGenerateSchema
     * @param alternateSchema
     */
    DBSchema(boolean autoGenerateSchema,
            boolean alternateSchema) {
        this(autoGenerateSchema);
        if (alternateSchema) {
            Collections.shuffle(this.sqlTypes);
        }
    }

    /**
     * 
     * @param index
     * @return
     */
    SqlType getSqlType(int index) {
        return sqlTypes.get(index);
    }

    /**
     * 
     * @param sqlType
     */
    void addSqlTpe(SqlType sqlType) {
        sqlTypes.add(sqlType);
    }

    /**
     * 
     * @return number of sqlTypes in the schema object
     */
    int getNumberOfSqlTypes() {
        return sqlTypes.size();
    }
}
