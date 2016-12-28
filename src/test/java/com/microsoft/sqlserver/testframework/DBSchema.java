//---------------------------------------------------------------------------------------------------------------------------------
// File: DBSchema.java
//
//
// Microsoft JDBC Driver for SQL Server
// Copyright(c) Microsoft Corporation
// All rights reserved.
// MIT License
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), 
//  to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
//  and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS 
//  IN THE SOFTWARE.
//---------------------------------------------------------------------------------------------------------------------------------
 

package com.microsoft.sqlserver.testframework;
import java.util.ArrayList;
import java.util.List;

import com.microsoft.sqlserver.testframework.sqlType.SqlBigInt;
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
import com.microsoft.sqlserver.testframework.sqlType.SqlVarChar;

/**
 * Collection of SqlType used to create table in {@link DBTable}
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
		sqlTypes = new ArrayList<SqlType>();
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
			// TODO:
			// Binary
			// Other types
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
	void addSqlTpe(SqlType sqlType){
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
