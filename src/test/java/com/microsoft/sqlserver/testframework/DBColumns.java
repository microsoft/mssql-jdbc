//---------------------------------------------------------------------------------------------------------------------------------
// File: DBColumns.java
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

import com.microsoft.sqlserver.testframework.sqlType.SqlType;
import com.microsoft.sqlserver.testframework.util.RandomUtil;

/**
 * Container for all the columns
 */
class DBColumns {
	List<DBColumn> columns;
	int totalColumns = 0;
	
	/**
	 * called if autoGenerateSchema = true
	 * @param schema
	 */
	DBColumns(DBSchema schema) {
		addColumns(schema);
	}
	
	/**
	 * called if autoGenerateSchema = false 
	 */
	DBColumns(){
		columns = new ArrayList<DBColumn>();
	}
	
	/**
	 * adds a columns for each SQL type in DBSchema
	 * @param schema
	 */
	private void addColumns(DBSchema schema)
	{
		totalColumns = schema.getNumberOfSqlTypes();
		columns = new ArrayList<DBColumn>(totalColumns);
		
		for(int i=0;i<totalColumns;i++)
		{
			SqlType sqlType = schema.getSqlType(i);
			DBColumn column =new DBColumn(RandomUtil.getIdentifier(sqlType.getName()), sqlType); 
			columns.add(column);
		}
	}
	
	/**
	 * 
	 * @return total number of columns
	 */
	int totalColumns(){
		return totalColumns;
	}
	
	/**
	 * 
	 * @param index
	 * @return DBColumn
	 */
	DBColumn getColumn(int index)
	{
		return columns.get(index);
	}

	/**
	 * adds new columns based on the SqlType passed
	 * @param sqlType
	 */
	void addColumn(SqlType sqlType) {
		DBColumn column =new DBColumn(RandomUtil.getIdentifier(sqlType.getName()), sqlType); 
		columns.add(column);
	}
	
}
