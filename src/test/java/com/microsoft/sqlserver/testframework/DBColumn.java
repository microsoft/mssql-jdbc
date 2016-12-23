//---------------------------------------------------------------------------------------------------------------------------------
// File: DBColumn.java
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

/**
 * This class holds data for Column.
 * Think about encrypted columns. <B>createCMK code should not add here.</B> 
 */
public class DBColumn {
	
	/*
	 *  TODO: add nullable, defaultValue, alwaysEncrypted
	 */
	private String columnName;
	private SqlType sqlType;
	private List<Object> columnValues;
	
	public DBColumn(String columnName, SqlType sqlType)
	{
		this.columnName = columnName;
		this.sqlType = sqlType;
	}
	
	/**
	 * @return the columnName
	 */
	String getColumnName() {
		return columnName;
	}

	/**
	 * @param columnName the columnName to set
	 */
	void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	/**
	 * 
	 * @return SqlType for the column
	 */
	SqlType getSqlType() {
		return sqlType;
	}

	/**
	 * 
	 * @param sqlType
	 */
	void setSqlType(SqlType sqlType) {
		this.sqlType = sqlType;
	}
	
	/**
	 * generate value for the column
	 * @param rows number of rows
	 */
	void populateValues(int rows)
	{
		columnValues = new ArrayList<Object>();
		for(int i=0;i<rows;i++)
			columnValues.add(sqlType.createdata());
	}
	
	/**
	 * 
	 * @param row
	 * @return the value populated for the column
	 */
	Object getRowValue(int row)
	{
		// handle exceptions
		return columnValues.get(row);
	}
	
}
