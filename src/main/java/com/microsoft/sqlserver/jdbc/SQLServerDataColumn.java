//---------------------------------------------------------------------------------------------------------------------------------
// File: SQLServerDataColumn.java
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
 
 
package com.microsoft.sqlserver.jdbc;

import java.util.*;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Map.Entry;

/**
 * This class represents a column of the in-memory data table represented by SQLServerDataTable. 
 */
public final class SQLServerDataColumn
{
	String columnName;
	int javaSqlType;
	int precision = 0;
	int scale = 0;
	
	/**
	 * Initializes a new instance of SQLServerDataColumn with the column name and type.
	 * @param columnName the name of the column
	 * @param sqlType the type of the column
	 */
	public SQLServerDataColumn(String columnName, int sqlType)
	{
		this.columnName = columnName;
		this.javaSqlType = sqlType;
	}
		
	/**
	 * Retrieves the column name.
	 * @return the name of the column.
	 */
	public String getColumnName()
	{
		return columnName;
	}
	
	/**
	 * Retrieves the column type.
	 * @return the column type.
	 */
	public int getColumnType()
	{
		return javaSqlType;
	}	
}