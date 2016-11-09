//---------------------------------------------------------------------------------------------------------------------------------
// File: SQLServerMetaData.java
//
//
// Microsoft JDBC Driver for SQL Server
// Copyright(c) Microsoft Corporation
// All rights reserved.
// MIT License
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the ""Software""), 
//  to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
//  and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS 
//  IN THE SOFTWARE.
//---------------------------------------------------------------------------------------------------------------------------------
 
 
package com.microsoft.sqlserver.jdbc;

import java.text.MessageFormat;

public class SQLServerMetaData {
	
	String columnName = null;
	int javaSqlType;
	int precision = 0;
	int scale = 0;
	boolean useServerDefault = false;
	boolean isUniqueKey = false;
	SQLServerSortOrder sortOrder = SQLServerSortOrder.Unspecified;
	int sortOrdinal;
	
	static final int defaultSortOrdinal = -1;
	
	public SQLServerMetaData(
			String columnName, 
			int sqlType)
	{
		this.columnName = columnName;
		this.javaSqlType = sqlType;
	}	
	
	public SQLServerMetaData(
			String columnName, 
			int sqlType, 
			int precision, 
			int scale)
	{
		this.columnName = columnName;
		this.javaSqlType = sqlType;
		this.precision = precision;
		this.scale = scale; 
	}	
		
	public SQLServerMetaData(
			String columnName, 
			int sqlType, 
			int precision, 
			int scale, 
			boolean useServerDefault, 
			boolean isUniqueKey, 
			SQLServerSortOrder sortOrder,
			int sortOrdinal) throws SQLServerException
	{
		this.columnName = columnName;
		this.javaSqlType = sqlType;
		this.precision = precision;
		this.scale = scale; 
		this.useServerDefault = useServerDefault; 
		this.isUniqueKey = isUniqueKey; 
		this.sortOrder = sortOrder; 
		this.sortOrdinal = sortOrdinal; 
		validateSortOrder();
	}	
	
	public SQLServerMetaData(SQLServerMetaData sqlServerMetaData)
	{
		this.columnName = sqlServerMetaData.columnName;
		this.javaSqlType = sqlServerMetaData.javaSqlType;
		this.precision = sqlServerMetaData.precision;
		this.scale = sqlServerMetaData.scale; 
		this.useServerDefault = sqlServerMetaData.useServerDefault; 
		this.isUniqueKey = sqlServerMetaData.isUniqueKey; 
		this.sortOrder = sqlServerMetaData.sortOrder; 
		this.sortOrdinal = sqlServerMetaData.sortOrdinal; 
	}		
	
	public String getColumName()
	{
		return columnName;
	}
	
	public int getSqlType()
	{
		return javaSqlType;
	}
	
	public int getPrecision()
	{
		return precision;
	}
	
	public int getScale()
	{
		return scale;
	}
	
	public boolean useServerDefault()
	{
		return useServerDefault;
	}

	public boolean isUniqueKey()
	{
		return isUniqueKey;
	}
	
	public SQLServerSortOrder getSortOrder()
	{
		return sortOrder;
	}
	
	public int getSortOrdinal()
	{
		return sortOrdinal;
	}

	void validateSortOrder() throws SQLServerException
	{
		// should specify both sort order and ordinal, or neither
		if ( (SQLServerSortOrder.Unspecified == sortOrder) != ( defaultSortOrdinal == sortOrdinal)) 
        {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_TVPMissingSortOrderOrOrdinal"));
	        throw new SQLServerException(form.format(new Object[] {sortOrder , sortOrdinal}), null, 0, null);
        }
	}
}

