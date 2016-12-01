//---------------------------------------------------------------------------------------------------------------------------------
// File: SQLServerResultSet42.java
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

import java.sql.SQLType;

/**
 * 
 * This class is separated from SQLServerResultSet class in order to resolve compiling error of
 * missing Java 8 Types when running with Java 7.
 * 
 * This class will be initialized instead of SQLServerResultSet when Java 8 and JDBC 4.2 are used.
 * 
 */
public class SQLServerResultSet42 extends SQLServerResultSet implements ISQLServerResultSet42{

	/**
	 * Makes a new result set
	 * @param stmtIn the generating statement
	 * @throws SQLServerException when an error occurs
	 */
	public SQLServerResultSet42(SQLServerStatement stmtIn) throws SQLServerException {
		super(stmtIn);
	}

	public void updateObject(int index, Object obj, SQLType targetSqlType) throws SQLServerException
	{
		DriverJDBCVersion.checkSupportsJDBC42();

		if(loggerExternal.isLoggable(java.util.logging.Level.FINER))
			loggerExternal.entering(getClassNameLogging(),  "updateObject",  new Object[]{index, obj, targetSqlType});

		checkClosed();
		// getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
		updateObject(index, obj, (Integer) null, JDBCType.of(targetSqlType.getVendorTypeNumber()), null, false);

		loggerExternal.exiting(getClassNameLogging(), "updateObject");
	}

	public void updateObject(int index, Object obj, SQLType targetSqlType, int scale) throws SQLServerException
	{
		DriverJDBCVersion.checkSupportsJDBC42();

		if(loggerExternal.isLoggable(java.util.logging.Level.FINER))
			loggerExternal.entering(getClassNameLogging(),  "updateObject",  new Object[]{index, obj, targetSqlType, scale});

		checkClosed();
		// getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
		updateObject(index, obj, Integer.valueOf(scale), JDBCType.of(targetSqlType.getVendorTypeNumber()), null, false);

		loggerExternal.exiting(getClassNameLogging(), "updateObject");
	}

	public void updateObject(int index, Object obj, SQLType targetSqlType, int scale, boolean forceEncrypt) throws SQLServerException
	{
		DriverJDBCVersion.checkSupportsJDBC42();

		if(loggerExternal.isLoggable(java.util.logging.Level.FINER))
			loggerExternal.entering(getClassNameLogging(),  "updateObject",  new Object[]{index, obj, targetSqlType, scale, forceEncrypt});

		checkClosed();
		// getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
		updateObject(index, obj, Integer.valueOf(scale), JDBCType.of(targetSqlType.getVendorTypeNumber()), null, forceEncrypt);

		loggerExternal.exiting(getClassNameLogging(), "updateObject");
	}

	public void updateObject(String columnName, Object obj, SQLType targetSqlType, int scale) throws SQLServerException
	{
		DriverJDBCVersion.checkSupportsJDBC42();

		if(loggerExternal.isLoggable(java.util.logging.Level.FINER))
			loggerExternal.entering(getClassNameLogging(),  "updateObject",  new Object[]{columnName, obj, targetSqlType, scale});

		checkClosed();

		// getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
		updateObject(
				findColumn(columnName),
				obj,
				Integer.valueOf(scale),
				JDBCType.of(targetSqlType.getVendorTypeNumber()), 
				null,
				false);

		loggerExternal.exiting(getClassNameLogging(), "updateObject");
	}

	public void updateObject(String columnName, Object obj, SQLType targetSqlType, int scale, boolean forceEncrypt) throws SQLServerException
	{
		DriverJDBCVersion.checkSupportsJDBC42();

		if(loggerExternal.isLoggable(java.util.logging.Level.FINER))
			loggerExternal.entering(getClassNameLogging(),  "updateObject",  new Object[]{columnName, obj, targetSqlType, scale, forceEncrypt});

		checkClosed();

		// getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
		updateObject(
				findColumn(columnName),
				obj,
				Integer.valueOf(scale),
				JDBCType.of(targetSqlType.getVendorTypeNumber()), 
				null,
				forceEncrypt);

		loggerExternal.exiting(getClassNameLogging(), "updateObject");
	}

	public void updateObject(String columnName, Object obj, SQLType targetSqlType) throws SQLServerException
	{
		DriverJDBCVersion.checkSupportsJDBC42();

		if(loggerExternal.isLoggable(java.util.logging.Level.FINER))
			loggerExternal.entering(getClassNameLogging(),  "updateObject",  new Object[]{columnName, obj, targetSqlType});

		checkClosed();

		// getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
		updateObject(
				findColumn(columnName),
				obj,
				(Integer)null,
				JDBCType.of(targetSqlType.getVendorTypeNumber()), 
				null,
				false);

		loggerExternal.exiting(getClassNameLogging(), "updateObject");
	}

}
