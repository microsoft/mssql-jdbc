//---------------------------------------------------------------------------------------------------------------------------------
// File: SQLServerPreparedStatement42Helper.java
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
 * This class provides the underlying implementation of ISQLServerPreparedStatement42 interface to 
 * SQLServerPreparedStatement42 and SQLServerCallableStatement42, so that SQLServerPreparedStatement42 
 * and SQLServerCallableStatement42 have the same implementation for same methods.
 *
 */
class SQLServerPreparedStatement42Helper {
	
	static final void setObject(SQLServerPreparedStatement ps, int index, Object obj, SQLType jdbcType) throws SQLServerException
	{
		DriverJDBCVersion.checkSupportsJDBC42();

		if(SQLServerStatement.loggerExternal.isLoggable(java.util.logging.Level.FINER)) 
			SQLServerStatement.loggerExternal.entering(ps.getClassNameLogging(),  "setObject", new Object[]{index, obj, jdbcType});

		// getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
		ps.setObject(index, obj, jdbcType.getVendorTypeNumber().intValue());

		SQLServerStatement.loggerExternal.exiting(ps.getClassNameLogging(),  "setObject");
	}
	
	static final void setObject(SQLServerPreparedStatement ps, int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLServerException
	{
		DriverJDBCVersion.checkSupportsJDBC42();    

		if(SQLServerStatement.loggerExternal.isLoggable(java.util.logging.Level.FINER)) 
			SQLServerStatement.loggerExternal.entering(ps.getClassNameLogging(),  "setObject", new Object[]{parameterIndex, x, targetSqlType, scaleOrLength}); 

		// getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
		ps.setObject(parameterIndex, x, targetSqlType.getVendorTypeNumber().intValue(), scaleOrLength);

		SQLServerStatement.loggerExternal.exiting(ps.getClassNameLogging(),  "setObject");
	}

	static final void setObject(SQLServerPreparedStatement ps, int parameterIndex, Object x, SQLType targetSqlType, Integer precision, Integer scale) throws SQLServerException
	{
		DriverJDBCVersion.checkSupportsJDBC42();    

		if(SQLServerStatement.loggerExternal.isLoggable(java.util.logging.Level.FINER)) 
			SQLServerStatement.loggerExternal.entering(ps.getClassNameLogging(),  "setObject", new Object[]{parameterIndex, x, targetSqlType, precision, scale}); 

		// getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
		ps.setObject(parameterIndex, x, targetSqlType.getVendorTypeNumber().intValue(), precision, scale, false);

		SQLServerStatement.loggerExternal.exiting(ps.getClassNameLogging(),  "setObject");
	}

	static final void setObject(SQLServerPreparedStatement ps, int parameterIndex, Object x, SQLType targetSqlType, Integer precision, Integer scale, boolean forceEncrypt) throws SQLServerException
	{
		DriverJDBCVersion.checkSupportsJDBC42();    

		if(SQLServerStatement.loggerExternal.isLoggable(java.util.logging.Level.FINER)) 
			SQLServerStatement.loggerExternal.entering(ps.getClassNameLogging(),  "setObject", new Object[]{parameterIndex, x, targetSqlType, precision, scale, forceEncrypt}); 

		// getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
		ps.setObject(parameterIndex, x, targetSqlType.getVendorTypeNumber().intValue(), precision, scale, forceEncrypt);

		SQLServerStatement.loggerExternal.exiting(ps.getClassNameLogging(),  "setObject");
	}
}
