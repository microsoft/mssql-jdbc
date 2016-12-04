//---------------------------------------------------------------------------------------------------------------------------------
// File: SQLServerPreparedStatement42.java
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
 * This class is separated from SQLServerPreparedStatement class in order to resolve compiling error of
 * missing Java 8 Types when running with Java 7.
 * 
 * This class will be initialized instead of SQLServerPreparedStatement when Java 8 and JDBC 4.2 are used.
 * 
 * It shares the same PreparedStatement implementation with SQLServerCallableStatement42.
 * 
 */
public class SQLServerPreparedStatement42 extends SQLServerPreparedStatement implements ISQLServerPreparedStatement42{

	SQLServerPreparedStatement42(SQLServerConnection conn, String sql, int nRSType, int nRSConcur,
			SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLServerException {
		super(conn, sql, nRSType, nRSConcur, stmtColEncSetting);
	}

	public final void setObject(int index, Object obj, SQLType jdbcType) throws SQLServerException
	{		
		SQLServerPreparedStatement42Helper.setObject(this, index, obj, jdbcType);
	}

	public final void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLServerException
	{
		SQLServerPreparedStatement42Helper.setObject(this, parameterIndex, x, targetSqlType, scaleOrLength);
	}

	public final void setObject(int parameterIndex, Object x, SQLType targetSqlType, Integer precision, Integer scale) throws SQLServerException
	{
		SQLServerPreparedStatement42Helper.setObject(this, parameterIndex, x, targetSqlType, precision, scale);
	}

	public final void setObject(int parameterIndex, Object x, SQLType targetSqlType, Integer precision, Integer scale, boolean forceEncrypt) throws SQLServerException
	{
		SQLServerPreparedStatement42Helper.setObject(this, parameterIndex, x, targetSqlType, precision, scale, forceEncrypt);
	}
}
