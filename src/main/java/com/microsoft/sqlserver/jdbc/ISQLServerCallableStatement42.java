//---------------------------------------------------------------------------------------------------------------------------------
// File: ISQLServerCallableStatement42.java
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

import java.sql.SQLType;

/**
 * This interface requires all the CallableStatement methods including those are specific to JDBC 4.2
 *
 */
public interface ISQLServerCallableStatement42 extends ISQLServerCallableStatement, ISQLServerPreparedStatement42{

	public void registerOutParameter(int index, SQLType sqlType) throws SQLServerException;

	public void registerOutParameter (int index, SQLType sqlType, String typeName) throws SQLServerException;

	public void registerOutParameter(int index, SQLType sqlType, int scale) throws SQLServerException;

	public void registerOutParameter(int index, SQLType sqlType, int precision, int scale) throws SQLServerException;

	public void setObject(String sCol, Object obj, SQLType jdbcType) throws SQLServerException;

	public void setObject(String sCol, Object obj, SQLType jdbcType, int scale) throws SQLServerException;

	public void setObject(String sCol, Object obj, SQLType jdbcType, int scale, boolean forceEncrypt) throws SQLServerException;

	public void registerOutParameter(String parameterName, SQLType sqlType, String typeName) throws SQLServerException;

	public void registerOutParameter(String parameterName, SQLType sqlType, int scale) throws SQLServerException;

	public void registerOutParameter(String parameterName, SQLType sqlType, int precision, int scale) throws SQLServerException;

	public void registerOutParameter(String parameterName, SQLType sqlType) throws SQLServerException;
}
