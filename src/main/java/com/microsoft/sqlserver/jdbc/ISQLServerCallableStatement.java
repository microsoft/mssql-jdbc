//---------------------------------------------------------------------------------------------------------------------------------
// File: ISQLServerCallableStatement.java
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

import java.sql.*;

/**
 * 
 * This interface is implemented by SQLServerCallableStatement Class.
 *
 */
public interface ISQLServerCallableStatement extends java.sql.CallableStatement, ISQLServerPreparedStatement
{
	/**
	 * Sets parameter parameterName to DateTimeOffset x
	 * @param parameterName the name of the parameter
	 * @param x DateTimeOffset value
	 * @throws SQLException if parameterName does not correspond to a named parameter; 
	 * if the driver can detect that a data conversion error could occur; 
	 * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
	 */
    public void setDateTimeOffset(String parameterName, microsoft.sql.DateTimeOffset x)  throws SQLException;
	
	/**
     * Gets the DateTimeOffset value of parameter with index parameterIndex
     * @param parameterIndex the first parameter is 1, the second is 2, and so on
     * @return DateTimeOffset value
     * @throws SQLException if parameterIndex is out of range; 
	 * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     */
    public microsoft.sql.DateTimeOffset getDateTimeOffset(int parameterIndex) throws SQLException;
	
	/**
     * Gets the DateTimeOffset value of parameter with name parameterName
     * @param parameterName the name of the parameter
     * @return DateTimeOffset value
     * @throws SQLException if parameterName does not correspond to a named parameter; 
	 * if a database access error occurs or
     * this method is called on a closed <code>CallableStatement</code>
     */
    public microsoft.sql.DateTimeOffset getDateTimeOffset(String parameterName) throws SQLException;
}

