//---------------------------------------------------------------------------------------------------------------------------------
// File: ISQLServerPreparedStatement.java
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

import java.sql.SQLException;

public interface ISQLServerPreparedStatement extends java.sql.PreparedStatement, ISQLServerStatement
{
	/**
	 * Sets the designated parameter to the given <code>microsoft.sql.DateTimeOffset</code> value.
	 * 
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if parameterIndex does not correspond to a parameter
     * marker in the SQL statement; if a database access error occurs or
     * this method is called on a closed <code>PreparedStatement</code>
	 */
    void setDateTimeOffset(int parameterIndex, microsoft.sql.DateTimeOffset x)  throws SQLException;
}

