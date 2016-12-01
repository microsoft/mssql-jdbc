//---------------------------------------------------------------------------------------------------------------------------------
// File: SQLServerJdbc41.java
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

/**
 * Shims for JDBC 4.1 JAR.
 *
 * JDBC 4.1 public methods should always check the SQLServerJdbcVersion first to make
 * sure that they are not operable in any earlier driver version.  That is, they should throw
 * an exception, be a no-op, or whatever.
 */

final class DriverJDBCVersion
{
    static final int major = 4;
    static final int minor = 1;

    static final void checkSupportsJDBC4() {}
    
    static final void checkSupportsJDBC41() {}

    static final void checkSupportsJDBC42() 
    {
        throw new UnsupportedOperationException(SQLServerException.getErrString("R_notSupported"));			
    }

	// Stub for the new overloaded method in BatchUpdateException in JDBC 4.2
	static final void throwBatchUpdateException(SQLServerException lastError, long[] updateCounts)
	{
        throw new UnsupportedOperationException(SQLServerException.getErrString("R_notSupported"));		
	}
}
