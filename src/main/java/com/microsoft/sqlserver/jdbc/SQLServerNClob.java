//---------------------------------------------------------------------------------------------------------------------------------
// File: SQLServerNClob.java
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
import java.io.*;
import java.util.logging.*;

/**
* SQLServerNClob represents a National Character Set LOB object and implements java.sql.NClob.
*/

public final class SQLServerNClob extends SQLServerClobBase implements NClob
{
    // Loggers should be class static to avoid lock contention with multiple threads
    private static final Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerNClob");

    SQLServerNClob(SQLServerConnection connection)
    {
        super(connection, "", connection.getDatabaseCollation(), logger);
    }

    SQLServerNClob(BaseInputStream stream, TypeInfo typeInfo) throws SQLServerException, UnsupportedEncodingException
    {
        super(null, new String(stream.getBytes(), typeInfo.getCharset()), typeInfo.getSQLCollation(), logger);
    }

    final JDBCType getJdbcType() { return JDBCType.NCLOB; }
}
