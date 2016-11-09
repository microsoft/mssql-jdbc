//---------------------------------------------------------------------------------------------------------------------------------
// File: ISQLServerResultSet.java
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

import java.sql.*;

public interface ISQLServerResultSet extends java.sql.ResultSet
{
    
    public static final int TYPE_SS_DIRECT_FORWARD_ONLY        = 2003; // TYPE_FORWARD_ONLY + 1000
    public static final int TYPE_SS_SERVER_CURSOR_FORWARD_ONLY = 2004; // TYPE_FORWARD_ONLY + 1001
    public static final int TYPE_SS_SCROLL_STATIC              = 1004; // TYPE_SCROLL_INSENSITIVE
    public static final int TYPE_SS_SCROLL_KEYSET              = 1005; // TYPE_SCROLL_SENSITIVE
    public static final int TYPE_SS_SCROLL_DYNAMIC             = 1006; // TYPE_SCROLL_SENSITIVE + 1

    /* SQL Server concurrency values */
    public static final int CONCUR_SS_OPTIMISTIC_CC    = 1008; // CONCUR_UPDATABLE
    public static final int CONCUR_SS_SCROLL_LOCKS     = 1009; // CONCUR_UPDATABLE + 1
    public static final int CONCUR_SS_OPTIMISTIC_CCVAL = 1010; // CONCUR_UPDATABLE + 2

    public microsoft.sql.DateTimeOffset getDateTimeOffset(int columnIndex) throws SQLException;
    public microsoft.sql.DateTimeOffset getDateTimeOffset(String columnName) throws SQLException;
    public void updateDateTimeOffset(int index, microsoft.sql.DateTimeOffset x) throws SQLException;
    public void updateDateTimeOffset(String columnName, microsoft.sql.DateTimeOffset x) throws SQLException;
    
}
