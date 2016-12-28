// ---------------------------------------------------------------------------------------------------------------------------------
// File: PrepUtil.java
//
//
// Microsoft JDBC Driver for SQL Server
// Copyright(c) Microsoft Corporation
// All rights reserved.
// MIT License
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files(the "Software"),
// to deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense,
// and / or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions :
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.
// ---------------------------------------------------------------------------------------------------------------------------------

package com.microsoft.sqlserver.testframework;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;

/**
 * Utility Class for Tests. This will contains methods like Create Table, Drop
 * Table, Initialize connection, create statement etc. logger settings etc.
 */
public class PrepUtil {

    private PrepUtil() {
        // Just hide to restrict constructor invocation.
    }

    /**
     * It will create {@link SQLServerConnection} TODO : Think of AE
     * functionality on off etc.
     * 
     * @param connectionString
     * @param info
     * @return {@link SQLServerConnection}
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static SQLServerConnection getConnection(String connectionString, Properties info)
            throws SQLException, ClassNotFoundException {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        return (SQLServerConnection) DriverManager.getConnection(connectionString, info);
    }

    /**
     * It will create {@link SQLServerConnection}
     * 
     * @param connectionString
     * @return {@link SQLServerConnection}
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static SQLServerConnection getConnection(String connectionString)
            throws SQLException, ClassNotFoundException {
        return getConnection(connectionString, null);
    }

}
