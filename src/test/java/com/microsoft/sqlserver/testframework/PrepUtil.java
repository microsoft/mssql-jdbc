/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) 2016 Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.testframework;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;

/**
 * Utility Class for Tests. This will contains methods like Create Table, Drop Table, Initialize connection, create statement etc. logger settings
 * etc.
 * 
 * TODO : We can delete PrepUtil & move getConnection method in {@link DBEngine}
 * 
 * @since 6.1.2
 */
public class PrepUtil {

    private PrepUtil() {
        // Just hide to restrict constructor invocation.
    }

    /**
     * It will create {@link SQLServerConnection} TODO : Think of AE functionality on off etc.
     * 
     * @param connectionString
     * @param info
     * @return {@link SQLServerConnection}
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static SQLServerConnection getConnection(String connectionString,
            Properties info) throws SQLException, ClassNotFoundException {
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
    public static SQLServerConnection getConnection(String connectionString) throws SQLException, ClassNotFoundException {
        return getConnection(connectionString, null);
    }

}
