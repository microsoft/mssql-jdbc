/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) 2016 Microsoft Corporation All rights reserved. This program is
 * made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.testframework;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerDriverStringProperty;


/**
 * Utility Class for Tests. This will contains methods like Create Table, Drop Table, Initialize connection, create
 * statement etc. logger settings etc.
 * 
 * TODO : We can delete PrepUtil & move getConnection method in {@link DBEngine}
 * 
 * @since 6.1.2
 */
public class PrepUtil {

    private static final String ACCESS_TOKEN_ENV_VAR = "ACCESS_TOKEN";

    private PrepUtil() {
        // Just hide to restrict constructor invocation.
    }

    /**
     * It will create {@link SQLServerConnection}. If the ACCESS_TOKEN environment variable is set,
     * it will be used for authentication.
     * 
     * @param connectionString
     * @param info
     * @return {@link SQLServerConnection}
     * @throws SQLException
     */
    public static SQLServerConnection getConnection(String connectionString, Properties info) throws SQLException {
        Properties connectionProps = info != null ? new Properties(info) : new Properties();
        
        String accessToken = System.getenv(ACCESS_TOKEN_ENV_VAR);
        if (accessToken != null && !accessToken.isEmpty()) {
            connectionProps.setProperty(SQLServerDriverStringProperty.ACCESS_TOKEN.toString(), accessToken);
        }
        
        return (SQLServerConnection) DriverManager.getConnection(connectionString, connectionProps);
    }

    /**
     * It will create {@link SQLServerConnection}. If the ACCESS_TOKEN environment variable is set,
     * it will be used for authentication.
     * 
     * @param connectionString
     * @return {@link SQLServerConnection}
     * @throws SQLException
     */
    public static SQLServerConnection getConnection(String connectionString) throws SQLException {
        return getConnection(connectionString, null);
    }

}
