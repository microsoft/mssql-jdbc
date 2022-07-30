/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.Properties;

import com.microsoft.sqlserver.jdbc.SQLServerConnection.SqlFedAuthInfo;

public interface SqlFedAuthTokenProvider {
    SqlFedAuthToken getSqlFedAuthToken(SqlFedAuthInfo fedAuthInfo, Properties activeConnectionProperties) throws SQLServerException;

    boolean supportAuthenticationType(String authenticationType);

    /**
     * should this provider instance be cached and reuse?
     */
    default boolean singleton(){
        return true;
    }
}
