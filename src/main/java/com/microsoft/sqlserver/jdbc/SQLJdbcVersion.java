/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

final class SQLJdbcVersion {
    static final int MAJOR = 13;
    static final int MINOR = 1;
    static final int PATCH = 1;
    static final int BUILD = 0;
    /*
     * Used to load mssql-jdbc_auth DLL.
     * 1. Set to "-preview" for preview release.
     * 2. Set to "" (empty String) for official release.
     */
    static final String RELEASE_EXT = "-preview";

    private SQLJdbcVersion() {
        throw new UnsupportedOperationException(SQLServerException.getErrString("R_notSupported"));
    }
}
