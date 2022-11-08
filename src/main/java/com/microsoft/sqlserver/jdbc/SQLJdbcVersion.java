/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

final class SQLJdbcVersion {
    static final int major = 9;
    static final int minor = 2;
    static final int patch = 0;
    static final int build = 0;
    /*
     * Used to load mssql-jdbc_auth DLL.
     * 1. Set to "-preview" for preview release.
     * 2. Set to "" (empty String) for official release.
     */
    static final String releaseExt = "";
}
