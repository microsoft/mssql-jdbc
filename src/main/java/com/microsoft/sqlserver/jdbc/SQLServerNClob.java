/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.UnsupportedEncodingException;
import java.sql.NClob;

/**
 * SQLServerNClob represents a National Character Set LOB object and implements java.sql.NClob.
 */

public final class SQLServerNClob extends SQLServerClobBase implements NClob {

	private static final long serialVersionUID = 1L;

	SQLServerNClob(SQLServerConnection connection) {
        super(connection, "", connection.getDatabaseCollation(), null);
    }

    SQLServerNClob(BaseInputStream stream,
            TypeInfo typeInfo) throws SQLServerException, UnsupportedEncodingException {
        super(null, stream, typeInfo.getSQLCollation(), typeInfo);
    }

    final JDBCType getJdbcType() {
        return JDBCType.NCLOB;
    }
}
