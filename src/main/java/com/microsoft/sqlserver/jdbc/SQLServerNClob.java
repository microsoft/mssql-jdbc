/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.InputStream;
import java.sql.NClob;
import java.sql.SQLException;
import java.util.logging.Logger;


/**
 * Represents a National Character Set LOB object and implements java.sql.NClob.
 */
public final class SQLServerNClob extends SQLServerClobBase implements NClob {

    /**
     * Always refresh SerialVersionUID when prompted
     */
    private static final long serialVersionUID = 3593610902551842327L;

    // Loggers should be class static to avoid lock contention with multiple
    // threads
    private static final Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerNClob");

    SQLServerNClob(SQLServerConnection connection) {
        super(connection, "", connection.getDatabaseCollation(), logger, null);
        this.setDefaultCharset(java.nio.charset.StandardCharsets.UTF_16LE);
    }

    SQLServerNClob(BaseInputStream stream, TypeInfo typeInfo) {
        super(null, stream, typeInfo.getSQLCollation(), logger, typeInfo);
        this.setDefaultCharset(java.nio.charset.StandardCharsets.UTF_16LE);
    }

    @Override
    public InputStream getAsciiStream() throws SQLException {
        // NClobs are mapped to Nvarchar(max), and are always UTF-16 encoded. This API expects a US_ASCII stream.
        // It's not possible to modify the stream without loading it into memory. Users should use getCharacterStream.
        this.fillFromStream();
        return super.getAsciiStream();
    }

    @Override
    final JDBCType getJdbcType() {
        return JDBCType.NCLOB;
    }
}
