/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.sql.Clob;
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
    }

    SQLServerNClob(BaseInputStream stream, TypeInfo typeInfo) throws SQLServerException, UnsupportedEncodingException {
        super(null, stream, typeInfo.getSQLCollation(), logger, typeInfo);
    }

    @Override
    public void free() throws SQLException {
        super.free();
    }

    @Override
    public InputStream getAsciiStream() throws SQLException {
        return super.getAsciiStream();
    }

    @Override
    public Reader getCharacterStream() throws SQLException {
        return super.getCharacterStream();
    }

    @Override
    public Reader getCharacterStream(long pos, long length) throws SQLException {
        return super.getCharacterStream(pos, length);
    }

    @Override
    public String getSubString(long pos, int length) throws SQLException {
        return super.getSubString(pos, length);
    }

    @Override
    public long length() throws SQLException {
        return super.length();
    }

    @Override
    void fillFromStream() throws SQLException {
        super.fillFromStream();
    }

    @Override
    public long position(Clob searchstr, long start) throws SQLException {
        return super.position(searchstr, start);
    }

    @Override
    public long position(String searchstr, long start) throws SQLException {
        return super.position(searchstr, start);
    }

    @Override
    public void truncate(long len) throws SQLException {
        super.truncate(len);
    }

    @Override
    public OutputStream setAsciiStream(long pos) throws SQLException {
        return super.setAsciiStream(pos);
    }

    @Override
    public Writer setCharacterStream(long pos) throws SQLException {
        return super.setCharacterStream(pos);
    }

    @Override
    public int setString(long pos, String s) throws SQLException {
        return super.setString(pos, s);
    }

    @Override
    public int setString(long pos, String str, int offset, int len) throws SQLException {
        return super.setString(pos, str, offset, len);
    }

    @Override
    final JDBCType getJdbcType() {
        return JDBCType.NCLOB;
    }
}
