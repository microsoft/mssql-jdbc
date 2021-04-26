/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Provides an implementation of the result set metadata to the SQL Server. A ResultSetMetaData object can be used to
 * obtain the meta data (types and type properties) of the columns in a ResultSet.
 *
 * The API javadoc for JDBC API methods that this class implements are not repeated here. Please see Sun's JDBC API
 * interfaces javadoc for those details.
 */
public final class SQLServerResultSetMetaData implements ISQLServerResultSetMetaData {
    /**
     * Always update serialVersionUID when prompted.
     */
    private static final long serialVersionUID = -5747558730471411712L;

    /** connection */
    private SQLServerConnection con;

    /** resultset */
    private final SQLServerResultSet rs;
    static final private java.util.logging.Logger logger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerResultSetMetaData");

    /**
     * Unique id generator for each instance (used for logging).
     */
    static private final AtomicInteger baseID = new AtomicInteger(0);

    /** trace ID */
    final private String traceID;

    // Returns unique id for each instance.
    private static int nextInstanceID() {
        return baseID.incrementAndGet();
    }

    final public String toString() {
        return traceID;
    }

    /**
     * Constructs a SQLServerResultSetMetaData meta data object for the result set.
     * 
     * @param con
     *        the connection
     * @param rs
     *        the parent result set
     */
    SQLServerResultSetMetaData(SQLServerConnection con, SQLServerResultSet rs) {
        traceID = " SQLServerResultSetMetaData:" + nextInstanceID();
        this.con = con;
        this.rs = rs;
        assert rs != null;
        if (logger.isLoggable(java.util.logging.Level.FINE)) {
            logger.fine(toString() + " created by (" + rs.toString() + ")");
        }
    }

    /* ------------------ JDBC API Methods --------------------- */

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        boolean f = iface.isInstance(this);
        return f;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        T t;
        try {
            t = iface.cast(this);
        } catch (ClassCastException e) {
            throw new SQLServerException(e.getMessage(), e);
        }
        return t;
    }

    @Override
    public String getCatalogName(int column) throws SQLServerException {
        return rs.getColumn(column).getTableName().getDatabaseName();
    }

    @Override
    public int getColumnCount() throws SQLServerException {
        return rs.getColumnCount();
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLServerException {
        CryptoMetadata cryptoMetadata = rs.getColumn(column).getCryptoMetadata();
        if (null != cryptoMetadata) {
            return cryptoMetadata.getBaseTypeInfo().getDisplaySize();
        }

        return rs.getColumn(column).getTypeInfo().getDisplaySize();
    }

    @Override
    public String getColumnLabel(int column) throws SQLServerException {
        return rs.getColumn(column).getColumnName();
    }

    @Override
    public String getColumnName(int column) throws SQLServerException {
        return rs.getColumn(column).getColumnName();
    }

    @Override
    public int getColumnType(int column) throws SQLServerException {
        // under Katmai map the max types to non max to be inline with DBMD.
        TypeInfo typeInfo = rs.getColumn(column).getTypeInfo();

        CryptoMetadata cryptoMetadata = rs.getColumn(column).getCryptoMetadata();
        if (null != cryptoMetadata) {
            typeInfo = cryptoMetadata.getBaseTypeInfo();
        }

        JDBCType jdbcType = typeInfo.getSSType().getJDBCType();
        SSType sqlType = typeInfo.getSSType();
        // in bulkcopy for instance, we need to return the real jdbc type which is sql variant and not the default Char
        // one.
        if (SSType.SQL_VARIANT == sqlType) {
            jdbcType = JDBCType.SQL_VARIANT;
        }
        if (SSType.UDT == sqlType) {
            if (typeInfo.getSSTypeName().equalsIgnoreCase(SSType.GEOMETRY.name())) {
                jdbcType = JDBCType.GEOMETRY;
            }
            if (typeInfo.getSSTypeName().equalsIgnoreCase(SSType.GEOGRAPHY.name())) {
                jdbcType = JDBCType.GEOGRAPHY;
            }
        }
        int r = jdbcType.asJavaSqlType();
        if (con.isKatmaiOrLater()) {
            switch (sqlType) {
                case VARCHARMAX:
                    r = SSType.VARCHAR.getJDBCType().asJavaSqlType();
                    break;
                case NVARCHARMAX:
                    r = SSType.NVARCHAR.getJDBCType().asJavaSqlType();
                    break;
                case VARBINARYMAX:
                    r = SSType.VARBINARY.getJDBCType().asJavaSqlType();
                    break;
                case DATETIME:
                case SMALLDATETIME:
                    r = SSType.DATETIME2.getJDBCType().asJavaSqlType();
                    break;
                case MONEY:
                case SMALLMONEY:
                    r = SSType.DECIMAL.getJDBCType().asJavaSqlType();
                    break;
                case GUID:
                    r = SSType.CHAR.getJDBCType().asJavaSqlType();
                    break;
                default:
                    // Do nothing
                    break;
            }
        }
        return r;
    }

    @Override
    public String getColumnTypeName(int column) throws SQLServerException {
        CryptoMetadata cryptoMetadata = rs.getColumn(column).getCryptoMetadata();
        if (null != cryptoMetadata) {
            return cryptoMetadata.getBaseTypeInfo().getSSTypeName();
        }

        return rs.getColumn(column).getTypeInfo().getSSTypeName();
    }

    @Override
    public int getPrecision(int column) throws SQLServerException {
        CryptoMetadata cryptoMetadata = rs.getColumn(column).getCryptoMetadata();
        if (null != cryptoMetadata) {
            return cryptoMetadata.getBaseTypeInfo().getPrecision();
        }

        return rs.getColumn(column).getTypeInfo().getPrecision();
    }

    @Override
    public int getScale(int column) throws SQLServerException {
        CryptoMetadata cryptoMetadata = rs.getColumn(column).getCryptoMetadata();
        if (null != cryptoMetadata) {
            return cryptoMetadata.getBaseTypeInfo().getScale();
        }

        return rs.getColumn(column).getTypeInfo().getScale();
    }

    @Override
    public String getSchemaName(int column) throws SQLServerException {
        return rs.getColumn(column).getTableName().getSchemaName();
    }

    @Override
    public String getTableName(int column) throws SQLServerException {
        return rs.getColumn(column).getTableName().getObjectName();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLServerException {
        CryptoMetadata cryptoMetadata = rs.getColumn(column).getCryptoMetadata();
        if (null != cryptoMetadata) {
            return cryptoMetadata.getBaseTypeInfo().isIdentity();
        }

        return rs.getColumn(column).getTypeInfo().isIdentity();
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLServerException {
        CryptoMetadata cryptoMetadata = rs.getColumn(column).getCryptoMetadata();
        if (null != cryptoMetadata) {
            return cryptoMetadata.getBaseTypeInfo().isCaseSensitive();
        }

        return rs.getColumn(column).getTypeInfo().isCaseSensitive();
    }

    @Override
    public boolean isCurrency(int column) throws SQLServerException {
        SSType ssType = rs.getColumn(column).getTypeInfo().getSSType();

        CryptoMetadata cryptoMetadata = rs.getColumn(column).getCryptoMetadata();
        if (null != cryptoMetadata) {
            ssType = cryptoMetadata.getBaseTypeInfo().getSSType();
        }

        return SSType.MONEY == ssType || SSType.SMALLMONEY == ssType;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLServerException {
        CryptoMetadata cryptoMetadata = rs.getColumn(column).getCryptoMetadata();
        if (null != cryptoMetadata) {
            return TypeInfo.UPDATABLE_READ_WRITE == cryptoMetadata.getBaseTypeInfo().getUpdatability();
        }

        return TypeInfo.UPDATABLE_READ_WRITE == rs.getColumn(column).getTypeInfo().getUpdatability();
    }

    @Override
    public int isNullable(int column) throws SQLServerException {
        CryptoMetadata cryptoMetadata = rs.getColumn(column).getCryptoMetadata();
        if (null != cryptoMetadata) {
            return cryptoMetadata.getBaseTypeInfo().isNullable() ? columnNullable : columnNoNulls;
        }

        return rs.getColumn(column).getTypeInfo().isNullable() ? columnNullable : columnNoNulls;
    }

    @Override
    public boolean isReadOnly(int column) throws SQLServerException {
        CryptoMetadata cryptoMetadata = rs.getColumn(column).getCryptoMetadata();
        if (null != cryptoMetadata) {
            return TypeInfo.UPDATABLE_READ_ONLY == cryptoMetadata.getBaseTypeInfo().getUpdatability();
        }

        return TypeInfo.UPDATABLE_READ_ONLY == rs.getColumn(column).getTypeInfo().getUpdatability();
    }

    @Override
    public boolean isSearchable(int column) throws SQLServerException {
        SSType ssType = null;
        CryptoMetadata cryptoMetadata = rs.getColumn(column).getCryptoMetadata();

        if (null != cryptoMetadata) {
            ssType = cryptoMetadata.getBaseTypeInfo().getSSType();
        } else {
            ssType = rs.getColumn(column).getTypeInfo().getSSType();
        }

        switch (ssType) {
            case IMAGE:
            case TEXT:
            case NTEXT:
            case UDT:
            case XML:
                return false;

            default:
                return true;
        }
    }

    @Override
    public boolean isSigned(int column) throws SQLServerException {
        CryptoMetadata cryptoMetadata = rs.getColumn(column).getCryptoMetadata();
        if (null != cryptoMetadata) {
            return cryptoMetadata.getBaseTypeInfo().getSSType().getJDBCType().isSigned();
        }

        return rs.getColumn(column).getTypeInfo().getSSType().getJDBCType().isSigned();
    }

    @Override
    public boolean isSparseColumnSet(int column) throws SQLServerException {
        CryptoMetadata cryptoMetadata = rs.getColumn(column).getCryptoMetadata();
        if (null != cryptoMetadata) {
            return cryptoMetadata.getBaseTypeInfo().isSparseColumnSet();
        }

        return rs.getColumn(column).getTypeInfo().isSparseColumnSet();
    }

    @Override
    public boolean isWritable(int column) throws SQLServerException {
        int updatability = -1;
        CryptoMetadata cryptoMetadata = rs.getColumn(column).getCryptoMetadata();
        if (null != cryptoMetadata) {
            updatability = cryptoMetadata.getBaseTypeInfo().getUpdatability();
        } else {
            updatability = rs.getColumn(column).getTypeInfo().getUpdatability();
        }
        return TypeInfo.UPDATABLE_READ_WRITE == updatability || TypeInfo.UPDATABLE_UNKNOWN == updatability;
    }

    @Override
    public String getColumnClassName(int column) throws SQLServerException {
        CryptoMetadata cryptoMetadata = rs.getColumn(column).getCryptoMetadata();
        if (null != cryptoMetadata) {
            return cryptoMetadata.getBaseTypeInfo().getSSType().getJDBCType().className();
        }

        return rs.getColumn(column).getTypeInfo().getSSType().getJDBCType().className();
    }
}
