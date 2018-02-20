/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLPermission;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.text.MessageFormat;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * SQLServerConnectionPoolProxy is a wrapper around SQLServerConnection object. When returning a connection object from PooledConnection.getConnection
 * we return this proxy per SPEC.
 * <p>
 * This class's public functions need to be kept identical to the SQLServerConnection's.
 * <p>
 * The API javadoc for JDBC API methods that this class implements are not repeated here. Please see Sun's JDBC API interfaces javadoc for those
 * details.
 */

class SQLServerConnectionPoolProxy implements ISQLServerConnection {
    private SQLServerConnection wrappedConnection;
    private boolean bIsOpen;
    static private final AtomicInteger baseConnectionID = new AtomicInteger(0);       // connection id dispenser
    final private String traceID;

    // Permission targets
    // currently only callAbort is implemented
    private static final String callAbortPerm = "callAbort";

    /**
     * Generate the next unique connection id.
     * 
     * @return the next conn id
     */
    /* L0 */ private static int nextConnectionID() {
        return baseConnectionID.incrementAndGet();
    }

    public String toString() {
        return traceID;
    }

    /* L0 */ SQLServerConnectionPoolProxy(SQLServerConnection con) {
        traceID = " ProxyConnectionID:" + nextConnectionID();
        wrappedConnection = con;
        // the Proxy is created with an open conn
        con.setAssociatedProxy(this);
        bIsOpen = true;
    }

    /* L0 */ void checkClosed() throws SQLServerException {
        if (!bIsOpen) {
            SQLServerException.makeFromDriverError(null, null, SQLServerException.getErrString("R_connectionIsClosed"), null, false);
        }
    }

    /* L0 */ public Statement createStatement() throws SQLServerException {
        checkClosed();
        return wrappedConnection.createStatement();
    }

    /* L0 */ public PreparedStatement prepareStatement(String sql) throws SQLServerException {
        checkClosed();
        return wrappedConnection.prepareStatement(sql);
    }

    /* L0 */ public CallableStatement prepareCall(String sql) throws SQLServerException {
        checkClosed();
        return wrappedConnection.prepareCall(sql);
    }

    /* L0 */ public String nativeSQL(String sql) throws SQLServerException {
        checkClosed();
        return wrappedConnection.nativeSQL(sql);
    }

    public void setAutoCommit(boolean newAutoCommitMode) throws SQLServerException {
        checkClosed();
        wrappedConnection.setAutoCommit(newAutoCommitMode);
    }

    /* L0 */ public boolean getAutoCommit() throws SQLServerException {
        checkClosed();
        return wrappedConnection.getAutoCommit();
    }

    public void commit() throws SQLServerException {
        checkClosed();
        wrappedConnection.commit();
    }

    /**
     * Rollback a transaction.
     *
     * @throws SQLServerException
     *             if no transaction exists or if the connection is in auto-commit mode.
     */
    public void rollback() throws SQLServerException {
        checkClosed();
        wrappedConnection.rollback();
    }

    public void abort(Executor executor) throws SQLException {
        if (!bIsOpen || (null == wrappedConnection))
            return;

        if (null == executor) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidArgument"));
            Object[] msgArgs = {"executor"};
            SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), null, false);
        }

        // check for callAbort permission
        SecurityManager secMgr = System.getSecurityManager();
        if (secMgr != null) {
            try {
                SQLPermission perm = new SQLPermission(callAbortPerm);
                secMgr.checkPermission(perm);
            }
            catch (SecurityException ex) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_permissionDenied"));
                Object[] msgArgs = {callAbortPerm};
                throw new SQLServerException(form.format(msgArgs), null, 0, ex);
            }
        }

        bIsOpen = false;

        executor.execute(new Runnable() {
            public void run() {
                if (wrappedConnection.getConnectionLogger().isLoggable(Level.FINER))
                    wrappedConnection.getConnectionLogger().finer(toString() + " Connection proxy aborted ");
                try {
                    wrappedConnection.poolCloseEventNotify();
                    wrappedConnection = null;
                }
                catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    /* L0 */ public void close() throws SQLServerException {
        if (bIsOpen && (null != wrappedConnection)) {
            if (wrappedConnection.getConnectionLogger().isLoggable(Level.FINER))
                wrappedConnection.getConnectionLogger().finer(toString() + " Connection proxy closed ");

            wrappedConnection.poolCloseEventNotify();
            wrappedConnection = null;
        }
        bIsOpen = false;
    }

    /* L0 */ void internalClose() {
        bIsOpen = false;
        wrappedConnection = null;
    }

    /* L0 */ public boolean isClosed() throws SQLServerException {
        return !bIsOpen;
    }

    /* L0 */ public DatabaseMetaData getMetaData() throws SQLServerException {
        checkClosed();
        return wrappedConnection.getMetaData();
    }

    /* L0 */ public void setReadOnly(boolean readOnly) throws SQLServerException {
        checkClosed();
        wrappedConnection.setReadOnly(readOnly);
    }

    /* L0 */ public boolean isReadOnly() throws SQLServerException {
        checkClosed();
        return wrappedConnection.isReadOnly();
    }

    /* L0 */ public void setCatalog(String catalog) throws SQLServerException {
        checkClosed();
        wrappedConnection.setCatalog(catalog);
    }

    /* L0 */ public String getCatalog() throws SQLServerException {
        checkClosed();
        return wrappedConnection.getCatalog();
    }

    /* L0 */ public void setTransactionIsolation(int level) throws SQLServerException {
        checkClosed();
        wrappedConnection.setTransactionIsolation(level);
    }

    /* L0 */ public int getTransactionIsolation() throws SQLServerException {
        checkClosed();
        return wrappedConnection.getTransactionIsolation();
    }

    /* L0 */ public SQLWarning getWarnings() throws SQLServerException {
        checkClosed();
        return wrappedConnection.getWarnings(); // Warnings support added
    }

    /* L2 */ public void clearWarnings() throws SQLServerException {
        checkClosed();
        wrappedConnection.clearWarnings();
    }

    // --------------------------JDBC 2.0-----------------------------

    /* L2 */ public Statement createStatement(int resultSetType,
            int resultSetConcurrency) throws SQLException {
        checkClosed();
        return wrappedConnection.createStatement(resultSetType, resultSetConcurrency);
    }

    /* L2 */ public PreparedStatement prepareStatement(String sSql,
            int resultSetType,
            int resultSetConcurrency) throws SQLException {
        checkClosed();
        return wrappedConnection.prepareStatement(sSql, resultSetType, resultSetConcurrency);
    }

    /* L2 */ public CallableStatement prepareCall(String sql,
            int resultSetType,
            int resultSetConcurrency) throws SQLException {
        checkClosed();
        return wrappedConnection.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    /* L2 */ public void setTypeMap(java.util.Map<String, Class<?>> map) throws SQLServerException {
        checkClosed();
        wrappedConnection.setTypeMap(map);
    }

    public java.util.Map<String, Class<?>> getTypeMap() throws SQLServerException {
        checkClosed();
        return wrappedConnection.getTypeMap();
    }

    /* L3 */ public Statement createStatement(int nType,
            int nConcur,
            int nHold) throws SQLServerException {
        checkClosed();
        return wrappedConnection.createStatement(nType, nConcur, nHold);
    }

    /**
     * Creates a <code>Statement</code> object that will generate <code>ResultSet</code> objects with the given type, concurrency, and holdability.
     * This method is the same as the <code>createStatement</code> method above, but it allows the default result set type, concurrency, and
     * holdability to be overridden.
     *
     * @param nType
     *            one of the following <code>ResultSet</code> constants: <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *            <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param nConcur
     *            one of the following <code>ResultSet</code> constants: <code>ResultSet.CONCUR_READ_ONLY</code> or
     *            <code>ResultSet.CONCUR_UPDATABLE</code>
     * @param nHold
     *            one of the following <code>ResultSet</code> constants: <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or
     *            <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
     * @param stmtColEncSetting
     *            Specifies how data will be sent and received when reading and writing encrypted columns.
     * @return a new <code>Statement</code> object that will generate <code>ResultSet</code> objects with the given type, concurrency, and holdability
     * @exception SQLException
     *                if a database access error occurs, this method is called on a closed connection or the given parameters are not
     *                <code>ResultSet</code> constants indicating type, concurrency, and holdability
     * @exception SQLFeatureNotSupportedException
     *                if the JDBC driver does not support this method or this method is not supported for the specified result set type, result set
     *                holdability and result set concurrency.
     */
    public Statement createStatement(int nType,
            int nConcur,
            int nHold,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLServerException {
        checkClosed();
        return wrappedConnection.createStatement(nType, nConcur, nHold, stmtColEncSetting);
    }

    /* L3 */ public PreparedStatement prepareStatement(java.lang.String sql,
            int nType,
            int nConcur,
            int nHold) throws SQLServerException {
        checkClosed();
        return wrappedConnection.prepareStatement(sql, nType, nConcur, nHold);
    }

    /**
     * Creates a <code>PreparedStatement</code> object that will generate <code>ResultSet</code> objects with the given type, concurrency, and
     * holdability.
     * <P>
     * This method is the same as the <code>prepareStatement</code> method above, but it allows the default result set type, concurrency, and
     * holdability to be overridden.
     *
     * @param sql
     *            a <code>String</code> object that is the SQL statement to be sent to the database; may contain one or more '?' IN parameters
     * @param nType
     *            one of the following <code>ResultSet</code> constants: <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *            <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param nConcur
     *            one of the following <code>ResultSet</code> constants: <code>ResultSet.CONCUR_READ_ONLY</code> or
     *            <code>ResultSet.CONCUR_UPDATABLE</code>
     * @param nHold
     *            one of the following <code>ResultSet</code> constants: <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or
     *            <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
     * @param stmtColEncSetting
     *            Specifies how data will be sent and received when reading and writing encrypted columns.
     * @return a new <code>PreparedStatement</code> object, containing the pre-compiled SQL statement, that will generate <code>ResultSet</code>
     *         objects with the given type, concurrency, and holdability
     * @exception SQLException
     *                if a database access error occurs, this method is called on a closed connection or the given parameters are not
     *                <code>ResultSet</code> constants indicating type, concurrency, and holdability
     * @exception SQLFeatureNotSupportedException
     *                if the JDBC driver does not support this method or this method is not supported for the specified result set type, result set
     *                holdability and result set concurrency.
     */
    public PreparedStatement prepareStatement(String sql,
            int nType,
            int nConcur,
            int nHold,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLServerException {
        checkClosed();
        return wrappedConnection.prepareStatement(sql, nType, nConcur, nHold, stmtColEncSetting);
    }

    /* L3 */ public CallableStatement prepareCall(String sql,
            int nType,
            int nConcur,
            int nHold) throws SQLServerException {
        checkClosed();
        return wrappedConnection.prepareCall(sql, nType, nConcur, nHold);
    }

    /**
     * Creates a <code>CallableStatement</code> object that will generate <code>ResultSet</code> objects with the given type and concurrency. This
     * method is the same as the <code>prepareCall</code> method above, but it allows the default result set type, result set concurrency type and
     * holdability to be overridden.
     *
     * @param sql
     *            a <code>String</code> object that is the SQL statement to be sent to the database; may contain on or more '?' parameters
     * @param nType
     *            one of the following <code>ResultSet</code> constants: <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *            <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param nConcur
     *            one of the following <code>ResultSet</code> constants: <code>ResultSet.CONCUR_READ_ONLY</code> or
     *            <code>ResultSet.CONCUR_UPDATABLE</code>
     * @param nHold
     *            one of the following <code>ResultSet</code> constants: <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or
     *            <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
     * @param stmtColEncSetting
     *            Specifies how data will be sent and received when reading and writing encrypted columns.
     * @return a new <code>CallableStatement</code> object, containing the pre-compiled SQL statement, that will generate <code>ResultSet</code>
     *         objects with the given type, concurrency, and holdability
     * @exception SQLException
     *                if a database access error occurs, this method is called on a closed connection or the given parameters are not
     *                <code>ResultSet</code> constants indicating type, concurrency, and holdability
     * @exception SQLFeatureNotSupportedException
     *                if the JDBC driver does not support this method or this method is not supported for the specified result set type, result set
     *                holdability and result set concurrency.
     */
    public CallableStatement prepareCall(String sql,
            int nType,
            int nConcur,
            int nHold,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetiing) throws SQLServerException {
        checkClosed();
        return wrappedConnection.prepareCall(sql, nType, nConcur, nHold, stmtColEncSetiing);
    }

    /* JDBC 3.0 Auto generated keys */

    /* L3 */ public PreparedStatement prepareStatement(String sql,
            int flag) throws SQLServerException {
        checkClosed();
        return wrappedConnection.prepareStatement(sql, flag);
    }

    /**
     * Creates a default <code>PreparedStatement</code> object that has the capability to retrieve auto-generated keys. The given constant tells the
     * driver whether it should make auto-generated keys available for retrieval. This parameter is ignored if the SQL statement is not an
     * <code>INSERT</code> statement, or an SQL statement able to return auto-generated keys (the list of such statements is vendor-specific).
     * <P>
     * <B>Note:</B> This method is optimized for handling parametric SQL statements that benefit from precompilation. If the driver supports
     * precompilation, the method <code>prepareStatement</code> will send the statement to the database for precompilation. Some drivers may not
     * support precompilation. In this case, the statement may not be sent to the database until the <code>PreparedStatement</code> object is
     * executed. This has no direct effect on users; however, it does affect which methods throw certain SQLExceptions.
     * <P>
     * Result sets created using the returned <code>PreparedStatement</code> object will by default be type <code>TYPE_FORWARD_ONLY</code> and have a
     * concurrency level of <code>CONCUR_READ_ONLY</code>. The holdability of the created result sets can be determined by calling
     * {@link #getHoldability}.
     *
     * @param sql
     *            an SQL statement that may contain one or more '?' IN parameter placeholders
     * @param flag
     *            a flag indicating whether auto-generated keys should be returned; one of <code>Statement.RETURN_GENERATED_KEYS</code> or
     *            <code>Statement.NO_GENERATED_KEYS</code>
     * @param stmtColEncSetting
     *            Specifies how data will be sent and received when reading and writing encrypted columns.
     * @return a new <code>PreparedStatement</code> object, containing the pre-compiled SQL statement, that will have the capability of returning
     *         auto-generated keys
     * @exception SQLException
     *                if a database access error occurs, this method is called on a closed connection or the given parameter is not a
     *                <code>Statement</code> constant indicating whether auto-generated keys should be returned
     * @exception SQLFeatureNotSupportedException
     *                if the JDBC driver does not support this method with a constant of Statement.RETURN_GENERATED_KEYS
     */
    public PreparedStatement prepareStatement(String sql,
            int flag,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLServerException {
        checkClosed();
        return wrappedConnection.prepareStatement(sql, flag, stmtColEncSetting);
    }

    /* L3 */ public PreparedStatement prepareStatement(String sql,
            int[] columnIndexes) throws SQLServerException {
        checkClosed();
        return wrappedConnection.prepareStatement(sql, columnIndexes);
    }

    /**
     * Creates a default <code>PreparedStatement</code> object capable of returning the auto-generated keys designated by the given array. This array
     * contains the indexes of the columns in the target table that contain the auto-generated keys that should be made available. The driver will
     * ignore the array if the SQL statement is not an <code>INSERT</code> statement, or an SQL statement able to return auto-generated keys (the list
     * of such statements is vendor-specific).
     * <p>
     * An SQL statement with or without IN parameters can be pre-compiled and stored in a <code>PreparedStatement</code> object. This object can then
     * be used to efficiently execute this statement multiple times.
     * <P>
     * <B>Note:</B> This method is optimized for handling parametric SQL statements that benefit from precompilation. If the driver supports
     * precompilation, the method <code>prepareStatement</code> will send the statement to the database for precompilation. Some drivers may not
     * support precompilation. In this case, the statement may not be sent to the database until the <code>PreparedStatement</code> object is
     * executed. This has no direct effect on users; however, it does affect which methods throw certain SQLExceptions.
     * <P>
     * Result sets created using the returned <code>PreparedStatement</code> object will by default be type <code>TYPE_FORWARD_ONLY</code> and have a
     * concurrency level of <code>CONCUR_READ_ONLY</code>. The holdability of the created result sets can be determined by calling
     * {@link #getHoldability}.
     *
     * @param sql
     *            an SQL statement that may contain one or more '?' IN parameter placeholders
     * @param columnIndexes
     *            an array of column indexes indicating the columns that should be returned from the inserted row or rows
     * @param stmtColEncSetting
     *            Specifies how data will be sent and received when reading and writing encrypted columns.
     * @return a new <code>PreparedStatement</code> object, containing the pre-compiled statement, that is capable of returning the auto-generated
     *         keys designated by the given array of column indexes
     * @exception SQLException
     *                if a database access error occurs or this method is called on a closed connection
     * @exception SQLFeatureNotSupportedException
     *                if the JDBC driver does not support this method
     */
    public PreparedStatement prepareStatement(String sql,
            int[] columnIndexes,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLServerException {
        checkClosed();
        return wrappedConnection.prepareStatement(sql, columnIndexes, stmtColEncSetting);
    }

    /* L3 */ public PreparedStatement prepareStatement(String sql,
            String[] columnNames) throws SQLServerException {
        checkClosed();
        return wrappedConnection.prepareStatement(sql, columnNames);
    }

    /**
     * Creates a default <code>PreparedStatement</code> object capable of returning the auto-generated keys designated by the given array. This array
     * contains the names of the columns in the target table that contain the auto-generated keys that should be returned. The driver will ignore the
     * array if the SQL statement is not an <code>INSERT</code> statement, or an SQL statement able to return auto-generated keys (the list of such
     * statements is vendor-specific).
     * <P>
     * An SQL statement with or without IN parameters can be pre-compiled and stored in a <code>PreparedStatement</code> object. This object can then
     * be used to efficiently execute this statement multiple times.
     * <P>
     * <B>Note:</B> This method is optimized for handling parametric SQL statements that benefit from precompilation. If the driver supports
     * precompilation, the method <code>prepareStatement</code> will send the statement to the database for precompilation. Some drivers may not
     * support precompilation. In this case, the statement may not be sent to the database until the <code>PreparedStatement</code> object is
     * executed. This has no direct effect on users; however, it does affect which methods throw certain SQLExceptions.
     * <P>
     * Result sets created using the returned <code>PreparedStatement</code> object will by default be type <code>TYPE_FORWARD_ONLY</code> and have a
     * concurrency level of <code>CONCUR_READ_ONLY</code>. The holdability of the created result sets can be determined by calling
     * {@link #getHoldability}.
     *
     * @param sql
     *            an SQL statement that may contain one or more '?' IN parameter placeholders
     * @param columnNames
     *            an array of column names indicating the columns that should be returned from the inserted row or rows
     * @param stmtColEncSetting
     *            Specifies how data will be sent and received when reading and writing encrypted columns.
     * @return a new <code>PreparedStatement</code> object, containing the pre-compiled statement, that is capable of returning the auto-generated
     *         keys designated by the given array of column names
     * @exception SQLException
     *                if a database access error occurs or this method is called on a closed connection
     * @exception SQLFeatureNotSupportedException
     *                if the JDBC driver does not support this method
     */
    public PreparedStatement prepareStatement(String sql,
            String[] columnNames,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLServerException {
        checkClosed();
        return wrappedConnection.prepareStatement(sql, columnNames, stmtColEncSetting);
    }

    /* JDBC 3.0 Savepoints */

    /* L3 */ public void releaseSavepoint(Savepoint savepoint) throws SQLServerException {
        checkClosed();
        wrappedConnection.releaseSavepoint(savepoint);
    }

    /* L3 */ public Savepoint setSavepoint(String sName) throws SQLServerException {
        checkClosed();
        return wrappedConnection.setSavepoint(sName);
    }

    /* L3 */ public Savepoint setSavepoint() throws SQLServerException {
        checkClosed();
        return wrappedConnection.setSavepoint();
    }

    /* L3 */ public void rollback(Savepoint s) throws SQLServerException {
        checkClosed();
        wrappedConnection.rollback(s);
    }

    /* L3 */ public int getHoldability() throws SQLServerException {
        checkClosed();
        return wrappedConnection.getHoldability();
    }

    /* L3 */ public void setHoldability(int nNewHold) throws SQLServerException {
        checkClosed();
        wrappedConnection.setHoldability(nNewHold);
    }

    public int getNetworkTimeout() throws SQLException {
        checkClosed();
        return wrappedConnection.getNetworkTimeout();
    }

    public void setNetworkTimeout(Executor executor,
            int timeout) throws SQLException {
        checkClosed();
        wrappedConnection.setNetworkTimeout(executor, timeout);
    }

    public String getSchema() throws SQLException {
        checkClosed();
        return wrappedConnection.getSchema();
    }

    public void setSchema(String schema) throws SQLException {
        checkClosed();
        wrappedConnection.setSchema(schema);
    }

    public java.sql.Array createArrayOf(String typeName,
            Object[] elements) throws SQLException {
        checkClosed();
        return wrappedConnection.createArrayOf(typeName, elements);
    }

    public Blob createBlob() throws SQLException {
        checkClosed();
        return wrappedConnection.createBlob();
    }

    public Clob createClob() throws SQLException {
        checkClosed();
        return wrappedConnection.createClob();
    }

    public NClob createNClob() throws SQLException {
        checkClosed();
        return wrappedConnection.createNClob();
    }

    public SQLXML createSQLXML() throws SQLException {
        checkClosed();
        return wrappedConnection.createSQLXML();
    }

    public Struct createStruct(String typeName,
            Object[] attributes) throws SQLException {
        checkClosed();
        return wrappedConnection.createStruct(typeName, attributes);
    }

    public Properties getClientInfo() throws SQLException {
        checkClosed();
        return wrappedConnection.getClientInfo();
    }

    public String getClientInfo(String name) throws SQLException {
        checkClosed();
        return wrappedConnection.getClientInfo(name);
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        // No checkClosed() call since we can only throw SQLClientInfoException from here
        wrappedConnection.setClientInfo(properties);
    }

    public void setClientInfo(String name,
            String value) throws SQLClientInfoException {
        // No checkClosed() call since we can only throw SQLClientInfoException from here
        wrappedConnection.setClientInfo(name, value);
    }

    public boolean isValid(int timeout) throws SQLException {
        checkClosed();
        return wrappedConnection.isValid(timeout);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        wrappedConnection.getConnectionLogger().entering(toString(), "isWrapperFor", iface);
        boolean f = iface.isInstance(this);
        wrappedConnection.getConnectionLogger().exiting(toString(), "isWrapperFor", f);
        return f;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        wrappedConnection.getConnectionLogger().entering(toString(), "unwrap", iface);
        T t;
        try {
            t = iface.cast(this);
        }
        catch (ClassCastException e) {
            SQLServerException newe = new SQLServerException(e.getMessage(), e);
            throw newe;
        }
        wrappedConnection.getConnectionLogger().exiting(toString(), "unwrap", t);
        return t;
    }

    public UUID getClientConnectionId() throws SQLServerException {
        checkClosed();
        return wrappedConnection.getClientConnectionId();
    }

    /**
     * Modifies the setting of the sendTimeAsDatetime connection property. When true, java.sql.Time values will be sent to the server as SQL
     * Serverdatetime values. When false, java.sql.Time values will be sent to the server as SQL Servertime values. sendTimeAsDatetime can also be
     * modified programmatically with SQLServerDataSource.setSendTimeAsDatetime. The default value for this property may change in a future release.
     * 
     * @param sendTimeAsDateTimeValue
     *            enables/disables setting the sendTimeAsDatetime connection property. For more information about how the Microsoft JDBC Driver for
     *            SQL Server configures java.sql.Time values before sending them to the server, see
     *            <a href="https://msdn.microsoft.com/en-us/library/ff427224(v=sql.110).aspx" > Configuring How java.sql.Time Values are Sent to the
     *            Server.
     */
    public synchronized void setSendTimeAsDatetime(boolean sendTimeAsDateTimeValue) throws SQLServerException {
        checkClosed();
        wrappedConnection.setSendTimeAsDatetime(sendTimeAsDateTimeValue);
    }

    /**
     * Returns the setting of the sendTimeAsDatetime connection property.
     * 
     * @return if enabled, returns true. Otherwise, false.
     * @throws SQLServerException
     *             when an error occurs.
     */
    public synchronized final boolean getSendTimeAsDatetime() throws SQLServerException {
        checkClosed();
        return wrappedConnection.getSendTimeAsDatetime();
    }
}
