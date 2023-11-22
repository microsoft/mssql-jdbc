/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Provides a wrapper around SQLServerConnection object. When returning a connection object from
 * PooledConnection.getConnection we return this proxy per SPEC.
 * <p>
 * This class's public functions need to be kept identical to the SQLServerConnection's.
 * <p>
 * The API javadoc for JDBC API methods that this class implements are not repeated here. Please see Sun's JDBC API
 * interfaces javadoc for those details.
 */
class SQLServerConnectionPoolProxy implements ISQLServerConnection, java.io.Serializable {
    /**
     * Always refresh SerialVersionUID when prompted
     */
    private static final long serialVersionUID = 5752599482349578127L;

    private SQLServerConnection wrappedConnection;
    private boolean bIsOpen;
    static private final AtomicInteger baseConnectionID = new AtomicInteger(0); // connection
                                                                                // id
                                                                                // dispenser
    final private String traceID;

    /**
     * Permission targets currently only callAbort is implemented
     */
    private static final String CALL_ABORT_PERM = "callAbort";

    /**
     * Generates the next unique connection id.
     * 
     * @return the next conn id
     */
    private static int nextConnectionID() {
        return baseConnectionID.incrementAndGet();
    }

    @Override
    public String toString() {
        return traceID;
    }

    SQLServerConnectionPoolProxy(SQLServerConnection con) {
        traceID = " ProxyConnectionID:" + nextConnectionID();
        wrappedConnection = con;
        // the Proxy is created with an open conn
        con.setAssociatedProxy(this);
        bIsOpen = true;
    }

    SQLServerConnection getWrappedConnection() {
        return wrappedConnection;
    }

    void checkClosed() throws SQLServerException {
        if (!bIsOpen) {
            SQLServerException.makeFromDriverError(null, null, SQLServerException.getErrString("R_connectionIsClosed"),
                    SQLServerException.EXCEPTION_XOPEN_CONNECTION_FAILURE, false);
        }
    }

    @Override
    public Statement createStatement() throws SQLServerException {
        checkClosed();
        return wrappedConnection.createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLServerException {
        checkClosed();
        return wrappedConnection.prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLServerException {
        checkClosed();
        return wrappedConnection.prepareCall(sql);
    }

    @Override
    public String nativeSQL(String sql) throws SQLServerException {
        checkClosed();
        return wrappedConnection.nativeSQL(sql);
    }

    @Override
    public void setAutoCommit(boolean newAutoCommitMode) throws SQLServerException {
        checkClosed();
        wrappedConnection.setAutoCommit(newAutoCommitMode);
    }

    @Override
    public boolean getAutoCommit() throws SQLServerException {
        checkClosed();
        return wrappedConnection.getAutoCommit();
    }

    @Override
    public void commit() throws SQLServerException {
        checkClosed();
        wrappedConnection.commit();
    }

    @Override
    public void rollback() throws SQLServerException {
        checkClosed();
        wrappedConnection.rollback();
    }

    @Override
    @SuppressWarnings("deprecation")
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
                java.sql.SQLPermission perm = new java.sql.SQLPermission(CALL_ABORT_PERM);
                secMgr.checkPermission(perm);
            } catch (SecurityException ex) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_permissionDenied"));
                Object[] msgArgs = {CALL_ABORT_PERM};
                throw new SQLServerException(form.format(msgArgs), null, 0, ex);
            }
        }

        bIsOpen = false;

        if (null == executor) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidArgument"));
            Object[] msgArgs = {"executor"};
            SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), null, false);
        } else {
            executor.execute(new Runnable() {
                public void run() {
                    if (wrappedConnection.getConnectionLogger().isLoggable(java.util.logging.Level.FINER))
                        wrappedConnection.getConnectionLogger().finer(toString() + " Connection proxy aborted ");
                    try {
                        wrappedConnection.poolCloseEventNotify();
                        wrappedConnection = null;
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
    }

    @Override
    public void close() throws SQLServerException {
        if (bIsOpen && (null != wrappedConnection)) {
            if (wrappedConnection.getConnectionLogger().isLoggable(java.util.logging.Level.FINER))
                wrappedConnection.getConnectionLogger().finer(toString() + " Connection proxy closed ");

            wrappedConnection.poolCloseEventNotify();
            wrappedConnection = null;
        }
        bIsOpen = false;
    }

    void internalClose() {
        bIsOpen = false;
        wrappedConnection = null;
    }

    @Override
    public boolean isClosed() throws SQLServerException {
        return !bIsOpen;
    }

    @Override
    public java.sql.DatabaseMetaData getMetaData() throws SQLServerException {
        checkClosed();
        return wrappedConnection.getMetaData();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLServerException {
        checkClosed();
        wrappedConnection.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLServerException {
        checkClosed();
        return wrappedConnection.isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLServerException {
        checkClosed();
        wrappedConnection.setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLServerException {
        checkClosed();
        return wrappedConnection.getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLServerException {
        checkClosed();
        wrappedConnection.setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLServerException {
        checkClosed();
        return wrappedConnection.getTransactionIsolation();
    }

    @Override
    public java.sql.SQLWarning getWarnings() throws SQLServerException {
        checkClosed();
        return wrappedConnection.getWarnings(); // Warnings support added
    }

    @Override
    public void clearWarnings() throws SQLServerException {
        checkClosed();
        wrappedConnection.clearWarnings();
    }

    // --------------------------JDBC 2.0-----------------------------

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        return wrappedConnection.createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sSql, int resultSetType,
            int resultSetConcurrency) throws SQLException {
        checkClosed();
        return wrappedConnection.prepareStatement(sSql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        return wrappedConnection.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public void setTypeMap(java.util.Map<String, Class<?>> map) throws SQLException {
        checkClosed();
        wrappedConnection.setTypeMap(map);
    }

    @Override
    public java.util.Map<String, Class<?>> getTypeMap() throws SQLServerException {
        checkClosed();
        return wrappedConnection.getTypeMap();
    }

    @Override
    public Statement createStatement(int nType, int nConcur, int nHold) throws SQLServerException {
        checkClosed();
        return wrappedConnection.createStatement(nType, nConcur, nHold);
    }

    @Override
    public Statement createStatement(int nType, int nConcur, int nHold,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLServerException {
        checkClosed();
        return wrappedConnection.createStatement(nType, nConcur, nHold, stmtColEncSetting);
    }

    @Override
    public PreparedStatement prepareStatement(java.lang.String sql, int nType, int nConcur,
            int nHold) throws SQLServerException {
        checkClosed();
        return wrappedConnection.prepareStatement(sql, nType, nConcur, nHold);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int nType, int nConcur, int nHold,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLServerException {
        checkClosed();
        return wrappedConnection.prepareStatement(sql, nType, nConcur, nHold, stmtColEncSetting);
    }

    @Override
    public CallableStatement prepareCall(String sql, int nType, int nConcur, int nHold) throws SQLServerException {
        checkClosed();
        return wrappedConnection.prepareCall(sql, nType, nConcur, nHold);
    }

    @Override
    public CallableStatement prepareCall(String sql, int nType, int nConcur, int nHold,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetiing) throws SQLServerException {
        checkClosed();
        return wrappedConnection.prepareCall(sql, nType, nConcur, nHold, stmtColEncSetiing);
    }

    /* JDBC 3.0 Auto generated keys */

    @Override
    public PreparedStatement prepareStatement(String sql, int flag) throws SQLServerException {
        checkClosed();
        return wrappedConnection.prepareStatement(sql, flag);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int flag,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLServerException {
        checkClosed();
        return wrappedConnection.prepareStatement(sql, flag, stmtColEncSetting);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLServerException {
        checkClosed();
        return wrappedConnection.prepareStatement(sql, columnIndexes);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLServerException {
        checkClosed();
        return wrappedConnection.prepareStatement(sql, columnIndexes, stmtColEncSetting);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLServerException {
        checkClosed();
        return wrappedConnection.prepareStatement(sql, columnNames);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLServerException {
        checkClosed();
        return wrappedConnection.prepareStatement(sql, columnNames, stmtColEncSetting);
    }

    /* JDBC 3.0 Savepoints */

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        checkClosed();
        wrappedConnection.releaseSavepoint(savepoint);
    }

    @Override
    public Savepoint setSavepoint(String sName) throws SQLServerException {
        checkClosed();
        return wrappedConnection.setSavepoint(sName);
    }

    @Override
    public Savepoint setSavepoint() throws SQLServerException {
        checkClosed();
        return wrappedConnection.setSavepoint();
    }

    @Override
    public void rollback(Savepoint s) throws SQLServerException {
        checkClosed();
        wrappedConnection.rollback(s);
    }

    @Override
    public int getHoldability() throws SQLServerException {
        checkClosed();
        return wrappedConnection.getHoldability();
    }

    @Override
    public void setHoldability(int nNewHold) throws SQLServerException {
        checkClosed();
        wrappedConnection.setHoldability(nNewHold);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        checkClosed();
        return wrappedConnection.getNetworkTimeout();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int timeout) throws SQLException {
        checkClosed();
        wrappedConnection.setNetworkTimeout(executor, timeout);
    }

    @Override
    public String getSchema() throws SQLException {
        checkClosed();
        return wrappedConnection.getSchema();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        checkClosed();
        wrappedConnection.setSchema(schema);
    }

    @Override
    public java.sql.Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        checkClosed();
        return wrappedConnection.createArrayOf(typeName, elements);
    }

    @Override
    public java.sql.Blob createBlob() throws SQLException {
        checkClosed();
        return wrappedConnection.createBlob();
    }

    @Override
    public java.sql.Clob createClob() throws SQLException {
        checkClosed();
        return wrappedConnection.createClob();
    }

    @Override
    public java.sql.NClob createNClob() throws SQLException {
        checkClosed();
        return wrappedConnection.createNClob();
    }

    @Override
    public java.sql.SQLXML createSQLXML() throws SQLException {
        checkClosed();
        return wrappedConnection.createSQLXML();
    }

    @Override
    public java.sql.Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        checkClosed();
        return wrappedConnection.createStruct(typeName, attributes);
    }

    @Override
    public java.util.Properties getClientInfo() throws SQLException {
        checkClosed();
        return wrappedConnection.getClientInfo();
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        checkClosed();
        return wrappedConnection.getClientInfo(name);
    }

    @Override
    public void setClientInfo(java.util.Properties properties) throws SQLClientInfoException {
        // No checkClosed() call since we can only throw SQLClientInfoException
        // from here
        wrappedConnection.setClientInfo(properties);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        // No checkClosed() call since we can only throw SQLClientInfoException
        // from here
        wrappedConnection.setClientInfo(name, value);
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        checkClosed();
        return wrappedConnection.isValid(timeout);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        wrappedConnection.getConnectionLogger().entering(toString(), "isWrapperFor", iface);
        boolean f = iface.isInstance(this);
        wrappedConnection.getConnectionLogger().exiting(toString(), "isWrapperFor", f);
        return f;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        wrappedConnection.getConnectionLogger().entering(toString(), "unwrap", iface);
        T t;
        try {
            t = iface.cast(this);
        } catch (ClassCastException e) {
            throw new SQLServerException(e.getMessage(), e);
        }
        wrappedConnection.getConnectionLogger().exiting(toString(), "unwrap", t);
        return t;
    }

    @Override
    public java.util.UUID getClientConnectionId() throws SQLServerException {
        checkClosed();
        return wrappedConnection.getClientConnectionId();
    }

    @Override
    public void setSendTimeAsDatetime(boolean sendTimeAsDateTimeValue) throws SQLServerException {
        checkClosed();
        wrappedConnection.setSendTimeAsDatetime(sendTimeAsDateTimeValue);
    }

    @Override
    public boolean getSendTimeAsDatetime() throws SQLServerException {
        checkClosed();
        return wrappedConnection.getSendTimeAsDatetime();
    }

    @Override
    public void setDatetimeParameterType(String datetimeParameterTypeValue) throws SQLServerException {
        checkClosed();
        wrappedConnection.setDatetimeParameterType(datetimeParameterTypeValue);
    }

    @Override
    public String getDatetimeParameterType() throws SQLServerException {
        checkClosed();
        return wrappedConnection.getDatetimeParameterType();
    }

    @Override
    public int getDiscardedServerPreparedStatementCount() {
        return wrappedConnection.getDiscardedServerPreparedStatementCount();
    }

    @Override
    public void closeUnreferencedPreparedStatementHandles() {
        wrappedConnection.closeUnreferencedPreparedStatementHandles();
    }

    @Override
    public boolean getEnablePrepareOnFirstPreparedStatementCall() {
        return wrappedConnection.getEnablePrepareOnFirstPreparedStatementCall();
    }

    @Override
    public void setEnablePrepareOnFirstPreparedStatementCall(boolean value) {
        wrappedConnection.setEnablePrepareOnFirstPreparedStatementCall(value);
    }

    @Override
    public String getPrepareMethod() {
        return wrappedConnection.getPrepareMethod();
    }

    @Override
    public void setPrepareMethod(String prepareMethod) {
        wrappedConnection.setPrepareMethod(prepareMethod);
    }

    @Override
    public int getServerPreparedStatementDiscardThreshold() {
        return wrappedConnection.getServerPreparedStatementDiscardThreshold();
    }

    @Override
    public void setServerPreparedStatementDiscardThreshold(int value) {
        wrappedConnection.setServerPreparedStatementDiscardThreshold(value);
    }

    @Override
    public void setStatementPoolingCacheSize(int value) {
        wrappedConnection.setStatementPoolingCacheSize(value);
    }

    @Override
    public int getStatementPoolingCacheSize() {
        return wrappedConnection.getStatementPoolingCacheSize();
    }

    @Override
    public boolean isStatementPoolingEnabled() {
        return wrappedConnection.isStatementPoolingEnabled();
    }

    @Override
    public int getStatementHandleCacheEntryCount() {
        return wrappedConnection.getStatementHandleCacheEntryCount();
    }

    @Override
    public void setDisableStatementPooling(boolean value) {
        wrappedConnection.setDisableStatementPooling(value);
    }

    @Override
    public boolean getDisableStatementPooling() {
        return wrappedConnection.getDisableStatementPooling();
    }

    @Override
    public void setUseFmtOnly(boolean useFmtOnly) {
        wrappedConnection.setUseFmtOnly(useFmtOnly);
    }

    @Override
    public boolean getUseFmtOnly() {
        return wrappedConnection.getUseFmtOnly();
    }

    @Override
    public boolean getDelayLoadingLobs() {
        return wrappedConnection.getDelayLoadingLobs();
    }

    @Override
    public void setDelayLoadingLobs(boolean delayLoadingLobs) {
        wrappedConnection.setDelayLoadingLobs(delayLoadingLobs);
    }

    @Override
    public boolean getIgnoreOffsetOnDateTimeOffsetConversion() {
        return wrappedConnection.getIgnoreOffsetOnDateTimeOffsetConversion();
    }

    @Override
    public void setIgnoreOffsetOnDateTimeOffsetConversion(boolean ignoreOffsetOnDateTimeOffsetConversion) {
        wrappedConnection.setIgnoreOffsetOnDateTimeOffsetConversion(ignoreOffsetOnDateTimeOffsetConversion);
    }

    @Override
    public void setIPAddressPreference(String iPAddressPreference) {
        wrappedConnection.setIPAddressPreference(iPAddressPreference);

    }

    @Override
    public String getIPAddressPreference() {
        return wrappedConnection.getIPAddressPreference();
    }

    /**
     * @deprecated Time-to-live is no longer supported for the cached Managed Identity tokens.
     *             This method will always return 0 and is for backwards compatibility only.
     */
    @Deprecated(since = "12.1.0", forRemoval = true)
    @Override
    public int getMsiTokenCacheTtl() {
        return 0;
    }

    /**
     * @deprecated Time-to-live is no longer supported for the cached Managed Identity tokens.
     *             This method is a no-op for backwards compatibility only.
     */
    @Deprecated(since = "12.1.0", forRemoval = true)
    @Override
    public void setMsiTokenCacheTtl(int timeToLive) {}

    /**
     * Returns the fully qualified class name of the implementing class for {@link SQLServerAccessTokenCallback}.
     *
     * @return accessTokenCallbackClass
     */
    @Override
    public String getAccessTokenCallbackClass() {
        return wrappedConnection.getAccessTokenCallbackClass();
    }

    /**
     * Sets 'accessTokenCallbackClass' to the fully qualified class name
     * of the implementing class for {@link SQLServerAccessTokenCallback}.
     *
     * @param accessTokenCallbackClass
     */
    @Override
    public void setAccessTokenCallbackClass(String accessTokenCallbackClass) {
        wrappedConnection.setAccessTokenCallbackClass(accessTokenCallbackClass);
    }

    /**
     * Returns the current value for 'calcBigDecimalScale'.
     *
     * @return calcBigDecimalScale
     *         a boolean
     */
    @Override
    public boolean getCalcBigDecimalScale() {
        return wrappedConnection.getCalcBigDecimalScale();
    }

    /**
     * Sets the current value of 'calculateBigDecimalScale' for the driver.
     *
     * @param calcBigDecimalScale
     */
    @Override
    public void setCalcBigDecimalScale(boolean calcBigDecimalScale) {
        wrappedConnection.setCalcBigDecimalScale(calcBigDecimalScale);
    }
}
