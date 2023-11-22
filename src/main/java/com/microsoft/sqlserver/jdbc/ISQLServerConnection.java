/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.UUID;


/**
 * Provides an interface to the {@link SQLServerConnection} and {@link SQLServerConnectionPoolProxy} classes.
 */
public interface ISQLServerConnection extends java.sql.Connection {

    /**
     * Transaction types
     * 
     * "TRANSACTION_SNAPSHOT corresponds to" "SET TRANSACTION ISOLATION LEVEL SNAPSHOT"
     */
    int TRANSACTION_SNAPSHOT = 0x1000;

    /**
     * Returns the connection ID of the most recent connection attempt, regardless of whether the attempt succeeded or
     * failed.
     * 
     * @return 16-byte GUID representing the connection ID of the most recent connection attempt. Or, NULL if there is a
     *         failure after the connection request is initiated and the pre-login handshake.
     * @throws SQLServerException
     *         If any errors occur.
     */
    UUID getClientConnectionId() throws SQLServerException;

    /**
     * Creates a <code>Statement</code> object that will generate <code>ResultSet</code> objects with the given type,
     * concurrency, and holdability. This method is the same as the <code>createStatement</code> method above, but it
     * allows the default result set type, concurrency, and holdability to be overridden.
     *
     * @param nType
     *        one of the following <code>ResultSet</code> constants: <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *        <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param nConcur
     *        one of the following <code>ResultSet</code> constants: <code>ResultSet.CONCUR_READ_ONLY</code> or
     *        <code>ResultSet.CONCUR_UPDATABLE</code>
     * @param nHold
     *        one of the following <code>ResultSet</code> constants: <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or
     *        <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
     * @param stmtColEncSetting
     *        Specifies how data will be sent and received when reading and writing encrypted columns.
     * @return a new <code>Statement</code> object that will generate <code>ResultSet</code> objects with the given
     *         type, concurrency, and holdability
     * @throws SQLServerException
     *         if a database access error occurs, this method is called on a closed connection or the given parameters
     *         are not <code>ResultSet</code> constants indicating type, concurrency, and holdability
     */
    Statement createStatement(int nType, int nConcur, int nHold,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLServerException;

    /**
     * Creates a default <code>PreparedStatement</code> object that has the capability to retrieve auto-generated keys.
     * The given constant tells the driver whether it should make auto-generated keys available for retrieval. This
     * parameter is ignored if the SQL statement is not an <code>INSERT</code> statement, or an SQL statement able to
     * return auto-generated keys (the list of such statements is vendor-specific).
     * <P>
     * <B>Note:</B> This method is optimized for handling parametric SQL statements that benefit from precompilation. If
     * the driver supports precompilation, the method <code>prepareStatement</code> will send the statement to the
     * database for precompilation. Some drivers may not support precompilation. In this case, the statement may not be
     * sent to the database until the <code>PreparedStatement</code> object is executed. This has no direct effect on
     * users; however, it does affect which methods throw certain SQLExceptions.
     * <P>
     * Result sets created using the returned <code>PreparedStatement</code> object will by default be type
     * <code>TYPE_FORWARD_ONLY</code> and have a concurrency level of <code>CONCUR_READ_ONLY</code>. The holdability of
     * the created result sets can be determined by calling {@link #getHoldability}.
     *
     * @param sql
     *        an SQL statement that may contain one or more '?' IN parameter placeholders
     * @param flag
     *        a flag indicating whether auto-generated keys should be returned; one of
     *        <code>Statement.RETURN_GENERATED_KEYS</code> or <code>Statement.NO_GENERATED_KEYS</code>
     * @param stmtColEncSetting
     *        Specifies how data will be sent and received when reading and writing encrypted columns.
     * @return a new <code>PreparedStatement</code> object, containing the pre-compiled SQL statement, that will have
     *         the capability of returning auto-generated keys
     * @throws SQLServerException
     *         if a database access error occurs, this method is called on a closed connection or the given parameter is
     *         not a <code>Statement</code> constant indicating whether auto-generated keys should be returned
     */
    PreparedStatement prepareStatement(String sql, int flag,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLServerException;

    /**
     * Creates a default <code>PreparedStatement</code> object capable of returning the auto-generated keys designated
     * by the given array. This array contains the indexes of the columns in the target table that contain the
     * auto-generated keys that should be made available. The driver will ignore the array if the SQL statement is not
     * an <code>INSERT</code> statement, or an SQL statement able to return auto-generated keys (the list of such
     * statements is vendor-specific).
     * <p>
     * An SQL statement with or without IN parameters can be pre-compiled and stored in a <code>PreparedStatement</code>
     * object. This object can then be used to efficiently execute this statement multiple times.
     * <P>
     * <B>Note:</B> This method is optimized for handling parametric SQL statements that benefit from precompilation. If
     * the driver supports precompilation, the method <code>prepareStatement</code> will send the statement to the
     * database for precompilation. Some drivers may not support precompilation. In this case, the statement may not be
     * sent to the database until the <code>PreparedStatement</code> object is executed. This has no direct effect on
     * users; however, it does affect which methods throw certain SQLExceptions.
     * <P>
     * Result sets created using the returned <code>PreparedStatement</code> object will by default be type
     * <code>TYPE_FORWARD_ONLY</code> and have a concurrency level of <code>CONCUR_READ_ONLY</code>. The holdability of
     * the created result sets can be determined by calling {@link #getHoldability}.
     *
     * @param sql
     *        an SQL statement that may contain one or more '?' IN parameter placeholders
     * @param columnIndexes
     *        an array of column indexes indicating the columns that should be returned from the inserted row or rows
     * @param stmtColEncSetting
     *        Specifies how data will be sent and received when reading and writing encrypted columns.
     * @return a new <code>PreparedStatement</code> object, containing the pre-compiled statement, that is capable of
     *         returning the auto-generated keys designated by the given array of column indexes
     * @throws SQLServerException
     *         if a database access error occurs or this method is called on a closed connection
     */
    PreparedStatement prepareStatement(String sql, int[] columnIndexes,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLServerException;

    /**
     * Creates a default <code>PreparedStatement</code> object capable of returning the auto-generated keys designated
     * by the given array. This array contains the names of the columns in the target table that contain the
     * auto-generated keys that should be returned. The driver will ignore the array if the SQL statement is not an
     * <code>INSERT</code> statement, or an SQL statement able to return auto-generated keys (the list of such
     * statements is vendor-specific).
     * <P>
     * An SQL statement with or without IN parameters can be pre-compiled and stored in a <code>PreparedStatement</code>
     * object. This object can then be used to efficiently execute this statement multiple times.
     * <P>
     * <B>Note:</B> This method is optimized for handling parametric SQL statements that benefit from precompilation. If
     * the driver supports precompilation, the method <code>prepareStatement</code> will send the statement to the
     * database for precompilation. Some drivers may not support precompilation. In this case, the statement may not be
     * sent to the database until the <code>PreparedStatement</code> object is executed. This has no direct effect on
     * users; however, it does affect which methods throw certain SQLExceptions.
     * <P>
     * Result sets created using the returned <code>PreparedStatement</code> object will by default be type
     * <code>TYPE_FORWARD_ONLY</code> and have a concurrency level of <code>CONCUR_READ_ONLY</code>. The holdability of
     * the created result sets can be determined by calling {@link #getHoldability}.
     *
     * @param sql
     *        an SQL statement that may contain one or more '?' IN parameter placeholders
     * @param columnNames
     *        an array of column names indicating the columns that should be returned from the inserted row or rows
     * @param stmtColEncSetting
     *        Specifies how data will be sent and received when reading and writing encrypted columns.
     * @return a new <code>PreparedStatement</code> object, containing the pre-compiled statement, that is capable of
     *         returning the auto-generated keys designated by the given array of column names
     * @throws SQLServerException
     *         if a database access error occurs or this method is called on a closed connection
     */
    PreparedStatement prepareStatement(String sql, String[] columnNames,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLServerException;

    /**
     * Creates a <code>PreparedStatement</code> object that will generate <code>ResultSet</code> objects with the given
     * type, concurrency, and holdability.
     * <P>
     * This method is the same as the <code>prepareStatement</code> method above, but it allows the default result set
     * type, concurrency, and holdability to be overridden.
     *
     * @param sql
     *        a <code>String</code> object that is the SQL statement to be sent to the database; may contain one or more
     *        '?' IN parameters
     * @param nType
     *        one of the following <code>ResultSet</code> constants: <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *        <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param nConcur
     *        one of the following <code>ResultSet</code> constants: <code>ResultSet.CONCUR_READ_ONLY</code> or
     *        <code>ResultSet.CONCUR_UPDATABLE</code>
     * @param resultSetHoldability
     *        one of the following <code>ResultSet</code> constants: <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or
     *        <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
     * @param stmtColEncSetting
     *        Specifies how data will be sent and received when reading and writing encrypted columns.
     * @return a new <code>PreparedStatement</code> object, containing the pre-compiled SQL statement, that will
     *         generate <code>ResultSet</code> objects with the given type, concurrency, and holdability
     * @throws SQLServerException
     *         if a database access error occurs, this method is called on a closed connection or the given parameters
     *         are not <code>ResultSet</code> constants indicating type, concurrency, and holdability
     */
    PreparedStatement prepareStatement(java.lang.String sql, int nType, int nConcur, int resultSetHoldability,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLServerException;

    /**
     * Creates a <code>CallableStatement</code> object that will generate <code>ResultSet</code> objects with the given
     * type and concurrency. This method is the same as the <code>prepareCall</code> method above, but it allows the
     * default result set type, result set concurrency type and holdability to be overridden.
     *
     * @param sql
     *        a <code>String</code> object that is the SQL statement to be sent to the database; may contain on or more
     *        '?' parameters
     * @param nType
     *        one of the following <code>ResultSet</code> constants: <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *        <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param nConcur
     *        one of the following <code>ResultSet</code> constants: <code>ResultSet.CONCUR_READ_ONLY</code> or
     *        <code>ResultSet.CONCUR_UPDATABLE</code>
     * @param nHold
     *        one of the following <code>ResultSet</code> constants: <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or
     *        <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
     * @param stmtColEncSetting
     *        Specifies how data will be sent and received when reading and writing encrypted columns.
     * @return a new <code>CallableStatement</code> object, containing the pre-compiled SQL statement, that will
     *         generate <code>ResultSet</code> objects with the given type, concurrency, and holdability
     * @throws SQLServerException
     *         if a database access error occurs, this method is called on a closed connection or the given parameters
     *         are not <code>ResultSet</code> constants indicating type, concurrency, and holdability
     */
    CallableStatement prepareCall(String sql, int nType, int nConcur, int nHold,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLServerException;

    /**
     * Sets the value of the sendTimeAsDatetime connection property. When true, java.sql.Time values will be sent to the
     * server as SQL Serverdatetime values. When false, java.sql.Time values will be sent to the server as SQL
     * Servertime values. sendTimeAsDatetime can also be modified programmatically with
     * SQLServerDataSource.setSendTimeAsDatetime. The default value for this property may change in a future release.
     * 
     * @param sendTimeAsDateTimeValue
     *        enables/disables setting the sendTimeAsDatetime connection property. For more information about how the
     *        Microsoft JDBC Driver for SQL Server configures java.sql.Time values before sending them to the server,
     *        see <a href="https://msdn.microsoft.com/en-us/library/ff427224(v=sql.110).aspx" > Configuring How
     *        java.sql.Time Values are Sent to the Server.</a>
     * 
     * @throws SQLServerException
     *         if a database access error occurs
     */
    void setSendTimeAsDatetime(boolean sendTimeAsDateTimeValue) throws SQLServerException;

    /**
     * Sets the value of the datetimeParameterType connection property, which controls how date and time parameters are sent to the server against SQL Server 2008+.
     * This setting can affect server-side date conversions and comparisons during statement execution, particularly against SQL Server 2016+. By default, the value is set to "datetime2".
     * Valid values are: datetime, datetime2 or datetimeoffset.
     * 
     * @param datetimeParameterTypeValue
     *        The datatype to use when encoding Java dates into SQL Server. Valid values are:
     *        datetime, datetime2 or datetimeoffset.
     * 
     * @throws SQLServerException
     *         if a database access error occurs
     */
    void setDatetimeParameterType(String datetimeParameterTypeValue) throws SQLServerException;

    /**
     * Returns the value of the sendTimeAsDatetime property.
     * 
     * @return boolean value of sendTimeAsDatetime
     * 
     * @throws SQLServerException
     *         if a database access error occurs
     */
    boolean getSendTimeAsDatetime() throws SQLServerException;

    /**
     * Returns the value of the datetimeParameterType property.
     * 
     * @return The string value of the datetimeParameterType property.
     * 
     * @throws SQLServerException
     *         if a database access error occurs
     */
    String getDatetimeParameterType() throws SQLServerException;

    /**
     * Returns the number of currently outstanding prepared statement un-prepare actions.
     * 
     * @return Returns the current value per the description.
     */
    int getDiscardedServerPreparedStatementCount();

    /**
     * Forces the un-prepare requests for any outstanding discarded prepared statements to be executed.
     */
    void closeUnreferencedPreparedStatementHandles();

    /**
     * Returns the behavior for a specific connection instance. If false the first execution will call sp_executesql and
     * not prepare a statement, once the second execution happens it will call sp_prepexec and actually setup a prepared
     * statement handle. Following executions will call sp_execute. This relieves the need for sp_unprepare on prepared
     * statement close if the statement is only executed once. The default for this option can be changed by calling
     * setDefaultEnablePrepareOnFirstPreparedStatementCall().
     * 
     * @return Returns the current setting per the description.
     */
    boolean getEnablePrepareOnFirstPreparedStatementCall();

    /**
     * Sets the behavior for a specific connection instance. If value is false the first execution will call
     * sp_executesql and not prepare a statement, once the second execution happens it will call sp_prepexec and
     * actually setup a prepared statement handle. Following executions will call sp_execute. This relieves the need for
     * sp_unprepare on prepared statement close if the statement is only executed once.
     * 
     * @param value
     *        Changes the setting per the description.
     */
    void setEnablePrepareOnFirstPreparedStatementCall(boolean value);

    /**
     * Returns the behavior for a specific connection instance. {@link PrepareMethod}
     *
     * @return Returns current setting for prepareMethod connection property.
     */
    String getPrepareMethod();

    /**
     * Sets the behavior for the prepare method. {@link PrepareMethod}
     *
     * @param prepareMethod
     *        Changes the setting as per description
     */
    void setPrepareMethod(String prepareMethod);

    /**
     * Returns the behavior for a specific connection instance. This setting controls how many outstanding prepared
     * statement discard actions (sp_unprepare) can be outstanding per connection before a call to clean-up the
     * outstanding handles on the server is executed. If the setting is {@literal <=} 1, unprepare actions will be
     * executed immediately on prepared statement close. If it is set to {@literal >} 1, these calls will be batched
     * together to avoid overhead of calling sp_unprepare too often. The default for this option can be changed by
     * calling getDefaultServerPreparedStatementDiscardThreshold().
     * 
     * @return Returns the current setting per the description.
     */
    int getServerPreparedStatementDiscardThreshold();

    /**
     * Sets the behavior for a specific connection instance. This setting controls how many outstanding prepared
     * statement discard actions (sp_unprepare) can be outstanding per connection before a call to clean-up the
     * outstanding handles on the server is executed. If the setting is {@literal <=} 1 unprepare actions will be
     * executed immedietely on prepared statement close. If it is set to {@literal >} 1 these calls will be batched
     * together to avoid overhead of calling sp_unprepare too often.
     * 
     * @param value
     *        Changes the setting per the description.
     */
    void setServerPreparedStatementDiscardThreshold(int value);

    /**
     * Sets the size of the prepared statement cache for this connection. A value less than 1 means no cache.
     * 
     * @param value
     *        The new cache size.
     * 
     */
    void setStatementPoolingCacheSize(int value);

    /**
     * Returns the size of the prepared statement cache for this connection. A value less than 1 means no cache.
     * 
     * @return Returns the current setting per the description.
     */
    int getStatementPoolingCacheSize();

    /**
     * Returns whether statement pooling is enabled or not for this connection.
     * 
     * @return Returns the current setting per the description.
     */
    boolean isStatementPoolingEnabled();

    /**
     * Returns the current number of pooled prepared statement handles.
     * 
     * @return Returns the current setting per the description.
     */
    int getStatementHandleCacheEntryCount();

    /**
     * Sets the value to Disable/enable statement pooling.
     * 
     * @param value
     *        true to disable statement pooling, false to enable it.
     */
    void setDisableStatementPooling(boolean value);

    /**
     * Returns the value whether statement pooling is disabled.
     * 
     * @return true if statement pooling is disabled, false if it is enabled.
     */
    boolean getDisableStatementPooling();

    /**
     * Returns the current flag value for useFmtOnly.
     *
     * @return 'useFmtOnly' property value.
     */
    boolean getUseFmtOnly();

    /**
     * Specifies the flag to use FMTONLY for parameter metadata queries.
     *
     * @param useFmtOnly
     *        boolean value for 'useFmtOnly'.
     */
    void setUseFmtOnly(boolean useFmtOnly);

    /**
     * Returns the current flag value for delayLoadingLobs.
     *
     * @return 'delayLoadingLobs' property value.
     */
    boolean getDelayLoadingLobs();

    /**
     * Specifies the flag to immediately load LOB objects into memory.
     *
     * @param delayLoadingLobs
     *        boolean value for 'delayLoadingLobs'.
     */
    void setDelayLoadingLobs(boolean delayLoadingLobs);

    /**
     * Returns the current flag value for ignoreOffsetOnDateTimeOffsetConversion.
     *
     * @return 'ignoreOffsetOnDateTimeOffsetConversion' property value.
     */
    boolean getIgnoreOffsetOnDateTimeOffsetConversion();

    /**
     * Specifies the flag to ignore offset when converting DATETIMEOFFSET to LocalDateTime.
     *
     * @param ignoreOffsetOnDateTimeOffsetConversion
     *        boolean value for 'ignoreOffsetOnDateTimeOffsetConversion'.
     */
    void setIgnoreOffsetOnDateTimeOffsetConversion(boolean ignoreOffsetOnDateTimeOffsetConversion);

    /**
     * Sets the name of the preferred type of IP Address.
     * 
     * @param iPAddressPreference
     *        A String that contains the preferred type of IP Address.
     */
    void setIPAddressPreference(String iPAddressPreference);

    /**
     * Gets the name of the preferred type of IP Address.
     * 
     * @return IPAddressPreference
     *         A String that contains the preferred type of IP Address.
     */
    String getIPAddressPreference();

    /**
     * This method will always return 0 and is for backwards compatibility only.
     *
     * @return Method will always return 0.
     * @deprecated Time-to-live is no longer supported for the cached Managed Identity tokens.
     */
    @Deprecated(since = "12.1.0", forRemoval = true)
    int getMsiTokenCacheTtl();

    /**
     * Time-to-live is no longer supported for the cached Managed Identity tokens.
     * This method is a no-op for backwards compatibility only.
     *
     * @param timeToLive
     *        Time-to-live is no longer supported.
     * @deprecated Time-to-live is no longer supported for the cached Managed Identity tokens.
     */
    @Deprecated(since = "12.1.0", forRemoval = true)
    void setMsiTokenCacheTtl(int timeToLive);

    /**
     * Returns the fully qualified class name of the implementing class for {@link SQLServerAccessTokenCallback}.
     *
     * @return accessTokenCallbackClass
     */
    String getAccessTokenCallbackClass();

    /**
     * Sets 'accessTokenCallbackClass' to the fully qualified class name
     * of the implementing class for {@link SQLServerAccessTokenCallback}.
     *
     * @param accessTokenCallbackClass
     */
    void setAccessTokenCallbackClass(String accessTokenCallbackClass);

    /**
     * Returns the current flag for calcBigDecimalScale.
     *
     * @return calcBigDecimalScale
     *         Whether calculating big decimal scale from input values is enabled.
     */
    boolean getCalcBigDecimalScale();

    /**
     * Specifies whether to calculate scale from inputted big decimal values.
     *
     * @param calcBigDecimalScale
     *        A boolean that indicates if the driver should calculate scale from inputted big decimal values.
     */
    void setCalcBigDecimalScale(boolean calcBigDecimalScale);
}
