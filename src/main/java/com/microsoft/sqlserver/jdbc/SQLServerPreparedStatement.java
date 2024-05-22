/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static com.microsoft.sqlserver.jdbc.SQLServerConnection.getCachedParsedSQL;
import static com.microsoft.sqlserver.jdbc.SQLServerConnection.parseAndCacheSQL;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.BatchUpdateException;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.logging.Level;
import java.util.regex.Pattern;

import com.microsoft.sqlserver.jdbc.SQLServerConnection.CityHash128Key;
import com.microsoft.sqlserver.jdbc.SQLServerConnection.PreparedStatementHandle;


/**
 * Provides an implementation of java.sql.PreparedStatement interface that assists in preparing Statements for SQL
 * Server.
 * <p>
 * SQLServerPreparedStatement prepares a statement using SQL Server's sp_prepexec and re-uses the returned statement
 * handle for each subsequent execution of the statement (typically using different parameters provided by the user)
 * <p>
 * SQLServerPreparedStatement supports batching whereby a set of prepared statements are executed in a single database
 * round trip to improve runtime performance.
 * <p>
 * The API javadoc for JDBC API methods that this class implements are not repeated here. Please see Sun's JDBC API
 * interfaces javadoc for those details.
 */
public class SQLServerPreparedStatement extends SQLServerStatement implements ISQLServerPreparedStatement {
    /**
     * Always update serialVersionUID when prompted.
     */
    private static final long serialVersionUID = -6292257029445685221L;

    /** delimiter for multiple statements in a single batch */
    @SuppressWarnings("unused")
    private static final int BATCH_STATEMENT_DELIMITER_TDS_71 = 0x80;
    private static final int BATCH_STATEMENT_DELIMITER_TDS_72 = 0xFF;

    private static final String EXECUTE_BATCH_STRING = "executeBatch";
    private static final String ACTIVITY_ID = " ActivityId: ";

    /** batch statement delimiter */
    static final int NBATCH_STATEMENT_DELIMITER = BATCH_STATEMENT_DELIMITER_TDS_72;

    /** The prepared type definitions */
    private String preparedTypeDefinitions;

    /** Processed SQL statement text, may not be same as what user initially passed. */
    final String userSQL;

    // flag whether is exec escape syntax
    private boolean isExecEscapeSyntax;

    // flag whether is call escape syntax
    private boolean isCallEscapeSyntax;

    /** Parameter positions in processed SQL statement text. */
    final int[] userSQLParamPositions;

    /** SQL statement with expanded parameter tokens */
    private String preparedSQL;

    /** True if this execute has been called for this statement at least once */
    private boolean isExecutedAtLeastOnce = false;

    /** True if sp_prepare was called **/
    private boolean isSpPrepareExecuted = false;

    /** Reference to cache item for statement handle pooling. Only used to decrement ref count on statement close. */
    private transient PreparedStatementHandle cachedPreparedStatementHandle;

    /** Hash of user supplied SQL statement used for various cache lookups */
    private CityHash128Key sqlTextCacheKey;

    /**
     * Array with parameter names generated in buildParamTypeDefinitions For mapping encryption information to
     * parameters, as the second result set returned by sp_describe_parameter_encryption doesn't depend on order of
     * input parameter
     **/
    private ArrayList<String> parameterNames;

    /** Set to true if the statement is a stored procedure call that expects a return value */
    final boolean bReturnValueSyntax;

    /** user FMTOnly flag */
    private boolean useFmtOnly = this.connection.getUseFmtOnly();

    /**
     * The number of OUT parameters to skip in the response to get to the first app-declared OUT parameter.
     *
     * When executing prepared and callable statements and/or statements that produce cursored results, the first OUT
     * parameters returned by the server contain the internal values like the prepared statement handle and the cursor
     * ID and row count. This value indicates how many of those internal OUT parameters were in the response.
     */
    int outParamIndexAdjustment;

    /** Set of parameter values in the current batch */
    ArrayList<Parameter[]> batchParamValues;

    /** The prepared statement handle returned by the server */
    private int prepStmtHandle = 0;

    /** Statement used for getMetadata(). Declared as a field to facilitate closing the statement. */
    private SQLServerStatement internalStmt = null;

    private void setPreparedStatementHandle(int handle) {
        this.prepStmtHandle = handle;
    }

    /**
     * boolean value for deciding if the driver should use bulk copy API for batch inserts
     */
    private boolean useBulkCopyForBatchInsert;

    /**
     * Regex for JDBC 'call' escape syntax
     */
    private static final Pattern callEscapePattern = Pattern
            .compile("^\\s*(?i)\\{(\\s*\\??\\s*=?\\s*)call (.+)\\s*\\(?\\?*,?\\)?\\s*}\\s*$");

    /**
     * Regex for 'exec' escape syntax
     */
    private static final Pattern execEscapePattern = Pattern.compile("^\\s*(?i)(?:exec|execute)\\b");

    /** Returns the prepared statement SQL */
    @Override
    public String toString() {
        return "sp_executesql SQL: " + preparedSQL;
    }

    /**
     * Returns the prepared statement's useBulkCopyForBatchInsert value.
     * 
     * @return Per the description.
     * @throws SQLServerException
     *         when an error occurs
     */
    @SuppressWarnings("unused")
    private boolean getUseBulkCopyForBatchInsert() throws SQLServerException {
        checkClosed();
        return useBulkCopyForBatchInsert;
    }

    /**
     * Sets the prepared statement's useBulkCopyForBatchInsert value.
     * 
     * @param useBulkCopyForBatchInsert
     *        the boolean value
     * @throws SQLServerException
     *         when an error occurs
     */
    @SuppressWarnings("unused")
    private void setUseBulkCopyForBatchInsert(boolean useBulkCopyForBatchInsert) throws SQLServerException {
        checkClosed();
        this.useBulkCopyForBatchInsert = useBulkCopyForBatchInsert;
    }

    @Override
    public int getPreparedStatementHandle() throws SQLServerException {
        checkClosed();
        return prepStmtHandle;
    }

    /**
     * Returns true if this statement has a server handle.
     * 
     * @return Per the description.
     */
    private boolean hasPreparedStatementHandle() {
        return 0 < prepStmtHandle;
    }

    /**
     * Resets the server handle for this prepared statement to no handle.
     */
    private boolean resetPrepStmtHandle(boolean discardCurrentCacheItem) {
        boolean statementPoolingUsed = null != cachedPreparedStatementHandle;
        // Return to pool and decrement reference count
        // Make sure the cached handle does not get re-used more.
        if (statementPoolingUsed && discardCurrentCacheItem) {
            cachedPreparedStatementHandle.setIsExplicitlyDiscarded();
        }
        prepStmtHandle = 0;
        return statementPoolingUsed;
    }

    /** Flag set to true when statement execution is expected to return the prepared statement handle */
    private boolean expectPrepStmtHandle = false;

    /**
     * Flag set to true when all encryption metadata of inOutParam is retrieved
     */
    private boolean encryptionMetadataIsRetrieved = false;

    /**
     * local user SQL
     */
    private String localUserSQL;

    /**
     * crypto meta batch
     */
    private Vector<CryptoMetadata> cryptoMetaBatch = new Vector<>();

    /**
     * Listener to clear the {@link SQLServerPreparedStatement#prepStmtHandle} and
     * {@link SQLServerPreparedStatement#cachedPreparedStatementHandle} before reconnecting.
     */
    private ReconnectListener clearPrepStmtHandleOnReconnectListener;

    /**
     * Constructs a SQLServerPreparedStatement.
     * 
     * @param conn
     *        the connection
     * @param sql
     *        the user's sql
     * @param nRSType
     *        the result set type
     * @param nRSConcur
     *        the result set concurrency
     * @param stmtColEncSetting
     *        the statement column encryption setting
     * @throws SQLServerException
     *         when an error occurs
     */
    SQLServerPreparedStatement(SQLServerConnection conn, String sql, int nRSType, int nRSConcur,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLServerException {
        super(conn, nRSType, nRSConcur, stmtColEncSetting);

        clearPrepStmtHandleOnReconnectListener = this::clearPrepStmtHandle;
        connection.registerBeforeReconnectListener(clearPrepStmtHandleOnReconnectListener);

        if (null == sql) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_NullValue"));
            Object[] msgArgs1 = {"Statement SQL"};
            throw new SQLServerException(form.format(msgArgs1), null);
        }

        stmtPoolable = true;

        // Create a cache key for this statement.
        sqlTextCacheKey = new CityHash128Key(sql);

        // Parse or fetch SQL metadata from cache.
        ParsedSQLCacheItem parsedSQL = getCachedParsedSQL(sqlTextCacheKey);
        if (null != parsedSQL) {
            if (null != connection && connection.isStatementPoolingEnabled()) {
                isExecutedAtLeastOnce = true;
            }
        } else {
            parsedSQL = parseAndCacheSQL(sqlTextCacheKey, sql);
        }

        // Retrieve meta data from cache item.
        procedureName = parsedSQL.procedureName;
        bReturnValueSyntax = parsedSQL.bReturnValueSyntax;
        userSQL = parsedSQL.processedSQL;
        isExecEscapeSyntax = isExecEscapeSyntax(sql);
        isCallEscapeSyntax = isCallEscapeSyntax(sql);
        userSQLParamPositions = parsedSQL.parameterPositions;
        initParams(userSQLParamPositions.length);
        useBulkCopyForBatchInsert = conn.getUseBulkCopyForBatchInsert();
    }

    /**
     * Closes the prepared statement's prepared handle.
     */
    private void closePreparedHandle() {
        connection.removeBeforeReconnectListener(clearPrepStmtHandleOnReconnectListener);

        if (!hasPreparedStatementHandle())
            return;

        // If the connection is already closed, don't bother trying to close
        // the prepared handle. We won't be able to, and it's already closed
        // on the server anyway.
        if (connection.isSessionUnAvailable()) {
            if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
                loggerExternal.finer(
                        this + ": Not closing PreparedHandle:" + prepStmtHandle + "; connection is already closed.");
        } else {
            isExecutedAtLeastOnce = false;
            final int handleToClose = prepStmtHandle;

            // Handle unprepare actions through statement pooling.
            if (resetPrepStmtHandle(false)) {
                connection.returnCachedPreparedStatementHandle(cachedPreparedStatementHandle);
            }
            // If no reference to a statement pool cache item is found handle unprepare actions through batching @
            // connection level.
            else if (connection.isPreparedStatementUnprepareBatchingEnabled()) {
                connection.enqueueUnprepareStatementHandle(
                        connection.new PreparedStatementHandle(null, handleToClose, executedSqlDirectly, true));
            } else {
                // Non batched behavior (same as pre batch clean-up implementation)
                if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
                    loggerExternal.finer(this + ": Closing PreparedHandle:" + handleToClose);

                final class PreparedHandleClose extends UninterruptableTDSCommand {
                    /**
                     * Always update serialVersionUID when prompted.
                     */
                    private static final long serialVersionUID = -8944096664249990764L;

                    PreparedHandleClose() {
                        super("closePreparedHandle");
                    }

                    final boolean doExecute() throws SQLServerException {
                        TDSWriter tdsWriter = startRequest(TDS.PKT_RPC);
                        tdsWriter.writeShort((short) 0xFFFF); // procedure name length -> use ProcIDs
                        tdsWriter.writeShort(
                                executedSqlDirectly ? TDS.PROCID_SP_UNPREPARE : TDS.PROCID_SP_CURSORUNPREPARE);
                        tdsWriter.writeByte((byte) 0); // RPC procedure option 1
                        tdsWriter.writeByte((byte) 0); // RPC procedure option 2
                        tdsWriter.sendEnclavePackage(null, null);
                        tdsWriter.writeRPCInt(null, handleToClose, false);
                        TDSParser.parse(startResponse(), getLogContext());
                        return true;
                    }
                }

                // Try to close the server cursor. Any failure is caught, logged, and ignored.
                try {
                    executeCommand(new PreparedHandleClose());
                } catch (SQLServerException e) {
                    if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
                        loggerExternal.log(Level.FINER,
                                this + ": Error (ignored) closing PreparedHandle:" + handleToClose, e);
                }

                if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
                    loggerExternal.finer(this + ": Closed PreparedHandle:" + handleToClose);
            }

            // Always run any outstanding discard actions as statement pooling always uses batched sp_unprepare.
            connection.unprepareUnreferencedPreparedStatementHandles(false);
        }
    }

    /**
     * Closes this prepared statement.
     *
     * Note that the public Statement.close() method performs all of the cleanup work through this internal method which
     * cannot throw any exceptions. This is done deliberately to ensure that ALL of the object's client-side and
     * server-side state is cleaned up as best as possible, even under conditions which would normally result in
     * exceptions being thrown.
     */
    @Override
    final void closeInternal() {
        super.closeInternal();

        // If we have a prepared statement handle, close it.
        closePreparedHandle();

        // Close the statement that was used to generate empty statement from getMetadata().
        try {
            if (null != internalStmt)
                internalStmt.close();
        } catch (SQLServerException e) {
            if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
                loggerExternal
                        .finer("Ignored error closing internal statement: " + e.getErrorCode() + " " + e.getMessage());
        } finally {
            internalStmt = null;
        }

        // Clean up client-side state
        batchParamValues = null;
    }

    /**
     * Initializes the statement parameters.
     * 
     * @param nParams
     *        Number of parameters to initialize.
     */
    final void initParams(int nParams) {
        inOutParam = new Parameter[nParams];
        for (int i = 0; i < nParams; i++) {
            inOutParam[i] = new Parameter(Util.shouldHonorAEForParameters(stmtColumnEncriptionSetting, connection));
        }

        if (bReturnValueSyntax) {
            inOutParam[0].setReturnValue(true);
        }
    }

    @Override
    public final void clearParameters() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "clearParameters");
        checkClosed();
        encryptionMetadataIsRetrieved = false;
        cryptoMetaBatch.clear();
        int i;
        if (inOutParam == null)
            return;
        for (i = 0; i < inOutParam.length; i++) {
            inOutParam[i].clearInputValue();
        }
        loggerExternal.exiting(getClassNameLogging(), "clearParameters");
    }

    /**
     * Determines whether the statement needs to be reprepared based on a change in any of the type definitions of any
     * of the parameters due to changes in scale, length, etc., and, if so, sets the new type definition string.
     */
    private boolean buildPreparedStrings(Parameter[] params, boolean renewDefinition) throws SQLServerException {
        String newTypeDefinitions = buildParamTypeDefinitions(params, renewDefinition);
        if (null != preparedTypeDefinitions && newTypeDefinitions.equalsIgnoreCase(preparedTypeDefinitions))
            return false;

        preparedTypeDefinitions = newTypeDefinitions;

        /* Replace the parameter marker '?' with the param numbers @p1, @p2 etc */
        preparedSQL = connection.replaceParameterMarkers(userSQL, userSQLParamPositions, params, bReturnValueSyntax);
        if (bRequestedGeneratedKeys)
            preparedSQL = preparedSQL + IDENTITY_QUERY;

        return true;
    }

    /**
     * Builds the parameter type definitons for a JDBC prepared statement that will be used to prepare the statement.
     * 
     * @param params
     *        the statement parameters
     * @param renewDefinition
     *        True if renewing parameter definition, False otherwise
     * @throws SQLServerException
     *         when an error occurs.
     * @return the required data type definitions.
     */
    private String buildParamTypeDefinitions(Parameter[] params, boolean renewDefinition) throws SQLServerException {
        int nCols = params.length;
        if (nCols == 0)
            return "";

        // Output looks like @P0 timestamp, @P1 varchar
        int stringLen = nCols * 2; // @P
        stringLen += nCols; // spaces
        stringLen += nCols - 1; // commas
        if (nCols > 10) {
            stringLen += 10 + ((nCols - 10) * 2); // @P{0-99} Numbers after p
        } else {
            stringLen += nCols; // @P{0-9} Numbers after p less than 10
        }

        // Computing the type definitions up front, so we can get exact string lengths needed for the string builder.
        String[] typeDefinitions = new String[nCols];
        for (int i = 0; i < nCols; i++) {
            Parameter param = params[i];
            param.renewDefinition = renewDefinition;
            String typeDefinition = param.getTypeDefinition(connection, resultsReader());
            if (null == typeDefinition) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_valueNotSetForParameter"));
                Object[] msgArgs = {i + 1};
                SQLServerException.makeFromDriverError(connection, this, form.format(msgArgs), null, false);
            }
            typeDefinitions[i] = typeDefinition;
            stringLen += typeDefinition.length();

            // While we are getting type definitions, check if the params are output and extend the builder if so.
            stringLen += param.isOutput() ? 7 : 0;
        }

        StringBuilder sb = new StringBuilder(stringLen);
        char[] cParamName = new char[10];
        parameterNames = new ArrayList<>(nCols);

        for (int i = 0; i < nCols; i++) {
            if (i > 0)
                sb.append(',');

            int l = SQLServerConnection.makeParamName(i, cParamName, 0, false);
            String parameterName = String.valueOf(cParamName, 0, l);
            sb.append(parameterName);
            sb.append(' ');

            parameterNames.add(parameterName);
            sb.append(typeDefinitions[i]);

            if (params[i].isOutput())
                sb.append(" OUTPUT");
        }
        return sb.toString();
    }

    @Override
    public java.sql.ResultSet executeQuery() throws SQLServerException, SQLTimeoutException {
        loggerExternal.entering(getClassNameLogging(), "executeQuery");
        if (loggerExternal.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
            loggerExternal.finer(toString() + ACTIVITY_ID + ActivityCorrelator.getCurrent().toString());
        }
        checkClosed();
        connection.unprepareUnreferencedPreparedStatementHandles(false);
        executeStatement(new PrepStmtExecCmd(this, EXECUTE_QUERY));
        loggerExternal.exiting(getClassNameLogging(), "executeQuery");
        return resultSet;
    }

    /**
     * Executes a query without cursoring for metadata.
     *
     * @throws SQLServerException
     * @return ResultSet
     * @throws SQLTimeoutException
     */
    final java.sql.ResultSet executeQueryInternal() throws SQLServerException, SQLTimeoutException {
        checkClosed();
        connection.unprepareUnreferencedPreparedStatementHandles(false);
        executeStatement(new PrepStmtExecCmd(this, EXECUTE_QUERY_INTERNAL));
        return resultSet;
    }

    @Override
    public int executeUpdate() throws SQLServerException, SQLTimeoutException {
        loggerExternal.entering(getClassNameLogging(), "executeUpdate");
        if (loggerExternal.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
            loggerExternal.finer(toString() + ACTIVITY_ID + ActivityCorrelator.getCurrent().toString());
        }

        checkClosed();
        connection.unprepareUnreferencedPreparedStatementHandles(false);
        executeStatement(new PrepStmtExecCmd(this, EXECUTE_UPDATE));

        // this shouldn't happen, caller probably meant to call executeLargeUpdate
        if (updateCount < Integer.MIN_VALUE || updateCount > Integer.MAX_VALUE)
            SQLServerException.makeFromDriverError(connection, this,
                    SQLServerException.getErrString("R_updateCountOutofRange"), null, true);

        loggerExternal.exiting(getClassNameLogging(), "executeUpdate", updateCount);

        return (int) updateCount;
    }

    @Override
    public long executeLargeUpdate() throws SQLServerException, SQLTimeoutException {

        loggerExternal.entering(getClassNameLogging(), "executeLargeUpdate");
        if (loggerExternal.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
            loggerExternal.finer(toString() + ACTIVITY_ID + ActivityCorrelator.getCurrent().toString());
        }
        checkClosed();
        connection.unprepareUnreferencedPreparedStatementHandles(false);
        executeStatement(new PrepStmtExecCmd(this, EXECUTE_UPDATE));
        loggerExternal.exiting(getClassNameLogging(), "executeLargeUpdate", updateCount);
        return updateCount;
    }

    @Override
    public boolean execute() throws SQLServerException, SQLTimeoutException {
        loggerExternal.entering(getClassNameLogging(), "execute");
        if (loggerExternal.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
            loggerExternal.finer(toString() + ACTIVITY_ID + ActivityCorrelator.getCurrent().toString());
        }
        checkClosed();
        connection.unprepareUnreferencedPreparedStatementHandles(false);
        executeStatement(new PrepStmtExecCmd(this, EXECUTE));
        loggerExternal.exiting(getClassNameLogging(), "execute", null != resultSet);
        return null != resultSet;
    }

    /**
     * Prepare statement exec command
     */
    private final class PrepStmtExecCmd extends TDSCommand {
        /**
         * Always update serialVersionUID when prompted.
         */
        private static final long serialVersionUID = 4098801171124750861L;

        private final SQLServerPreparedStatement stmt;

        PrepStmtExecCmd(SQLServerPreparedStatement stmt, int executeMethod) {
            super(stmt.toString() + " executeXXX", queryTimeout, cancelQueryTimeoutSeconds);
            this.stmt = stmt;
            stmt.executeMethod = executeMethod;
        }

        final boolean doExecute() throws SQLServerException {
            stmt.doExecutePreparedStatement(this);
            return false;
        }

        @Override
        final void processResponse(TDSReader tdsReader) throws SQLServerException {
            ensureExecuteResultsReader(tdsReader);
            processExecuteResults();
        }
    }

    final void doExecutePreparedStatement(PrepStmtExecCmd command) throws SQLServerException {
        resetForReexecute();

        // If this request might be a query (as opposed to an update) then make
        // sure we set the max number of rows and max field size for any ResultSet
        // that may be returned.
        //
        // If the app uses Statement.execute to execute an UPDATE or DELETE statement
        // and has called Statement.setMaxRows to limit the number of rows from an
        // earlier query, then the number of rows updated/deleted will be limited as
        // well.
        //
        // Note: similar logic in SQLServerStatement.doExecuteStatement
        setMaxRowsAndMaxFieldSize();

        if (loggerExternal.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
            loggerExternal.finer(toString() + ACTIVITY_ID + ActivityCorrelator.getCurrent().toString());
        }

        boolean hasExistingTypeDefinitions = preparedTypeDefinitions != null;
        boolean hasNewTypeDefinitions = true;
        boolean inRetry = false; // Used to indicate if this execution is a retry
        if (!encryptionMetadataIsRetrieved) {
            hasNewTypeDefinitions = buildPreparedStrings(inOutParam, false);
        }

        if (connection.isAEv2() && !isInternalEncryptionQuery) {
            this.enclaveCEKs = connection.initEnclaveParameters(this, preparedSQL, preparedTypeDefinitions, inOutParam,
                    parameterNames);
            encryptionMetadataIsRetrieved = true;
            setMaxRowsAndMaxFieldSize();
            hasNewTypeDefinitions = buildPreparedStrings(inOutParam, true);
        }

        if ((Util.shouldHonorAEForParameters(stmtColumnEncriptionSetting, connection)) && (0 < inOutParam.length)
                && !isInternalEncryptionQuery) {

            // retrieve parameter encryption metadata if they are not retrieved yet
            if (!encryptionMetadataIsRetrieved) {
                getParameterEncryptionMetadata(inOutParam);
                encryptionMetadataIsRetrieved = true;

                // maxRows is set to 0 when retrieving encryption metadata,
                // need to set it back
                setMaxRowsAndMaxFieldSize();
            }

            // fix an issue when inserting unicode into non-encrypted nchar column using setString() and AE is on on
            // Connection
            hasNewTypeDefinitions = buildPreparedStrings(inOutParam, true);
        }

        boolean needsPrepare = true;
        // Retry execution if existing handle could not be re-used.
        for (int attempt = 1; attempt <= 2; ++attempt) {
            try {
                // Re-use handle if available, requires parameter definitions which are not available until here.
                if (reuseCachedHandle(hasNewTypeDefinitions, 1 < attempt)) {
                    hasNewTypeDefinitions = false;
                }

                // Start the request and detach the response reader so that we can
                // continue using it after we return.
                TDSWriter tdsWriter = command.startRequest(TDS.PKT_RPC);

                needsPrepare = doPrepExec(tdsWriter, inOutParam, hasNewTypeDefinitions, hasExistingTypeDefinitions,
                        command);

                ensureExecuteResultsReader(command.startResponse(getIsResponseBufferingAdaptive()));
                startResults();
                getNextResult(true);
            } catch (SQLException e) {
                if (retryBasedOnFailedReuseOfCachedHandle(e, attempt, needsPrepare, false)) {
                    continue;
                } else if (!inRetry && connection.doesServerSupportEnclaveRetry()) {
                    // We only want to retry once, so no retrying if we're already in the second pass.
                    // If we are AE_v3, remove the failed entry and try again.
                    ParameterMetaDataCache.removeCacheEntry(connection, preparedSQL);
                    inRetry = true;
                    doExecutePreparedStatement(command);
                } else {
                    throw e;
                }
            }
            break;
        }

        if (EXECUTE_QUERY == executeMethod && null == resultSet) {
            SQLServerException.makeFromDriverError(connection, this, SQLServerException.getErrString("R_noResultset"),
                    null, true);
        } else if (EXECUTE_UPDATE == executeMethod && null != resultSet) {
            SQLServerException.makeFromDriverError(connection, this,
                    SQLServerException.getErrString("R_resultsetGeneratedForUpdate"), null, false);
        }
    }

    /**
     * Returns if the execution should be retried because the re-used cached handle could not be re-used due to server
     * side state changes.
     */
    private boolean retryBasedOnFailedReuseOfCachedHandle(SQLException e, int attempt, boolean needsPrepare,
            boolean isBatch) {
        // Only retry based on these error codes and if statementPooling is enabled:
        // 586: The prepared statement handle %d is not valid in this context. Please verify that current database, user
        // default schema, and
        // ANSI_NULLS and QUOTED_IDENTIFIER set options are not changed since the handle is prepared.
        // 8179: Could not find prepared statement with handle %d.
        if (needsPrepare && !isBatch)
            return false;
        return 1 == attempt && (586 == e.getErrorCode() || 8179 == e.getErrorCode())
                && connection.isStatementPoolingEnabled();
    }

    /**
     * Consumes the OUT parameter for the statement object itself.
     *
     * When a prepared statement handle is expected as the first OUT parameter from PreparedStatement or
     * CallableStatement execution, then it gets consumed here.
     */
    @Override
    boolean consumeExecOutParam(TDSReader tdsReader) throws SQLServerException {
        final class PrepStmtExecOutParamHandler extends StmtExecOutParamHandler {

            PrepStmtExecOutParamHandler(SQLServerStatement statement) {
                super(statement);
            }

            @Override
            boolean onRetValue(TDSReader tdsReader) throws SQLServerException {
                // If no prepared statement handle is expected at this time
                // then don't consume this OUT parameter as it does not contain
                // a prepared statement handle.
                if (!expectPrepStmtHandle)
                    return super.onRetValue(tdsReader);

                // If a prepared statement handle is expected then consume it
                // and continue processing.
                expectPrepStmtHandle = false;
                Parameter param = new Parameter(
                        Util.shouldHonorAEForParameters(stmtColumnEncriptionSetting, connection));
                param.skipRetValStatus(tdsReader);

                setPreparedStatementHandle(param.getInt(tdsReader, statement));

                // Cache the reference to the newly created handle, NOT for cursorable handles.
                if (null == cachedPreparedStatementHandle && !isCursorable(executeMethod)) {
                    cachedPreparedStatementHandle = connection.registerCachedPreparedStatementHandle(
                            new CityHash128Key(preparedSQL, preparedTypeDefinitions), prepStmtHandle,
                            executedSqlDirectly);
                }

                param.skipValue(tdsReader, true);
                if (getStatementLogger().isLoggable(java.util.logging.Level.FINER))
                    getStatementLogger().finer(toString() + ": Setting PreparedHandle:" + prepStmtHandle);

                return true;
            }
        }

        if (expectPrepStmtHandle || expectCursorOutParams) {
            TDSParser.parse(tdsReader, new PrepStmtExecOutParamHandler(this));
            return true;
        }

        return false;
    }

    /**
     * Sends the statement parameters by RPC.
     */
    void sendParamsByRPC(TDSWriter tdsWriter, Parameter[] params, boolean bReturnValueSyntax,
            boolean callRpcDirectly) throws SQLServerException {
        char[] cParamName;
        int index = 0;
        if (bReturnValueSyntax && !isCursorable(executeMethod) && !isTVPType && callRpcDirectly) {
            returnParam = params[index];
            params[index].setReturnValue(true);
            index++;
        }
        for (; index < params.length; index++) {
            if (JDBCType.TVP == params[index].getJdbcType()) {
                cParamName = new char[10];
                int paramNameLen = SQLServerConnection.makeParamName(index, cParamName, 0, false);
                tdsWriter.writeByte((byte) paramNameLen);
                tdsWriter.writeString(new String(cParamName, 0, paramNameLen));
            }
            params[index].sendByRPC(tdsWriter, callRpcDirectly, this);
        }
    }

    private void buildServerCursorPrepExecParams(TDSWriter tdsWriter) throws SQLServerException {
        if (getStatementLogger().isLoggable(java.util.logging.Level.FINE))
            getStatementLogger().fine(toString() + ": calling sp_cursorprepexec: PreparedHandle:"
                    + getPreparedStatementHandle() + ", SQL:" + preparedSQL);

        expectPrepStmtHandle = true;
        executedSqlDirectly = false;
        expectCursorOutParams = true;
        outParamIndexAdjustment = 7;

        tdsWriter.writeShort((short) 0xFFFF); // procedure name length -> use ProcIDs
        tdsWriter.writeShort(TDS.PROCID_SP_CURSORPREPEXEC);
        tdsWriter.writeByte((byte) 0); // RPC procedure option 1
        tdsWriter.writeByte((byte) 0); // RPC procedure option 2
        tdsWriter.sendEnclavePackage(preparedSQL, enclaveCEKs);

        // <prepared handle>
        // IN (reprepare): Old handle to unprepare before repreparing
        // OUT: The newly prepared handle
        tdsWriter.writeRPCInt(null, getPreparedStatementHandle(), true);
        resetPrepStmtHandle(false);

        // <cursor> OUT
        tdsWriter.writeRPCInt(null, 0, true); // cursor ID (OUTPUT)

        // <formal parameter defn> IN
        tdsWriter.writeRPCStringUnicode((preparedTypeDefinitions.length() > 0) ? preparedTypeDefinitions : null);

        // <stmt> IN
        tdsWriter.writeRPCStringUnicode(preparedSQL);

        // <scrollopt> IN
        // Note: we must strip out SCROLLOPT_PARAMETERIZED_STMT if we don't
        // actually have any parameters.
        tdsWriter.writeRPCInt(null, getResultSetScrollOpt()
                & ~((0 == preparedTypeDefinitions.length()) ? TDS.SCROLLOPT_PARAMETERIZED_STMT : 0), false);

        // <ccopt> IN
        tdsWriter.writeRPCInt(null, getResultSetCCOpt(), false);

        // <rowcount> OUT
        tdsWriter.writeRPCInt(null, 0, true);
    }

    private void buildRPCExecParams(TDSWriter tdsWriter) throws SQLServerException {
        if (getStatementLogger().isLoggable(java.util.logging.Level.FINE)) {
            getStatementLogger().fine(toString() + ": calling PROC" + ", SQL:" + preparedSQL);
        }

        expectPrepStmtHandle = false;
        executedSqlDirectly = true;
        expectCursorOutParams = false;
        outParamIndexAdjustment = 0;
        tdsWriter.writeShort((short) procedureName.length()); // procedure name length
        tdsWriter.writeString(procedureName);
        if (connection.isAEv2()) {
            tdsWriter.sendEnclavePackage(preparedSQL, enclaveCEKs);
        }

        tdsWriter.writeByte((byte) 0); // RPC procedure option 1
        tdsWriter.writeByte((byte) 0); // RPC procedure option 2
    }

    private void buildPrepParams(TDSWriter tdsWriter) throws SQLServerException {
        if (getStatementLogger().isLoggable(java.util.logging.Level.FINE))
            getStatementLogger().fine(toString() + ": calling sp_prepare: PreparedHandle:"
                    + getPreparedStatementHandle() + ", SQL:" + preparedSQL);

        expectPrepStmtHandle = true;
        executedSqlDirectly = false;
        expectCursorOutParams = false;
        outParamIndexAdjustment = 4;

        tdsWriter.writeShort((short) 0xFFFF); // procedure name length -> use ProcIDs
        tdsWriter.writeShort(TDS.PROCID_SP_PREPARE);
        tdsWriter.writeByte((byte) 0);
        tdsWriter.writeByte((byte) 0);
        tdsWriter.sendEnclavePackage(preparedSQL, enclaveCEKs);

        tdsWriter.writeRPCInt(null, getPreparedStatementHandle(), true);
        resetPrepStmtHandle(false);

        tdsWriter.writeRPCStringUnicode((preparedTypeDefinitions.length() > 0) ? preparedTypeDefinitions : null);

        tdsWriter.writeRPCStringUnicode(preparedSQL);
        tdsWriter.writeRPCInt(null, 1, false);
    }

    private void buildPrepExecParams(TDSWriter tdsWriter) throws SQLServerException {
        if (getStatementLogger().isLoggable(java.util.logging.Level.FINE))
            getStatementLogger().fine(toString() + ": calling sp_prepexec: PreparedHandle:"
                    + getPreparedStatementHandle() + ", SQL:" + preparedSQL);

        expectPrepStmtHandle = true;
        executedSqlDirectly = true;
        expectCursorOutParams = false;
        outParamIndexAdjustment = 3;

        tdsWriter.writeShort((short) 0xFFFF); // procedure name length -> use ProcIDs
        tdsWriter.writeShort(TDS.PROCID_SP_PREPEXEC);
        tdsWriter.writeByte((byte) 0); // RPC procedure option 1
        tdsWriter.writeByte((byte) 0); // RPC procedure option 2
        tdsWriter.sendEnclavePackage(preparedSQL, enclaveCEKs);

        // <prepared handle>
        // IN (reprepare): Old handle to unprepare before repreparing
        // OUT: The newly prepared handle
        tdsWriter.writeRPCInt(null, getPreparedStatementHandle(), true);
        resetPrepStmtHandle(false);

        // <formal parameter defn> IN
        tdsWriter.writeRPCStringUnicode((preparedTypeDefinitions.length() > 0) ? preparedTypeDefinitions : null);

        // <stmt> IN
        tdsWriter.writeRPCStringUnicode(preparedSQL);
    }

    private void buildExecSQLParams(TDSWriter tdsWriter) throws SQLServerException {
        if (getStatementLogger().isLoggable(java.util.logging.Level.FINE))
            getStatementLogger().fine(toString() + ": calling sp_executesql: SQL:" + preparedSQL);

        expectPrepStmtHandle = false;
        executedSqlDirectly = true;
        expectCursorOutParams = false;
        outParamIndexAdjustment = 2;

        tdsWriter.writeShort((short) 0xFFFF); // procedure name length -> use ProcIDs
        tdsWriter.writeShort(TDS.PROCID_SP_EXECUTESQL);
        tdsWriter.writeByte((byte) 0); // RPC procedure option 1
        tdsWriter.writeByte((byte) 0); // RPC procedure option 2
        tdsWriter.sendEnclavePackage(preparedSQL, enclaveCEKs);

        // No handle used.
        resetPrepStmtHandle(false);

        // <stmt> IN
        tdsWriter.writeRPCStringUnicode(preparedSQL);

        // <formal parameter defn> IN
        if (preparedTypeDefinitions.length() > 0)
            tdsWriter.writeRPCStringUnicode(preparedTypeDefinitions);
    }

    private void buildServerCursorExecParams(TDSWriter tdsWriter) throws SQLServerException {
        if (getStatementLogger().isLoggable(java.util.logging.Level.FINE))
            getStatementLogger().fine(toString() + ": calling sp_cursorexecute: PreparedHandle:"
                    + getPreparedStatementHandle() + ", SQL:" + preparedSQL);

        expectPrepStmtHandle = false;
        executedSqlDirectly = false;
        expectCursorOutParams = true;
        outParamIndexAdjustment = 5;

        tdsWriter.writeShort((short) 0xFFFF); // procedure name length -> use ProcIDs
        tdsWriter.writeShort(TDS.PROCID_SP_CURSOREXECUTE);
        tdsWriter.writeByte((byte) 0); // RPC procedure option 1
        tdsWriter.writeByte((byte) 0); // RPC procedure option 2 */
        tdsWriter.sendEnclavePackage(preparedSQL, enclaveCEKs);

        // <handle> IN
        assert hasPreparedStatementHandle();
        tdsWriter.writeRPCInt(null, getPreparedStatementHandle(), false);

        // <cursor> OUT
        tdsWriter.writeRPCInt(null, 0, true);

        // <scrollopt> IN
        tdsWriter.writeRPCInt(null, getResultSetScrollOpt() & ~TDS.SCROLLOPT_PARAMETERIZED_STMT, false);

        // <ccopt> IN
        tdsWriter.writeRPCInt(null, getResultSetCCOpt(), false);

        // <rowcount> OUT
        tdsWriter.writeRPCInt(null, 0, true);
    }

    private void buildExecParams(TDSWriter tdsWriter) throws SQLServerException {
        if (getStatementLogger().isLoggable(java.util.logging.Level.FINE))
            getStatementLogger().fine(toString() + ": calling sp_execute: PreparedHandle:"
                    + getPreparedStatementHandle() + ", SQL:" + preparedSQL);

        expectPrepStmtHandle = false;
        executedSqlDirectly = true;
        expectCursorOutParams = false;
        outParamIndexAdjustment = 1;

        tdsWriter.writeShort((short) 0xFFFF); // procedure name length -> use ProcIDs
        tdsWriter.writeShort(TDS.PROCID_SP_EXECUTE);
        tdsWriter.writeByte((byte) 0); // RPC procedure option 1
        tdsWriter.writeByte((byte) 0); // RPC procedure option 2 */
        tdsWriter.sendEnclavePackage(preparedSQL, enclaveCEKs);

        // <handle> IN
        assert hasPreparedStatementHandle();
        tdsWriter.writeRPCInt(null, getPreparedStatementHandle(), false);
    }

    private void getParameterEncryptionMetadata(Parameter[] params) throws SQLServerException {
        /*
         * The parameter list is created from the data types provided by the user for the parameters. the data types do
         * not need to be the same as in the table definition. Also, when string is sent to an int field, the parameter
         * is defined as nvarchar(<size of string>). Same for varchar datatypes, exact length is used.
         */
        assert connection != null : "Connection should not be null";

        try (Statement stmt = connection.prepareCall("exec sp_describe_parameter_encryption ?,?")) {
            if (getStatementLogger().isLoggable(java.util.logging.Level.FINE)) {
                getStatementLogger().fine(
                        "Calling stored procedure sp_describe_parameter_encryption to get parameter encryption information.");
            }

            // pass registered custom provider to stmt
            if (this.hasColumnEncryptionKeyStoreProvidersRegistered()) {
                ((SQLServerCallableStatement) stmt).registerColumnEncryptionKeyStoreProvidersOnStatement(
                        this.statementColumnEncryptionKeyStoreProviders);
            }

            ((SQLServerCallableStatement) stmt).isInternalEncryptionQuery = true;
            ((SQLServerCallableStatement) stmt).setNString(1, preparedSQL);
            ((SQLServerCallableStatement) stmt).setNString(2, preparedTypeDefinitions);
            try (ResultSet rs = ((SQLServerCallableStatement) stmt).executeQueryInternal()) {
                if (null == rs) {
                    // No results. Meaning no parameter.
                    // Should never happen.
                    return;
                }

                Map<Integer, CekTableEntry> cekList = new HashMap<>();
                CekTableEntry cekEntry = null;
                while (rs.next()) {
                    int currentOrdinal = rs.getInt(DescribeParameterEncryptionResultSet1.KEYORDINAL.value());
                    if (!cekList.containsKey(currentOrdinal)) {
                        cekEntry = new CekTableEntry(currentOrdinal);
                        cekList.put(cekEntry.ordinal, cekEntry);
                    } else {
                        cekEntry = cekList.get(currentOrdinal);
                    }
                    cekEntry.add(rs.getBytes(DescribeParameterEncryptionResultSet1.ENCRYPTEDKEY.value()),
                            rs.getInt(DescribeParameterEncryptionResultSet1.DBID.value()),
                            rs.getInt(DescribeParameterEncryptionResultSet1.KEYID.value()),
                            rs.getInt(DescribeParameterEncryptionResultSet1.KEYVERSION.value()),
                            rs.getBytes(DescribeParameterEncryptionResultSet1.KEYMDVERSION.value()),
                            rs.getString(DescribeParameterEncryptionResultSet1.KEYPATH.value()),
                            rs.getString(DescribeParameterEncryptionResultSet1.PROVIDERNAME.value()),
                            rs.getString(DescribeParameterEncryptionResultSet1.KEYENCRYPTIONALGORITHM.value()));
                }
                if (getStatementLogger().isLoggable(java.util.logging.Level.FINE)) {
                    getStatementLogger().fine("Matadata of CEKs is retrieved.");
                }

                // Process the second resultset.
                if (!stmt.getMoreResults()) {
                    throw new SQLServerException(this,
                            SQLServerException.getErrString("R_UnexpectedDescribeParamFormat"), null, 0, false);
                }

                // Parameter count in the result set.
                int paramCount = 0;
                try (ResultSet secondRs = stmt.getResultSet()) {
                    while (secondRs.next()) {
                        paramCount++;
                        String paramName = secondRs
                                .getString(DescribeParameterEncryptionResultSet2.PARAMETERNAME.value());
                        int paramIndex = parameterNames.indexOf(paramName);
                        int cekOrdinal = secondRs
                                .getInt(DescribeParameterEncryptionResultSet2.COLUMNENCRYPTIONKEYORDINAL.value());
                        cekEntry = cekList.get(cekOrdinal);

                        // cekEntry will be null if none of the parameters are encrypted.
                        if ((null != cekEntry) && (cekList.size() < cekOrdinal)) {
                            MessageFormat form = new MessageFormat(
                                    SQLServerException.getErrString("R_InvalidEncryptionKeyOrdinal"));
                            Object[] msgArgs = {cekOrdinal, cekEntry.getSize()};
                            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
                        }
                        SQLServerEncryptionType encType = SQLServerEncryptionType.of((byte) secondRs
                                .getInt(DescribeParameterEncryptionResultSet2.COLUMNENCRYPTIONTYPE.value()));
                        if (SQLServerEncryptionType.PLAINTEXT != encType) {
                            params[paramIndex].cryptoMeta = new CryptoMetadata(cekEntry, (short) cekOrdinal,
                                    (byte) secondRs.getInt(
                                            DescribeParameterEncryptionResultSet2.COLUMNENCRYPTIONALGORITHM.value()),
                                    null, encType.value, (byte) secondRs.getInt(
                                            DescribeParameterEncryptionResultSet2.NORMALIZATIONRULEVERSION.value()));

                            SQLServerStatement statement = (SQLServerStatement) stmt;
                            // Decrypt the symmetric key.(This will also validate and throw if needed).
                            SQLServerSecurityUtility.decryptSymmetricKey(params[paramIndex].cryptoMeta, connection,
                                    statement);
                        } else {
                            if (params[paramIndex].getForceEncryption()) {
                                MessageFormat form = new MessageFormat(SQLServerException
                                        .getErrString("R_ForceEncryptionTrue_HonorAETrue_UnencryptedColumn"));
                                Object[] msgArgs = {userSQL, paramIndex + 1};
                                SQLServerException.makeFromDriverError(connection, this, form.format(msgArgs), null,
                                        true);
                            }
                        }
                    }
                    if (getStatementLogger().isLoggable(java.util.logging.Level.FINE)) {
                        getStatementLogger().fine("Parameter encryption metadata is set.");
                    }
                }

                if (paramCount != params.length) {
                    // Encryption metadata wasn't sent by the server.
                    // We expect the metadata to be sent for all the parameters in the original
                    // sp_describe_parameter_encryption.
                    // For parameters that don't need encryption, the encryption type is set to plaintext.
                    MessageFormat form = new MessageFormat(
                            SQLServerException.getErrString("R_MissingParamEncryptionMetadata"));
                    Object[] msgArgs = {userSQL};
                    throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
                }
            }
        } catch (SQLException e) {
            if (e instanceof SQLServerException) {
                throw (SQLServerException) e;
            } else {
                throw new SQLServerException(SQLServerException.getErrString("R_UnableRetrieveParameterMetadata"), null,
                        0, e);
            }
        }

        connection.resetCurrentCommand();
    }

    /**
     * Manages re-using cached handles.
     */
    private boolean reuseCachedHandle(boolean hasNewTypeDefinitions, boolean discardCurrentCacheItem) {
        // No re-use of caching for cursorable statements (statements that WILL use sp_cursor*)
        if (isCursorable(executeMethod))
            return false;

        // If current cache items needs to be discarded or New type definitions found with existing cached handle
        // reference then deregister cached
        // handle.
        if (discardCurrentCacheItem || hasNewTypeDefinitions) {
            if (null != cachedPreparedStatementHandle && (discardCurrentCacheItem
                    || (hasPreparedStatementHandle() && prepStmtHandle == cachedPreparedStatementHandle.getHandle()))) {
                cachedPreparedStatementHandle.removeReference();
            }

            // Make sure the cached handle does not get re-used more if it should be discarded
            resetPrepStmtHandle(discardCurrentCacheItem);
            cachedPreparedStatementHandle = null;
            if (discardCurrentCacheItem)
                return false;
        }

        // Check for new cache reference.
        if (null == cachedPreparedStatementHandle) {
            PreparedStatementHandle cachedHandle = connection
                    .getCachedPreparedStatementHandle(new CityHash128Key(preparedSQL, preparedTypeDefinitions));
            // If handle was found then re-use, only if AE is not on and is not a batch query with new type definitions
            // (We shouldn't reuse handle
            // if it is batch query and has new type definition, or if it is on, make sure encryptionMetadataIsRetrieved
            // is retrieved.
            if ((null != cachedHandle)
                    && (!connection.isColumnEncryptionSettingEnabled()
                            || (connection.isColumnEncryptionSettingEnabled() && encryptionMetadataIsRetrieved))
                    && cachedHandle.tryAddReference()) {
                setPreparedStatementHandle(cachedHandle.getHandle());
                cachedPreparedStatementHandle = cachedHandle;
                return true;
            }
        }
        return false;
    }

    /**
     * enclave CEKs
     */
    private ArrayList<byte[]> enclaveCEKs;

    private boolean doPrepExec(TDSWriter tdsWriter, Parameter[] params, boolean hasNewTypeDefinitions,
            boolean hasExistingTypeDefinitions, TDSCommand command) throws SQLServerException {

        boolean needsPrepare = (hasNewTypeDefinitions && hasExistingTypeDefinitions) || !hasPreparedStatementHandle();
        boolean isPrepareMethodSpPrepExec = connection.getPrepareMethod().equals(PrepareMethod.PREPEXEC.toString());
        boolean callRpcDirectly = callRPCDirectly(params);

        // Cursors don't use statement pooling.
        if (isCursorable(executeMethod)) {
            if (needsPrepare)
                buildServerCursorPrepExecParams(tdsWriter);
            else
                buildServerCursorExecParams(tdsWriter);
        } else {
            // if it is a parameterized stored procedure call and is not TVP, use sp_execute directly.
            if (needsPrepare && callRpcDirectly) {
                buildRPCExecParams(tdsWriter);
            }
            // Move overhead of needing to do prepare & unprepare to only use cases that need more than one execution.
            // First execution, use sp_executesql, optimizing for assumption we will not re-use statement.
            else if (needsPrepare && !connection.getEnablePrepareOnFirstPreparedStatementCall()
                    && !isExecutedAtLeastOnce) {
                buildExecSQLParams(tdsWriter);
                isExecutedAtLeastOnce = true;
            } else if (needsPrepare) { // Second execution, use prepared statements since we seem to be re-using it.
                if (isPrepareMethodSpPrepExec) { // If true, we're using sp_prepexec.
                    buildPrepExecParams(tdsWriter);
                } else { // Otherwise, we're using sp_prepare instead of sp_prepexec.
                    isSpPrepareExecuted = true;
                    // If we're preparing for a statement in a batch we just need to call sp_prepare because in the
                    // "batching" code it will start another tds request to execute the statement after preparing.
                    if (executeMethod == EXECUTE_BATCH) {
                        buildPrepParams(tdsWriter);
                        return needsPrepare;
                    } else { // Otherwise, if it is not a batch query, then prepare and start new TDS request to execute
                             // the statement.
                        isSpPrepareExecuted = false;
                        doPrep(tdsWriter, command);
                        command.startRequest(TDS.PKT_RPC);
                        buildExecParams(tdsWriter);
                    }
                }
            } else {
                buildExecParams(tdsWriter);
            }
        }

        sendParamsByRPC(tdsWriter, params, bReturnValueSyntax, callRpcDirectly);

        return needsPrepare;
    }

    /**
     * Checks if we should call RPC directly for stored procedures
     *
     * @param params
     * @return
     * @throws SQLServerException
     */
    boolean callRPCDirectly(Parameter[] params) throws SQLServerException {
        int paramCount = SQLServerConnection.countParams(userSQL);

        // In order to execute sprocs directly the following must be true:
        // 1. There must be a sproc name
        // 2. There must be parameters
        // 3. Parameters must not be a TVP type
        // 4. Compliant CALL escape syntax
        // If isExecEscapeSyntax is true, EXEC escape syntax is used then use prior behaviour of
        // wrapping call to execute the procedure
        return (null != procedureName && paramCount != 0 && !isTVPType(params) && isCallEscapeSyntax
                && !isExecEscapeSyntax);
    }

    /**
     * Checks if the parameter is a TVP type.
     *
     * @param params
     * @return
     * @throws SQLServerException
     */
    private boolean isTVPType(Parameter[] params) throws SQLServerException {
        for (int i = 0; i < params.length; i++) {
            if (JDBCType.TVP == params[i].getJdbcType()) {
                isTVPType = true;
                return true;
            }
        }
        return false;
    }

    private boolean isExecEscapeSyntax(String sql) {
        return execEscapePattern.matcher(sql).find();
    }

    private boolean isCallEscapeSyntax(String sql) {
        return callEscapePattern.matcher(sql).find();
    }

    /**
     * Executes sp_prepare to prepare a parameterized statement and sets the prepared statement handle
     *
     * @param tdsWriter
     *        TDS writer to write sp_prepare params to
     * @throws SQLServerException
     */
    private void doPrep(TDSWriter tdsWriter, TDSCommand command) throws SQLServerException {
        buildPrepParams(tdsWriter);
        ensureExecuteResultsReader(command.startResponse(getIsResponseBufferingAdaptive()));
        command.processResponse(resultsReader());
    }

    @Override
    public final java.sql.ResultSetMetaData getMetaData() throws SQLServerException, SQLTimeoutException {
        loggerExternal.entering(getClassNameLogging(), "getMetaData");
        checkClosed();
        boolean rsclosed = false;
        java.sql.ResultSetMetaData rsmd = null;
        try {
            // if the result is closed, cant get the metadata from it.
            if (resultSet != null)
                resultSet.checkClosed();
        } catch (SQLServerException e) {
            rsclosed = true;
        }
        if (resultSet == null || rsclosed) {
            SQLServerResultSet emptyResultSet = buildExecuteMetaData();
            if (null != emptyResultSet)
                rsmd = emptyResultSet.getMetaData();
        } else {
            rsmd = resultSet.getMetaData();
        }
        loggerExternal.exiting(getClassNameLogging(), "getMetaData", rsmd);
        return rsmd;
    }

    /**
     * Returns meta data for the statement before executing it. This is called in cases where the driver needs the meta
     * data prior to executing the statement.
     * 
     * @throws SQLServerException
     * @return the result set containing the meta data
     */
    private SQLServerResultSet buildExecuteMetaData() throws SQLServerException, SQLTimeoutException {
        String fmtSQL = userSQL;

        SQLServerResultSet emptyResultSet = null;
        try {
            fmtSQL = replaceMarkerWithNull(fmtSQL);
            internalStmt = (SQLServerStatement) connection.createStatement();
            emptyResultSet = internalStmt.executeQueryInternal("set fmtonly on " + fmtSQL + "\nset fmtonly off");
        } catch (SQLServerException sqle) {
            // Ignore empty result set errors, otherwise propagate the server error.
            if (!sqle.getMessage().equals(SQLServerException.getErrString("R_noResultset"))) {
                throw sqle;
            }
        }
        return emptyResultSet;
    }

    /* -------------- JDBC API Implementation ------------------ */

    /**
     * Sets the parameter value for a statement.
     *
     * @param index
     *        The index of the parameter to set starting at 1.
     * @return A reference the to Parameter object created or referenced.
     * @exception SQLServerException
     *            The index specified was outside the number of parameters for the statement.
     */
    final Parameter setterGetParam(int index) throws SQLServerException {
        if (!connection.getUseFlexibleCallableStatements() && isSetByName && isSetByIndex) {
            SQLServerException.makeFromDriverError(connection, this,
                    SQLServerException.getErrString("R_noNamedAndIndexedParameters"), null, false);
        }

        if (index < 1 || index > inOutParam.length) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_indexOutOfRange"));
            Object[] msgArgs = {index};
            SQLServerException.makeFromDriverError(connection, this, form.format(msgArgs), "07009", false);
        }

        return inOutParam[index - 1];
    }

    final void setValue(int parameterIndex, JDBCType jdbcType, Object value, JavaType javaType,
            String tvpName) throws SQLServerException {
        setterGetParam(parameterIndex).setValue(jdbcType, value, javaType, null, null, null, null, connection, false,
                stmtColumnEncriptionSetting, parameterIndex, userSQL, tvpName);
    }

    final void setValue(int parameterIndex, JDBCType jdbcType, Object value, JavaType javaType,
            boolean forceEncrypt) throws SQLServerException {
        setterGetParam(parameterIndex).setValue(jdbcType, value, javaType, null, null, null, null, connection,
                forceEncrypt, stmtColumnEncriptionSetting, parameterIndex, userSQL, null);
    }

    final void setValue(int parameterIndex, JDBCType jdbcType, Object value, JavaType javaType, Integer precision,
            Integer scale, boolean forceEncrypt) throws SQLServerException {
        setterGetParam(parameterIndex).setValue(jdbcType, value, javaType, null, null, precision, scale, connection,
                forceEncrypt, stmtColumnEncriptionSetting, parameterIndex, userSQL, null);
    }

    final void setValue(int parameterIndex, JDBCType jdbcType, Object value, JavaType javaType, Calendar cal,
            boolean forceEncrypt) throws SQLServerException {
        setterGetParam(parameterIndex).setValue(jdbcType, value, javaType, null, cal, null, null, connection,
                forceEncrypt, stmtColumnEncriptionSetting, parameterIndex, userSQL, null);
    }

    final void setStream(int parameterIndex, StreamType streamType, Object streamValue, JavaType javaType,
            long length) throws SQLServerException {
        setterGetParam(parameterIndex).setValue(streamType.getJDBCType(), streamValue, javaType,
                new StreamSetterArgs(streamType, length), null, null, null, connection, false,
                stmtColumnEncriptionSetting, parameterIndex, userSQL, null);
    }

    final void setSQLXMLInternal(int parameterIndex, SQLXML value) throws SQLServerException {
        setterGetParam(parameterIndex).setValue(JDBCType.SQLXML, value, JavaType.SQLXML,
                new StreamSetterArgs(StreamType.SQLXML, DataTypes.UNKNOWN_STREAM_LENGTH), null, null, null, connection,
                false, stmtColumnEncriptionSetting, parameterIndex, userSQL, null);
    }

    @Override
    public final void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setAsciiStream", new Object[] {parameterIndex, x});
        checkClosed();
        setStream(parameterIndex, StreamType.ASCII, x, JavaType.INPUTSTREAM, DataTypes.UNKNOWN_STREAM_LENGTH);
        loggerExternal.exiting(getClassNameLogging(), "setAsciiStream");
    }

    @Override
    public final void setAsciiStream(int n, java.io.InputStream x, int length) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setAsciiStream", new Object[] {n, x, length});
        checkClosed();
        setStream(n, StreamType.ASCII, x, JavaType.INPUTSTREAM, length);
        loggerExternal.exiting(getClassNameLogging(), "setAsciiStream");
    }

    @Override
    public final void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setAsciiStream", new Object[] {parameterIndex, x, length});
        checkClosed();
        setStream(parameterIndex, StreamType.ASCII, x, JavaType.INPUTSTREAM, length);
        loggerExternal.exiting(getClassNameLogging(), "setAsciiStream");
    }

    @Override
    public final void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBigDecimal", new Object[] {parameterIndex, x});
        checkClosed();
        setValue(parameterIndex, JDBCType.DECIMAL, x, JavaType.BIGDECIMAL, false);
        loggerExternal.exiting(getClassNameLogging(), "setBigDecimal");
    }

    @Override
    public final void setBigDecimal(int parameterIndex, BigDecimal x, int precision,
            int scale) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBigDecimal",
                    new Object[] {parameterIndex, x, precision, scale});
        checkClosed();
        setValue(parameterIndex, JDBCType.DECIMAL, x, JavaType.BIGDECIMAL, precision, scale, false);
        loggerExternal.exiting(getClassNameLogging(), "setBigDecimal");
    }

    @Override
    public final void setBigDecimal(int parameterIndex, BigDecimal x, int precision, int scale,
            boolean forceEncrypt) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBigDecimal",
                    new Object[] {parameterIndex, x, precision, scale, forceEncrypt});
        checkClosed();
        setValue(parameterIndex, JDBCType.DECIMAL, x, JavaType.BIGDECIMAL, precision, scale, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setBigDecimal");
    }

    @Override
    public final void setMoney(int n, BigDecimal x) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setMoney", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.MONEY, x, JavaType.BIGDECIMAL, false);
        loggerExternal.exiting(getClassNameLogging(), "setMoney");
    }

    @Override
    public final void setMoney(int n, BigDecimal x, boolean forceEncrypt) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setMoney", new Object[] {n, x, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.MONEY, x, JavaType.BIGDECIMAL, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setMoney");
    }

    @Override
    public final void setSmallMoney(int n, BigDecimal x) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setSmallMoney", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.SMALLMONEY, x, JavaType.BIGDECIMAL, false);
        loggerExternal.exiting(getClassNameLogging(), "setSmallMoney");
    }

    @Override
    public final void setSmallMoney(int n, BigDecimal x, boolean forceEncrypt) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setSmallMoney", new Object[] {n, x, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.SMALLMONEY, x, JavaType.BIGDECIMAL, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setSmallMoney");
    }

    @Override
    public final void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBinaryStreaml", new Object[] {parameterIndex, x});
        checkClosed();
        setStream(parameterIndex, StreamType.BINARY, x, JavaType.INPUTSTREAM, DataTypes.UNKNOWN_STREAM_LENGTH);
        loggerExternal.exiting(getClassNameLogging(), "setBinaryStream");
    }

    @Override
    public final void setBinaryStream(int n, java.io.InputStream x, int length) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBinaryStream", new Object[] {n, x, length});
        checkClosed();
        setStream(n, StreamType.BINARY, x, JavaType.INPUTSTREAM, length);
        loggerExternal.exiting(getClassNameLogging(), "setBinaryStream");
    }

    @Override
    public final void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBinaryStream", new Object[] {parameterIndex, x, length});
        checkClosed();
        setStream(parameterIndex, StreamType.BINARY, x, JavaType.INPUTSTREAM, length);
        loggerExternal.exiting(getClassNameLogging(), "setBinaryStream");
    }

    @Override
    public final void setBoolean(int n, boolean x) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBoolean", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.BIT, x, JavaType.BOOLEAN, false);
        loggerExternal.exiting(getClassNameLogging(), "setBoolean");
    }

    @Override
    public final void setBoolean(int n, boolean x, boolean forceEncrypt) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBoolean", new Object[] {n, x, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.BIT, x, JavaType.BOOLEAN, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setBoolean");
    }

    @Override
    public final void setByte(int n, byte x) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setByte", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.TINYINT, x, JavaType.BYTE, false);
        loggerExternal.exiting(getClassNameLogging(), "setByte");
    }

    @Override
    public final void setByte(int n, byte x, boolean forceEncrypt) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setByte", new Object[] {n, x, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.TINYINT, x, JavaType.BYTE, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setByte");
    }

    @Override
    public final void setBytes(int n, byte[] x) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBytes", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.BINARY, x, JavaType.BYTEARRAY, false);
        loggerExternal.exiting(getClassNameLogging(), "setBytes");
    }

    @Override
    public final void setBytes(int n, byte[] x, boolean forceEncrypt) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBytes", new Object[] {n, x, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.BINARY, x, JavaType.BYTEARRAY, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setBytes");
    }

    @Override
    public final void setUniqueIdentifier(int index, String guid) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setUniqueIdentifier", new Object[] {index, guid});
        checkClosed();
        setValue(index, JDBCType.GUID, guid, JavaType.STRING, false);
        loggerExternal.exiting(getClassNameLogging(), "setUniqueIdentifier");
    }

    @Override
    public final void setUniqueIdentifier(int index, String guid, boolean forceEncrypt) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setUniqueIdentifier",
                    new Object[] {index, guid, forceEncrypt});
        checkClosed();
        setValue(index, JDBCType.GUID, guid, JavaType.STRING, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setUniqueIdentifier");
    }

    @Override
    public final void setDouble(int n, double x) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setDouble", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.DOUBLE, x, JavaType.DOUBLE, false);
        loggerExternal.exiting(getClassNameLogging(), "setDouble");
    }

    @Override
    public final void setDouble(int n, double x, boolean forceEncrypt) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setDouble", new Object[] {n, x, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.DOUBLE, x, JavaType.DOUBLE, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setDouble");
    }

    @Override
    public final void setFloat(int n, float x) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setFloat", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.REAL, x, JavaType.FLOAT, false);
        loggerExternal.exiting(getClassNameLogging(), "setFloat");
    }

    @Override
    public final void setFloat(int n, float x, boolean forceEncrypt) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setFloat", new Object[] {n, x, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.REAL, x, JavaType.FLOAT, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setFloat");
    }

    @Override
    public final void setGeometry(int n, Geometry x) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setGeometry", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.GEOMETRY, x, JavaType.STRING, false);
        loggerExternal.exiting(getClassNameLogging(), "setGeometry");
    }

    @Override
    public final void setGeography(int n, Geography x) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setGeography", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.GEOGRAPHY, x, JavaType.STRING, false);
        loggerExternal.exiting(getClassNameLogging(), "setGeography");
    }

    @Override
    public final void setInt(int n, int value) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setInt", new Object[] {n, value});
        checkClosed();
        setValue(n, JDBCType.INTEGER, value, JavaType.INTEGER, false);
        loggerExternal.exiting(getClassNameLogging(), "setInt");
    }

    @Override
    public final void setInt(int n, int value, boolean forceEncrypt) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setInt", new Object[] {n, value, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.INTEGER, value, JavaType.INTEGER, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setInt");
    }

    @Override
    public final void setLong(int n, long x) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setLong", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.BIGINT, x, JavaType.LONG, false);
        loggerExternal.exiting(getClassNameLogging(), "setLong");
    }

    @Override
    public final void setLong(int n, long x, boolean forceEncrypt) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setLong", new Object[] {n, x, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.BIGINT, x, JavaType.LONG, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setLong");
    }

    @Override
    public final void setNull(int index, int jdbcType) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setNull", new Object[] {index, jdbcType});
        checkClosed();
        setObject(setterGetParam(index), null, JavaType.OBJECT, JDBCType.of(jdbcType), null, null, false, index, null);
        loggerExternal.exiting(getClassNameLogging(), "setNull");
    }

    final void setObjectNoType(int index, Object obj, boolean forceEncrypt) throws SQLServerException {
        // Default to the JDBC type of the parameter, determined by a previous setter call or through registerOutParam.
        // This avoids repreparing unnecessarily for null values.
        Parameter param = setterGetParam(index);
        JDBCType targetJDBCType = param.getJdbcType();
        String tvpName = null;

        if (null == obj) {
            // If the JDBC type of the parameter is UNKNOWN (i.e. this is the first time the parameter is being set),
            // then use a JDBC type the converts to most server types with a null value.
            if (JDBCType.UNKNOWN == targetJDBCType)
                targetJDBCType = JDBCType.CHAR;

            setObject(param, null, JavaType.OBJECT, targetJDBCType, null, null, forceEncrypt, index, null);
        } else {
            JavaType javaType = JavaType.of(obj);
            if (JavaType.TVP == javaType) {
                // May return null if called from preparedStatement.
                tvpName = getTVPNameFromObject(index, obj);

                if ((null == tvpName) && (obj instanceof ResultSet)) {
                    throw new SQLServerException(SQLServerException.getErrString("R_TVPnotWorkWithSetObjectResultSet"),
                            null);
                }
            }
            targetJDBCType = javaType.getJDBCType(SSType.UNKNOWN, targetJDBCType);

            if (JDBCType.UNKNOWN == targetJDBCType && obj instanceof java.util.UUID) {
                javaType = JavaType.STRING;
                targetJDBCType = JDBCType.GUID;
            }

            setObject(param, obj, javaType, targetJDBCType, null, null, forceEncrypt, index, tvpName);
        }
    }

    @Override
    public final void setObject(int index, Object obj) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setObject", new Object[] {index, obj});
        checkClosed();
        setObjectNoType(index, obj, false);
        loggerExternal.exiting(getClassNameLogging(), "setObject");
    }

    @Override
    public final void setObject(int n, Object obj, int jdbcType) throws SQLServerException {
        setByIndex();
        String tvpName = null;
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setObject", new Object[] {n, obj, jdbcType});
        checkClosed();
        if (microsoft.sql.Types.STRUCTURED == jdbcType) {
            tvpName = getTVPNameFromObject(n, obj);
        }
        setObject(setterGetParam(n), obj, JavaType.of(obj), JDBCType.of(jdbcType), null, null, false, n, tvpName);
        loggerExternal.exiting(getClassNameLogging(), "setObject");
    }

    @Override
    public final void setObject(int parameterIndex, Object x, int targetSqlType,
            int scaleOrLength) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setObject",
                    new Object[] {parameterIndex, x, targetSqlType, scaleOrLength});
        checkClosed();

        // scaleOrLength - for java.sql.Types.DECIMAL, java.sql.Types.NUMERIC or temporal types,
        // this is the number of digits after the decimal point. For Java Object types
        // InputStream and Reader, this is the length of the data in the stream or reader.
        // For all other types, this value will be ignored.

        setObject(setterGetParam(parameterIndex), x, JavaType.of(x), JDBCType.of(targetSqlType),
                (java.sql.Types.NUMERIC == targetSqlType || java.sql.Types.DECIMAL == targetSqlType
                        || java.sql.Types.TIMESTAMP == targetSqlType || java.sql.Types.TIME == targetSqlType
                        || microsoft.sql.Types.DATETIMEOFFSET == targetSqlType || InputStream.class.isInstance(x)
                        || Reader.class.isInstance(x)) ? scaleOrLength : null,
                null, false, parameterIndex, null);

        loggerExternal.exiting(getClassNameLogging(), "setObject");
    }

    @Override
    public final void setObject(int parameterIndex, Object x, int targetSqlType, Integer precision,
            int scale) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setObject",
                    new Object[] {parameterIndex, x, targetSqlType, precision, scale});
        checkClosed();

        // scale - for java.sql.Types.DECIMAL or java.sql.Types.NUMERIC types,
        // this is the number of digits after the decimal point. For Java Object types
        // InputStream and Reader, this is the length of the data in the stream or reader.
        // For all other types, this value will be ignored.

        setObject(setterGetParam(parameterIndex), x, JavaType.of(x), JDBCType.of(targetSqlType),
                (java.sql.Types.NUMERIC == targetSqlType || java.sql.Types.DECIMAL == targetSqlType
                        || InputStream.class.isInstance(x) || Reader.class.isInstance(x)) ? scale : null,
                precision, false, parameterIndex, null);

        loggerExternal.exiting(getClassNameLogging(), "setObject");
    }

    @Override
    public final void setObject(int parameterIndex, Object x, int targetSqlType, Integer precision, int scale,
            boolean forceEncrypt) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setObject",
                    new Object[] {parameterIndex, x, targetSqlType, precision, scale, forceEncrypt});
        checkClosed();

        // scale - for java.sql.Types.DECIMAL or java.sql.Types.NUMERIC types,
        // this is the number of digits after the decimal point. For Java Object types
        // InputStream and Reader, this is the length of the data in the stream or reader.
        // For all other types, this value will be ignored.

        setObject(setterGetParam(parameterIndex), x, JavaType.of(x), JDBCType.of(targetSqlType),
                (java.sql.Types.NUMERIC == targetSqlType || java.sql.Types.DECIMAL == targetSqlType
                        || InputStream.class.isInstance(x) || Reader.class.isInstance(x)) ? scale : null,
                precision, forceEncrypt, parameterIndex, null);

        loggerExternal.exiting(getClassNameLogging(), "setObject");
    }

    final void setObject(Parameter param, Object obj, JavaType javaType, JDBCType jdbcType, Integer scale,
            Integer precision, boolean forceEncrypt, int parameterIndex, String tvpName) throws SQLServerException {
        assert JDBCType.UNKNOWN != jdbcType;

        // For non-null values, infer the object's JDBC type from its Java type
        // and check whether the object is settable via the specified JDBC type.
        if ((null != obj) || (JavaType.TVP == javaType)) {
            // Returns the static JDBC type that is assigned to this java type (the parameters has no effect)
            JDBCType objectJDBCType = javaType.getJDBCType(SSType.UNKNOWN, jdbcType);

            // Check convertability of the value to the desired JDBC type.
            if (!objectJDBCType.convertsTo(jdbcType))
                DataTypes.throwConversionError(objectJDBCType.toString(), jdbcType.toString());

            StreamSetterArgs streamSetterArgs = null;

            switch (javaType) {
                case READER:
                    streamSetterArgs = new StreamSetterArgs(StreamType.CHARACTER, DataTypes.UNKNOWN_STREAM_LENGTH);
                    break;

                case INPUTSTREAM:
                    streamSetterArgs = new StreamSetterArgs(
                            jdbcType.isTextual() ? StreamType.CHARACTER : StreamType.BINARY,
                            DataTypes.UNKNOWN_STREAM_LENGTH);
                    break;

                case SQLXML:
                    streamSetterArgs = new StreamSetterArgs(StreamType.SQLXML, DataTypes.UNKNOWN_STREAM_LENGTH);
                    break;
                default:
                    // Do nothing
                    break;
            }

            // typeInfo is set as null
            param.setValue(jdbcType, obj, javaType, streamSetterArgs, null, precision, scale, connection, forceEncrypt,
                    stmtColumnEncriptionSetting, parameterIndex, userSQL, tvpName);
        }

        // For null values, use the specified JDBC type directly, with the exception
        // of unsupported JDBC types, which are mapped to BINARY so that they are minimally supported.
        else {
            assert JavaType.OBJECT == javaType;

            if (jdbcType.isUnsupported())
                jdbcType = JDBCType.BINARY;

            // typeInfo is set as null
            param.setValue(jdbcType, null, JavaType.OBJECT, null, null, precision, scale, connection, false,
                    stmtColumnEncriptionSetting, parameterIndex, userSQL, tvpName);
        }
    }

    @Override
    public final void setObject(int index, Object obj, SQLType jdbcType) throws SQLServerException {
        setObject(index, obj, jdbcType.getVendorTypeNumber());
    }

    @Override
    public final void setObject(int parameterIndex, Object x, SQLType targetSqlType,
            int scaleOrLength) throws SQLServerException {
        setObject(parameterIndex, x, targetSqlType.getVendorTypeNumber(), scaleOrLength);
    }

    @Override
    public final void setObject(int parameterIndex, Object x, SQLType targetSqlType, Integer precision,
            Integer scale) throws SQLServerException {
        setObject(parameterIndex, x, targetSqlType.getVendorTypeNumber(), precision, scale);
    }

    @Override
    public final void setObject(int parameterIndex, Object x, SQLType targetSqlType, Integer precision, Integer scale,
            boolean forceEncrypt) throws SQLServerException {
        setObject(parameterIndex, x, targetSqlType.getVendorTypeNumber(), precision, scale, forceEncrypt);
    }

    @Override
    public final void setShort(int index, short x) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setShort", new Object[] {index, x});
        checkClosed();
        setValue(index, JDBCType.SMALLINT, x, JavaType.SHORT, false);
        loggerExternal.exiting(getClassNameLogging(), "setShort");
    }

    @Override
    public final void setShort(int index, short x, boolean forceEncrypt) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setShort", new Object[] {index, x, forceEncrypt});
        checkClosed();
        setValue(index, JDBCType.SMALLINT, x, JavaType.SHORT, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setShort");
    }

    @Override
    public final void setString(int index, String str) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setString", new Object[] {index, str});
        checkClosed();
        setValue(index, JDBCType.VARCHAR, str, JavaType.STRING, false);
        loggerExternal.exiting(getClassNameLogging(), "setString");
    }

    @Override
    public final void setString(int index, String str, boolean forceEncrypt) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setString", new Object[] {index, str, forceEncrypt});
        checkClosed();
        setValue(index, JDBCType.VARCHAR, str, JavaType.STRING, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setString");
    }

    @Override
    public final void setNString(int parameterIndex, String value) throws SQLException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setNString", new Object[] {parameterIndex, value});
        checkClosed();
        setValue(parameterIndex, JDBCType.NVARCHAR, value, JavaType.STRING, false);
        loggerExternal.exiting(getClassNameLogging(), "setNString");
    }

    @Override
    public final void setNString(int parameterIndex, String value, boolean forceEncrypt) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setNString",
                    new Object[] {parameterIndex, value, forceEncrypt});
        checkClosed();
        setValue(parameterIndex, JDBCType.NVARCHAR, value, JavaType.STRING, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setNString");
    }

    @Override
    public final void setTime(int n, java.sql.Time x) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setTime", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.TIME, x, JavaType.TIME, false);
        loggerExternal.exiting(getClassNameLogging(), "setTime");
    }

    @Override
    public final void setTime(int n, java.sql.Time x, int scale) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setTime", new Object[] {n, x, scale});
        checkClosed();
        setValue(n, JDBCType.TIME, x, JavaType.TIME, null, scale, false);
        loggerExternal.exiting(getClassNameLogging(), "setTime");
    }

    @Override
    public final void setTime(int n, java.sql.Time x, int scale, boolean forceEncrypt) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setTime", new Object[] {n, x, scale, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.TIME, x, JavaType.TIME, null, scale, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setTime");
    }

    @Override
    public final void setTimestamp(int n, java.sql.Timestamp x) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setTimestamp", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.TIMESTAMP, x, JavaType.TIMESTAMP, false);
        loggerExternal.exiting(getClassNameLogging(), "setTimestamp");
    }

    @Override
    public final void setTimestamp(int n, java.sql.Timestamp x, int scale) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setTimestamp", new Object[] {n, x, scale});
        checkClosed();
        setValue(n, JDBCType.TIMESTAMP, x, JavaType.TIMESTAMP, null, scale, false);
        loggerExternal.exiting(getClassNameLogging(), "setTimestamp");
    }

    @Override
    public final void setTimestamp(int n, java.sql.Timestamp x, int scale,
            boolean forceEncrypt) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setTimestamp", new Object[] {n, x, scale, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.TIMESTAMP, x, JavaType.TIMESTAMP, null, scale, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setTimestamp");
    }

    @Override
    public final void setDateTimeOffset(int n, microsoft.sql.DateTimeOffset x) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setDateTimeOffset", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.DATETIMEOFFSET, x, JavaType.DATETIMEOFFSET, false);
        loggerExternal.exiting(getClassNameLogging(), "setDateTimeOffset");
    }

    @Override
    public final void setDateTimeOffset(int n, microsoft.sql.DateTimeOffset x, int scale) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setDateTimeOffset", new Object[] {n, x, scale});
        checkClosed();
        setValue(n, JDBCType.DATETIMEOFFSET, x, JavaType.DATETIMEOFFSET, null, scale, false);
        loggerExternal.exiting(getClassNameLogging(), "setDateTimeOffset");
    }

    @Override
    public final void setDateTimeOffset(int n, microsoft.sql.DateTimeOffset x, int scale,
            boolean forceEncrypt) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setDateTimeOffset",
                    new Object[] {n, x, scale, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.DATETIMEOFFSET, x, JavaType.DATETIMEOFFSET, null, scale, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setDateTimeOffset");
    }

    @Override
    public final void setDate(int n, java.sql.Date x) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setDate", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.DATE, x, JavaType.DATE, false);
        loggerExternal.exiting(getClassNameLogging(), "setDate");
    }

    @Override
    public final void setDateTime(int n, java.sql.Timestamp x) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setDateTime", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.DATETIME, x, JavaType.TIMESTAMP, false);
        loggerExternal.exiting(getClassNameLogging(), "setDateTime");
    }

    @Override
    public final void setDateTime(int n, java.sql.Timestamp x, boolean forceEncrypt) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setDateTime", new Object[] {n, x, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.DATETIME, x, JavaType.TIMESTAMP, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setDateTime");
    }

    @Override
    public final void setSmallDateTime(int n, java.sql.Timestamp x) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setSmallDateTime", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.SMALLDATETIME, x, JavaType.TIMESTAMP, false);
        loggerExternal.exiting(getClassNameLogging(), "setSmallDateTime");
    }

    @Override
    public final void setSmallDateTime(int n, java.sql.Timestamp x, boolean forceEncrypt) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setSmallDateTime", new Object[] {n, x, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.SMALLDATETIME, x, JavaType.TIMESTAMP, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setSmallDateTime");
    }

    @Override
    public final void setStructured(int n, String tvpName, SQLServerDataTable tvpDataTable) throws SQLServerException {
        setByIndex();
        tvpName = getTVPNameIfNull(n, tvpName);
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setStructured", new Object[] {n, tvpName, tvpDataTable});
        checkClosed();
        setValue(n, JDBCType.TVP, tvpDataTable, JavaType.TVP, tvpName);
        loggerExternal.exiting(getClassNameLogging(), "setStructured");
    }

    @Override
    public final void setStructured(int n, String tvpName, ResultSet tvpResultSet) throws SQLServerException {
        setByIndex();
        tvpName = getTVPNameIfNull(n, tvpName);
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setStructured", new Object[] {n, tvpName, tvpResultSet});
        checkClosed();
        setValue(n, JDBCType.TVP, tvpResultSet, JavaType.TVP, tvpName);
        loggerExternal.exiting(getClassNameLogging(), "setStructured");
    }

    @Override
    public final void setStructured(int n, String tvpName,
            ISQLServerDataRecord tvpBulkRecord) throws SQLServerException {
        setByIndex();
        tvpName = getTVPNameIfNull(n, tvpName);
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setStructured", new Object[] {n, tvpName, tvpBulkRecord});
        checkClosed();
        setValue(n, JDBCType.TVP, tvpBulkRecord, JavaType.TVP, tvpName);
        loggerExternal.exiting(getClassNameLogging(), "setStructured");
    }

    String getTVPNameFromObject(int n, Object obj) throws SQLServerException {
        String tvpName = null;
        if (obj instanceof SQLServerDataTable) {
            tvpName = ((SQLServerDataTable) obj).getTvpName();
        }
        // Get TVP name from SQLServerParameterMetaData if it is still null.
        return getTVPNameIfNull(n, tvpName);
    }

    String getTVPNameIfNull(int n, String tvpName) throws SQLServerException {
        if (((null == tvpName) || (0 == tvpName.length())) &&
        // Check if the CallableStatement/PreparedStatement is a stored procedure call
                (null != this.procedureName)) {
            SQLServerParameterMetaData pmd = (SQLServerParameterMetaData) this.getParameterMetaData();
            pmd.isTVP = true;

            if (!pmd.procedureIsFound) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_StoredProcedureNotFound"));
                Object[] msgArgs = {this.procedureName};
                SQLServerException.makeFromDriverError(connection, pmd, form.format(msgArgs), null, false);
            }

            try {
                String tvpNameWithoutSchema = pmd.getParameterTypeName(n);
                String tvpSchema = pmd.getTVPSchemaFromStoredProcedure(n);

                if (null != tvpSchema) {
                    tvpName = "[" + tvpSchema + "].[" + tvpNameWithoutSchema + "]";
                } else {
                    tvpName = tvpNameWithoutSchema;
                }
            } catch (SQLException e) {
                throw new SQLServerException(SQLServerException.getErrString("R_metaDataErrorForParameter"), null, 0,
                        e);
            }
        }

        return tvpName;
    }

    /**
     * @deprecated
     */
    @Deprecated
    @Override
    public final void setUnicodeStream(int n, java.io.InputStream x, int length) throws SQLException {
        SQLServerException.throwNotSupportedException(connection, this);
    }

    @Override
    public final void addBatch() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "addBatch");
        checkClosed();

        // Create the list of batch parameter values first time through
        if (batchParamValues == null)
            batchParamValues = new ArrayList<>();

        final int numParams = inOutParam.length;
        Parameter[] paramValues = new Parameter[numParams];
        for (int i = 0; i < numParams; i++)
            paramValues[i] = inOutParam[i].cloneForBatch();
        batchParamValues.add(paramValues);
        loggerExternal.exiting(getClassNameLogging(), "addBatch");
    }

    @Override
    public final void clearBatch() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "clearBatch");
        checkClosed();
        batchParamValues = null;
        loggerExternal.exiting(getClassNameLogging(), "clearBatch");
    }

    @Override
    public int[] executeBatch() throws SQLServerException, BatchUpdateException, SQLTimeoutException {
        loggerExternal.entering(getClassNameLogging(), EXECUTE_BATCH_STRING);
        if (loggerExternal.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
            loggerExternal.finer(toString() + ACTIVITY_ID + ActivityCorrelator.getCurrent().toString());
        }
        checkClosed();
        connection.unprepareUnreferencedPreparedStatementHandles(false);
        discardLastExecutionResults();

        try {
            int[] updateCounts;

            localUserSQL = userSQL;

            try {
                if (this.useBulkCopyForBatchInsert && isInsert(localUserSQL)) {
                    if (null == batchParamValues) {
                        updateCounts = new int[0];
                        loggerExternal.exiting(getClassNameLogging(), EXECUTE_BATCH_STRING, updateCounts);
                        return updateCounts;
                    }

                    // From the JDBC spec, section 9.1.4 - Making Batch Updates:
                    // The CallableStatement.executeBatch method (inherited from PreparedStatement) will
                    // throw a BatchUpdateException if the stored procedure returns anything other than an
                    // update count or takes OUT or INOUT parameters.
                    //
                    // Non-update count results (e.g. ResultSets) are treated as individual batch errors
                    // when they are encountered in the response.
                    //
                    // OUT and INOUT parameter checking is done here, before executing the batch. If any
                    // OUT or INOUT are present, the entire batch fails.
                    for (Parameter[] paramValues : batchParamValues) {
                        for (Parameter paramValue : paramValues) {
                            if (paramValue.isOutput()) {
                                throw new BatchUpdateException(
                                        SQLServerException.getErrString("R_outParamsNotPermittedinBatch"), null, 0,
                                        null);
                            }
                        }
                    }

                    String tableName = parseUserSQLForTableNameDW(false, false, false, false);
                    ArrayList<String> columnList = parseUserSQLForColumnListDW();
                    ArrayList<String> valueList = parseUserSQLForValueListDW(false);

                    checkAdditionalQuery();

                    try (SQLServerStatement stmt = (SQLServerStatement) connection.createStatement(
                            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, connection.getHoldability(),
                            stmtColumnEncriptionSetting);
                            SQLServerResultSet rs = stmt
                                    .executeQueryInternal("sp_executesql N'SET FMTONLY ON SELECT * FROM "
                                            + Util.escapeSingleQuotes(tableName) + " '")) {
                        Map<Integer, Integer> columnMappings = null;
                        if (null != columnList && !columnList.isEmpty()) {
                            if (columnList.size() != valueList.size()) {

                                MessageFormat form = new MessageFormat(
                                        SQLServerException.getErrString("R_colNotMatchTable"));
                                Object[] msgArgs = {columnList.size(), valueList.size()};
                                throw new IllegalArgumentException(form.format(msgArgs));
                            }
                            columnMappings = new HashMap<>(columnList.size());
                        } else {
                            if (rs.getColumnCount() != valueList.size()) {
                                MessageFormat form = new MessageFormat(
                                        SQLServerException.getErrString("R_colNotMatchTable"));
                                Object[] msgArgs = {rs.getColumnCount(), valueList.size()};
                                throw new IllegalArgumentException(form.format(msgArgs));
                            }
                        }

                        SQLServerBulkBatchInsertRecord batchRecord = new SQLServerBulkBatchInsertRecord(
                                batchParamValues, columnList, valueList, null);

                        for (int i = 1; i <= rs.getColumnCount(); i++) {
                            Column c = rs.getColumn(i);
                            CryptoMetadata cryptoMetadata = c.getCryptoMetadata();
                            int jdbctype;
                            TypeInfo ti = c.getTypeInfo();
                            if (ti.getUpdatability() == 0) { // Skip read only columns
                                continue;
                            }
                            checkValidColumns(ti);
                            if (null != cryptoMetadata) {
                                jdbctype = cryptoMetadata.getBaseTypeInfo().getSSType().getJDBCType().getIntValue();
                            } else {
                                jdbctype = ti.getSSType().getJDBCType().getIntValue();
                            }
                            if (null != columnList && !columnList.isEmpty()) {
                                int columnIndex = columnList.indexOf(c.getColumnName());
                                if (columnIndex > -1) {
                                    columnMappings.put(columnIndex + 1, i);
                                    batchRecord.addColumnMetadata(columnIndex + 1, c.getColumnName(), jdbctype,
                                            ti.getPrecision(), ti.getScale());
                                }
                            } else {
                                batchRecord.addColumnMetadata(i, c.getColumnName(), jdbctype, ti.getPrecision(),
                                        ti.getScale());
                            }
                        }

                        SQLServerBulkCopy bcOperation = new SQLServerBulkCopy(connection);
                        SQLServerBulkCopyOptions option = new SQLServerBulkCopyOptions();
                        option.setBulkCopyTimeout(queryTimeout);
                        bcOperation.setBulkCopyOptions(option);
                        bcOperation.setDestinationTableName(tableName);
                        if (columnMappings != null) {
                            for (Entry<Integer, Integer> pair : columnMappings.entrySet()) {
                                bcOperation.addColumnMapping(pair.getKey(), pair.getValue());
                            }
                        }
                        bcOperation.setStmtColumnEncriptionSetting(this.getStmtColumnEncriptionSetting());
                        bcOperation.setDestinationTableMetadata(rs);
                        bcOperation.writeToServer(batchRecord);
                        bcOperation.close();
                        updateCounts = new int[batchParamValues.size()];
                        for (int i = 0; i < batchParamValues.size(); ++i) {
                            updateCounts[i] = 1;
                        }

                        loggerExternal.exiting(getClassNameLogging(), EXECUTE_BATCH_STRING, updateCounts);
                        return updateCounts;
                    }
                }
            } catch (SQLException e) {
                // throw a BatchUpdateException with the given error message, and return null for the updateCounts.
                throw new BatchUpdateException(e.getMessage(), null, 0, null);
            } catch (IllegalArgumentException e) {
                // If we fail with IllegalArgumentException, fall back to the original batch insert logic.
                if (getStatementLogger().isLoggable(java.util.logging.Level.FINE)) {
                    getStatementLogger().fine("Parsing user's Batch Insert SQL Query failed: " + e.getMessage());
                    getStatementLogger().fine("Falling back to the original implementation for Batch Insert.");
                }
            }

            if (null == batchParamValues)
                updateCounts = new int[0];
            else {
                // From the JDBC spec, section 9.1.4 - Making Batch Updates:
                // The CallableStatement.executeBatch method (inherited from PreparedStatement) will
                // throw a BatchUpdateException if the stored procedure returns anything other than an
                // update count or takes OUT or INOUT parameters.
                //
                // Non-update count results (e.g. ResultSets) are treated as individual batch errors
                // when they are encountered in the response.
                //
                // OUT and INOUT parameter checking is done here, before executing the batch. If any
                // OUT or INOUT are present, the entire batch fails.
                for (Parameter[] paramValues : batchParamValues) {
                    for (Parameter paramValue : paramValues) {
                        if (paramValue.isOutput()) {
                            throw new BatchUpdateException(
                                    SQLServerException.getErrString("R_outParamsNotPermittedinBatch"), null, 0, null);
                        }
                    }
                }

                PrepStmtBatchExecCmd batchCommand = new PrepStmtBatchExecCmd(this);

                executeStatement(batchCommand);

                updateCounts = new int[batchCommand.updateCounts.length];
                for (int i = 0; i < batchCommand.updateCounts.length; ++i)
                    updateCounts[i] = (int) batchCommand.updateCounts[i];

                // Transform the SQLException into a BatchUpdateException with the update counts.
                if (null != batchCommand.batchException) {
                    throw new BatchUpdateException(batchCommand.batchException.getMessage(),
                            batchCommand.batchException.getSQLState(), batchCommand.batchException.getErrorCode(),
                            updateCounts);

                }
            }

            loggerExternal.exiting(getClassNameLogging(), EXECUTE_BATCH_STRING, updateCounts);
            return updateCounts;
        } finally {
            batchParamValues = null;
        }
    }

    @Override
    public long[] executeLargeBatch() throws SQLServerException, BatchUpdateException, SQLTimeoutException {
        loggerExternal.entering(getClassNameLogging(), "executeLargeBatch");
        if (loggerExternal.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
            loggerExternal.finer(toString() + ACTIVITY_ID + ActivityCorrelator.getCurrent().toString());
        }
        checkClosed();
        connection.unprepareUnreferencedPreparedStatementHandles(false);
        discardLastExecutionResults();

        try {
            long[] updateCounts;

            localUserSQL = userSQL;

            try {
                if (this.useBulkCopyForBatchInsert && isInsert(localUserSQL)) {
                    if (null == batchParamValues) {
                        updateCounts = new long[0];
                        loggerExternal.exiting(getClassNameLogging(), "executeLargeBatch", updateCounts);
                        return updateCounts;
                    }

                    // From the JDBC spec, section 9.1.4 - Making Batch Updates:
                    // The CallableStatement.executeBatch method (inherited from PreparedStatement) will
                    // throw a BatchUpdateException if the stored procedure returns anything other than an
                    // update count or takes OUT or INOUT parameters.
                    //
                    // Non-update count results (e.g. ResultSets) are treated as individual batch errors
                    // when they are encountered in the response.
                    //
                    // OUT and INOUT parameter checking is done here, before executing the batch. If any
                    // OUT or INOUT are present, the entire batch fails.
                    for (Parameter[] paramValues : batchParamValues) {
                        for (Parameter paramValue : paramValues) {
                            if (paramValue.isOutput()) {
                                throw new BatchUpdateException(
                                        SQLServerException.getErrString("R_outParamsNotPermittedinBatch"), null, 0,
                                        null);
                            }
                        }
                    }

                    String tableName = parseUserSQLForTableNameDW(false, false, false, false);
                    ArrayList<String> columnList = parseUserSQLForColumnListDW();
                    ArrayList<String> valueList = parseUserSQLForValueListDW(false);

                    checkAdditionalQuery();

                    try (SQLServerStatement stmt = (SQLServerStatement) connection.createStatement(
                            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, connection.getHoldability(),
                            stmtColumnEncriptionSetting);
                            SQLServerResultSet rs = stmt
                                    .executeQueryInternal("sp_executesql N'SET FMTONLY ON SELECT * FROM "
                                            + Util.escapeSingleQuotes(tableName) + " '")) {
                        if (null != columnList && !columnList.isEmpty()) {
                            if (columnList.size() != valueList.size()) {
                                MessageFormat form = new MessageFormat(
                                        SQLServerException.getErrString("R_colNotMatchTable"));
                                Object[] msgArgs = {columnList.size(), valueList.size()};
                                throw new IllegalArgumentException(form.format(msgArgs));
                            }
                        } else {
                            if (rs.getColumnCount() != valueList.size()) {
                                MessageFormat form = new MessageFormat(
                                        SQLServerException.getErrString("R_colNotMatchTable"));
                                Object[] msgArgs = {columnList != null ? columnList.size() : 0, valueList.size()};
                                throw new IllegalArgumentException(form.format(msgArgs));
                            }
                        }

                        SQLServerBulkBatchInsertRecord batchRecord = new SQLServerBulkBatchInsertRecord(
                                batchParamValues, columnList, valueList, null);

                        for (int i = 1; i <= rs.getColumnCount(); i++) {
                            Column c = rs.getColumn(i);
                            CryptoMetadata cryptoMetadata = c.getCryptoMetadata();
                            int jdbctype;
                            TypeInfo ti = c.getTypeInfo();
                            checkValidColumns(ti);
                            if (null != cryptoMetadata) {
                                jdbctype = cryptoMetadata.getBaseTypeInfo().getSSType().getJDBCType().getIntValue();
                            } else {
                                jdbctype = ti.getSSType().getJDBCType().getIntValue();
                            }
                            batchRecord.addColumnMetadata(i, c.getColumnName(), jdbctype, ti.getPrecision(),
                                    ti.getScale());
                        }

                        SQLServerBulkCopy bcOperation = new SQLServerBulkCopy(connection);
                        SQLServerBulkCopyOptions option = new SQLServerBulkCopyOptions();
                        option.setBulkCopyTimeout(queryTimeout);
                        bcOperation.setBulkCopyOptions(option);
                        bcOperation.setDestinationTableName(tableName);
                        bcOperation.setStmtColumnEncriptionSetting(this.getStmtColumnEncriptionSetting());
                        bcOperation.setDestinationTableMetadata(rs);
                        bcOperation.writeToServer(batchRecord);
                        bcOperation.close();
                        updateCounts = new long[batchParamValues.size()];
                        for (int i = 0; i < batchParamValues.size(); ++i) {
                            updateCounts[i] = 1;
                        }

                        loggerExternal.exiting(getClassNameLogging(), "executeLargeBatch", updateCounts);
                        return updateCounts;
                    }
                }
            } catch (SQLException e) {
                // throw a BatchUpdateException with the given error message, and return null for the updateCounts.
                throw new BatchUpdateException(e.getMessage(), null, 0, null);
            } catch (IllegalArgumentException e) {
                // If we fail with IllegalArgumentException, fall back to the original batch insert logic.
                if (getStatementLogger().isLoggable(java.util.logging.Level.FINE)) {
                    getStatementLogger().fine("Parsing user's Batch Insert SQL Query failed: " + e.getMessage());
                    getStatementLogger().fine("Falling back to the original implementation for Batch Insert.");
                }
            }

            if (null == batchParamValues)
                updateCounts = new long[0];
            else {
                // From the JDBC spec, section 9.1.4 - Making Batch Updates:
                // The CallableStatement.executeBatch method (inherited from PreparedStatement) will
                // throw a BatchUpdateException if the stored procedure returns anything other than an
                // update count or takes OUT or INOUT parameters.
                //
                // Non-update count results (e.g. ResultSets) are treated as individual batch errors
                // when they are encountered in the response.
                //
                // OUT and INOUT parameter checking is done here, before executing the batch. If any
                // OUT or INOUT are present, the entire batch fails.
                for (Parameter[] paramValues : batchParamValues) {
                    for (Parameter paramValue : paramValues) {
                        if (paramValue.isOutput()) {
                            throw new BatchUpdateException(
                                    SQLServerException.getErrString("R_outParamsNotPermittedinBatch"), null, 0, null);
                        }
                    }
                }

                PrepStmtBatchExecCmd batchCommand = new PrepStmtBatchExecCmd(this);

                executeStatement(batchCommand);

                updateCounts = new long[batchCommand.updateCounts.length];

                System.arraycopy(batchCommand.updateCounts, 0, updateCounts, 0, batchCommand.updateCounts.length);

                // Transform the SQLException into a BatchUpdateException with the update counts.
                if (null != batchCommand.batchException) {
                    DriverJDBCVersion.throwBatchUpdateException(batchCommand.batchException, updateCounts);
                }
            }

            loggerExternal.exiting(getClassNameLogging(), "executeLargeBatch", updateCounts);
            return updateCounts;
        } finally {
            batchParamValues = null;
        }
    }

    private void checkValidColumns(TypeInfo ti) throws SQLServerException {
        int jdbctype = ti.getSSType().getJDBCType().getIntValue();
        String typeName;
        MessageFormat form;
        switch (jdbctype) {
            case microsoft.sql.Types.MONEY:
            case microsoft.sql.Types.SMALLMONEY:
            case java.sql.Types.DATE:
            case microsoft.sql.Types.DATETIME:
            case microsoft.sql.Types.DATETIMEOFFSET:
            case microsoft.sql.Types.SMALLDATETIME:
            case java.sql.Types.TIME:
                typeName = ti.getSSTypeName();
                form = new MessageFormat(SQLServerException.getErrString("R_BulkTypeNotSupportedDW"));
                throw new IllegalArgumentException(form.format(new Object[] {typeName}));
            case java.sql.Types.INTEGER:
            case java.sql.Types.SMALLINT:
            case java.sql.Types.BIGINT:
            case java.sql.Types.BIT:
            case java.sql.Types.TINYINT:
            case java.sql.Types.DOUBLE:
            case java.sql.Types.REAL:
            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
            case microsoft.sql.Types.GUID:
            case java.sql.Types.CHAR:
            case java.sql.Types.NCHAR:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.VARCHAR:
            case java.sql.Types.LONGNVARCHAR:
            case java.sql.Types.NVARCHAR:
            case java.sql.Types.BINARY:
            case java.sql.Types.LONGVARBINARY:
            case java.sql.Types.VARBINARY:
                // Spatial datatypes fall under Varbinary, check if the UDT is geometry/geography.
                typeName = ti.getSSTypeName();
                if ("geometry".equalsIgnoreCase(typeName) || "geography".equalsIgnoreCase(typeName)) {
                    form = new MessageFormat(SQLServerException.getErrString("R_BulkTypeNotSupported"));
                    throw new IllegalArgumentException(form.format(new Object[] {typeName}));
                }
            case java.sql.Types.TIMESTAMP:
            case 2013: // java.sql.Types.TIME_WITH_TIMEZONE
            case 2014: // java.sql.Types.TIMESTAMP_WITH_TIMEZONE
            case microsoft.sql.Types.SQL_VARIANT:
                return;
            default: {
                form = new MessageFormat(SQLServerException.getErrString("R_BulkTypeNotSupported"));
                String unsupportedDataType = JDBCType.of(jdbctype).toString();
                throw new IllegalArgumentException(form.format(new Object[] {unsupportedDataType}));
            }
        }
    }

    private void checkAdditionalQuery() {
        while (checkAndRemoveCommentsAndSpace(true)) {}

        // At this point, if localUserSQL is not empty (after removing all whitespaces, semicolons and comments), we
        // have a
        // new query. reject this.
        if (localUserSQL.length() > 0) {
            throw new IllegalArgumentException(SQLServerException.getErrString("R_multipleQueriesNotAllowed"));
        }
    }

    private String parseUserSQLForTableNameDW(boolean hasInsertBeenFound, boolean hasIntoBeenFound,
            boolean hasTableBeenFound, boolean isExpectingTableName) throws SQLServerException {
        // As far as finding the table name goes, There are two cases:
        // Insert into <tableName> and Insert <tableName>
        // And there could be in-line comments (with /* and */) in between.
        // This method assumes the localUserSQL string starts with "insert".
        while (checkAndRemoveCommentsAndSpace(false)) {}

        StringBuilder sb = new StringBuilder();

        // If table has been found and the next character is not a . at this point, we've finished parsing the table
        // name.
        // This if statement is needed to handle the case where the user has something like:
        // [dbo] . /* random comment */ [tableName]
        if (hasTableBeenFound && !isExpectingTableName) {
            if (checkSQLLength(1) && ".".equalsIgnoreCase(localUserSQL.substring(0, 1))) {
                sb.append(".");
                localUserSQL = localUserSQL.substring(1);
                return sb.toString() + parseUserSQLForTableNameDW(true, true, true, true);
            } else {
                return "";
            }
        }

        if (!hasInsertBeenFound && checkSQLLength(6) && "insert".equalsIgnoreCase(localUserSQL.substring(0, 6))) {
            localUserSQL = localUserSQL.substring(6);
            return parseUserSQLForTableNameDW(true, hasIntoBeenFound, hasTableBeenFound, isExpectingTableName);
        }

        if (!hasIntoBeenFound && checkSQLLength(6) && "into".equalsIgnoreCase(localUserSQL.substring(0, 4))) {
            // is it really "into"?
            // if the "into" is followed by a blank space or /*, then yes.
            if (Character.isWhitespace(localUserSQL.charAt(4))
                    || (localUserSQL.charAt(4) == '/' && localUserSQL.charAt(5) == '*')) {
                localUserSQL = localUserSQL.substring(4);
                return parseUserSQLForTableNameDW(hasInsertBeenFound, true, hasTableBeenFound, isExpectingTableName);
            }

            // otherwise, we found the token that either contains the databasename.tablename or tablename.
            // Recursively handle this, but into has been found. (or rather, it's absent in the query - the "into"
            // keyword is optional)
            return parseUserSQLForTableNameDW(hasInsertBeenFound, true, hasTableBeenFound, isExpectingTableName);
        }

        // At this point, the next token has to be the table name.
        // It could be encapsulated in [], "", or have a database name preceding the table name.
        // If it's encapsulated in [] or "", we need be more careful with parsing as anything could go into []/"".
        // For ] or ", they can be escaped by ]] or "", watch out for this too.
        if (checkSQLLength(1) && "[".equalsIgnoreCase(localUserSQL.substring(0, 1))) {
            int tempint = localUserSQL.indexOf(']', 1);

            // ] has not been found, this is wrong.
            if (tempint < 0) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidSQL"));
                Object[] msgArgs = {localUserSQL};
                throw new IllegalArgumentException(form.format(msgArgs));
            }

            // keep checking if it's escaped
            while (tempint >= 0 && checkSQLLength(tempint + 2) && localUserSQL.charAt(tempint + 1) == ']') {
                tempint = localUserSQL.indexOf(']', tempint + 2);
            }

            // we've found a ] that is actually trying to close the square bracket.
            // return tablename + potentially more that's part of the table name
            sb.append(localUserSQL.substring(0, tempint + 1));
            localUserSQL = localUserSQL.substring(tempint + 1);
            return sb.toString() + parseUserSQLForTableNameDW(true, true, true, false);
        }

        // do the same for ""
        if (checkSQLLength(1) && "\"".equalsIgnoreCase(localUserSQL.substring(0, 1))) {
            int tempint = localUserSQL.indexOf('"', 1);

            // \" has not been found, this is wrong.
            if (tempint < 0) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidSQL"));
                Object[] msgArgs = {localUserSQL};
                throw new IllegalArgumentException(form.format(msgArgs));
            }

            // keep checking if it's escaped
            while (tempint >= 0 && checkSQLLength(tempint + 2) && localUserSQL.charAt(tempint + 1) == '\"') {
                tempint = localUserSQL.indexOf('"', tempint + 2);
            }

            // we've found a " that is actually trying to close the quote.
            // return tablename + potentially more that's part of the table name
            sb.append(localUserSQL.substring(0, tempint + 1));
            localUserSQL = localUserSQL.substring(tempint + 1);
            return sb.toString() + parseUserSQLForTableNameDW(true, true, true, false);
        }

        // At this point, the next chunk of string is the table name, without starting with [ or ".
        while (localUserSQL.length() > 0) {
            // Keep going until the end of the table name is signalled - either a ., whitespace, bracket ; or comment is
            // encountered.
            if (localUserSQL.charAt(0) == '.' || localUserSQL.charAt(0) == '('
                    || Character.isWhitespace(localUserSQL.charAt(0)) || checkAndRemoveCommentsAndSpace(false)) {
                return sb.toString() + parseUserSQLForTableNameDW(true, true, true, false);
            } else if (localUserSQL.charAt(0) == ';') {
                throw new IllegalArgumentException(SQLServerException.getErrString("R_endOfQueryDetected"));
            } else {
                sb.append(localUserSQL.charAt(0));
                localUserSQL = localUserSQL.substring(1);
            }
        }

        // It shouldn't come here. If we did, something is wrong.
        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidSQL"));
        Object[] msgArgs = {localUserSQL};
        throw new IllegalArgumentException(form.format(msgArgs));
    }

    private ArrayList<String> parseUserSQLForColumnListDW() {
        // ignore all comments
        while (checkAndRemoveCommentsAndSpace(false)) {}

        // check if optional column list was provided
        // Columns can have the form of c1, [c1] or "c1". It can escape ] or " by ]] or "".
        if (checkSQLLength(1) && "(".equalsIgnoreCase(localUserSQL.substring(0, 1))) {
            localUserSQL = localUserSQL.substring(1);
            return parseUserSQLForColumnListDWHelper(new ArrayList<String>());
        }
        return null;
    }

    private ArrayList<String> parseUserSQLForColumnListDWHelper(ArrayList<String> listOfColumns) {
        // ignore all comments
        while (checkAndRemoveCommentsAndSpace(false)) {}

        StringBuilder sb = new StringBuilder();
        while (localUserSQL.length() > 0) {
            while (checkAndRemoveCommentsAndSpace(false)) {}

            // exit condition
            if (checkSQLLength(1) && localUserSQL.charAt(0) == ')') {
                localUserSQL = localUserSQL.substring(1);
                return listOfColumns;
            }

            // ignore ,
            // we've confirmed length is more than 0.
            if (localUserSQL.charAt(0) == ',') {
                localUserSQL = localUserSQL.substring(1);
                while (checkAndRemoveCommentsAndSpace(false)) {}
            }

            // handle [] case
            if (localUserSQL.charAt(0) == '[') {
                int tempint = localUserSQL.indexOf(']', 1);

                // ] has not been found, this is wrong.
                if (tempint < 0) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidSQL"));
                    Object[] msgArgs = {localUserSQL};
                    throw new IllegalArgumentException(form.format(msgArgs));
                }

                // keep checking if it's escaped
                while (tempint >= 0 && checkSQLLength(tempint + 2) && localUserSQL.charAt(tempint + 1) == ']') {
                    localUserSQL = localUserSQL.substring(0, tempint) + localUserSQL.substring(tempint + 1);
                    tempint = localUserSQL.indexOf(']', tempint + 1);
                }

                // we've found a ] that is actually trying to close the square bracket.
                String tempstr = localUserSQL.substring(1, tempint);
                localUserSQL = localUserSQL.substring(tempint + 1);
                listOfColumns.add(tempstr);
                continue; // proceed with the rest of the string
            }

            // handle "" case
            if (localUserSQL.charAt(0) == '\"') {
                int tempint = localUserSQL.indexOf('"', 1);

                // \" has not been found, this is wrong.
                if (tempint < 0) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidSQL"));
                    Object[] msgArgs = {localUserSQL};
                    throw new IllegalArgumentException(form.format(msgArgs));
                }

                // keep checking if it's escaped
                while (tempint >= 0 && checkSQLLength(tempint + 2) && localUserSQL.charAt(tempint + 1) == '\"') {
                    localUserSQL = localUserSQL.substring(0, tempint) + localUserSQL.substring(tempint + 1);
                    tempint = localUserSQL.indexOf('"', tempint + 1);
                }

                // we've found a " that is actually trying to close the quote.
                String tempstr = localUserSQL.substring(1, tempint);
                localUserSQL = localUserSQL.substring(tempint + 1);
                listOfColumns.add(tempstr);
                continue; // proceed with the rest of the string
            }

            // At this point, the next chunk of string is the column name, without starting with [ or ".
            while (localUserSQL.length() > 0) {
                if (checkAndRemoveCommentsAndSpace(false)) {
                    continue;
                }
                if (localUserSQL.charAt(0) == ',') {
                    localUserSQL = localUserSQL.substring(1);
                    listOfColumns.add(sb.toString());
                    sb.setLength(0);
                    break; // exit this while loop, but continue parsing.
                } else if (localUserSQL.charAt(0) == ')') {
                    localUserSQL = localUserSQL.substring(1);
                    listOfColumns.add(sb.toString());
                    return listOfColumns; // reached exit condition.
                } else {
                    sb.append(localUserSQL.charAt(0));
                    localUserSQL = localUserSQL.substring(1);
                    localUserSQL = localUserSQL.trim(); // add an entry.
                }
            }
        }

        // It shouldn't come here. If we did, something is wrong.
        // most likely we couldn't hit the exit condition and just parsed until the end of the string.
        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidSQL"));
        Object[] msgArgs = {localUserSQL};
        throw new IllegalArgumentException(form.format(msgArgs));
    }

    private ArrayList<String> parseUserSQLForValueListDW(boolean hasValuesBeenFound) {
        // ignore all comments
        if (checkAndRemoveCommentsAndSpace(false)) {}

        if (!hasValuesBeenFound) {
            // look for keyword "VALUES"
            if (checkSQLLength(6) && "VALUES".equalsIgnoreCase(localUserSQL.substring(0, 6))) {
                localUserSQL = localUserSQL.substring(6);

                // ignore all comments
                while (checkAndRemoveCommentsAndSpace(false)) {}

                if (checkSQLLength(1) && "(".equalsIgnoreCase(localUserSQL.substring(0, 1))) {
                    localUserSQL = localUserSQL.substring(1);
                    return parseUserSQLForValueListDWHelper(new ArrayList<String>());
                }
            }
        } else {
            // ignore all comments
            while (checkAndRemoveCommentsAndSpace(false)) {}

            if (checkSQLLength(1) && "(".equalsIgnoreCase(localUserSQL.substring(0, 1))) {
                localUserSQL = localUserSQL.substring(1);
                return parseUserSQLForValueListDWHelper(new ArrayList<String>());
            }
        }

        // shouldn't come here, as the list of values is mandatory.
        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidSQL"));
        Object[] msgArgs = {localUserSQL};
        throw new IllegalArgumentException(form.format(msgArgs));
    }

    private ArrayList<String> parseUserSQLForValueListDWHelper(ArrayList<String> listOfValues) {
        // ignore all comments
        while (checkAndRemoveCommentsAndSpace(false)) {}

        // At this point, the next chunk of string is the value, without starting with ' (most likely a ?).
        StringBuilder sb = new StringBuilder();
        while (localUserSQL.length() > 0) {
            if (checkAndRemoveCommentsAndSpace(false)) {
                continue;
            }
            if (localUserSQL.charAt(0) == ',' || localUserSQL.charAt(0) == ')') {
                if (localUserSQL.charAt(0) == ',') {
                    localUserSQL = localUserSQL.substring(1);
                    if (!"?".equals(sb.toString())) {
                        // throw IllegalArgumentException and fallback to original logic for batch insert
                        throw new IllegalArgumentException(SQLServerException.getErrString("R_onlyFullParamAllowed"));
                    }
                    listOfValues.add(sb.toString());
                    sb.setLength(0);
                } else {
                    localUserSQL = localUserSQL.substring(1);
                    listOfValues.add(sb.toString());
                    return listOfValues; // reached exit condition.
                }
            } else {
                sb.append(localUserSQL.charAt(0));
                localUserSQL = localUserSQL.substring(1);
                localUserSQL = localUserSQL.trim(); // add entry.
            }
        }

        // It shouldn't come here. If we did, something is wrong.
        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidSQL"));
        Object[] msgArgs = {localUserSQL};
        throw new IllegalArgumentException(form.format(msgArgs));
    }

    private boolean checkAndRemoveCommentsAndSpace(boolean checkForSemicolon) {
        localUserSQL = localUserSQL.trim();

        while (checkForSemicolon && null != localUserSQL && localUserSQL.length() > 0
                && localUserSQL.charAt(0) == ';') {
            localUserSQL = localUserSQL.substring(1);
        }

        if (null == localUserSQL || localUserSQL.length() < 2) {
            return false;
        }

        if ("/*".equalsIgnoreCase(localUserSQL.substring(0, 2))) {
            int temp = localUserSQL.indexOf("*/") + 2;
            if (temp <= 0) {
                localUserSQL = "";
                return false;
            }
            localUserSQL = localUserSQL.substring(temp);
            return true;
        }

        if ("--".equalsIgnoreCase(localUserSQL.substring(0, 2))) {
            int temp = localUserSQL.indexOf('\n') + 1;
            if (temp <= 0) {
                localUserSQL = "";
                return false;
            }
            localUserSQL = localUserSQL.substring(temp);
            return true;
        }

        return false;
    }

    private boolean checkSQLLength(int length) {
        if (null == localUserSQL || localUserSQL.length() < length) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidSQL"));
            Object[] msgArgs = {localUserSQL};
            throw new IllegalArgumentException(form.format(msgArgs));
        }
        return true;
    }

    /**
     * Prepare statement batch execute command
     */
    private final class PrepStmtBatchExecCmd extends TDSCommand {
        /**
         * Always update serialVersionUID when prompted.
         */
        private static final long serialVersionUID = 5225705304799552318L;

        private final SQLServerPreparedStatement stmt;
        SQLServerException batchException;
        long[] updateCounts;

        PrepStmtBatchExecCmd(SQLServerPreparedStatement stmt) {
            super(stmt.toString() + " executeBatch", queryTimeout, cancelQueryTimeoutSeconds);
            this.stmt = stmt;
        }

        final boolean doExecute() throws SQLServerException {
            stmt.doExecutePreparedStatementBatch(this);
            return true;
        }

        @Override
        final void processResponse(TDSReader tdsReader) throws SQLServerException {
            ensureExecuteResultsReader(tdsReader);
            processExecuteResults();
        }
    }

    final void doExecutePreparedStatementBatch(PrepStmtBatchExecCmd batchCommand) throws SQLServerException {
        executeMethod = EXECUTE_BATCH;
        batchCommand.batchException = null;
        final int numBatches = batchParamValues.size();
        batchCommand.updateCounts = new long[numBatches];
        for (int i = 0; i < numBatches; i++)
            batchCommand.updateCounts[i] = Statement.EXECUTE_FAILED; // Init to unknown status EXECUTE_FAILED

        int numBatchesPrepared = 0;
        int numBatchesExecuted = 0;

        if (isSelect(userSQL)) {
            SQLServerException.makeFromDriverError(connection, this,
                    SQLServerException.getErrString("R_selectNotPermittedinBatch"), null, true);
        }

        // Make sure any previous maxRows limitation on the connection is removed.
        connection.setMaxRows(0);

        if (loggerExternal.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
            loggerExternal.finer(toString() + ACTIVITY_ID + ActivityCorrelator.getCurrent().toString());
        }
        // Create the parameter array that we'll use for all the items in this batch.
        Parameter[] batchParam = new Parameter[inOutParam.length];

        TDSWriter tdsWriter = null;
        while (numBatchesExecuted < numBatches) {
            // Fill in the parameter values for this batch
            Parameter[] paramValues = batchParamValues.get(numBatchesPrepared);
            assert paramValues.length == batchParam.length;
            System.arraycopy(paramValues, 0, batchParam, 0, paramValues.length);

            boolean hasExistingTypeDefinitions = preparedTypeDefinitions != null;
            boolean hasNewTypeDefinitions = buildPreparedStrings(batchParam, false);

            if ((0 == numBatchesExecuted) && !isInternalEncryptionQuery && connection.isAEv2()
                    && !encryptionMetadataIsRetrieved) {
                this.enclaveCEKs = connection.initEnclaveParameters(this, preparedSQL, preparedTypeDefinitions,
                        batchParam, parameterNames);
                encryptionMetadataIsRetrieved = true;

                /*
                 * fix an issue when inserting unicode into non-encrypted nchar column using setString() and AE is on
                 * one Connection
                 */
                buildPreparedStrings(batchParam, true);

                /*
                 * Save the crypto metadata retrieved for the first batch. We will re-use these for the rest of the
                 * batches.
                 */
                for (Parameter aBatchParam : batchParam) {
                    cryptoMetaBatch.add(aBatchParam.cryptoMeta);
                }
            }

            // Get the encryption metadata for the first batch only.
            if ((0 == numBatchesExecuted) && (Util.shouldHonorAEForParameters(stmtColumnEncriptionSetting, connection))
                    && (0 < batchParam.length) && !isInternalEncryptionQuery && !encryptionMetadataIsRetrieved) {
                encryptionMetadataIsRetrieved = true;
                getParameterEncryptionMetadata(batchParam);

                /*
                 * fix an issue when inserting unicode into non-encrypted nchar column using setString() and AE is on
                 * one Connection
                 */
                buildPreparedStrings(batchParam, true);

                /*
                 * Save the crypto metadata retrieved for the first batch. We will re-use these for the rest of the
                 * batches.
                 */
                for (Parameter aBatchParam : batchParam) {
                    cryptoMetaBatch.add(aBatchParam.cryptoMeta);
                }
            } else {
                // cryptoMetaBatch will be empty for non-AE connections/statements.
                for (int i = 0; i < cryptoMetaBatch.size(); i++) {
                    batchParam[i].cryptoMeta = cryptoMetaBatch.get(i);
                }
            }

            boolean needsPrepare = true;
            // Retry execution if existing handle could not be re-used.
            for (int attempt = 1; attempt <= 2; ++attempt) {
                try {

                    // If the command was interrupted, that means the TDS.PKT_CANCEL_REQ was sent to the server.
                    // Since the cancellation request was sent, stop processing the batch query and process the
                    // cancellation request and then return.
                    //
                    // Otherwise, if we do continue processing the batch query, in the case where a query requires
                    // prepexec/sp_prepare, the TDS request for prepexec/sp_prepare will be sent regardless of
                    // query cancellation. This will cause a TDS token error in the post processing when we
                    // close the query.
                    if (batchCommand.wasInterrupted()) {
                        ensureExecuteResultsReader(batchCommand.startResponse(getIsResponseBufferingAdaptive()));
                        startResults();
                        getNextResult(true);
                        return;
                    }

                    // Re-use handle if available, requires parameter definitions which are not available until here.
                    if (reuseCachedHandle(hasNewTypeDefinitions, 1 < attempt)) {
                        hasNewTypeDefinitions = false;
                    }

                    if (numBatchesExecuted < numBatchesPrepared) {
                        // assert null != tdsWriter;
                        tdsWriter.writeByte((byte) NBATCH_STATEMENT_DELIMITER);
                    } else {
                        resetForReexecute();
                        tdsWriter = batchCommand.startRequest(TDS.PKT_RPC);
                    }

                    // If we have to (re)prepare the statement then we must execute it so
                    // that we get back a (new) prepared statement handle to use to
                    // execute additional batches.
                    //
                    // We must always prepare the statement the first time through.
                    // But we may also need to reprepare the statement if, for example,
                    // the size of a batch's string parameter values changes such
                    // that repreparation is necessary.
                    ++numBatchesPrepared;
                    needsPrepare = doPrepExec(tdsWriter, batchParam, hasNewTypeDefinitions, hasExistingTypeDefinitions,
                            batchCommand);
                    if (needsPrepare || numBatchesPrepared == numBatches) {
                        ensureExecuteResultsReader(batchCommand.startResponse(getIsResponseBufferingAdaptive()));

                        boolean retry = false;
                        while (numBatchesExecuted < numBatchesPrepared) {
                            // NOTE:
                            // When making changes to anything below, consider whether similar changes need
                            // to be made to Statement batch execution.

                            startResults();

                            try {
                                // Get the first result from the batch. If there is no result for this batch
                                // then bail, leaving EXECUTE_FAILED in the current and remaining slots of
                                // the update count array.
                                if (!getNextResult(true))
                                    return;

                                // If sp_prepare was executed, but a handle doesn't exist that means
                                // the TDS response for sp_prepare has not been processed yet. Rather, it means
                                // that another result was processed from a sp_execute query instead. Therefore, we
                                // skip the if-block below and continue until the handle is set from the processed
                                // sp_prepare TDS response.
                                if (isSpPrepareExecuted && hasPreparedStatementHandle()) {
                                    isSpPrepareExecuted = false;
                                    resetForReexecute();
                                    tdsWriter = batchCommand.startRequest(TDS.PKT_RPC);
                                    buildExecParams(tdsWriter);
                                    sendParamsByRPC(tdsWriter, batchParam, bReturnValueSyntax, false);
                                    ensureExecuteResultsReader(
                                            batchCommand.startResponse(getIsResponseBufferingAdaptive()));
                                    startResults();
                                    if (!getNextResult(true))
                                        return;
                                }

                                // If the result is a ResultSet (rather than an update count) then throw an
                                // exception for this result. The exception gets caught immediately below and
                                // translated into (or added to) a BatchUpdateException.
                                if (null != resultSet) {
                                    SQLServerException.makeFromDriverError(connection, this,
                                            SQLServerException.getErrString("R_resultsetGeneratedForUpdate"), null,
                                            false);
                                }
                            } catch (SQLServerException e) {
                                // If the failure was severe enough to close the connection or roll back a
                                // manual transaction, then propagate the error up as a SQLServerException
                                // now, rather than continue with the batch.
                                if (connection.isSessionUnAvailable() || connection.rolledBackTransaction())
                                    throw e;

                                // Retry if invalid handle exception.
                                if (retryBasedOnFailedReuseOfCachedHandle(e, attempt, needsPrepare, true)) {
                                    // reset number of batches prepare
                                    numBatchesPrepared = numBatchesExecuted;
                                    retry = true;
                                    break;
                                }

                                // Otherwise, the connection is OK and the transaction is still intact,
                                // so just record the failure for the particular batch item.
                                updateCount = Statement.EXECUTE_FAILED;
                                if (null == batchCommand.batchException)
                                    batchCommand.batchException = e;

                                String sqlState = batchCommand.batchException.getSQLState();
                                if (null != sqlState
                                        && sqlState.equals(SQLState.STATEMENT_CANCELED.getSQLStateCode())) {
                                    processBatch();
                                    continue;
                                }
                            }

                            // In batch execution, we have a special update count
                            // to indicate that no information was returned
                            batchCommand.updateCounts[numBatchesExecuted] = (-1 == updateCount) ? Statement.SUCCESS_NO_INFO
                                                                                                : updateCount;
                            processBatch();

                            numBatchesExecuted++;
                        }
                        if (retry)
                            continue;

                        // Only way to proceed with preparing the next set of batches is if
                        // we successfully executed the previously prepared set.
                        assert numBatchesExecuted == numBatchesPrepared;
                    }
                } catch (SQLException e) {
                    if (retryBasedOnFailedReuseOfCachedHandle(e, attempt, needsPrepare, true)
                            && connection.isStatementPoolingEnabled()) {
                        // Reset number of batches prepared.
                        numBatchesPrepared = numBatchesExecuted;
                        continue;
                    } else if (null != batchCommand.batchException) {
                        // if batch exception occurred, loop out to throw the initial batchException
                        numBatchesExecuted = numBatchesPrepared;
                        attempt++;
                        continue;
                    } else {
                        throw e;
                    }
                }
                break;
            }
        }
    }

    @Override
    public final void setUseFmtOnly(boolean useFmtOnly) throws SQLServerException {
        checkClosed();
        this.useFmtOnly = useFmtOnly;
    }

    @Override
    public final boolean getUseFmtOnly() throws SQLServerException {
        checkClosed();
        return this.useFmtOnly;
    }

    @Override
    public final void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setCharacterStream", new Object[] {parameterIndex, reader});
        checkClosed();
        setStream(parameterIndex, StreamType.CHARACTER, reader, JavaType.READER, DataTypes.UNKNOWN_STREAM_LENGTH);
        loggerExternal.exiting(getClassNameLogging(), "setCharacterStream");
    }

    @Override
    public final void setCharacterStream(int n, java.io.Reader reader, int length) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setCharacterStream", new Object[] {n, reader, length});
        checkClosed();
        setStream(n, StreamType.CHARACTER, reader, JavaType.READER, length);
        loggerExternal.exiting(getClassNameLogging(), "setCharacterStream");
    }

    @Override
    public final void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setCharacterStream",
                    new Object[] {parameterIndex, reader, length});
        checkClosed();
        setStream(parameterIndex, StreamType.CHARACTER, reader, JavaType.READER, length);
        loggerExternal.exiting(getClassNameLogging(), "setCharacterStream");
    }

    @Override
    public final void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setNCharacterStream", new Object[] {parameterIndex, value});
        checkClosed();
        setStream(parameterIndex, StreamType.NCHARACTER, value, JavaType.READER, DataTypes.UNKNOWN_STREAM_LENGTH);
        loggerExternal.exiting(getClassNameLogging(), "setNCharacterStream");
    }

    @Override
    public final void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setNCharacterStream",
                    new Object[] {parameterIndex, value, length});
        checkClosed();
        setStream(parameterIndex, StreamType.NCHARACTER, value, JavaType.READER, length);
        loggerExternal.exiting(getClassNameLogging(), "setNCharacterStream");
    }

    @Override
    public final void setRef(int i, java.sql.Ref x) throws SQLException {
        SQLServerException.throwNotSupportedException(connection, this);
    }

    @Override
    public final void setBlob(int i, java.sql.Blob x) throws SQLException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBlob", new Object[] {i, x});
        checkClosed();
        setValue(i, JDBCType.BLOB, x, JavaType.BLOB, false);
        loggerExternal.exiting(getClassNameLogging(), "setBlob");
    }

    @Override
    public final void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBlob", new Object[] {parameterIndex, inputStream});
        checkClosed();
        setStream(parameterIndex, StreamType.BINARY, inputStream, JavaType.INPUTSTREAM,
                DataTypes.UNKNOWN_STREAM_LENGTH);
        loggerExternal.exiting(getClassNameLogging(), "setBlob");
    }

    @Override
    public final void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBlob",
                    new Object[] {parameterIndex, inputStream, length});
        checkClosed();
        setStream(parameterIndex, StreamType.BINARY, inputStream, JavaType.INPUTSTREAM, length);
        loggerExternal.exiting(getClassNameLogging(), "setBlob");
    }

    @Override
    public final void setClob(int parameterIndex, java.sql.Clob clobValue) throws SQLException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setClob", new Object[] {parameterIndex, clobValue});
        checkClosed();
        setValue(parameterIndex, JDBCType.CLOB, clobValue, JavaType.CLOB, false);
        loggerExternal.exiting(getClassNameLogging(), "setClob");
    }

    @Override
    public final void setClob(int parameterIndex, Reader reader) throws SQLException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setClob", new Object[] {parameterIndex, reader});
        checkClosed();
        setStream(parameterIndex, StreamType.CHARACTER, reader, JavaType.READER, DataTypes.UNKNOWN_STREAM_LENGTH);
        loggerExternal.exiting(getClassNameLogging(), "setClob");
    }

    @Override
    public final void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setClob", new Object[] {parameterIndex, reader, length});
        checkClosed();
        setStream(parameterIndex, StreamType.CHARACTER, reader, JavaType.READER, length);
        loggerExternal.exiting(getClassNameLogging(), "setClob");
    }

    @Override
    public final void setNClob(int parameterIndex, NClob value) throws SQLException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setNClob", new Object[] {parameterIndex, value});
        checkClosed();
        setValue(parameterIndex, JDBCType.NCLOB, value, JavaType.NCLOB, false);
        loggerExternal.exiting(getClassNameLogging(), "setNClob");
    }

    @Override
    public final void setNClob(int parameterIndex, Reader reader) throws SQLException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setNClob", new Object[] {parameterIndex, reader});
        checkClosed();
        setStream(parameterIndex, StreamType.NCHARACTER, reader, JavaType.READER, DataTypes.UNKNOWN_STREAM_LENGTH);
        loggerExternal.exiting(getClassNameLogging(), "setNClob");
    }

    @Override
    public final void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setNClob", new Object[] {parameterIndex, reader, length});
        checkClosed();
        setStream(parameterIndex, StreamType.NCHARACTER, reader, JavaType.READER, length);
        loggerExternal.exiting(getClassNameLogging(), "setNClob");
    }

    @Override
    public final void setArray(int i, java.sql.Array x) throws SQLException {
        SQLServerException.throwNotSupportedException(connection, this);
    }

    @Override
    public final void setDate(int n, java.sql.Date x, java.util.Calendar cal) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setDate", new Object[] {n, x, cal});
        checkClosed();
        setValue(n, JDBCType.DATE, x, JavaType.DATE, cal, false);
        loggerExternal.exiting(getClassNameLogging(), "setDate");
    }

    @Override
    public final void setDate(int n, java.sql.Date x, java.util.Calendar cal,
            boolean forceEncrypt) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setDate", new Object[] {n, x, cal, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.DATE, x, JavaType.DATE, cal, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setDate");
    }

    @Override
    public final void setTime(int n, java.sql.Time x, java.util.Calendar cal) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setTime", new Object[] {n, x, cal});
        checkClosed();
        setValue(n, JDBCType.TIME, x, JavaType.TIME, cal, false);
        loggerExternal.exiting(getClassNameLogging(), "setTime");
    }

    @Override
    public final void setTime(int n, java.sql.Time x, java.util.Calendar cal,
            boolean forceEncrypt) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setTime", new Object[] {n, x, cal, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.TIME, x, JavaType.TIME, cal, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setTime");
    }

    @Override
    public final void setTimestamp(int n, java.sql.Timestamp x, java.util.Calendar cal) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setTimestamp", new Object[] {n, x, cal});
        checkClosed();

        setValue(n, JDBCType.TIMESTAMP, x, JavaType.TIMESTAMP, cal, false);
        loggerExternal.exiting(getClassNameLogging(), "setTimestamp");
    }

    @Override
    public final void setTimestamp(int n, java.sql.Timestamp x, java.util.Calendar cal,
            boolean forceEncrypt) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setTimestamp", new Object[] {n, x, cal, forceEncrypt});
        checkClosed();

        setValue(n, JDBCType.TIMESTAMP, x, JavaType.TIMESTAMP, cal, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setTimestamp");
    }

    @Override
    public final void setNull(int paramIndex, int sqlType, String typeName) throws SQLServerException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setNull", new Object[] {paramIndex, sqlType, typeName});
        checkClosed();
        if (microsoft.sql.Types.STRUCTURED == sqlType) {
            setObject(setterGetParam(paramIndex), null, JavaType.TVP, JDBCType.of(sqlType), null, null, false,
                    paramIndex, typeName);
        } else {
            setObject(setterGetParam(paramIndex), null, JavaType.OBJECT, JDBCType.of(sqlType), null, null, false,
                    paramIndex, typeName);
        }
        loggerExternal.exiting(getClassNameLogging(), "setNull");
    }

    @Override
    public final ParameterMetaData getParameterMetaData(boolean forceRefresh) throws SQLServerException {

        SQLServerParameterMetaData pmd = this.connection.getCachedParameterMetadata(sqlTextCacheKey);

        if (!forceRefresh && null != pmd) {
            return pmd;
        } else {
            loggerExternal.entering(getClassNameLogging(), "getParameterMetaData");
            checkClosed();
            pmd = new SQLServerParameterMetaData(this, userSQL);
            connection.registerCachedParameterMetadata(sqlTextCacheKey, pmd);
            loggerExternal.exiting(getClassNameLogging(), "getParameterMetaData", pmd);
            return pmd;
        }
    }

    /* JDBC 3.0 */

    @Override
    public final ParameterMetaData getParameterMetaData() throws SQLServerException {
        return getParameterMetaData(false);
    }

    @Override
    public final void setURL(int parameterIndex, java.net.URL x) throws SQLException {
        SQLServerException.throwNotSupportedException(connection, this);
    }

    @Override
    public final void setRowId(int parameterIndex, RowId x) throws SQLException {
        SQLServerException.throwNotSupportedException(connection, this);
    }

    @Override
    public final void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        setByIndex();
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setSQLXML", new Object[] {parameterIndex, xmlObject});
        checkClosed();
        setSQLXMLInternal(parameterIndex, xmlObject);
        loggerExternal.exiting(getClassNameLogging(), "setSQLXML");
    }

    @Override
    public final int executeUpdate(String sql) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "executeUpdate", sql);
        MessageFormat form = new MessageFormat(
                SQLServerException.getErrString("R_cannotTakeArgumentsPreparedOrCallable"));
        Object[] msgArgs = {"executeUpdate()"};
        throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
    }

    @Override
    public final boolean execute(String sql) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "execute", sql);
        MessageFormat form = new MessageFormat(
                SQLServerException.getErrString("R_cannotTakeArgumentsPreparedOrCallable"));
        Object[] msgArgs = {"execute()"};
        throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
    }

    @Override
    public final java.sql.ResultSet executeQuery(String sql) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "executeQuery", sql);
        MessageFormat form = new MessageFormat(
                SQLServerException.getErrString("R_cannotTakeArgumentsPreparedOrCallable"));
        Object[] msgArgs = {"executeQuery()"};
        throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
    }

    @Override
    public void addBatch(String sql) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "addBatch", sql);
        MessageFormat form = new MessageFormat(
                SQLServerException.getErrString("R_cannotTakeArgumentsPreparedOrCallable"));
        Object[] msgArgs = {"addBatch()"};
        throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
    }

    private void clearPrepStmtHandle() {
        prepStmtHandle = 0;
        cachedPreparedStatementHandle = null;
        if (getStatementLogger().isLoggable(Level.FINER)) {
            getStatementLogger().finer(toString() + " cleared cachedPrepStmtHandle!");
        }
    }
}
