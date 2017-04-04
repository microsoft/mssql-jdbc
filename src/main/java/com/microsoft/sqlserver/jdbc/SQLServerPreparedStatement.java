/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.BatchUpdateException;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.logging.Level;

/**
 * SQLServerPreparedStatement provides JDBC prepared statement functionality. SQLServerPreparedStatement provides methods for the user to supply
 * parameters as any native Java type and many Java object types.
 * <p>
 * SQLServerPreparedStatement prepares a statement using SQL Server's sp_prepexec and re-uses the returned statement handle for each subsequent
 * execution of the statement (typically using different parameters provided by the user)
 * <p>
 * SQLServerPreparedStatement supports batching whereby a set of prepared statements are executed in a single database round trip to improve runtime
 * performance.
 * <p>
 * The API javadoc for JDBC API methods that this class implements are not repeated here. Please see Sun's JDBC API interfaces javadoc for those
 * details.
 */

public class SQLServerPreparedStatement extends SQLServerStatement implements ISQLServerPreparedStatement {
    /** Flag to indicate that it is an internal query to retrieve encryption metadata. */
    boolean isInternalEncryptionQuery = false;

    /** delimiter for multiple statements in a single batch */
    private static final int BATCH_STATEMENT_DELIMITER_TDS_71 = 0x80;
    private static final int BATCH_STATEMENT_DELIMITER_TDS_72 = 0xFF;
    final int nBatchStatementDelimiter = BATCH_STATEMENT_DELIMITER_TDS_72;

    /** the user's prepared sql syntax */
    private String sqlCommand;

    /** The prepared type definitions */
    private String preparedTypeDefinitions;

    /** The users SQL statement text */
    final String userSQL;

    /** SQL statement with expanded parameter tokens */
    private String preparedSQL;

    /** True if this execute has been called for this statement at least once */
    private boolean isExecutedAtLeastOnce = false;

    /**
     * Array with parameter names generated in buildParamTypeDefinitions For mapping encryption information to parameters, as the second result set
     * returned by sp_describe_parameter_encryption doesn't depend on order of input parameter
     **/
    private ArrayList<String> parameterNames;

    /** Set to true if the statement is a stored procedure call that expects a return value */
    final boolean bReturnValueSyntax;

    /**
     * The number of OUT parameters to skip in the response to get to the first app-declared OUT parameter.
     *
     * When executing prepared and callable statements and/or statements that produce cursored results, the first OUT parameters returned by the
     * server contain the internal values like the prepared statement handle and the cursor ID and row count. This value indicates how many of those
     * internal OUT parameters were in the response.
     */
    int outParamIndexAdjustment;

    /** Set of parameter values in the current batch */
    ArrayList<Parameter[]> batchParamValues;

    /** The prepared statement handle returned by the server */
    private int prepStmtHandle = 0;

    /** Flag set to true when statement execution is expected to return the prepared statement handle */
    private boolean expectPrepStmtHandle = false;
    
    /**
     * Flag set to true when all encryption metadata of inOutParam is retrieved
     */
    private boolean encryptionMetadataIsRetrieved = false;

    // Internal function used in tracing
    String getClassNameInternal() {
        return "SQLServerPreparedStatement";
    }

    /**
     * Create a new prepaed statement.
     * 
     * @param conn
     *            the connection
     * @param sql
     *            the user's sql
     * @param nRSType
     *            the result set type
     * @param nRSConcur
     *            the result set concurrency
     * @param stmtColEncSetting
     *            the statement column encryption setting
     * @throws SQLServerException
     *             when an error occurs
     */
    SQLServerPreparedStatement(SQLServerConnection conn,
            String sql,
            int nRSType,
            int nRSConcur,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLServerException {
        super(conn, nRSType, nRSConcur, stmtColEncSetting);
        stmtPoolable = true;
        sqlCommand = sql;

        JDBCSyntaxTranslator translator = new JDBCSyntaxTranslator();
        sql = translator.translate(sql);
        procedureName = translator.getProcedureName(); // may return null
        bReturnValueSyntax = translator.hasReturnValueSyntax();

        userSQL = sql;
        initParams(userSQL);
    }

    /**
     * Close the prepared statement's prepared handle.
     */
    private void closePreparedHandle() {
        if (0 == prepStmtHandle)
            return;

        // If the connection is already closed, don't bother trying to close
        // the prepared handle. We won't be able to, and it's already closed
        // on the server anyway.
        if (connection.isSessionUnAvailable()) {
            if (getStatementLogger().isLoggable(java.util.logging.Level.FINER))
                getStatementLogger().finer(this + ": Not closing PreparedHandle:" + prepStmtHandle + "; connection is already closed.");
        }
        else {
            isExecutedAtLeastOnce = false;
            final int handleToClose = prepStmtHandle;
            prepStmtHandle = 0;

            // Using batched clean-up? If not, use old method of calling sp_unprepare.
            if(1 < connection.getServerPreparedStatementDiscardThreshold()) {
                // Handle unprepare actions through batching @ connection level. 
                connection.enqueuePreparedStatementDiscardItem(handleToClose, executedSqlDirectly);
                connection.handlePreparedStatementDiscardActions(false);
            }
            else {
                // Non batched behavior (same as pre batch impl.)
                if (getStatementLogger().isLoggable(java.util.logging.Level.FINER))
                    getStatementLogger().finer(this + ": Closing PreparedHandle:" + handleToClose);

                final class PreparedHandleClose extends UninterruptableTDSCommand {
                    PreparedHandleClose() {
                        super("closePreparedHandle");
                    }

                    final boolean doExecute() throws SQLServerException {
                        TDSWriter tdsWriter = startRequest(TDS.PKT_RPC);
                        tdsWriter.writeShort((short) 0xFFFF); // procedure name length -> use ProcIDs
                        tdsWriter.writeShort(executedSqlDirectly ? TDS.PROCID_SP_UNPREPARE : TDS.PROCID_SP_CURSORUNPREPARE);
                        tdsWriter.writeByte((byte) 0);  // RPC procedure option 1
                        tdsWriter.writeByte((byte) 0);  // RPC procedure option 2
                        tdsWriter.writeRPCInt(null, new Integer(handleToClose), false);
                        TDSParser.parse(startResponse(), getLogContext());
                        return true;
                    }
                }

                // Try to close the server cursor. Any failure is caught, logged, and ignored.
                try {
                    executeCommand(new PreparedHandleClose());
                }
                catch (SQLServerException e) {
                    if (getStatementLogger().isLoggable(java.util.logging.Level.FINER))
                        getStatementLogger().log(Level.FINER, this + ": Error (ignored) closing PreparedHandle:" + handleToClose, e);
                }

                if (getStatementLogger().isLoggable(java.util.logging.Level.FINER))
                    getStatementLogger().finer(this + ": Closed PreparedHandle:" + handleToClose);
            }
        }
    }

    /**
     * Closes this prepared statement.
     *
     * Note that the public Statement.close() method performs all of the cleanup work through this internal method which cannot throw any exceptions.
     * This is done deliberately to ensure that ALL of the object's client-side and server-side state is cleaned up as best as possible, even under
     * conditions which would normally result in exceptions being thrown.
     */
    final void closeInternal() {
        super.closeInternal();

        // If we have a prepared statement handle, close it.
        closePreparedHandle();

        // Clean up client-side state
        batchParamValues = null;
    }

    /**
     * Intialize the statement parameters.
     * 
     * @param sql
     */
    /* L0 */ final void initParams(String sql) {
        encryptionMetadataIsRetrieved = false;
        int nParams = 0;

        // Figure out the expected number of parameters by counting the
        // parameter placeholders in the SQL string.
        int offset = -1;
        while ((offset = ParameterUtils.scanSQLForChar('?', sql, ++offset)) < sql.length())
            ++nParams;

        inOutParam = new Parameter[nParams];
        for (int i = 0; i < nParams; i++) {
            inOutParam[i] = new Parameter(Util.shouldHonorAEForParameters(stmtColumnEncriptionSetting, connection));
        }
    }

    /* L0 */ public final void clearParameters() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "clearParameters");
        checkClosed();
        encryptionMetadataIsRetrieved = false;
        int i;
        if (inOutParam == null)
            return;
        for (i = 0; i < inOutParam.length; i++) {
            inOutParam[i].clearInputValue();
        }
        loggerExternal.exiting(getClassNameLogging(), "clearParameters");
    }

    /**
     * Determines whether the statement needs to be reprepared based on a change in any of the type definitions of any of the parameters due to
     * changes in scale, length, etc., and, if so, sets the new type definition string.
     */
    private boolean buildPreparedStrings(Parameter[] params,
            boolean renewDefinition) throws SQLServerException {
        String newTypeDefinitions = buildParamTypeDefinitions(params, renewDefinition);
        if (null != preparedTypeDefinitions && newTypeDefinitions.equals(preparedTypeDefinitions))
            return false;

        preparedTypeDefinitions = newTypeDefinitions;

        /* Replace the parameter marker '?' with the param numbers @p1, @p2 etc */
        preparedSQL = connection.replaceParameterMarkers(userSQL, params, bReturnValueSyntax);
        if (bRequestedGeneratedKeys)
            preparedSQL = preparedSQL + identityQuery;

        return true;
    }

    /**
     * Build the parameter type definitons for a JDBC prepared statement that will be used to prepare the statement.
     * 
     * @param params
     *            the statement parameters
     * @param renewDefinition
     *            True if renewing parameter definition, False otherwise
     * @throws SQLServerException
     *             when an error occurs.
     * @return the required data type defintions.
     */
    private String buildParamTypeDefinitions(Parameter[] params,
            boolean renewDefinition) throws SQLServerException {
        StringBuilder sb = new StringBuilder();
        int nCols = params.length;
        char cParamName[] = new char[10];
        parameterNames = new ArrayList<String>();

        for (int i = 0; i < nCols; i++) {
            if (i > 0)
                sb.append(',');

            int l = SQLServerConnection.makeParamName(i, cParamName, 0);
            for (int j = 0; j < l; j++)
                sb.append(cParamName[j]);
            sb.append(' ');

            parameterNames.add(i, (new String(cParamName)).trim());

            params[i].renewDefinition = renewDefinition;
            String typeDefinition = params[i].getTypeDefinition(connection, resultsReader());
            if (null == typeDefinition) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_valueNotSetForParameter"));
                Object[] msgArgs = {new Integer(i + 1)};
                SQLServerException.makeFromDriverError(connection, this, form.format(msgArgs), null, false);
            }

            sb.append(typeDefinition);

            if (params[i].isOutput())
                sb.append(" OUTPUT");
        }
        return sb.toString();
    }

    /**
     * Execute a query.
     *
     * @throws SQLServerException
     *             when an error occurs
     * @return ResultSet
     */
    public java.sql.ResultSet executeQuery() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "executeQuery");
        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
        executeStatement(new PrepStmtExecCmd(this, EXECUTE_QUERY));
        loggerExternal.exiting(getClassNameLogging(), "executeQuery");
        return resultSet;
    }

    /**
     * Execute a query without cursoring for metadata.
     *
     * @throws SQLServerException
     * @return ResultSet
     */
    final java.sql.ResultSet executeQueryInternal() throws SQLServerException {
        checkClosed();
        executeStatement(new PrepStmtExecCmd(this, EXECUTE_QUERY_INTERNAL));
        return resultSet;
    }

    public int executeUpdate() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "executeUpdate");
        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }

        checkClosed();

        executeStatement(new PrepStmtExecCmd(this, EXECUTE_UPDATE));

        // this shouldn't happen, caller probably meant to call executeLargeUpdate
        if (updateCount < Integer.MIN_VALUE || updateCount > Integer.MAX_VALUE)
            SQLServerException.makeFromDriverError(connection, this, SQLServerException.getErrString("R_updateCountOutofRange"), null, true);

        loggerExternal.exiting(getClassNameLogging(), "executeUpdate", new Long(updateCount));

        return (int) updateCount;
    }

    public long executeLargeUpdate() throws SQLServerException {
        DriverJDBCVersion.checkSupportsJDBC42();

        loggerExternal.entering(getClassNameLogging(), "executeLargeUpdate");
        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
        executeStatement(new PrepStmtExecCmd(this, EXECUTE_UPDATE));
        loggerExternal.exiting(getClassNameLogging(), "executeLargeUpdate", new Long(updateCount));
        return updateCount;
    }

    /**
     * Execute a query or non query statement.
     * 
     * @throws SQLServerException
     *             when an error occurs
     * @return true if the statement returned a result set
     */
    public boolean execute() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "execute");
        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
        executeStatement(new PrepStmtExecCmd(this, EXECUTE));
        loggerExternal.exiting(getClassNameLogging(), "execute", Boolean.valueOf(null != resultSet));
        return null != resultSet;
    }

    private final class PrepStmtExecCmd extends TDSCommand {
        private final SQLServerPreparedStatement stmt;

        PrepStmtExecCmd(SQLServerPreparedStatement stmt,
                int executeMethod) {
            super(stmt.toString() + " executeXXX", queryTimeout);
            this.stmt = stmt;
            stmt.executeMethod = executeMethod;
        }

        final boolean doExecute() throws SQLServerException {
            stmt.doExecutePreparedStatement(this);
            return false;
        }

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

        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }

        boolean hasNewTypeDefinitions = true;
        if (!encryptionMetadataIsRetrieved) {
            hasNewTypeDefinitions = buildPreparedStrings(inOutParam, false);
        }

        if ((Util.shouldHonorAEForParameters(stmtColumnEncriptionSetting, connection)) && (0 < inOutParam.length) && !isInternalEncryptionQuery) {

            // retrieve paramater encryption metadata if they are not retrieved yet
            if (!encryptionMetadataIsRetrieved) {
                getParameterEncryptionMetadata(inOutParam);
                encryptionMetadataIsRetrieved = true;

                // maxRows is set to 0 when retreving encryption metadata,
                // need to set it back
                setMaxRowsAndMaxFieldSize();
            }

            // fix an issue when inserting unicode into non-encrypted nchar column using setString() and AE is on on Connection
            hasNewTypeDefinitions = buildPreparedStrings(inOutParam, true);
        }

        // Start the request and detach the response reader so that we can
        // continue using it after we return.
        TDSWriter tdsWriter = command.startRequest(TDS.PKT_RPC);

        doPrepExec(tdsWriter, inOutParam, hasNewTypeDefinitions);

        ensureExecuteResultsReader(command.startResponse(getIsResponseBufferingAdaptive()));
        startResults();
        getNextResult();

        if (EXECUTE_QUERY == executeMethod && null == resultSet) {
            SQLServerException.makeFromDriverError(connection, this, SQLServerException.getErrString("R_noResultset"), null, true);
        }
        else if (EXECUTE_UPDATE == executeMethod && null != resultSet) {
            SQLServerException.makeFromDriverError(connection, this, SQLServerException.getErrString("R_resultsetGeneratedForUpdate"), null, false);
        }
    }

    /**
     * Consume the OUT parameter for the statement object itself.
     *
     * When a prepared statement handle is expected as the first OUT parameter from PreparedStatement or CallableStatement execution, then it gets
     * consumed here.
     */
    boolean consumeExecOutParam(TDSReader tdsReader) throws SQLServerException {
        final class PrepStmtExecOutParamHandler extends StmtExecOutParamHandler {
            boolean onRetValue(TDSReader tdsReader) throws SQLServerException {
                // If no prepared statement handle is expected at this time
                // then don't consume this OUT parameter as it does not contain
                // a prepared statement handle.
                if (!expectPrepStmtHandle)
                    return super.onRetValue(tdsReader);

                // If a prepared statement handle is expected then consume it
                // and continue processing.
                expectPrepStmtHandle = false;
                Parameter param = new Parameter(Util.shouldHonorAEForParameters(stmtColumnEncriptionSetting, connection));
                param.skipRetValStatus(tdsReader);
                prepStmtHandle = param.getInt(tdsReader);
                param.skipValue(tdsReader, true);
                if (getStatementLogger().isLoggable(java.util.logging.Level.FINER))
                    getStatementLogger().finer(toString() + ": Setting PreparedHandle:" + prepStmtHandle);

                return true;
            }
        }

        if (expectPrepStmtHandle || expectCursorOutParams) {
            TDSParser.parse(tdsReader, new PrepStmtExecOutParamHandler());
            return true;
        }

        return false;
    }

    /**
     * Send the statement parameters by RPC
     */
    void sendParamsByRPC(TDSWriter tdsWriter,
            Parameter[] params) throws SQLServerException {
        char cParamName[];
        for (int index = 0; index < params.length; index++) {
            if (JDBCType.TVP == params[index].getJdbcType()) {
                cParamName = new char[10];
                int paramNameLen = SQLServerConnection.makeParamName(index, cParamName, 0);
                tdsWriter.writeByte((byte) paramNameLen);
                tdsWriter.writeString(new String(cParamName, 0, paramNameLen));
            }
            params[index].sendByRPC(tdsWriter, connection);
        }
    }

    private void buildServerCursorPrepExecParams(TDSWriter tdsWriter) throws SQLServerException {
        if (getStatementLogger().isLoggable(java.util.logging.Level.FINE))
            getStatementLogger().fine(toString() + ": calling sp_cursorprepexec: PreparedHandle:" + prepStmtHandle + ", SQL:" + preparedSQL);

        expectPrepStmtHandle = true;
        executedSqlDirectly = false;
        expectCursorOutParams = true;
        outParamIndexAdjustment = 7;

        tdsWriter.writeShort((short) 0xFFFF); // procedure name length -> use ProcIDs
        tdsWriter.writeShort(TDS.PROCID_SP_CURSORPREPEXEC);
        tdsWriter.writeByte((byte) 0);  // RPC procedure option 1
        tdsWriter.writeByte((byte) 0);  // RPC procedure option 2

        // <prepared handle>
        // IN (reprepare): Old handle to unprepare before repreparing
        // OUT: The newly prepared handle
        tdsWriter.writeRPCInt(null, new Integer(prepStmtHandle), true);
        prepStmtHandle = 0;

        // <cursor> OUT
        tdsWriter.writeRPCInt(null, new Integer(0), true); // cursor ID (OUTPUT)

        // <formal parameter defn> IN
        tdsWriter.writeRPCStringUnicode((preparedTypeDefinitions.length() > 0) ? preparedTypeDefinitions : null);

        // <stmt> IN
        tdsWriter.writeRPCStringUnicode(preparedSQL);

        // <scrollopt> IN
        // Note: we must strip out SCROLLOPT_PARAMETERIZED_STMT if we don't
        // actually have any parameters.
        tdsWriter.writeRPCInt(null,
                new Integer(getResultSetScrollOpt() & ~((0 == preparedTypeDefinitions.length()) ? TDS.SCROLLOPT_PARAMETERIZED_STMT : 0)), false);

        // <ccopt> IN
        tdsWriter.writeRPCInt(null, new Integer(getResultSetCCOpt()), false);

        // <rowcount> OUT
        tdsWriter.writeRPCInt(null, new Integer(0), true);
    }

    private void buildPrepExecParams(TDSWriter tdsWriter) throws SQLServerException {
        if (getStatementLogger().isLoggable(java.util.logging.Level.FINE))
            getStatementLogger().fine(toString() + ": calling sp_prepexec: PreparedHandle:" + prepStmtHandle + ", SQL:" + preparedSQL);

        expectPrepStmtHandle = true;
        executedSqlDirectly = true;
        expectCursorOutParams = false;
        outParamIndexAdjustment = 3;

        tdsWriter.writeShort((short) 0xFFFF); // procedure name length -> use ProcIDs
        tdsWriter.writeShort(TDS.PROCID_SP_PREPEXEC);
        tdsWriter.writeByte((byte) 0);  // RPC procedure option 1
        tdsWriter.writeByte((byte) 0);  // RPC procedure option 2

        // <prepared handle>
        // IN (reprepare): Old handle to unprepare before repreparing
        // OUT: The newly prepared handle
        tdsWriter.writeRPCInt(null, new Integer(prepStmtHandle), true);
        prepStmtHandle = 0;

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
        tdsWriter.writeByte((byte) 0);  // RPC procedure option 1
        tdsWriter.writeByte((byte) 0);  // RPC procedure option 2

        // No handle used.
        prepStmtHandle = 0;

        // <stmt> IN
        tdsWriter.writeRPCStringUnicode(preparedSQL);

        // <formal parameter defn> IN
        tdsWriter.writeRPCStringUnicode((preparedTypeDefinitions.length() > 0) ? preparedTypeDefinitions : null);
    }

    private void buildServerCursorExecParams(TDSWriter tdsWriter) throws SQLServerException {
        if (getStatementLogger().isLoggable(java.util.logging.Level.FINE))
            getStatementLogger().fine(toString() + ": calling sp_cursorexecute: PreparedHandle:" + prepStmtHandle + ", SQL:" + preparedSQL);

        expectPrepStmtHandle = false;
        executedSqlDirectly = false;
        expectCursorOutParams = true;
        outParamIndexAdjustment = 5;

        tdsWriter.writeShort((short) 0xFFFF); // procedure name length -> use ProcIDs
        tdsWriter.writeShort(TDS.PROCID_SP_CURSOREXECUTE);
        tdsWriter.writeByte((byte) 0);  // RPC procedure option 1
        tdsWriter.writeByte((byte) 0);  // RPC procedure option 2 */

        // <handle> IN
        assert 0 != prepStmtHandle;
        tdsWriter.writeRPCInt(null, new Integer(prepStmtHandle), false);

        // <cursor> OUT
        tdsWriter.writeRPCInt(null, new Integer(0), true);

        // <scrollopt> IN
        tdsWriter.writeRPCInt(null, new Integer(getResultSetScrollOpt() & ~TDS.SCROLLOPT_PARAMETERIZED_STMT), false);

        // <ccopt> IN
        tdsWriter.writeRPCInt(null, new Integer(getResultSetCCOpt()), false);

        // <rowcount> OUT
        tdsWriter.writeRPCInt(null, new Integer(0), true);
    }

    private void buildExecParams(TDSWriter tdsWriter) throws SQLServerException {
        if (getStatementLogger().isLoggable(java.util.logging.Level.FINE))
            getStatementLogger().fine(toString() + ": calling sp_execute: PreparedHandle:" + prepStmtHandle + ", SQL:" + preparedSQL);

        expectPrepStmtHandle = false;
        executedSqlDirectly = true;
        expectCursorOutParams = false;
        outParamIndexAdjustment = 1;

        tdsWriter.writeShort((short) 0xFFFF); // procedure name length -> use ProcIDs
        tdsWriter.writeShort(TDS.PROCID_SP_EXECUTE);
        tdsWriter.writeByte((byte) 0);  // RPC procedure option 1
        tdsWriter.writeByte((byte) 0);  // RPC procedure option 2 */

        // <handle> IN
        assert 0 != prepStmtHandle;
        tdsWriter.writeRPCInt(null, new Integer(prepStmtHandle), false);
    }

    private void getParameterEncryptionMetadata(Parameter[] params) throws SQLServerException {
        /*
         * The parameter list is created from the data types provided by the user for the parameters. the data types do not need to be the same as in
         * the table definition. Also, when string is sent to an int field, the parameter is defined as nvarchar(<size of string>). Same for varchar
         * datatypes, exact length is used.
         */
        SQLServerResultSet rs = null;
        SQLServerCallableStatement stmt = null;

        assert connection != null : "Connection should not be null";

        try {
            if (getStatementLogger().isLoggable(java.util.logging.Level.FINE)) {
                getStatementLogger().fine("Calling stored procedure sp_describe_parameter_encryption to get parameter encryption information.");
            }

            stmt = (SQLServerCallableStatement) connection.prepareCall("exec sp_describe_parameter_encryption ?,?");
            stmt.isInternalEncryptionQuery = true;
            stmt.setNString(1, preparedSQL);
            stmt.setNString(2, preparedTypeDefinitions);
            rs = (SQLServerResultSet) stmt.executeQueryInternal();
        }
        catch (SQLException e) {
            if (e instanceof SQLServerException) {
                throw (SQLServerException) e;
            }
            else {
                throw new SQLServerException(SQLServerException.getErrString("R_UnableRetrieveParameterMetadata"), null, 0, e);
            }
        }

        if (null == rs) {
            // No results. Meaning no parameter.
            // Should never happen.
            return;
        }

        Map<Integer, CekTableEntry> cekList = new HashMap<Integer, CekTableEntry>();
        CekTableEntry cekEntry = null;
        try {
            while (rs.next()) {
                int currentOrdinal = rs.getInt(DescribeParameterEncryptionResultSet1.KeyOrdinal.value());
                if (!cekList.containsKey(currentOrdinal)) {
                    cekEntry = new CekTableEntry(currentOrdinal);
                    cekList.put(cekEntry.ordinal, cekEntry);
                }
                else {
                    cekEntry = cekList.get(currentOrdinal);
                }
                cekEntry.add(rs.getBytes(DescribeParameterEncryptionResultSet1.EncryptedKey.value()),
                        rs.getInt(DescribeParameterEncryptionResultSet1.DbId.value()), rs.getInt(DescribeParameterEncryptionResultSet1.KeyId.value()),
                        rs.getInt(DescribeParameterEncryptionResultSet1.KeyVersion.value()),
                        rs.getBytes(DescribeParameterEncryptionResultSet1.KeyMdVersion.value()),
                        rs.getString(DescribeParameterEncryptionResultSet1.KeyPath.value()),
                        rs.getString(DescribeParameterEncryptionResultSet1.ProviderName.value()),
                        rs.getString(DescribeParameterEncryptionResultSet1.KeyEncryptionAlgorithm.value()));
            }
            if (getStatementLogger().isLoggable(java.util.logging.Level.FINE)) {
                getStatementLogger().fine("Matadata of CEKs is retrieved.");
            }
        }
        catch (SQLException e) {
            if (e instanceof SQLServerException) {
                throw (SQLServerException) e;
            }
            else {
                throw new SQLServerException(SQLServerException.getErrString("R_UnableRetrieveParameterMetadata"), null, 0, e);
            }
        }

        // Process the second resultset.
        if (!stmt.getMoreResults()) {
            throw new SQLServerException(this, SQLServerException.getErrString("R_UnexpectedDescribeParamFormat"), null, 0, false);
        }

        // Parameter count in the result set.
        int paramCount = 0;
        try {
            rs = (SQLServerResultSet) stmt.getResultSet();
            while (rs.next()) {
                paramCount++;
                String paramName = rs.getString(DescribeParameterEncryptionResultSet2.ParameterName.value());
                int paramIndex = parameterNames.indexOf(paramName);
                int cekOrdinal = rs.getInt(DescribeParameterEncryptionResultSet2.ColumnEncryptionKeyOrdinal.value());
                cekEntry = cekList.get(cekOrdinal);

                // cekEntry will be null if none of the parameters are encrypted.
                if ((null != cekEntry) && (cekList.size() < cekOrdinal)) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidEncryptionKeyOridnal"));
                    Object[] msgArgs = {cekOrdinal, cekEntry.getSize()};
                    throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
                }
                SQLServerEncryptionType encType = SQLServerEncryptionType
                        .of((byte) rs.getInt(DescribeParameterEncryptionResultSet2.ColumnEncrytionType.value()));
                if (SQLServerEncryptionType.PlainText != encType) {
                    params[paramIndex].cryptoMeta = new CryptoMetadata(cekEntry, (short) cekOrdinal,
                            (byte) rs.getInt(DescribeParameterEncryptionResultSet2.ColumnEncryptionAlgorithm.value()), null, encType.value,
                            (byte) rs.getInt(DescribeParameterEncryptionResultSet2.NormalizationRuleVersion.value()));
                    // Decrypt the symmetric key.(This will also validate and throw if needed).
                    SQLServerSecurityUtility.decryptSymmetricKey(params[paramIndex].cryptoMeta, connection);
                }
                else {
                    if (true == params[paramIndex].getForceEncryption()) {
                        MessageFormat form = new MessageFormat(
                                SQLServerException.getErrString("R_ForceEncryptionTrue_HonorAETrue_UnencryptedColumn"));
                        Object[] msgArgs = {userSQL, paramIndex + 1};
                        SQLServerException.makeFromDriverError(connection, this, form.format(msgArgs), null, true);
                    }
                }
            }
            if (getStatementLogger().isLoggable(java.util.logging.Level.FINE)) {
                getStatementLogger().fine("Parameter encryption metadata is set.");
            }
        }
        catch (SQLException e) {
            if (e instanceof SQLServerException) {
                throw (SQLServerException) e;
            }
            else {
                throw new SQLServerException(SQLServerException.getErrString("R_UnableRetrieveParameterMetadata"), null, 0, e);
            }
        }

        if (paramCount != params.length) {
            // Encryption metadata wasn't sent by the server.
            // We expect the metadata to be sent for all the parameters in the original sp_describe_parameter_encryption.
            // For parameters that don't need encryption, the encryption type is set to plaintext.
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_MissingParamEncryptionMetadata"));
            Object[] msgArgs = {userSQL};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
        }

        // Null check for rs is done already.
        rs.close();

        if (null != stmt) {
            stmt.close();
        }
        connection.resetCurrentCommand();
    }

    private boolean doPrepExec(TDSWriter tdsWriter,
            Parameter[] params,
            boolean hasNewTypeDefinitions) throws SQLServerException {
       
        boolean needsPrepare = hasNewTypeDefinitions || 0 == prepStmtHandle;

        // Cursors never go the non-prepared statement route.
        if (isCursorable(executeMethod)) {
            if (needsPrepare) 
                buildServerCursorPrepExecParams(tdsWriter);
            else
                buildServerCursorExecParams(tdsWriter);
        }
        else {
            // Move overhead of needing to do prepare & unprepare to only use cases that need more than one execution.
            // First execution, use sp_executesql, optimizing for asumption we will not re-use statement.
            if (!connection.getEnablePrepareOnFirstPreparedStatementCall() && !isExecutedAtLeastOnce) {
                buildExecSQLParams(tdsWriter);
                isExecutedAtLeastOnce = true;
            }
            // Second execution, use prepared statements since we seem to be re-using it.
            else if(needsPrepare)
                buildPrepExecParams(tdsWriter);
            else
                buildExecParams(tdsWriter);
        }

        sendParamsByRPC(tdsWriter, params);

        return needsPrepare;
    }

    /* L0 */ public final java.sql.ResultSetMetaData getMetaData() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getMetaData");
        checkClosed();
        boolean rsclosed = false;
        java.sql.ResultSetMetaData rsmd = null;
        try {
            // if the result is closed, cant get the metadata from it.
            if (resultSet != null)
                resultSet.checkClosed();
        }
        catch (SQLServerException e) {
            rsclosed = true;
        }
        if (resultSet == null || rsclosed) {
            SQLServerResultSet emptyResultSet = (SQLServerResultSet) buildExecuteMetaData();
            if (null != emptyResultSet)
                rsmd = emptyResultSet.getMetaData();
        }
        else if (resultSet != null) {
            rsmd = resultSet.getMetaData();
        }
        loggerExternal.exiting(getClassNameLogging(), "getMetaData", rsmd);
        return rsmd;
    }

    /**
     * Retreive meta data for the statement before executing it. This is called in cases where the driver needs the meta data prior to executing the
     * statement.
     * 
     * @throws SQLServerException
     * @return the result set containing the meta data
     */
    /* L0 */ private ResultSet buildExecuteMetaData() throws SQLServerException {
        String fmtSQL = sqlCommand;
        if (fmtSQL.indexOf(LEFT_CURLY_BRACKET) >= 0) {
            fmtSQL = (new JDBCSyntaxTranslator()).translate(fmtSQL);
        }

        ResultSet emptyResultSet = null;
        try {
            fmtSQL = replaceMarkerWithNull(fmtSQL);
            SQLServerStatement stmt = (SQLServerStatement) connection.createStatement();
            emptyResultSet = stmt.executeQueryInternal("set fmtonly on " + fmtSQL + "\nset fmtonly off");
        }
        catch (SQLException sqle) {
            if (false == sqle.getMessage().equals(SQLServerException.getErrString("R_noResultset"))) {
                // if the error is not no resultset then throw a processings error.
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_processingError"));
                Object[] msgArgs = {sqle.getMessage()};

                SQLServerException.makeFromDriverError(connection, this, form.format(msgArgs), null, true);
            }
        }
        return emptyResultSet;
    }

    /* -------------- JDBC API Implementation ------------------ */

    /**
     * Set the parameter value for a statement.
     *
     * @param index
     *            The index of the parameter to set starting at 1.
     * @return A reference the to Parameter object created or referenced.
     * @exception SQLServerException
     *                The index specified was outside the number of paramters for the statement.
     */
    /* L0 */ final Parameter setterGetParam(int index) throws SQLServerException {
        if (index < 1 || index > inOutParam.length) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_indexOutOfRange"));
            Object[] msgArgs = {new Integer(index)};
            SQLServerException.makeFromDriverError(connection, this, form.format(msgArgs), "07009", false);
        }

        return inOutParam[index - 1];
    }

    final void setValue(int parameterIndex,
            JDBCType jdbcType,
            Object value,
            JavaType javaType,
            String tvpName) throws SQLServerException {
        setterGetParam(parameterIndex).setValue(jdbcType, value, javaType, null, null, null, null, connection, false, stmtColumnEncriptionSetting,
                parameterIndex, userSQL, tvpName);
    }

    final void setValue(int parameterIndex,
            JDBCType jdbcType,
            Object value,
            JavaType javaType,
            boolean forceEncrypt) throws SQLServerException {
        setterGetParam(parameterIndex).setValue(jdbcType, value, javaType, null, null, null, null, connection, forceEncrypt,
                stmtColumnEncriptionSetting, parameterIndex, userSQL, null);
    }

    final void setValue(int parameterIndex,
            JDBCType jdbcType,
            Object value,
            JavaType javaType,
            Integer precision,
            Integer scale,
            boolean forceEncrypt) throws SQLServerException {
        setterGetParam(parameterIndex).setValue(jdbcType, value, javaType, null, null, precision, scale, connection, forceEncrypt,
                stmtColumnEncriptionSetting, parameterIndex, userSQL, null);
    }

    final void setValue(int parameterIndex,
            JDBCType jdbcType,
            Object value,
            JavaType javaType,
            Calendar cal,
            boolean forceEncrypt) throws SQLServerException {
        setterGetParam(parameterIndex).setValue(jdbcType, value, javaType, null, cal, null, null, connection, forceEncrypt,
                stmtColumnEncriptionSetting, parameterIndex, userSQL, null);
    }

    final void setStream(int parameterIndex,
            StreamType streamType,
            Object streamValue,
            JavaType javaType,
            long length) throws SQLServerException {
        setterGetParam(parameterIndex).setValue(streamType.getJDBCType(), streamValue, javaType, new StreamSetterArgs(streamType, length), null, null,
                null, connection, false, stmtColumnEncriptionSetting, parameterIndex, userSQL, null);
    }

    final void setSQLXMLInternal(int parameterIndex,
            SQLXML value) throws SQLServerException {
        setterGetParam(parameterIndex).setValue(JDBCType.SQLXML, value, JavaType.SQLXML,
                new StreamSetterArgs(StreamType.SQLXML, DataTypes.UNKNOWN_STREAM_LENGTH), null, null, null, connection, false,
                stmtColumnEncriptionSetting, parameterIndex, userSQL, null);
    }

    public final void setAsciiStream(int parameterIndex,
            InputStream x) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setAsciiStream", new Object[] {parameterIndex, x});
        checkClosed();
        setStream(parameterIndex, StreamType.ASCII, x, JavaType.INPUTSTREAM, DataTypes.UNKNOWN_STREAM_LENGTH);
        loggerExternal.exiting(getClassNameLogging(), "setAsciiStream");
    }

    public final void setAsciiStream(int n,
            java.io.InputStream x,
            int length) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setAsciiStream", new Object[] {n, x, length});
        checkClosed();
        setStream(n, StreamType.ASCII, x, JavaType.INPUTSTREAM, length);
        loggerExternal.exiting(getClassNameLogging(), "setAsciiStream");
    }

    public final void setAsciiStream(int parameterIndex,
            InputStream x,
            long length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setAsciiStream", new Object[] {parameterIndex, x, length});
        checkClosed();
        setStream(parameterIndex, StreamType.ASCII, x, JavaType.INPUTSTREAM, length);
        loggerExternal.exiting(getClassNameLogging(), "setAsciiStream");
    }

    private Parameter getParam(int index) throws SQLServerException {
        index--;
        if (index < 0 || index >= inOutParam.length) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_indexOutOfRange"));
            Object[] msgArgs = {new Integer(index + 1)};
            SQLServerException.makeFromDriverError(connection, this, form.format(msgArgs), "07009", false);
        }
        return inOutParam[index];
    }

    public final void setBigDecimal(int n,
            BigDecimal x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBigDecimal", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.DECIMAL, x, JavaType.BIGDECIMAL, false);
        loggerExternal.exiting(getClassNameLogging(), "setBigDecimal");
    }

    /**
     * Sets the designated parameter to the given <code>java.math.BigDecimal</code> value. The driver converts this to an SQL <code>NUMERIC</code>
     * value when it sends it to the database.
     *
     * @param n
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @param precision
     *            the precision of the column
     * @param scale
     *            the scale of the column
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setBigDecimal(int n,
            BigDecimal x,
            int precision,
            int scale) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBigDecimal", new Object[] {n, x, precision, scale});
        checkClosed();
        setValue(n, JDBCType.DECIMAL, x, JavaType.BIGDECIMAL, precision, scale, false);
        loggerExternal.exiting(getClassNameLogging(), "setBigDecimal");
    }

    /**
     * Sets the designated parameter to the given <code>java.math.BigDecimal</code> value. The driver converts this to an SQL <code>NUMERIC</code>
     * value when it sends it to the database.
     *
     * @param n
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @param precision
     *            the precision of the column
     * @param scale
     *            the scale of the column
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setBigDecimal(int n,
            BigDecimal x,
            int precision,
            int scale,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBigDecimal", new Object[] {n, x, precision, scale, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.DECIMAL, x, JavaType.BIGDECIMAL, precision, scale, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setBigDecimal");
    }

    /**
     * Sets the designated parameter to the given <code>java.math.BigDecimal</code> value. The driver converts this to an SQL <code>NUMERIC</code>
     * value when it sends it to the database.
     *
     * @param n
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setMoney(int n,
            BigDecimal x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setMoney", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.MONEY, x, JavaType.BIGDECIMAL, false);
        loggerExternal.exiting(getClassNameLogging(), "setMoney");
    }

    /**
     * Sets the designated parameter to the given <code>java.math.BigDecimal</code> value. The driver converts this to an SQL <code>NUMERIC</code>
     * value when it sends it to the database.
     *
     * @param n
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setMoney(int n,
            BigDecimal x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setMoney", new Object[] {n, x, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.MONEY, x, JavaType.BIGDECIMAL, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setMoney");
    }

    /**
     * Sets the designated parameter to the given <code>java.math.BigDecimal</code> value. The driver converts this to an SQL <code>NUMERIC</code>
     * value when it sends it to the database.
     *
     * @param n
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setSmallMoney(int n,
            BigDecimal x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setSmallMoney", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.SMALLMONEY, x, JavaType.BIGDECIMAL, false);
        loggerExternal.exiting(getClassNameLogging(), "setSmallMoney");
    }

    /**
     * Sets the designated parameter to the given <code>java.math.BigDecimal</code> value. The driver converts this to an SQL <code>NUMERIC</code>
     * value when it sends it to the database.
     *
     * @param n
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setSmallMoney(int n,
            BigDecimal x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setSmallMoney", new Object[] {n, x, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.SMALLMONEY, x, JavaType.BIGDECIMAL, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setSmallMoney");
    }

    public final void setBinaryStream(int parameterIndex,
            InputStream x) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBinaryStreaml", new Object[] {parameterIndex, x});
        checkClosed();
        setStream(parameterIndex, StreamType.BINARY, x, JavaType.INPUTSTREAM, DataTypes.UNKNOWN_STREAM_LENGTH);
        loggerExternal.exiting(getClassNameLogging(), "setBinaryStream");
    }

    public final void setBinaryStream(int n,
            java.io.InputStream x,
            int length) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBinaryStream", new Object[] {n, x, length});
        checkClosed();
        setStream(n, StreamType.BINARY, x, JavaType.INPUTSTREAM, length);
        loggerExternal.exiting(getClassNameLogging(), "setBinaryStream");
    }

    public final void setBinaryStream(int parameterIndex,
            InputStream x,
            long length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBinaryStream", new Object[] {parameterIndex, x, length});
        checkClosed();
        setStream(parameterIndex, StreamType.BINARY, x, JavaType.INPUTSTREAM, length);
        loggerExternal.exiting(getClassNameLogging(), "setBinaryStream");
    }

    public final void setBoolean(int n,
            boolean x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBoolean", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.BIT, Boolean.valueOf(x), JavaType.BOOLEAN, false);
        loggerExternal.exiting(getClassNameLogging(), "setBoolean");
    }

    /**
     * Sets the designated parameter to the given Java <code>boolean</code> value. The driver converts this to an SQL <code>BIT</code> or
     * <code>BOOLEAN</code> value when it sends it to the database.
     *
     * @param n
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setBoolean(int n,
            boolean x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBoolean", new Object[] {n, x, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.BIT, Boolean.valueOf(x), JavaType.BOOLEAN, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setBoolean");
    }

    public final void setByte(int n,
            byte x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setByte", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.TINYINT, Byte.valueOf(x), JavaType.BYTE, false);
        loggerExternal.exiting(getClassNameLogging(), "setByte");
    }

    /**
     * Sets the designated parameter to the given Java <code>byte</code> value. The driver converts this to an SQL <code>TINYINT</code> value when it
     * sends it to the database.
     *
     * @param n
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setByte(int n,
            byte x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setByte", new Object[] {n, x, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.TINYINT, Byte.valueOf(x), JavaType.BYTE, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setByte");
    }

    public final void setBytes(int n,
            byte x[]) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBytes", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.BINARY, x, JavaType.BYTEARRAY, false);
        loggerExternal.exiting(getClassNameLogging(), "setBytes");
    }

    /**
     * Sets the designated parameter to the given Java array of bytes. The driver converts this to an SQL <code>VARBINARY</code> or
     * <code>LONGVARBINARY</code> (depending on the argument's size relative to the driver's limits on <code>VARBINARY</code> values) when it sends it
     * to the database.
     *
     * @param n
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setBytes(int n,
            byte x[],
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBytes", new Object[] {n, x, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.BINARY, x, JavaType.BYTEARRAY, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setBytes");
    }

    /**
     * Sets the designated parameter to the given String. The driver converts this to an SQL <code>GUID</code>
     * 
     * @param index
     *            the first parameter is 1, the second is 2, ...
     * @param guid
     *            string representation of the uniqueIdentifier value
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setUniqueIdentifier(int index,
            String guid) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setUniqueIdentifier", new Object[] {index, guid});
        checkClosed();
        setValue(index, JDBCType.GUID, guid, JavaType.STRING, false);
        loggerExternal.exiting(getClassNameLogging(), "setUniqueIdentifier");
    }

    /**
     * Sets the designated parameter to the given String. The driver converts this to an SQL <code>GUID</code>
     * 
     * @param index
     *            the first parameter is 1, the second is 2, ...
     * @param guid
     *            string representation of the uniqueIdentifier value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setUniqueIdentifier(int index,
            String guid,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setUniqueIdentifier", new Object[] {index, guid, forceEncrypt});
        checkClosed();
        setValue(index, JDBCType.GUID, guid, JavaType.STRING, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setUniqueIdentifier");
    }

    public final void setDouble(int n,
            double x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setDouble", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.DOUBLE, Double.valueOf(x), JavaType.DOUBLE, false);
        loggerExternal.exiting(getClassNameLogging(), "setDouble");
    }

    /**
     * Sets the designated parameter to the given Java <code>double</code> value. The driver converts this to an SQL <code>DOUBLE</code> value when it
     * sends it to the database.
     *
     * @param n
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setDouble(int n,
            double x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setDouble", new Object[] {n, x, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.DOUBLE, Double.valueOf(x), JavaType.DOUBLE, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setDouble");
    }

    public final void setFloat(int n,
            float x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setFloat", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.REAL, Float.valueOf(x), JavaType.FLOAT, false);
        loggerExternal.exiting(getClassNameLogging(), "setFloat");
    }

    /**
     * Sets the designated parameter to the given Java <code>float</code> value. The driver converts this to an SQL <code>REAL</code> value when it
     * sends it to the database.
     *
     * @param n
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setFloat(int n,
            float x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setFloat", new Object[] {n, x, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.REAL, Float.valueOf(x), JavaType.FLOAT, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setFloat");
    }

    public final void setInt(int n,
            int value) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setInt", new Object[] {n, value});
        checkClosed();
        setValue(n, JDBCType.INTEGER, Integer.valueOf(value), JavaType.INTEGER, false);
        loggerExternal.exiting(getClassNameLogging(), "setInt");
    }

    /**
     * Sets the designated parameter to the given Java <code>int</code> value. The driver converts this to an SQL <code>INTEGER</code> value when it
     * sends it to the database.
     *
     * @param n
     *            the first parameter is 1, the second is 2, ...
     * @param value
     *            the parameter value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setInt(int n,
            int value,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setInt", new Object[] {n, value, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.INTEGER, Integer.valueOf(value), JavaType.INTEGER, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setInt");
    }

    public final void setLong(int n,
            long x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setLong", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.BIGINT, Long.valueOf(x), JavaType.LONG, false);
        loggerExternal.exiting(getClassNameLogging(), "setLong");
    }

    /**
     * Sets the designated parameter to the given Java <code>long</code> value. The driver converts this to an SQL <code>BIGINT</code> value when it
     * sends it to the database.
     *
     * @param n
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setLong(int n,
            long x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setLong", new Object[] {n, x, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.BIGINT, Long.valueOf(x), JavaType.LONG, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setLong");
    }

    public final void setNull(int index,
            int jdbcType) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setNull", new Object[] {index, jdbcType});
        checkClosed();
        setObject(setterGetParam(index), null, JavaType.OBJECT, JDBCType.of(jdbcType), null, null, false, index, null);
        loggerExternal.exiting(getClassNameLogging(), "setNull");
    }

    final void setObjectNoType(int index,
            Object obj,
            boolean forceEncrypt) throws SQLServerException {
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
        }
        else {
            JavaType javaType = JavaType.of(obj);
            if (JavaType.TVP == javaType) {
                tvpName = getTVPNameIfNull(index, null);   // will return null if called from preparedStatement

                if ((null == tvpName) && (obj instanceof ResultSet)) {
                    throw new SQLServerException(SQLServerException.getErrString("R_TVPnotWorkWithSetObjectResultSet"), null);
                }
            }
            targetJDBCType = javaType.getJDBCType(SSType.UNKNOWN, targetJDBCType);

            if (JDBCType.UNKNOWN == targetJDBCType) {
                if (obj instanceof java.util.UUID) {
                    javaType = JavaType.STRING;
                    targetJDBCType = JDBCType.GUID;
                }
            }

            setObject(param, obj, javaType, targetJDBCType, null, null, forceEncrypt, index, tvpName);
        }
    }

    public final void setObject(int index,
            Object obj) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setObject", new Object[] {index, obj});
        checkClosed();
        setObjectNoType(index, obj, false);
        loggerExternal.exiting(getClassNameLogging(), "setObject");
    }

    public final void setObject(int n,
            Object obj,
            int jdbcType) throws SQLServerException {
        String tvpName = null;
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setObject", new Object[] {n, obj, jdbcType});
        checkClosed();
        if (microsoft.sql.Types.STRUCTURED == jdbcType)
            tvpName = getTVPNameIfNull(n, null);
        setObject(setterGetParam(n), obj, JavaType.of(obj), JDBCType.of(jdbcType), null, null, false, n, tvpName);
        loggerExternal.exiting(getClassNameLogging(), "setObject");
    }

    public final void setObject(int parameterIndex,
            Object x,
            int targetSqlType,
            int scaleOrLength) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setObject", new Object[] {parameterIndex, x, targetSqlType, scaleOrLength});
        checkClosed();

        // scaleOrLength - for java.sql.Types.DECIMAL, java.sql.Types.NUMERIC or temporal types,
        // this is the number of digits after the decimal point. For Java Object types
        // InputStream and Reader, this is the length of the data in the stream or reader.
        // For all other types, this value will be ignored.

        setObject(setterGetParam(parameterIndex), x, JavaType.of(x), JDBCType.of(targetSqlType),
                (java.sql.Types.NUMERIC == targetSqlType || java.sql.Types.DECIMAL == targetSqlType || java.sql.Types.TIMESTAMP == targetSqlType
                        || java.sql.Types.TIME == targetSqlType || microsoft.sql.Types.DATETIMEOFFSET == targetSqlType
                        || InputStream.class.isInstance(x) || Reader.class.isInstance(x)) ? Integer.valueOf(scaleOrLength) : null,
                null, false, parameterIndex, null);

        loggerExternal.exiting(getClassNameLogging(), "setObject");
    }

    /**
     * <p>
     * Sets the value of the designated parameter with the given object.
     *
     * <p>
     * The given Java object will be converted to the given targetSqlType before being sent to the database.
     *
     * If the object has a custom mapping (is of a class implementing the interface <code>SQLData</code>), the JDBC driver should call the method
     * <code>SQLData.writeSQL</code> to write it to the SQL data stream. If, on the other hand, the object is of a class implementing
     * <code>Ref</code>, <code>Blob</code>, <code>Clob</code>, <code>NClob</code>, <code>Struct</code>, <code>java.net.URL</code>, or
     * <code>Array</code>, the driver should pass it to the database as a value of the corresponding SQL type.
     *
     * <p>
     * Note that this method may be used to pass database-specific abstract data types.
     *
     * @param parameterIndex
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the object containing the input parameter value
     * @param targetSqlType
     *            the SQL type (as defined in java.sql.Types) to be sent to the database. The scale argument may further qualify this type.
     * @param precision
     *            the precision of the column
     * @param scale
     *            scale of the column
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setObject(int parameterIndex,
            Object x,
            int targetSqlType,
            Integer precision,
            int scale) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setObject", new Object[] {parameterIndex, x, targetSqlType, precision, scale});
        checkClosed();

        // scale - for java.sql.Types.DECIMAL or java.sql.Types.NUMERIC types,
        // this is the number of digits after the decimal point. For Java Object types
        // InputStream and Reader, this is the length of the data in the stream or reader.
        // For all other types, this value will be ignored.

        setObject(setterGetParam(parameterIndex), x, JavaType.of(x),
                JDBCType.of(targetSqlType), (java.sql.Types.NUMERIC == targetSqlType || java.sql.Types.DECIMAL == targetSqlType
                        || InputStream.class.isInstance(x) || Reader.class.isInstance(x)) ? Integer.valueOf(scale) : null,
                precision, false, parameterIndex, null);

        loggerExternal.exiting(getClassNameLogging(), "setObject");
    }

    /**
     * <p>
     * Sets the value of the designated parameter with the given object.
     *
     * <p>
     * The given Java object will be converted to the given targetSqlType before being sent to the database.
     *
     * If the object has a custom mapping (is of a class implementing the interface <code>SQLData</code>), the JDBC driver should call the method
     * <code>SQLData.writeSQL</code> to write it to the SQL data stream. If, on the other hand, the object is of a class implementing
     * <code>Ref</code>, <code>Blob</code>, <code>Clob</code>, <code>NClob</code>, <code>Struct</code>, <code>java.net.URL</code>, or
     * <code>Array</code>, the driver should pass it to the database as a value of the corresponding SQL type.
     *
     * <p>
     * Note that this method may be used to pass database-specific abstract data types.
     *
     * @param parameterIndex
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the object containing the input parameter value
     * @param targetSqlType
     *            the SQL type (as defined in java.sql.Types) to be sent to the database. The scale argument may further qualify this type.
     * @param precision
     *            the precision of the column
     * @param scale
     *            scale of the column
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setObject(int parameterIndex,
            Object x,
            int targetSqlType,
            Integer precision,
            int scale,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setObject",
                    new Object[] {parameterIndex, x, targetSqlType, precision, scale, forceEncrypt});
        checkClosed();

        // scale - for java.sql.Types.DECIMAL or java.sql.Types.NUMERIC types,
        // this is the number of digits after the decimal point. For Java Object types
        // InputStream and Reader, this is the length of the data in the stream or reader.
        // For all other types, this value will be ignored.

        setObject(setterGetParam(parameterIndex), x, JavaType.of(x),
                JDBCType.of(targetSqlType), (java.sql.Types.NUMERIC == targetSqlType || java.sql.Types.DECIMAL == targetSqlType
                        || InputStream.class.isInstance(x) || Reader.class.isInstance(x)) ? Integer.valueOf(scale) : null,
                precision, forceEncrypt, parameterIndex, null);

        loggerExternal.exiting(getClassNameLogging(), "setObject");
    }

    final void setObject(Parameter param,
            Object obj,
            JavaType javaType,
            JDBCType jdbcType,
            Integer scale,
            Integer precision,
            boolean forceEncrypt,
            int parameterIndex,
            String tvpName) throws SQLServerException {
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
                    streamSetterArgs = new StreamSetterArgs(jdbcType.isTextual() ? StreamType.CHARACTER : StreamType.BINARY,
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
            param.setValue(jdbcType, obj, javaType, streamSetterArgs, null, precision, scale, connection, forceEncrypt, stmtColumnEncriptionSetting,
                    parameterIndex, userSQL, tvpName);
        }

        // For null values, use the specified JDBC type directly, with the exception
        // of unsupported JDBC types, which are mapped to BINARY so that they are minimally supported.
        else {
            assert JavaType.OBJECT == javaType;

            if (jdbcType.isUnsupported())
                jdbcType = JDBCType.BINARY;

            // typeInfo is set as null
            param.setValue(jdbcType, null, JavaType.OBJECT, null, null, precision, scale, connection, false, stmtColumnEncriptionSetting,
                    parameterIndex, userSQL, tvpName);
        }
    }

    public final void setShort(int index,
            short x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setShort", new Object[] {index, x});
        checkClosed();
        setValue(index, JDBCType.SMALLINT, Short.valueOf(x), JavaType.SHORT, false);
        loggerExternal.exiting(getClassNameLogging(), "setShort");
    }

    /**
     * Sets the designated parameter to the given Java <code>short</code> value. The driver converts this to an SQL <code>SMALLINT</code> value when
     * it sends it to the database.
     *
     * @param index
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setShort(int index,
            short x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setShort", new Object[] {index, x, forceEncrypt});
        checkClosed();
        setValue(index, JDBCType.SMALLINT, Short.valueOf(x), JavaType.SHORT, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setShort");
    }

    public final void setString(int index,
            String str) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setString", new Object[] {index, str});
        checkClosed();
        setValue(index, JDBCType.VARCHAR, str, JavaType.STRING, false);
        loggerExternal.exiting(getClassNameLogging(), "setString");
    }

    /**
     * Sets the designated parameter to the given Java <code>String</code> value. The driver converts this to an SQL <code>VARCHAR</code> or
     * <code>LONGVARCHAR</code> value (depending on the argument's size relative to the driver's limits on <code>VARCHAR</code> values) when it sends
     * it to the database.
     *
     * @param index
     *            the first parameter is 1, the second is 2, ...
     * @param str
     *            the parameter value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setString(int index,
            String str,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setString", new Object[] {index, str, forceEncrypt});
        checkClosed();
        setValue(index, JDBCType.VARCHAR, str, JavaType.STRING, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setString");
    }

    public final void setNString(int parameterIndex,
            String value) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setNString", new Object[] {parameterIndex, value});
        checkClosed();
        setValue(parameterIndex, JDBCType.NVARCHAR, value, JavaType.STRING, false);
        loggerExternal.exiting(getClassNameLogging(), "setNString");
    }

    /**
     * Sets the designated parameter to the given <code>String</code> object. The driver converts this to a SQL <code>NCHAR</code> or
     * <code>NVARCHAR</code> or <code>LONGNVARCHAR</code> value (depending on the argument's size relative to the driver's limits on
     * <code>NVARCHAR</code> values) when it sends it to the database.
     *
     * @param parameterIndex
     *            of the first parameter is 1, the second is 2, ...
     * @param value
     *            the parameter value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLException
     *             when an error occurs
     */
    public final void setNString(int parameterIndex,
            String value,
            boolean forceEncrypt) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setNString", new Object[] {parameterIndex, value, forceEncrypt});
        checkClosed();
        setValue(parameterIndex, JDBCType.NVARCHAR, value, JavaType.STRING, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setNString");
    }

    public final void setTime(int n,
            java.sql.Time x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setTime", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.TIME, x, JavaType.TIME, false);
        loggerExternal.exiting(getClassNameLogging(), "setTime");
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Time</code> value
     * 
     * @param n
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @param scale
     *            the scale of the column
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setTime(int n,
            java.sql.Time x,
            int scale) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setTime", new Object[] {n, x, scale});
        checkClosed();
        setValue(n, JDBCType.TIME, x, JavaType.TIME, null, scale, false);
        loggerExternal.exiting(getClassNameLogging(), "setTime");
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Time</code> value
     * 
     * @param n
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @param scale
     *            the scale of the column
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setTime(int n,
            java.sql.Time x,
            int scale,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setTime", new Object[] {n, x, scale, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.TIME, x, JavaType.TIME, null, scale, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setTime");
    }

    public final void setTimestamp(int n,
            java.sql.Timestamp x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setTimestamp", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.TIMESTAMP, x, JavaType.TIMESTAMP, false);
        loggerExternal.exiting(getClassNameLogging(), "setTimestamp");
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Timestamp</code> value
     * 
     * @param n
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @param scale
     *            the scale of the column
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setTimestamp(int n,
            java.sql.Timestamp x,
            int scale) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setTimestamp", new Object[] {n, x, scale});
        checkClosed();
        setValue(n, JDBCType.TIMESTAMP, x, JavaType.TIMESTAMP, null, scale, false);
        loggerExternal.exiting(getClassNameLogging(), "setTimestamp");
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Timestamp</code> value
     * 
     * @param n
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @param scale
     *            the scale of the column
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setTimestamp(int n,
            java.sql.Timestamp x,
            int scale,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setTimestamp", new Object[] {n, x, scale, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.TIMESTAMP, x, JavaType.TIMESTAMP, null, scale, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setTimestamp");
    }

    /**
     * Sets the designated parameter to the given <code>microsoft.sql.DatetimeOffset</code> value
     * 
     * @param n
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @throws SQLException
     *             if an error occurs.
     */
    public final void setDateTimeOffset(int n,
            microsoft.sql.DateTimeOffset x) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setDateTimeOffset", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.DATETIMEOFFSET, x, JavaType.DATETIMEOFFSET, false);
        loggerExternal.exiting(getClassNameLogging(), "setDateTimeOffset");
    }

    /**
     * Sets the designated parameter to the given <code>microsoft.sql.DatetimeOffset</code> value
     * 
     * @param n
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @param scale
     *            the scale of the column
     * @throws SQLException
     *             when an error occurs
     */
    public final void setDateTimeOffset(int n,
            microsoft.sql.DateTimeOffset x,
            int scale) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setDateTimeOffset", new Object[] {n, x, scale});
        checkClosed();
        setValue(n, JDBCType.DATETIMEOFFSET, x, JavaType.DATETIMEOFFSET, null, scale, false);
        loggerExternal.exiting(getClassNameLogging(), "setDateTimeOffset");
    }

    /**
     * Sets the designated parameter to the given <code>microsoft.sql.DatetimeOffset</code> value
     * 
     * @param n
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @param scale
     *            the scale of the column
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLException
     *             when an error occurs
     */
    public final void setDateTimeOffset(int n,
            microsoft.sql.DateTimeOffset x,
            int scale,
            boolean forceEncrypt) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setDateTimeOffset", new Object[] {n, x, scale, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.DATETIMEOFFSET, x, JavaType.DATETIMEOFFSET, null, scale, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setDateTimeOffset");
    }

    public final void setDate(int n,
            java.sql.Date x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setDate", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.DATE, x, JavaType.DATE, false);
        loggerExternal.exiting(getClassNameLogging(), "setDate");
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Timestamp</code> value
     * 
     * @param n
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setDateTime(int n,
            java.sql.Timestamp x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setDateTime", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.DATETIME, x, JavaType.TIMESTAMP, false);
        loggerExternal.exiting(getClassNameLogging(), "setDateTime");
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Timestamp</code> value
     * 
     * @param n
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setDateTime(int n,
            java.sql.Timestamp x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setDateTime", new Object[] {n, x, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.DATETIME, x, JavaType.TIMESTAMP, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setDateTime");
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Timestamp</code> value
     * 
     * @param n
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setSmallDateTime(int n,
            java.sql.Timestamp x) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setSmallDateTime", new Object[] {n, x});
        checkClosed();
        setValue(n, JDBCType.SMALLDATETIME, x, JavaType.TIMESTAMP, false);
        loggerExternal.exiting(getClassNameLogging(), "setSmallDateTime");
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Timestamp</code> value
     * 
     * @param n
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setSmallDateTime(int n,
            java.sql.Timestamp x,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setSmallDateTime", new Object[] {n, x, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.SMALLDATETIME, x, JavaType.TIMESTAMP, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setSmallDateTime");
    }

    /**
     * Populates a table valued parameter with a data table
     * 
     * @param n
     *            the first parameter is 1, the second is 2, ...
     * @param tvpName
     *            the name of the table valued parameter
     * @param tvpDataTable
     *            the source datatable object
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setStructured(int n,
            String tvpName,
            SQLServerDataTable tvpDataTable) throws SQLServerException {
        tvpName = getTVPNameIfNull(n, tvpName);
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setStructured", new Object[] {n, tvpName, tvpDataTable});
        checkClosed();
        setValue(n, JDBCType.TVP, tvpDataTable, JavaType.TVP, tvpName);
        loggerExternal.exiting(getClassNameLogging(), "setStructured");
    }

    /**
     * Populates a table valued parameter with a data table
     * 
     * @param n
     *            the first parameter is 1, the second is 2, ...
     * @param tvpName
     *            the name of the table valued parameter
     * @param tvpResultSet
     *            the source resultset object
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setStructured(int n,
            String tvpName,
            ResultSet tvpResultSet) throws SQLServerException {
        tvpName = getTVPNameIfNull(n, tvpName);
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setStructured", new Object[] {n, tvpName, tvpResultSet});
        checkClosed();
        setValue(n, JDBCType.TVP, tvpResultSet, JavaType.TVP, tvpName);
        loggerExternal.exiting(getClassNameLogging(), "setStructured");
    }

    /**
     * Populates a table valued parameter with a data table
     * 
     * @param n
     *            the first parameter is 1, the second is 2, ...
     * @param tvpName
     *            the name of the table valued parameter
     * @param tvpBulkRecord
     *            an ISQLServerDataRecord object
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setStructured(int n,
            String tvpName,
            ISQLServerDataRecord tvpBulkRecord) throws SQLServerException {
        tvpName = getTVPNameIfNull(n, tvpName);
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setStructured", new Object[] {n, tvpName, tvpBulkRecord});
        checkClosed();
        setValue(n, JDBCType.TVP, tvpBulkRecord, JavaType.TVP, tvpName);
        loggerExternal.exiting(getClassNameLogging(), "setStructured");
    }

    String getTVPNameIfNull(int n,
            String tvpName) throws SQLServerException {
        if ((null == tvpName) || (0 == tvpName.length())) {
            // Check if the CallableStatement/PreparedStatement is a stored procedure call
            if(null != this.procedureName) {
                SQLServerParameterMetaData pmd = (SQLServerParameterMetaData) this.getParameterMetaData();
                pmd.isTVP = true;
                try {
                    String tvpNameWithoutSchema = pmd.getParameterTypeName(n);
                    String tvpSchema = pmd.getTVPSchemaFromStoredProcedure(n);

                    if (null != tvpSchema) {
                        tvpName = "[" + tvpSchema + "].[" + tvpNameWithoutSchema + "]";
                    }
                    else {
                        tvpName = tvpNameWithoutSchema;
                    }
                }
                catch (SQLException e) {
                    throw new SQLServerException(SQLServerException.getErrString("R_metaDataErrorForParameter"), null, 0, e);
                }
            }
        }
        return tvpName;
    }

    @Deprecated
    public final void setUnicodeStream(int n,
            java.io.InputStream x,
            int length) throws SQLException {
        NotImplemented();
    }

    public final void addBatch() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "addBatch");
        checkClosed();

        // Create the list of batch parameter values first time through
        if (batchParamValues == null)
            batchParamValues = new ArrayList<Parameter[]>();

        final int numParams = inOutParam.length;
        Parameter paramValues[] = new Parameter[numParams];
        for (int i = 0; i < numParams; i++)
            paramValues[i] = inOutParam[i].cloneForBatch();
        batchParamValues.add(paramValues);
        loggerExternal.exiting(getClassNameLogging(), "addBatch");
    }

    /* L0 */ public final void clearBatch() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "clearBatch");
        checkClosed();
        batchParamValues = null;
        loggerExternal.exiting(getClassNameLogging(), "clearBatch");
    }

    public int[] executeBatch() throws SQLServerException, BatchUpdateException {
        loggerExternal.entering(getClassNameLogging(), "executeBatch");
        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
        discardLastExecutionResults();

        int updateCounts[];

        if (batchParamValues == null)
            updateCounts = new int[0];
        else
            try {
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
                for (int batch = 0; batch < batchParamValues.size(); ++batch) {
                    Parameter paramValues[] = batchParamValues.get(batch);
                    for (int param = 0; param < paramValues.length; ++param) {
                        if (paramValues[param].isOutput()) {
                            throw new BatchUpdateException(SQLServerException.getErrString("R_outParamsNotPermittedinBatch"), null, 0, null);
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
                    throw new BatchUpdateException(batchCommand.batchException.getMessage(), batchCommand.batchException.getSQLState(),
                            batchCommand.batchException.getErrorCode(), updateCounts);

                }
            }
            finally {
                batchParamValues = null;
            }

        loggerExternal.exiting(getClassNameLogging(), "executeBatch", updateCounts);
        return updateCounts;
    }

    public long[] executeLargeBatch() throws SQLServerException, BatchUpdateException {
        DriverJDBCVersion.checkSupportsJDBC42();

        loggerExternal.entering(getClassNameLogging(), "executeLargeBatch");
        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
        discardLastExecutionResults();

        long updateCounts[];

        if (batchParamValues == null)
            updateCounts = new long[0];
        else
            try {
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
                for (int batch = 0; batch < batchParamValues.size(); ++batch) {
                    Parameter paramValues[] = batchParamValues.get(batch);
                    for (int param = 0; param < paramValues.length; ++param) {
                        if (paramValues[param].isOutput()) {
                            throw new BatchUpdateException(SQLServerException.getErrString("R_outParamsNotPermittedinBatch"), null, 0, null);
                        }
                    }
                }

                PrepStmtBatchExecCmd batchCommand = new PrepStmtBatchExecCmd(this);

                executeStatement(batchCommand);

                updateCounts = new long[batchCommand.updateCounts.length];

                for (int i = 0; i < batchCommand.updateCounts.length; ++i)
                    updateCounts[i] = batchCommand.updateCounts[i];

                // Transform the SQLException into a BatchUpdateException with the update counts.
                if (null != batchCommand.batchException) {
                    DriverJDBCVersion.throwBatchUpdateException(batchCommand.batchException, updateCounts);
                }

            }
            finally {
                batchParamValues = null;
            }
        loggerExternal.exiting(getClassNameLogging(), "executeLargeBatch", updateCounts);
        return updateCounts;
    }

    private final class PrepStmtBatchExecCmd extends TDSCommand {
        private final SQLServerPreparedStatement stmt;
        SQLServerException batchException;
        long updateCounts[];

        PrepStmtBatchExecCmd(SQLServerPreparedStatement stmt) {
            super(stmt.toString() + " executeBatch", queryTimeout);
            this.stmt = stmt;
        }

        final boolean doExecute() throws SQLServerException {
            stmt.doExecutePreparedStatementBatch(this);
            return true;
        }

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
        Vector<CryptoMetadata> cryptoMetaBatch = new Vector<CryptoMetadata>();

        if (isSelect(userSQL)) {
            SQLServerException.makeFromDriverError(connection, this, SQLServerException.getErrString("R_selectNotPermittedinBatch"), null, true);
        }

        // Make sure any previous maxRows limitation on the connection is removed.
        connection.setMaxRows(0);

        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        // Create the parameter array that we'll use for all the items in this batch.
        Parameter[] batchParam = new Parameter[inOutParam.length];

        TDSWriter tdsWriter = null;
        while (numBatchesExecuted < numBatches) {
            // Fill in the parameter values for this batch
            Parameter paramValues[] = batchParamValues.get(numBatchesPrepared);
            assert paramValues.length == batchParam.length;
            for (int i = 0; i < paramValues.length; i++)
                batchParam[i] = paramValues[i];

            boolean hasNewTypeDefinitions = buildPreparedStrings(batchParam, false);
            // Get the encryption metadata for the first batch only.
            if ((0 == numBatchesExecuted) && (Util.shouldHonorAEForParameters(stmtColumnEncriptionSetting, connection)) && (0 < batchParam.length)
                    && !isInternalEncryptionQuery) {
                getParameterEncryptionMetadata(batchParam);

                // fix an issue when inserting unicode into non-encrypted nchar column using setString() and AE is on on Connection
                buildPreparedStrings(batchParam, true);

                // Save the crypto metadata retrieved for the first batch. We will re-use these for the rest of the batches.
                for (int i = 0; i < batchParam.length; i++) {
                    cryptoMetaBatch.add(batchParam[i].cryptoMeta);
                }
            }

            // Update the crypto metadata for this batch.
            if (0 < numBatchesExecuted) {
                // cryptoMetaBatch will be empty for non-AE connections/statements.
                for (int i = 0; i < cryptoMetaBatch.size(); i++) {
                    batchParam[i].cryptoMeta = cryptoMetaBatch.get(i);
                }
            }

            if (numBatchesExecuted < numBatchesPrepared) {
                // assert null != tdsWriter;
                tdsWriter.writeByte((byte) nBatchStatementDelimiter);
            }
            else {
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
            if (doPrepExec(tdsWriter, batchParam, hasNewTypeDefinitions) || numBatchesPrepared == numBatches) {
                ensureExecuteResultsReader(batchCommand.startResponse(getIsResponseBufferingAdaptive()));

                while (numBatchesExecuted < numBatchesPrepared) {
                    // NOTE:
                    // When making changes to anything below, consider whether similar changes need
                    // to be made to Statement batch execution.

                    startResults();

                    try {
                        // Get the first result from the batch. If there is no result for this batch
                        // then bail, leaving EXECUTE_FAILED in the current and remaining slots of
                        // the update count array.
                        if (!getNextResult())
                            return;

                        // If the result is a ResultSet (rather than an update count) then throw an
                        // exception for this result. The exception gets caught immediately below and
                        // translated into (or added to) a BatchUpdateException.
                        if (null != resultSet) {
                            SQLServerException.makeFromDriverError(connection, this, SQLServerException.getErrString("R_resultsetGeneratedForUpdate"),
                                    null, false);
                        }
                    }
                    catch (SQLServerException e) {
                        // If the failure was severe enough to close the connection or roll back a
                        // manual transaction, then propagate the error up as a SQLServerException
                        // now, rather than continue with the batch.
                        if (connection.isSessionUnAvailable() || connection.rolledBackTransaction())
                            throw e;

                        // Otherwise, the connection is OK and the transaction is still intact,
                        // so just record the failure for the particular batch item.
                        updateCount = Statement.EXECUTE_FAILED;
                        if (null == batchCommand.batchException)
                            batchCommand.batchException = e;
                    }

                    // In batch execution, we have a special update count
                    // to indicate that no information was returned
                    batchCommand.updateCounts[numBatchesExecuted++] = (-1 == updateCount) ? Statement.SUCCESS_NO_INFO : updateCount;

                    processBatch();
                }

                // Only way to proceed with preparing the next set of batches is if
                // we successfully executed the previously prepared set.
                assert numBatchesExecuted == numBatchesPrepared;
            }
        }
    }

    public final void setCharacterStream(int parameterIndex,
            Reader reader) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setCharacterStream", new Object[] {parameterIndex, reader});
        checkClosed();
        setStream(parameterIndex, StreamType.CHARACTER, reader, JavaType.READER, DataTypes.UNKNOWN_STREAM_LENGTH);
        loggerExternal.exiting(getClassNameLogging(), "setCharacterStream");
    }

    public final void setCharacterStream(int n,
            java.io.Reader reader,
            int length) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setCharacterStream", new Object[] {n, reader, length});
        checkClosed();
        setStream(n, StreamType.CHARACTER, reader, JavaType.READER, length);
        loggerExternal.exiting(getClassNameLogging(), "setCharacterStream");
    }

    public final void setCharacterStream(int parameterIndex,
            Reader reader,
            long length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setCharacterStream", new Object[] {parameterIndex, reader, length});
        checkClosed();
        setStream(parameterIndex, StreamType.CHARACTER, reader, JavaType.READER, length);
        loggerExternal.exiting(getClassNameLogging(), "setCharacterStream");
    }

    public final void setNCharacterStream(int parameterIndex,
            Reader value) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setNCharacterStream", new Object[] {parameterIndex, value});
        checkClosed();
        setStream(parameterIndex, StreamType.NCHARACTER, value, JavaType.READER, DataTypes.UNKNOWN_STREAM_LENGTH);
        loggerExternal.exiting(getClassNameLogging(), "setNCharacterStream");
    }

    public final void setNCharacterStream(int parameterIndex,
            Reader value,
            long length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setNCharacterStream", new Object[] {parameterIndex, value, length});
        checkClosed();
        setStream(parameterIndex, StreamType.NCHARACTER, value, JavaType.READER, length);
        loggerExternal.exiting(getClassNameLogging(), "setNCharacterStream");
    }

    /* L0 */ public final void setRef(int i,
            java.sql.Ref x) throws SQLServerException {
        NotImplemented();
    }

    public final void setBlob(int i,
            java.sql.Blob x) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBlob", new Object[] {i, x});
        checkClosed();
        setValue(i, JDBCType.BLOB, x, JavaType.BLOB, false);
        loggerExternal.exiting(getClassNameLogging(), "setBlob");
    }

    public final void setBlob(int parameterIndex,
            InputStream inputStream) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBlob", new Object[] {parameterIndex, inputStream});
        checkClosed();
        setStream(parameterIndex, StreamType.BINARY, inputStream, JavaType.INPUTSTREAM, DataTypes.UNKNOWN_STREAM_LENGTH);
        loggerExternal.exiting(getClassNameLogging(), "setBlob");
    }

    public final void setBlob(int parameterIndex,
            InputStream inputStream,
            long length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBlob", new Object[] {parameterIndex, inputStream, length});
        checkClosed();
        setStream(parameterIndex, StreamType.BINARY, inputStream, JavaType.INPUTSTREAM, length);
        loggerExternal.exiting(getClassNameLogging(), "setBlob");
    }

    public final void setClob(int parameterIndex,
            java.sql.Clob clobValue) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setClob", new Object[] {parameterIndex, clobValue});
        checkClosed();
        setValue(parameterIndex, JDBCType.CLOB, clobValue, JavaType.CLOB, false);
        loggerExternal.exiting(getClassNameLogging(), "setClob");
    }

    public final void setClob(int parameterIndex,
            Reader reader) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setClob", new Object[] {parameterIndex, reader});
        checkClosed();
        setStream(parameterIndex, StreamType.CHARACTER, reader, JavaType.READER, DataTypes.UNKNOWN_STREAM_LENGTH);
        loggerExternal.exiting(getClassNameLogging(), "setClob");
    }

    public final void setClob(int parameterIndex,
            Reader reader,
            long length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setClob", new Object[] {parameterIndex, reader, length});
        checkClosed();
        setStream(parameterIndex, StreamType.CHARACTER, reader, JavaType.READER, length);
        loggerExternal.exiting(getClassNameLogging(), "setClob");
    }

    public final void setNClob(int parameterIndex,
            NClob value) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setNClob", new Object[] {parameterIndex, value});
        checkClosed();
        setValue(parameterIndex, JDBCType.NCLOB, value, JavaType.NCLOB, false);
        loggerExternal.exiting(getClassNameLogging(), "setNClob");
    }

    public final void setNClob(int parameterIndex,
            Reader reader) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setNClob", new Object[] {parameterIndex, reader});
        checkClosed();
        setStream(parameterIndex, StreamType.NCHARACTER, reader, JavaType.READER, DataTypes.UNKNOWN_STREAM_LENGTH);
        loggerExternal.exiting(getClassNameLogging(), "setNClob");
    }

    public final void setNClob(int parameterIndex,
            Reader reader,
            long length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setNClob", new Object[] {parameterIndex, reader, length});
        checkClosed();
        setStream(parameterIndex, StreamType.NCHARACTER, reader, JavaType.READER, length);
        loggerExternal.exiting(getClassNameLogging(), "setNClob");
    }

    /* L0 */ public final void setArray(int i,
            java.sql.Array x) throws SQLServerException {
        NotImplemented();
    }

    public final void setDate(int n,
            java.sql.Date x,
            java.util.Calendar cal) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setDate", new Object[] {n, x, cal});
        checkClosed();
        setValue(n, JDBCType.DATE, x, JavaType.DATE, cal, false);
        loggerExternal.exiting(getClassNameLogging(), "setDate");
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Date</code> value, using the given <code>Calendar</code> object. The driver uses the
     * <code>Calendar</code> object to construct an SQL <code>DATE</code> value, which the driver then sends to the database. With a
     * <code>Calendar</code> object, the driver can calculate the date taking into account a custom timezone. If no <code>Calendar</code> object is
     * specified, the driver uses the default timezone, which is that of the virtual machine running the application.
     *
     * @param n
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @param cal
     *            the <code>Calendar</code> object the driver will use to construct the date
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setDate(int n,
            java.sql.Date x,
            java.util.Calendar cal,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setDate", new Object[] {n, x, cal, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.DATE, x, JavaType.DATE, cal, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setDate");
    }

    public final void setTime(int n,
            java.sql.Time x,
            java.util.Calendar cal) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setTime", new Object[] {n, x, cal});
        checkClosed();
        setValue(n, JDBCType.TIME, x, JavaType.TIME, cal, false);
        loggerExternal.exiting(getClassNameLogging(), "setTime");
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Time</code> value. The driver converts this to an SQL <code>TIME</code> value when it
     * sends it to the database.
     *
     * @param n
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @param cal
     *            the <code>Calendar</code> object the driver will use to construct the date
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setTime(int n,
            java.sql.Time x,
            java.util.Calendar cal,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setTime", new Object[] {n, x, cal, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.TIME, x, JavaType.TIME, cal, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setTime");
    }

    public final void setTimestamp(int n,
            java.sql.Timestamp x,
            java.util.Calendar cal) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setTimestamp", new Object[] {n, x, cal});
        checkClosed();
        setValue(n, JDBCType.TIMESTAMP, x, JavaType.TIMESTAMP, cal, false);
        loggerExternal.exiting(getClassNameLogging(), "setTimestamp");
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Timestamp</code> value. The driver converts this to an SQL <code>TIMESTAMP</code>
     * value when it sends it to the database.
     *
     * @param n
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @param cal
     *            the <code>Calendar</code> object the driver will use to construct the date
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             when an error occurs
     */
    public final void setTimestamp(int n,
            java.sql.Timestamp x,
            java.util.Calendar cal,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setTimestamp", new Object[] {n, x, cal, forceEncrypt});
        checkClosed();
        setValue(n, JDBCType.TIMESTAMP, x, JavaType.TIMESTAMP, cal, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setTimestamp");
    }

    public final void setNull(int paramIndex,
            int sqlType,
            String typeName) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setNull", new Object[] {paramIndex, sqlType, typeName});
        checkClosed();
        if (microsoft.sql.Types.STRUCTURED == sqlType) {
            setObject(setterGetParam(paramIndex), null, JavaType.TVP, JDBCType.of(sqlType), null, null, false, paramIndex, typeName);
        }
        else {
            setObject(setterGetParam(paramIndex), null, JavaType.OBJECT, JDBCType.of(sqlType), null, null, false, paramIndex, typeName);
        }
        loggerExternal.exiting(getClassNameLogging(), "setNull");
    }

    /* JDBC 3.0 */

    /* L3 */ public final ParameterMetaData getParameterMetaData() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getParameterMetaData");
        checkClosed();
        SQLServerParameterMetaData pmd = new SQLServerParameterMetaData(this, userSQL);
        loggerExternal.exiting(getClassNameLogging(), "getParameterMetaData", pmd);
        return pmd;
    }

    /* L3 */ public final void setURL(int parameterIndex,
            java.net.URL x) throws SQLServerException {
        NotImplemented();
    }

    public final void setRowId(int parameterIndex,
            RowId x) throws SQLException {
        // Not implemented
        throw new SQLFeatureNotSupportedException(SQLServerException.getErrString("R_notSupported"));
    }

    public final void setSQLXML(int parameterIndex,
            SQLXML xmlObject) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setSQLXML", new Object[] {parameterIndex, xmlObject});
        checkClosed();
        setSQLXMLInternal(parameterIndex, xmlObject);
        loggerExternal.exiting(getClassNameLogging(), "setSQLXML");
    }

    /* make sure we throw here */
    /* L0 */ public final int executeUpdate(String sql) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "executeUpdate", sql);
        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_cannotTakeArgumentsPreparedOrCallable"));
        Object[] msgArgs = {"executeUpdate()"};
        throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
    }

    /* L0 */ public final boolean execute(String sql) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "execute", sql);
        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_cannotTakeArgumentsPreparedOrCallable"));
        Object[] msgArgs = {"execute()"};
        throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
    }

    /* L0 */ public final java.sql.ResultSet executeQuery(String sql) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "executeQuery", sql);
        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_cannotTakeArgumentsPreparedOrCallable"));
        Object[] msgArgs = {"executeQuery()"};
        throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
    }

    /* L0 */ public void addBatch(String sql) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "addBatch", sql);
        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_cannotTakeArgumentsPreparedOrCallable"));
        Object[] msgArgs = {"addBatch()"};
        throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
    }
}
