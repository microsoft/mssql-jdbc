/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Provides implementation of JDBC callable statements. CallableStatement allows the caller to specify the procedure
 * name to call along with input parameter value and output parameter types. Callable statement also allows the return
 * of a return status with the ? = call( ?, ..) JDBC syntax
 * <p>
 * The API javadoc for JDBC API methods that this class implements are not repeated here. Please see Sun's JDBC API
 * interfaces javadoc for those details.
 */
public class SQLServerCallableStatement extends SQLServerPreparedStatement implements ISQLServerCallableStatement {

    /**
     * Always update serialVersionUID when prompted.
     */
    private static final long serialVersionUID = 5044984771674532350L;

    /** the call param names */
    private HashMap<String, Integer> parameterNames;

    /** insensitive param names */
    private TreeMap<String, Integer> insensitiveParameterNames;

    /** Number of registered OUT parameters */
    int nOutParams = 0;

    /** number of out params assigned already */
    int nOutParamsAssigned = 0;

    /** The index of the out params indexed - internal index */
    private int outParamIndex = -1;

    /** The last out param accessed. */
    private Parameter lastParamAccessed;

    /** Currently active Stream Note only one stream can be active at a time */
    private Closeable activeStream;

    // Internal function used in tracing
    String getClassNameInternal() {
        return "SQLServerCallableStatement";
    }

    /** map */
    Map<String, Integer> map = new ConcurrentHashMap<>();

    /** atomic integer */
    AtomicInteger ai = new AtomicInteger(0);

    /**
     * Create a new callable statement.
     * 
     * @param connection
     *        the connection
     * @param sql
     *        the users call syntax
     * @param nRSType
     *        the result set type
     * @param nRSConcur
     *        the result set concurrency
     * @param stmtColEncSetting
     *        the statement column encryption setting
     * @throws SQLServerException
     */
    SQLServerCallableStatement(SQLServerConnection connection, String sql, int nRSType, int nRSConcur,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLServerException {
        super(connection, sql, nRSType, nRSConcur, stmtColEncSetting);
    }

    @Override
    public void registerOutParameter(int index, int sqlType) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "registerOutParameter", new Object[] {index, sqlType});
        checkClosed();
        if (index < 1 || index > inOutParam.length) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_indexOutOfRange"));
            Object[] msgArgs = {index};
            SQLServerException.makeFromDriverError(connection, this, form.format(msgArgs), "7009", false);
        }

        // REF_CURSOR 2012 is a special type - should throw SQLFeatureNotSupportedException as per spec
        // but this will require changing API to throw SQLException.
        // This should be reviewed in 4199060
        if (2012 == sqlType) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_featureNotSupported"));
            Object[] msgArgs = {"REF_CURSOR"};
            SQLServerException.makeFromDriverError(connection, this, form.format(msgArgs), null, false);
        }

        JDBCType jdbcType = JDBCType.of(sqlType);

        // Registering an OUT parameter is an indication that the app is done
        // with the results from any previous execution
        discardLastExecutionResults();

        // OUT parameters registered as unsupported JDBC types map to BINARY
        // so that they are minimally supported.
        if (jdbcType.isUnsupported())
            jdbcType = JDBCType.BINARY;

        Parameter param = inOutParam[index - 1];
        assert null != param;

        // If the parameter was not previously registered for OUTPUT then
        // it is added to the set of OUTPUT parameters now.
        if (!param.isOutput())
            ++nOutParams;

        // (Re)register the parameter for OUTPUT with the specified SQL type
        // overriding any previous registration with another SQL type.
        param.registerForOutput(jdbcType, connection);
        switch (sqlType) {
            case microsoft.sql.Types.DATETIME:
                param.setOutScale(3);
                break;
            case java.sql.Types.TIME:
            case java.sql.Types.TIMESTAMP:
            case microsoft.sql.Types.DATETIMEOFFSET:
                param.setOutScale(7);
                break;
            default:
                break;
        }

        loggerExternal.exiting(getClassNameLogging(), "registerOutParameter");
    }

    /**
     * Locate any output parameter values returned from the procedure call
     */
    private Parameter getOutParameter(int i) throws SQLServerException {
        // Process any remaining result sets and update counts. This positions
        // us for retrieving the OUT parameters. Note that after retrieving
        // an OUT parameter, an SQLException is thrown if the application tries
        // to go back and process the results.
        processResults();

        // if this item has been indexed already leave!
        if (inOutParam[i - 1] == lastParamAccessed || inOutParam[i - 1].isValueGotten())
            return inOutParam[i - 1];

        // Skip OUT parameters (buffering them as we go) until we
        // reach the one we're looking for.
        while (outParamIndex != i - 1)
            skipOutParameters(1, false);

        return inOutParam[i - 1];
    }

    void startResults() {
        super.startResults();
        outParamIndex = -1;
        nOutParamsAssigned = 0;
        lastParamAccessed = null;
        assert null == activeStream;
    }

    void processBatch() throws SQLServerException {
        processResults();

        // If there were any OUT parameters, then process them
        // and the rest of the batch that follows them. If there were
        // no OUT parameters, than the entire batch was already processed
        // in the processResults call above.
        assert nOutParams >= 0;
        if (nOutParams > 0) {
            processOutParameters();
            processBatchRemainder();
        }
    }

    final void processOutParameters() throws SQLServerException {
        assert nOutParams > 0;
        assert null != inOutParam;

        // make sure if we have active streams they are closed out.
        closeActiveStream();

        // First, discard all of the previously indexed OUT parameters up to,
        // but not including, the last-indexed parameter.
        if (outParamIndex >= 0) {
            // Note: It doesn't matter that they're not cleared in the order they
            // appear in the response stream. What counts is that at the end
            // none of them has any TDSReaderMarks holding onto any portion of
            // the response stream.
            for (int index = 0; index < inOutParam.length; ++index) {
                if (index != outParamIndex && inOutParam[index].isValueGotten()) {
                    assert inOutParam[index].isOutput();
                    inOutParam[index].resetOutputValue();
                }
            }
        }

        // Next, if there are any unindexed parameters left then discard them too.
        assert nOutParamsAssigned <= nOutParams;
        if (nOutParamsAssigned < nOutParams)
            skipOutParameters(nOutParams - nOutParamsAssigned, true);

        // Finally, skip the last-indexed parameter. If there were no unindexed parameters
        // in the previous step, then this is the last-indexed parameter left from the first
        // step. If we skipped unindexed parameters in the previous step, then this is the
        // last-indexed parameter left at the end of that step.
        if (outParamIndex >= 0) {
            inOutParam[outParamIndex].skipValue(resultsReader(), true);
            inOutParam[outParamIndex].resetOutputValue();
            outParamIndex = -1;
        }
    }

    /**
     * Processes the remainder of the batch up to the final or batch-terminating DONE token that marks the end of a
     * sp_[cursor][prep]exec stored procedure call.
     */
    private void processBatchRemainder() throws SQLServerException {
        final class ExecDoneHandler extends TDSTokenHandler {
            ExecDoneHandler() {
                super("ExecDoneHandler");
            }

            boolean onDone(TDSReader tdsReader) throws SQLServerException {
                // Consume the done token and decide what to do with it...
                StreamDone doneToken = new StreamDone();
                doneToken.setFromTDS(tdsReader);
                connection.getSessionRecovery().decrementUnprocessedResponseCount();

                // If this is a non-final batch-terminating DONE token,
                // then stop parsing the response now and set up for
                // the next batch.
                if (doneToken.wasRPCInBatch()) {
                    startResults();
                    return false;
                }

                // Continue processing so that we pick up ENVCHANGE tokens.
                // Parsing stops automatically on response EOF.
                return true;
            }
        }

        ExecDoneHandler execDoneHandler = new ExecDoneHandler();
        TDSParser.parse(resultsReader(), execDoneHandler);
    }

    private void skipOutParameters(int numParamsToSkip, boolean discardValues) throws SQLServerException {
        /** TDS token handler for locating OUT parameters (RETURN_VALUE tokens) in the response token stream */
        final class OutParamHandler extends TDSTokenHandler {
            final StreamRetValue srv = new StreamRetValue();

            private boolean foundParam;

            final boolean foundParam() {
                return foundParam;
            }

            OutParamHandler() {
                super("OutParamHandler");
            }

            final void reset() {
                foundParam = false;
            }

            boolean onRetValue(TDSReader tdsReader) throws SQLServerException {
                srv.setFromTDS(tdsReader);
                foundParam = true;
                return false;
            }
        }

        OutParamHandler outParamHandler = new OutParamHandler();

        // Index the application OUT parameters
        assert numParamsToSkip <= nOutParams - nOutParamsAssigned;
        for (int paramsSkipped = 0; paramsSkipped < numParamsToSkip; ++paramsSkipped) {
            // Discard the last-indexed parameter by skipping over it and
            // discarding the value if it is no longer needed.
            if (-1 != outParamIndex) {
                inOutParam[outParamIndex].skipValue(resultsReader(), discardValues);
                if (discardValues)
                    inOutParam[outParamIndex].resetOutputValue();
            }

            // Look for the next parameter value in the response.
            outParamHandler.reset();
            TDSParser.parse(resultsReader(), outParamHandler);

            // If we don't find it, then most likely the server encountered some error that
            // was bad enough to halt statement execution before returning OUT params, but
            // not necessarily bad enough to close the connection.
            if (!outParamHandler.foundParam()) {
                // If we were just going to discard the OUT parameters we found anyway,
                // then it's no problem that we didn't find any of them. For exmaple,
                // when we are closing or reexecuting this CallableStatement (that is,
                // calling in through processResponse), we don't care that execution
                // failed to return the OUT parameters.
                if (discardValues)
                    break;

                // If we were asked to retain the OUT parameters as we skip past them,
                // then report an error if we did not find any.
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_valueNotSetForParameter"));
                Object[] msgArgs = {outParamIndex + 1};
                SQLServerException.makeFromDriverError(connection, this, form.format(msgArgs), null, false);
            }

            // In Yukon and later, large Object output parameters are reordered to appear at
            // the end of the stream. First group of small parameters is sent, followed by
            // group of large output parameters. There is no reordering within the groups.

            // Note that parameter ordinals are 0-indexed and that the return status is not
            // considered to be an output parameter.
            outParamIndex = outParamHandler.srv.getOrdinalOrLength();

            // Statements need to have their out param indices adjusted by the number
            // of sp_[cursor][prep]exec params.
            outParamIndex -= outParamIndexAdjustment;
            if ((outParamIndex < 0 || outParamIndex >= inOutParam.length) || (!inOutParam[outParamIndex].isOutput())) {
                getStatementLogger().info(toString() + " Unexpected outParamIndex: " + outParamIndex + "; adjustment: "
                        + outParamIndexAdjustment);
                connection.throwInvalidTDS();
            }

            ++nOutParamsAssigned;
        }
    }

    @Override
    public void registerOutParameter(int index, int sqlType, String typeName) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "registerOutParameter",
                    new Object[] {index, sqlType, typeName});

        checkClosed();

        registerOutParameter(index, sqlType);

        loggerExternal.exiting(getClassNameLogging(), "registerOutParameter");
    }

    @Override
    public void registerOutParameter(int index, int sqlType, int scale) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "registerOutParameter",
                    new Object[] {index, sqlType, scale});

        checkClosed();

        registerOutParameter(index, sqlType);
        inOutParam[index - 1].setOutScale(scale);

        loggerExternal.exiting(getClassNameLogging(), "registerOutParameter");
    }

    @Override
    public void registerOutParameter(int index, int sqlType, int precision, int scale) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "registerOutParameter",
                    new Object[] {index, sqlType, scale, precision});

        checkClosed();

        registerOutParameter(index, sqlType);
        inOutParam[index - 1].setValueLength(precision);
        inOutParam[index - 1].setOutScale(scale);

        loggerExternal.exiting(getClassNameLogging(), "registerOutParameter");
    }

    /* ---------------------- JDBC API: Get Output Params -------------------------- */

    private Parameter getterGetParam(int index) throws SQLServerException {
        checkClosed();

        // Check for valid index
        if (index < 1 || index > inOutParam.length) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidOutputParameter"));
            Object[] msgArgs = {index};
            SQLServerException.makeFromDriverError(connection, this, form.format(msgArgs), "07009", false);
        }

        // Check index refers to a registered OUT parameter
        if (!inOutParam[index - 1].isOutput()) {
            MessageFormat form = new MessageFormat(
                    SQLServerException.getErrString("R_outputParameterNotRegisteredForOutput"));
            Object[] msgArgs = {index};
            SQLServerException.makeFromDriverError(connection, this, form.format(msgArgs), "07009", true);
        }

        // If we haven't executed the statement yet then throw a nice friendly exception.
        if (!wasExecuted())
            SQLServerException.makeFromDriverError(connection, this,
                    SQLServerException.getErrString("R_statementMustBeExecuted"), "07009", false);

        resultsReader().getCommand().checkForInterrupt();

        closeActiveStream();
        if (getStatementLogger().isLoggable(java.util.logging.Level.FINER))
            getStatementLogger().finer(toString() + " Getting Param:" + index);

        // Dynamically load OUT params from TDS response buffer
        lastParamAccessed = getOutParameter(index);
        return lastParamAccessed;
    }

    private Object getValue(int parameterIndex, JDBCType jdbcType) throws SQLServerException {
        return getterGetParam(parameterIndex).getValue(jdbcType, null, null, resultsReader(), this);
    }

    private Object getValue(int parameterIndex, JDBCType jdbcType, Calendar cal) throws SQLServerException {
        return getterGetParam(parameterIndex).getValue(jdbcType, null, cal, resultsReader(), this);
    }

    private Object getStream(int parameterIndex, StreamType streamType) throws SQLServerException {
        Object value = getterGetParam(parameterIndex).getValue(streamType.getJDBCType(),
                new InputStreamGetterArgs(streamType, getIsResponseBufferingAdaptive(),
                        getIsResponseBufferingAdaptive(), toString()),
                null, // calendar
                resultsReader(), this);

        activeStream = (Closeable) value;
        return value;
    }

    private Object getSQLXMLInternal(int parameterIndex) throws SQLServerException {
        SQLServerSQLXML value = (SQLServerSQLXML) getterGetParam(parameterIndex).getValue(JDBCType.SQLXML,
                new InputStreamGetterArgs(StreamType.SQLXML, getIsResponseBufferingAdaptive(),
                        getIsResponseBufferingAdaptive(), toString()),
                null, // calendar
                resultsReader(), this);

        if (null != value)
            activeStream = value.getStream();
        return value;
    }

    @Override
    public int getInt(int index) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getInt", index);
        checkClosed();
        Integer value = (Integer) getValue(index, JDBCType.INTEGER);
        loggerExternal.exiting(getClassNameLogging(), "getInt", value);
        return null != value ? value : 0;
    }

    @Override
    public int getInt(String parameterName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getInt", parameterName);
        checkClosed();
        Integer value = (Integer) getValue(findColumn(parameterName), JDBCType.INTEGER);
        loggerExternal.exiting(getClassNameLogging(), "getInt", value);
        return null != value ? value : 0;
    }

    @Override
    public String getString(int index) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getString", index);
        checkClosed();
        String value = null;
        Object objectValue = getValue(index, JDBCType.CHAR);
        if (null != objectValue) {
            value = objectValue.toString();
        }
        loggerExternal.exiting(getClassNameLogging(), "getString", value);
        return value;
    }

    @Override
    public String getString(String parameterName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getString", parameterName);
        checkClosed();
        String value = null;
        Object objectValue = getValue(findColumn(parameterName), JDBCType.CHAR);
        if (null != objectValue) {
            value = objectValue.toString();
        }
        loggerExternal.exiting(getClassNameLogging(), "getString", value);
        return value;
    }

    @Override
    public final String getNString(int parameterIndex) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getNString", parameterIndex);
        checkClosed();
        String value = (String) getValue(parameterIndex, JDBCType.NCHAR);
        loggerExternal.exiting(getClassNameLogging(), "getNString", value);
        return value;
    }

    @Override
    public final String getNString(String parameterName) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getNString", parameterName);
        checkClosed();
        String value = (String) getValue(findColumn(parameterName), JDBCType.NCHAR);
        loggerExternal.exiting(getClassNameLogging(), "getNString", value);
        return value;
    }

    @Deprecated
    @Override
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getBigDecimal", new Object[] {parameterIndex, scale});
        checkClosed();
        BigDecimal value = (BigDecimal) getValue(parameterIndex, JDBCType.DECIMAL);
        if (null != value)
            value = value.setScale(scale, BigDecimal.ROUND_DOWN);
        loggerExternal.exiting(getClassNameLogging(), "getBigDecimal", value);
        return value;
    }

    @Deprecated
    @Override
    public BigDecimal getBigDecimal(String parameterName, int scale) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getBigDecimal", new Object[] {parameterName, scale});
        checkClosed();
        BigDecimal value = (BigDecimal) getValue(findColumn(parameterName), JDBCType.DECIMAL);
        if (null != value)
            value = value.setScale(scale, BigDecimal.ROUND_DOWN);
        loggerExternal.exiting(getClassNameLogging(), "getBigDecimal", value);
        return value;
    }

    @Override
    public boolean getBoolean(int index) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getBoolean", index);
        checkClosed();
        Boolean value = (Boolean) getValue(index, JDBCType.BIT);
        loggerExternal.exiting(getClassNameLogging(), "getBoolean", value);
        return null != value ? value : false;
    }

    @Override
    public boolean getBoolean(String parameterName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getBoolean", parameterName);
        checkClosed();
        Boolean value = (Boolean) getValue(findColumn(parameterName), JDBCType.BIT);
        loggerExternal.exiting(getClassNameLogging(), "getBoolean", value);
        return null != value ? value : false;
    }

    @Override
    public byte getByte(int index) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getByte", index);
        checkClosed();
        Short shortValue = (Short) getValue(index, JDBCType.TINYINT);
        byte byteValue = (null != shortValue) ? shortValue.byteValue() : 0;
        loggerExternal.exiting(getClassNameLogging(), "getByte", byteValue);
        return byteValue;
    }

    @Override
    public byte getByte(String parameterName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getByte", parameterName);
        checkClosed();
        Short shortValue = (Short) getValue(findColumn(parameterName), JDBCType.TINYINT);
        byte byteValue = (null != shortValue) ? shortValue.byteValue() : 0;
        loggerExternal.exiting(getClassNameLogging(), "getByte", byteValue);
        return byteValue;
    }

    @Override
    public byte[] getBytes(int index) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getBytes", index);
        checkClosed();
        byte[] value = (byte[]) getValue(index, JDBCType.BINARY);
        loggerExternal.exiting(getClassNameLogging(), "getBytes", value);
        return value;
    }

    @Override
    public byte[] getBytes(String parameterName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getBytes", parameterName);
        checkClosed();
        byte[] value = (byte[]) getValue(findColumn(parameterName), JDBCType.BINARY);
        loggerExternal.exiting(getClassNameLogging(), "getBytes", value);
        return value;
    }

    @Override
    public Date getDate(int index) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getDate", index);
        checkClosed();
        java.sql.Date value = (java.sql.Date) getValue(index, JDBCType.DATE);
        loggerExternal.exiting(getClassNameLogging(), "getDate", value);
        return value;
    }

    @Override
    public Date getDate(String parameterName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getDate", parameterName);
        checkClosed();
        java.sql.Date value = (java.sql.Date) getValue(findColumn(parameterName), JDBCType.DATE);
        loggerExternal.exiting(getClassNameLogging(), "getDate", value);
        return value;
    }

    @Override
    public Date getDate(int index, Calendar cal) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getDate", new Object[] {index, cal});
        checkClosed();
        java.sql.Date value = (java.sql.Date) getValue(index, JDBCType.DATE, cal);
        loggerExternal.exiting(getClassNameLogging(), "getDate", value);
        return value;
    }

    @Override
    public Date getDate(String parameterName, Calendar cal) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getDate", new Object[] {parameterName, cal});
        checkClosed();
        java.sql.Date value = (java.sql.Date) getValue(findColumn(parameterName), JDBCType.DATE, cal);
        loggerExternal.exiting(getClassNameLogging(), "getDate", value);
        return value;
    }

    @Override
    public double getDouble(int index) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getDouble", index);
        checkClosed();
        Double value = (Double) getValue(index, JDBCType.DOUBLE);
        loggerExternal.exiting(getClassNameLogging(), "getDouble", value);
        return null != value ? value : 0;
    }

    @Override
    public double getDouble(String parameterName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getDouble", parameterName);
        checkClosed();
        Double value = (Double) getValue(findColumn(parameterName), JDBCType.DOUBLE);
        loggerExternal.exiting(getClassNameLogging(), "getDouble", value);
        return null != value ? value : 0;
    }

    @Override
    public float getFloat(int index) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getFloat", index);
        checkClosed();
        Float value = (Float) getValue(index, JDBCType.REAL);
        loggerExternal.exiting(getClassNameLogging(), "getFloat", value);
        return null != value ? value : 0;
    }

    @Override
    public float getFloat(String parameterName) throws SQLServerException {

        loggerExternal.entering(getClassNameLogging(), "getFloat", parameterName);
        checkClosed();
        Float value = (Float) getValue(findColumn(parameterName), JDBCType.REAL);
        loggerExternal.exiting(getClassNameLogging(), "getFloat", value);
        return null != value ? value : 0;
    }

    @Override
    public long getLong(int index) throws SQLServerException {

        loggerExternal.entering(getClassNameLogging(), "getLong", index);
        checkClosed();
        Long value = (Long) getValue(index, JDBCType.BIGINT);
        loggerExternal.exiting(getClassNameLogging(), "getLong", value);
        return null != value ? value : 0;
    }

    @Override
    public long getLong(String parameterName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getLong", parameterName);
        checkClosed();
        Long value = (Long) getValue(findColumn(parameterName), JDBCType.BIGINT);
        loggerExternal.exiting(getClassNameLogging(), "getLong", value);
        return null != value ? value : 0;
    }

    @Override
    public Object getObject(int index) throws SQLServerException {

        loggerExternal.entering(getClassNameLogging(), "getObject", index);
        checkClosed();
        Object value = getValue(index,
                null != getterGetParam(index).getJdbcTypeSetByUser() ? getterGetParam(index).getJdbcTypeSetByUser()
                                                                     : getterGetParam(index).getJdbcType());
        loggerExternal.exiting(getClassNameLogging(), "getObject", value);
        return value;
    }

    @Override
    public <T> T getObject(int index, Class<T> type) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getObject", index);
        checkClosed();
        Object returnValue;
        if (type == String.class) {
            returnValue = getString(index);
        } else if (type == Byte.class) {
            byte byteValue = getByte(index);
            returnValue = wasNull() ? null : byteValue;
        } else if (type == Short.class) {
            short shortValue = getShort(index);
            returnValue = wasNull() ? null : shortValue;
        } else if (type == Integer.class) {
            int intValue = getInt(index);
            returnValue = wasNull() ? null : intValue;
        } else if (type == Long.class) {
            long longValue = getLong(index);
            returnValue = wasNull() ? null : longValue;
        } else if (type == BigDecimal.class) {
            returnValue = getBigDecimal(index);
        } else if (type == Boolean.class) {
            boolean booleanValue = getBoolean(index);
            returnValue = wasNull() ? null : booleanValue;
        } else if (type == java.sql.Date.class) {
            returnValue = getDate(index);
        } else if (type == java.sql.Time.class) {
            returnValue = getTime(index);
        } else if (type == java.sql.Timestamp.class) {
            returnValue = getTimestamp(index);
        } else if (type == java.time.LocalDateTime.class || type == java.time.LocalDate.class
                || type == java.time.LocalTime.class) {
            java.time.LocalDateTime ldt = getLocalDateTime(index);
            if (null == ldt) {
                returnValue = null;
            } else {
                if (type == java.time.LocalDateTime.class) {
                    returnValue = ldt;
                } else if (type == java.time.LocalDate.class) {
                    returnValue = ldt.toLocalDate();
                } else {
                    returnValue = ldt.toLocalTime();
                }
            }
        } else if (type == java.time.OffsetDateTime.class) {
            microsoft.sql.DateTimeOffset dateTimeOffset = getDateTimeOffset(index);
            if (dateTimeOffset == null) {
                returnValue = null;
            } else {
                returnValue = dateTimeOffset.getOffsetDateTime();
            }
        } else if (type == java.time.OffsetTime.class) {
            microsoft.sql.DateTimeOffset dateTimeOffset = getDateTimeOffset(index);
            if (dateTimeOffset == null) {
                returnValue = null;
            } else {
                returnValue = dateTimeOffset.getOffsetDateTime().toOffsetTime();
            }
        } else if (type == microsoft.sql.DateTimeOffset.class) {
            returnValue = getDateTimeOffset(index);
        } else if (type == UUID.class) {
            // read binary, avoid string allocation and parsing
            byte[] guid = getBytes(index);
            returnValue = null != guid ? Util.readGUIDtoUUID(guid) : null;
        } else if (type == SQLXML.class) {
            returnValue = getSQLXML(index);
        } else if (type == Blob.class) {
            returnValue = getBlob(index);
        } else if (type == Clob.class) {
            returnValue = getClob(index);
        } else if (type == NClob.class) {
            returnValue = getNClob(index);
        } else if (type == byte[].class) {
            returnValue = getBytes(index);
        } else if (type == Float.class) {
            float floatValue = getFloat(index);
            returnValue = wasNull() ? null : floatValue;
        } else if (type == Double.class) {
            double doubleValue = getDouble(index);
            returnValue = wasNull() ? null : doubleValue;
        } else {
            // if the type is not supported the specification says the should
            // a SQLException instead of SQLFeatureNotSupportedException
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_unsupportedConversionTo"));
            Object[] msgArgs = {type};
            throw new SQLServerException(form.format(msgArgs), SQLState.DATA_EXCEPTION_NOT_SPECIFIC,
                    DriverError.NOT_SET, null);
        }
        loggerExternal.exiting(getClassNameLogging(), "getObject", index);
        return type.cast(returnValue);
    }

    @Override
    public Object getObject(String parameterName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getObject", parameterName);
        checkClosed();
        int parameterIndex = findColumn(parameterName);
        Object value = getValue(parameterIndex,
                null != getterGetParam(parameterIndex).getJdbcTypeSetByUser() ? getterGetParam(parameterIndex)
                        .getJdbcTypeSetByUser() : getterGetParam(parameterIndex).getJdbcType());
        loggerExternal.exiting(getClassNameLogging(), "getObject", value);
        return value;
    }

    @Override
    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getObject", parameterName);
        checkClosed();
        int parameterIndex = findColumn(parameterName);
        T value = getObject(parameterIndex, type);
        loggerExternal.exiting(getClassNameLogging(), "getObject", value);
        return value;
    }

    @Override
    public short getShort(int index) throws SQLServerException {

        loggerExternal.entering(getClassNameLogging(), "getShort", index);
        checkClosed();
        Short value = (Short) getValue(index, JDBCType.SMALLINT);
        loggerExternal.exiting(getClassNameLogging(), "getShort", value);
        return null != value ? value : 0;
    }

    @Override
    public short getShort(String parameterName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getShort", parameterName);
        checkClosed();
        Short value = (Short) getValue(findColumn(parameterName), JDBCType.SMALLINT);
        loggerExternal.exiting(getClassNameLogging(), "getShort", value);
        return null != value ? value : 0;
    }

    @Override
    public Time getTime(int index) throws SQLServerException {

        loggerExternal.entering(getClassNameLogging(), "getTime", index);
        checkClosed();
        java.sql.Time value = (java.sql.Time) getValue(index, JDBCType.TIME);
        loggerExternal.exiting(getClassNameLogging(), "getTime", value);
        return value;
    }

    @Override
    public Time getTime(String parameterName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getTime", parameterName);
        checkClosed();
        java.sql.Time value = (java.sql.Time) getValue(findColumn(parameterName), JDBCType.TIME);
        loggerExternal.exiting(getClassNameLogging(), "getTime", value);
        return value;
    }

    @Override
    public Time getTime(int index, Calendar cal) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getTime", new Object[] {index, cal});
        checkClosed();
        java.sql.Time value = (java.sql.Time) getValue(index, JDBCType.TIME, cal);
        loggerExternal.exiting(getClassNameLogging(), "getTime", value);
        return value;
    }

    @Override
    public Time getTime(String parameterName, Calendar cal) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getTime", new Object[] {parameterName, cal});
        checkClosed();
        java.sql.Time value = (java.sql.Time) getValue(findColumn(parameterName), JDBCType.TIME, cal);
        loggerExternal.exiting(getClassNameLogging(), "getTime", value);
        return value;
    }

    @Override
    public Timestamp getTimestamp(int index) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getTimestamp", index);
        checkClosed();
        java.sql.Timestamp value = (java.sql.Timestamp) getValue(index, JDBCType.TIMESTAMP);
        loggerExternal.exiting(getClassNameLogging(), "getTimestamp", value);
        return value;
    }

    @Override
    public Timestamp getTimestamp(String parameterName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getTimestamp", parameterName);
        checkClosed();
        java.sql.Timestamp value = (java.sql.Timestamp) getValue(findColumn(parameterName), JDBCType.TIMESTAMP);
        loggerExternal.exiting(getClassNameLogging(), "getTimestamp", value);
        return value;
    }

    @Override
    public Timestamp getTimestamp(int index, Calendar cal) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getTimestamp", new Object[] {index, cal});
        checkClosed();
        java.sql.Timestamp value = (java.sql.Timestamp) getValue(index, JDBCType.TIMESTAMP, cal);
        loggerExternal.exiting(getClassNameLogging(), "getTimestamp", value);
        return value;
    }

    @Override
    public Timestamp getTimestamp(String name, Calendar cal) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getTimestamp", new Object[] {name, cal});
        checkClosed();
        java.sql.Timestamp value = (java.sql.Timestamp) getValue(findColumn(name), JDBCType.TIMESTAMP, cal);
        loggerExternal.exiting(getClassNameLogging(), "getTimestamp", value);
        return value;
    }

    LocalDateTime getLocalDateTime(int columnIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getLocalDateTime", columnIndex);
        checkClosed();
        LocalDateTime value = (LocalDateTime) getValue(columnIndex, JDBCType.LOCALDATETIME);
        loggerExternal.exiting(getClassNameLogging(), "getLocalDateTime", value);
        return value;
    }

    @Override
    public Timestamp getDateTime(int index) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getDateTime", index);
        checkClosed();
        java.sql.Timestamp value = (java.sql.Timestamp) getValue(index, JDBCType.DATETIME);
        loggerExternal.exiting(getClassNameLogging(), "getDateTime", value);
        return value;
    }

    @Override
    public Timestamp getDateTime(String parameterName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getDateTime", parameterName);
        checkClosed();
        java.sql.Timestamp value = (java.sql.Timestamp) getValue(findColumn(parameterName), JDBCType.DATETIME);
        loggerExternal.exiting(getClassNameLogging(), "getDateTime", value);
        return value;
    }

    @Override
    public Timestamp getDateTime(int index, Calendar cal) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getDateTime", new Object[] {index, cal});
        checkClosed();
        java.sql.Timestamp value = (java.sql.Timestamp) getValue(index, JDBCType.DATETIME, cal);
        loggerExternal.exiting(getClassNameLogging(), "getDateTime", value);
        return value;
    }

    @Override
    public Timestamp getDateTime(String name, Calendar cal) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getDateTime", new Object[] {name, cal});
        checkClosed();
        java.sql.Timestamp value = (java.sql.Timestamp) getValue(findColumn(name), JDBCType.DATETIME, cal);
        loggerExternal.exiting(getClassNameLogging(), "getDateTime", value);
        return value;
    }

    @Override
    public Timestamp getSmallDateTime(int index) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getSmallDateTime", index);
        checkClosed();
        java.sql.Timestamp value = (java.sql.Timestamp) getValue(index, JDBCType.SMALLDATETIME);
        loggerExternal.exiting(getClassNameLogging(), "getSmallDateTime", value);
        return value;
    }

    @Override
    public Timestamp getSmallDateTime(String parameterName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getSmallDateTime", parameterName);
        checkClosed();
        java.sql.Timestamp value = (java.sql.Timestamp) getValue(findColumn(parameterName), JDBCType.SMALLDATETIME);
        loggerExternal.exiting(getClassNameLogging(), "getSmallDateTime", value);
        return value;
    }

    @Override
    public Timestamp getSmallDateTime(int index, Calendar cal) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getSmallDateTime", new Object[] {index, cal});
        checkClosed();
        java.sql.Timestamp value = (java.sql.Timestamp) getValue(index, JDBCType.SMALLDATETIME, cal);
        loggerExternal.exiting(getClassNameLogging(), "getSmallDateTime", value);
        return value;
    }

    @Override
    public Timestamp getSmallDateTime(String name, Calendar cal) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getSmallDateTime", new Object[] {name, cal});
        checkClosed();
        java.sql.Timestamp value = (java.sql.Timestamp) getValue(findColumn(name), JDBCType.SMALLDATETIME, cal);
        loggerExternal.exiting(getClassNameLogging(), "getSmallDateTime", value);
        return value;
    }

    @Override
    public microsoft.sql.DateTimeOffset getDateTimeOffset(int index) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getDateTimeOffset", index);
        checkClosed();

        // DateTimeOffset is not supported with SQL Server versions earlier than Katmai
        if (!connection.isKatmaiOrLater())
            throw new SQLServerException(SQLServerException.getErrString("R_notSupported"),
                    SQLState.DATA_EXCEPTION_NOT_SPECIFIC, DriverError.NOT_SET, null);

        microsoft.sql.DateTimeOffset value = (microsoft.sql.DateTimeOffset) getValue(index, JDBCType.DATETIMEOFFSET);
        loggerExternal.exiting(getClassNameLogging(), "getDateTimeOffset", value);
        return value;
    }

    @Override
    public microsoft.sql.DateTimeOffset getDateTimeOffset(String parameterName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getDateTimeOffset", parameterName);
        checkClosed();

        // DateTimeOffset is not supported with SQL Server versions earlier than Katmai
        if (!connection.isKatmaiOrLater())
            throw new SQLServerException(SQLServerException.getErrString("R_notSupported"),
                    SQLState.DATA_EXCEPTION_NOT_SPECIFIC, DriverError.NOT_SET, null);

        microsoft.sql.DateTimeOffset value = (microsoft.sql.DateTimeOffset) getValue(findColumn(parameterName),
                JDBCType.DATETIMEOFFSET);
        loggerExternal.exiting(getClassNameLogging(), "getDateTimeOffset", value);
        return value;
    }

    @Override
    public boolean wasNull() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "wasNull");
        checkClosed();
        boolean bWasNull = false;
        if (null != lastParamAccessed) {
            bWasNull = lastParamAccessed.isNull();
        }
        loggerExternal.exiting(getClassNameLogging(), "wasNull", bWasNull);
        return bWasNull;
    }

    @Override
    public final java.io.InputStream getAsciiStream(int parameterIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getAsciiStream", parameterIndex);
        checkClosed();
        InputStream value = (InputStream) getStream(parameterIndex, StreamType.ASCII);
        loggerExternal.exiting(getClassNameLogging(), "getAsciiStream", value);
        return value;
    }

    @Override
    public final java.io.InputStream getAsciiStream(String parameterName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getAsciiStream", parameterName);
        checkClosed();
        InputStream value = (InputStream) getStream(findColumn(parameterName), StreamType.ASCII);
        loggerExternal.exiting(getClassNameLogging(), "getAsciiStream", value);
        return value;
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getBigDecimal", parameterIndex);
        checkClosed();
        BigDecimal value = (BigDecimal) getValue(parameterIndex, JDBCType.DECIMAL);
        loggerExternal.exiting(getClassNameLogging(), "getBigDecimal", value);
        return value;
    }

    @Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getBigDecimal", parameterName);
        checkClosed();
        BigDecimal value = (BigDecimal) getValue(findColumn(parameterName), JDBCType.DECIMAL);
        loggerExternal.exiting(getClassNameLogging(), "getBigDecimal", value);
        return value;
    }

    @Override
    public BigDecimal getMoney(int parameterIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getMoney", parameterIndex);
        checkClosed();
        BigDecimal value = (BigDecimal) getValue(parameterIndex, JDBCType.MONEY);
        loggerExternal.exiting(getClassNameLogging(), "getMoney", value);
        return value;
    }

    @Override
    public BigDecimal getMoney(String parameterName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getMoney", parameterName);
        checkClosed();
        BigDecimal value = (BigDecimal) getValue(findColumn(parameterName), JDBCType.MONEY);
        loggerExternal.exiting(getClassNameLogging(), "getMoney", value);
        return value;
    }

    @Override
    public BigDecimal getSmallMoney(int parameterIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getSmallMoney", parameterIndex);
        checkClosed();
        BigDecimal value = (BigDecimal) getValue(parameterIndex, JDBCType.SMALLMONEY);
        loggerExternal.exiting(getClassNameLogging(), "getSmallMoney", value);
        return value;
    }

    @Override
    public BigDecimal getSmallMoney(String parameterName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getSmallMoney", parameterName);
        checkClosed();
        BigDecimal value = (BigDecimal) getValue(findColumn(parameterName), JDBCType.SMALLMONEY);
        loggerExternal.exiting(getClassNameLogging(), "getSmallMoney", value);
        return value;
    }

    @Override
    public final java.io.InputStream getBinaryStream(int parameterIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getBinaryStream", parameterIndex);
        checkClosed();
        InputStream value = (InputStream) getStream(parameterIndex, StreamType.BINARY);
        loggerExternal.exiting(getClassNameLogging(), "getBinaryStream", value);
        return value;
    }

    @Override
    public final java.io.InputStream getBinaryStream(String parameterName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getBinaryStream", parameterName);
        checkClosed();
        InputStream value = (InputStream) getStream(findColumn(parameterName), StreamType.BINARY);
        loggerExternal.exiting(getClassNameLogging(), "getBinaryStream", value);
        return value;
    }

    @Override
    public Blob getBlob(int parameterIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getBlob", parameterIndex);
        checkClosed();
        Blob value = (Blob) getValue(parameterIndex, JDBCType.BLOB);
        loggerExternal.exiting(getClassNameLogging(), "getBlob", value);
        return value;
    }

    @Override
    public Blob getBlob(String parameterName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getBlob", parameterName);
        checkClosed();
        Blob value = (Blob) getValue(findColumn(parameterName), JDBCType.BLOB);
        loggerExternal.exiting(getClassNameLogging(), "getBlob", value);
        return value;
    }

    @Override
    public final java.io.Reader getCharacterStream(int parameterIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getCharacterStream", parameterIndex);
        checkClosed();
        Reader reader = (Reader) getStream(parameterIndex, StreamType.CHARACTER);
        loggerExternal.exiting(getClassNameLogging(), "getCharacterStream", reader);
        return reader;
    }

    @Override
    public final java.io.Reader getCharacterStream(String parameterName) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getCharacterStream", parameterName);
        checkClosed();
        Reader reader = (Reader) getStream(findColumn(parameterName), StreamType.CHARACTER);
        loggerExternal.exiting(getClassNameLogging(), "getCharacterSream", reader);
        return reader;
    }

    @Override
    public final java.io.Reader getNCharacterStream(int parameterIndex) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getNCharacterStream", parameterIndex);
        checkClosed();
        Reader reader = (Reader) getStream(parameterIndex, StreamType.NCHARACTER);
        loggerExternal.exiting(getClassNameLogging(), "getNCharacterStream", reader);
        return reader;
    }

    @Override
    public final java.io.Reader getNCharacterStream(String parameterName) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getNCharacterStream", parameterName);
        checkClosed();
        Reader reader = (Reader) getStream(findColumn(parameterName), StreamType.NCHARACTER);
        loggerExternal.exiting(getClassNameLogging(), "getNCharacterStream", reader);
        return reader;
    }

    void closeActiveStream() throws SQLServerException {
        if (null != activeStream) {
            try {
                activeStream.close();
            } catch (IOException e) {
                SQLServerException.makeFromDriverError(null, null, e.getMessage(), null, true);
            } finally {
                activeStream = null;
            }
        }
    }

    @Override
    public Clob getClob(int parameterIndex) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getClob", parameterIndex);
        checkClosed();
        Clob clob = (Clob) getValue(parameterIndex, JDBCType.CLOB);
        loggerExternal.exiting(getClassNameLogging(), "getClob", clob);
        return clob;
    }

    @Override
    public Clob getClob(String parameterName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getClob", parameterName);
        checkClosed();
        Clob clob = (Clob) getValue(findColumn(parameterName), JDBCType.CLOB);
        loggerExternal.exiting(getClassNameLogging(), "getClob", clob);
        return clob;
    }

    @Override
    public NClob getNClob(int parameterIndex) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getNClob", parameterIndex);
        checkClosed();
        NClob nClob = (NClob) getValue(parameterIndex, JDBCType.NCLOB);
        loggerExternal.exiting(getClassNameLogging(), "getNClob", nClob);
        return nClob;
    }

    @Override
    public NClob getNClob(String parameterName) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getNClob", parameterName);
        checkClosed();
        NClob nClob = (NClob) getValue(findColumn(parameterName), JDBCType.NCLOB);
        loggerExternal.exiting(getClassNameLogging(), "getNClob", nClob);
        return nClob;
    }

    @Override
    public Object getObject(int parameterIndex, java.util.Map<String, Class<?>> map) throws SQLException {
        SQLServerException.throwNotSupportedException(connection, this);
        return null;
    }

    @Override
    public Object getObject(String parameterName, java.util.Map<String, Class<?>> m) throws SQLException {
        checkClosed();
        return getObject(findColumn(parameterName), m);
    }

    @Override
    public Ref getRef(int parameterIndex) throws SQLException {
        SQLServerException.throwNotSupportedException(connection, this);
        return null;
    }

    @Override
    public Ref getRef(String parameterName) throws SQLException {
        checkClosed();
        return getRef(findColumn(parameterName));
    }

    @Override
    public java.sql.Array getArray(int parameterIndex) throws SQLException {
        SQLServerException.throwNotSupportedException(connection, this);
        return null;
    }

    @Override
    public java.sql.Array getArray(String parameterName) throws SQLException {
        checkClosed();
        return getArray(findColumn(parameterName));
    }

    /* JDBC 3.0 */

    /**
     * Find a column's index given its name.
     * 
     * @param columnName
     *        the name
     * @throws SQLServerException
     *         when an error occurs
     * @return the index
     */
    private int findColumn(String columnName) throws SQLServerException {
        if (null == parameterNames) {
            try (SQLServerStatement s = (SQLServerStatement) connection.createStatement()) {
                // Note we are concatenating the information from the passed in sql, not any arguments provided by the
                // user
                // if the user can execute the sql, any fragments of it is potentially executed via the meta data call
                // through injection
                // is not a security issue.

                ThreePartName threePartName = ThreePartName.parse(procedureName);
                StringBuilder metaQuery = new StringBuilder("exec sp_sproc_columns ");
                if (null != threePartName.getDatabasePart()) {
                    metaQuery.append("@procedure_qualifier=");
                    metaQuery.append(threePartName.getDatabasePart());
                    metaQuery.append(", ");
                }
                if (null != threePartName.getOwnerPart()) {
                    metaQuery.append("@procedure_owner=");
                    metaQuery.append(threePartName.getOwnerPart());
                    metaQuery.append(", ");
                }
                if (null != threePartName.getProcedurePart()) {
                    // we should always have a procedure name part
                    metaQuery.append("@procedure_name=");
                    metaQuery.append(threePartName.getProcedurePart());
                    metaQuery.append(" , @ODBCVer=3, @fUsePattern=0");
                } else {
                    // This should rarely happen, this will only happen if we can't find the stored procedure name
                    // invalidly formatted call syntax.
                    MessageFormat form = new MessageFormat(
                            SQLServerException.getErrString("R_parameterNotDefinedForProcedure"));
                    Object[] msgArgs = {columnName, ""};
                    SQLServerException.makeFromDriverError(connection, this, form.format(msgArgs), "07009", false);
                }

                try (ResultSet rs = s.executeQueryInternal(metaQuery.toString())) {
                    parameterNames = new HashMap<>();
                    insensitiveParameterNames = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                    int columnIndex = 0;
                    while (rs.next()) {
                        String p = rs.getString(4).trim();
                        parameterNames.put(p, columnIndex);
                        insensitiveParameterNames.put(p, columnIndex++);
                    }
                }
            } catch (SQLException e) {
                SQLServerException.makeFromDriverError(connection, this, e.toString(), null, false);
            }

        }

        // @RETURN_VALUE will always be in the parameterNames map, so parameterNamesSize will always be at least of size 1.
        // If the server didn't return anything (eg. the param names for the sproc), user might not have access.
        // So, parameterNamesSize must be of size 1.
        if (null != parameterNames && parameterNames.size() == 1) {
            return map.computeIfAbsent(columnName, ifAbsent -> ai.incrementAndGet());
        }

        // handle `@name` as well as `name`, since `@name` is what's returned
        // by DatabaseMetaData#getProcedureColumns
        String columnNameWithSign = columnName.startsWith("@") ? columnName : "@" + columnName;

        // In order to be as accurate as possible when locating parameter name
        // indexes, as well as be deterministic when running on various client
        // locales, we search for parameter names using the following scheme:

        // 1. Search using case-sensitive non-locale specific (binary) compare first.
        // 2. Search using case-insensitive, non-locale specific (binary) compare last.
        Integer matchPos = parameterNames.get(columnNameWithSign);
        if (null == matchPos) {
            matchPos = insensitiveParameterNames.get(columnNameWithSign);
        }
        if (null == matchPos) {
            MessageFormat form = new MessageFormat(
                    SQLServerException.getErrString("R_parameterNotDefinedForProcedure"));
            Object[] msgArgs = {columnName, procedureName};
            SQLServerException.makeFromDriverError(connection, this, form.format(msgArgs), "07009", false);
        }

        // @RETURN_VALUE is always in the list. If the user uses return value ?=call(@p1) syntax then
        // @p1 is index 2 otherwise its index 1.
        if (bReturnValueSyntax) // 3.2717
            return matchPos + 1;
        else
            return matchPos;
    }

    @Override
    public void setTimestamp(String parameterName, java.sql.Timestamp value,
            Calendar calendar) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setTimeStamp",
                    new Object[] {parameterName, value, calendar});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.TIMESTAMP, value, JavaType.TIMESTAMP, calendar, false);
        loggerExternal.exiting(getClassNameLogging(), "setTimeStamp");
    }

    @Override
    public void setTimestamp(String parameterName, java.sql.Timestamp value, Calendar calendar,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setTimeStamp",
                    new Object[] {parameterName, value, calendar, forceEncrypt});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.TIMESTAMP, value, JavaType.TIMESTAMP, calendar, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setTimeStamp");
    }

    @Override
    public void setTime(String parameterName, java.sql.Time value, Calendar calendar) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setTime", new Object[] {parameterName, value, calendar});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.TIME, value, JavaType.TIME, calendar, false);
        loggerExternal.exiting(getClassNameLogging(), "setTime");
    }

    @Override
    public void setTime(String parameterName, java.sql.Time value, Calendar calendar,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setTime",
                    new Object[] {parameterName, value, calendar, forceEncrypt});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.TIME, value, JavaType.TIME, calendar, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setTime");
    }

    @Override
    public void setDate(String parameterName, java.sql.Date value, Calendar calendar) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setDate", new Object[] {parameterName, value, calendar});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.DATE, value, JavaType.DATE, calendar, false);
        loggerExternal.exiting(getClassNameLogging(), "setDate");
    }

    @Override
    public void setDate(String parameterName, java.sql.Date value, Calendar calendar,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setDate",
                    new Object[] {parameterName, value, calendar, forceEncrypt});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.DATE, value, JavaType.DATE, calendar, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setDate");
    }

    @Override
    public final void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setCharacterStream", new Object[] {parameterName, reader});
        checkClosed();
        setStream(findColumn(parameterName), StreamType.CHARACTER, reader, JavaType.READER,
                DataTypes.UNKNOWN_STREAM_LENGTH);
        loggerExternal.exiting(getClassNameLogging(), "setCharacterStream");
    }

    @Override
    public final void setCharacterStream(String parameterName, Reader value, int length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setCharacterStream",
                    new Object[] {parameterName, value, length});
        checkClosed();
        setStream(findColumn(parameterName), StreamType.CHARACTER, value, JavaType.READER, length);
        loggerExternal.exiting(getClassNameLogging(), "setCharacterStream");
    }

    @Override
    public final void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {

        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setCharacterStream",
                    new Object[] {parameterName, reader, length});
        checkClosed();
        setStream(findColumn(parameterName), StreamType.CHARACTER, reader, JavaType.READER, length);
        loggerExternal.exiting(getClassNameLogging(), "setCharacterStream");
    }

    @Override
    public final void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setNCharacterStream", new Object[] {parameterName, value});
        checkClosed();
        setStream(findColumn(parameterName), StreamType.NCHARACTER, value, JavaType.READER,
                DataTypes.UNKNOWN_STREAM_LENGTH);
        loggerExternal.exiting(getClassNameLogging(), "setNCharacterStream");
    }

    @Override
    public final void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setNCharacterStream",
                    new Object[] {parameterName, value, length});
        checkClosed();
        setStream(findColumn(parameterName), StreamType.NCHARACTER, value, JavaType.READER, length);
        loggerExternal.exiting(getClassNameLogging(), "setNCharacterStream");
    }

    @Override
    public final void setClob(String parameterName, Clob value) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setClob", new Object[] {parameterName, value});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.CLOB, value, JavaType.CLOB, false);
        loggerExternal.exiting(getClassNameLogging(), "setClob");
    }

    @Override
    public final void setClob(String parameterName, Reader reader) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setClob", new Object[] {parameterName, reader});
        checkClosed();
        setStream(findColumn(parameterName), StreamType.CHARACTER, reader, JavaType.READER,
                DataTypes.UNKNOWN_STREAM_LENGTH);
        loggerExternal.exiting(getClassNameLogging(), "setClob");
    }

    @Override
    public final void setClob(String parameterName, Reader value, long length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setClob", new Object[] {parameterName, value, length});
        checkClosed();
        setStream(findColumn(parameterName), StreamType.CHARACTER, value, JavaType.READER, length);
        loggerExternal.exiting(getClassNameLogging(), "setClob");
    }

    @Override
    public final void setNClob(String parameterName, NClob value) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setNClob", new Object[] {parameterName, value});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.NCLOB, value, JavaType.NCLOB, false);
        loggerExternal.exiting(getClassNameLogging(), "setNClob");
    }

    @Override
    public final void setNClob(String parameterName, Reader reader) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setNClob", new Object[] {parameterName, reader});
        checkClosed();
        setStream(findColumn(parameterName), StreamType.NCHARACTER, reader, JavaType.READER,
                DataTypes.UNKNOWN_STREAM_LENGTH);
        loggerExternal.exiting(getClassNameLogging(), "setNClob");
    }

    @Override
    public final void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setNClob", new Object[] {parameterName, reader, length});
        checkClosed();
        setStream(findColumn(parameterName), StreamType.NCHARACTER, reader, JavaType.READER, length);
        loggerExternal.exiting(getClassNameLogging(), "setNClob");
    }

    @Override
    public final void setNString(String parameterName, String value) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setNString", new Object[] {parameterName, value});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.NVARCHAR, value, JavaType.STRING, false);
        loggerExternal.exiting(getClassNameLogging(), "setNString");
    }

    @Override
    public final void setNString(String parameterName, String value, boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setNString",
                    new Object[] {parameterName, value, forceEncrypt});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.NVARCHAR, value, JavaType.STRING, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setNString");
    }

    @Override
    public void setObject(String parameterName, Object value) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setObject", new Object[] {parameterName, value});
        checkClosed();
        setObjectNoType(findColumn(parameterName), value, false);
        loggerExternal.exiting(getClassNameLogging(), "setObject");
    }

    @Override
    public void setObject(String parameterName, Object value, int sqlType) throws SQLServerException {
        String tvpName = null;
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setObject", new Object[] {parameterName, value, sqlType});
        checkClosed();
        if (microsoft.sql.Types.STRUCTURED == sqlType) {
            tvpName = getTVPNameFromObject(findColumn(parameterName), value);
            setObject(setterGetParam(findColumn(parameterName)), value, JavaType.TVP, JDBCType.TVP, null, null, false,
                    findColumn(parameterName), tvpName);
        } else
            setObject(setterGetParam(findColumn(parameterName)), value, JavaType.of(value), JDBCType.of(sqlType), null,
                    null, false, findColumn(parameterName), tvpName);
        loggerExternal.exiting(getClassNameLogging(), "setObject");
    }

    @Override
    public void setObject(String parameterName, Object value, int sqlType, int decimals) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setObject",
                    new Object[] {parameterName, value, sqlType, decimals});
        checkClosed();
        setObject(setterGetParam(findColumn(parameterName)), value, JavaType.of(value), JDBCType.of(sqlType), decimals,
                null, false, findColumn(parameterName), null);
        loggerExternal.exiting(getClassNameLogging(), "setObject");
    }

    @Override
    public void setObject(String parameterName, Object value, int sqlType, int decimals,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setObject",
                    new Object[] {parameterName, value, sqlType, decimals, forceEncrypt});
        checkClosed();

        // scale - for java.sql.Types.DECIMAL or java.sql.Types.NUMERIC types,
        // this is the number of digits after the decimal point.
        // For all other types, this value will be ignored.

        setObject(setterGetParam(findColumn(parameterName)), value, JavaType.of(value), JDBCType.of(sqlType),
                (java.sql.Types.NUMERIC == sqlType || java.sql.Types.DECIMAL == sqlType) ? decimals : null, null,
                forceEncrypt, findColumn(parameterName), null);

        loggerExternal.exiting(getClassNameLogging(), "setObject");
    }

    @Override
    public final void setObject(String parameterName, Object value, int targetSqlType, Integer precision,
            int scale) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setObject",
                    new Object[] {parameterName, value, targetSqlType, precision, scale});
        checkClosed();

        // scale - for java.sql.Types.DECIMAL or java.sql.Types.NUMERIC types,
        // this is the number of digits after the decimal point. For Java Object types
        // InputStream and Reader, this is the length of the data in the stream or reader.
        // For all other types, this value will be ignored.

        setObject(setterGetParam(findColumn(parameterName)), value, JavaType.of(value), JDBCType.of(targetSqlType),
                (java.sql.Types.NUMERIC == targetSqlType || java.sql.Types.DECIMAL == targetSqlType
                        || InputStream.class.isInstance(value) || Reader.class.isInstance(value)) ? scale : null,
                precision, false, findColumn(parameterName), null);

        loggerExternal.exiting(getClassNameLogging(), "setObject");
    }

    @Override
    public final void setAsciiStream(String parameterName, InputStream value) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setAsciiStream", new Object[] {parameterName, value});
        checkClosed();
        setStream(findColumn(parameterName), StreamType.ASCII, value, JavaType.INPUTSTREAM,
                DataTypes.UNKNOWN_STREAM_LENGTH);
        loggerExternal.exiting(getClassNameLogging(), "setAsciiStream");
    }

    @Override
    public final void setAsciiStream(String parameterName, InputStream value, int length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setAsciiStream",
                    new Object[] {parameterName, value, length});
        checkClosed();
        setStream(findColumn(parameterName), StreamType.ASCII, value, JavaType.INPUTSTREAM, length);
        loggerExternal.exiting(getClassNameLogging(), "setAsciiStream");
    }

    @Override
    public final void setAsciiStream(String parameterName, InputStream value, long length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setAsciiStream",
                    new Object[] {parameterName, value, length});
        checkClosed();
        setStream(findColumn(parameterName), StreamType.ASCII, value, JavaType.INPUTSTREAM, length);
        loggerExternal.exiting(getClassNameLogging(), "setAsciiStream");
    }

    @Override
    public final void setBinaryStream(String parameterName, InputStream value) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBinaryStream", new Object[] {parameterName, value});
        checkClosed();
        setStream(findColumn(parameterName), StreamType.BINARY, value, JavaType.INPUTSTREAM,
                DataTypes.UNKNOWN_STREAM_LENGTH);
        loggerExternal.exiting(getClassNameLogging(), "setBinaryStream");
    }

    @Override
    public final void setBinaryStream(String parameterName, InputStream value, int length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBinaryStream",
                    new Object[] {parameterName, value, length});
        checkClosed();
        setStream(findColumn(parameterName), StreamType.BINARY, value, JavaType.INPUTSTREAM, length);
        loggerExternal.exiting(getClassNameLogging(), "setBinaryStream");
    }

    @Override
    public final void setBinaryStream(String parameterName, InputStream value, long length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBinaryStream",
                    new Object[] {parameterName, value, length});
        checkClosed();
        setStream(findColumn(parameterName), StreamType.BINARY, value, JavaType.INPUTSTREAM, length);
        loggerExternal.exiting(getClassNameLogging(), "setBinaryStream");
    }

    @Override
    public final void setBlob(String parameterName, Blob inputStream) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBlob", new Object[] {parameterName, inputStream});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.BLOB, inputStream, JavaType.BLOB, false);
        loggerExternal.exiting(getClassNameLogging(), "setBlob");
    }

    @Override
    public final void setBlob(String parameterName, InputStream value) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBlob", new Object[] {parameterName, value});
        checkClosed();
        setStream(findColumn(parameterName), StreamType.BINARY, value, JavaType.INPUTSTREAM,
                DataTypes.UNKNOWN_STREAM_LENGTH);
        loggerExternal.exiting(getClassNameLogging(), "setBlob");
    }

    @Override
    public final void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBlob",
                    new Object[] {parameterName, inputStream, length});
        checkClosed();
        setStream(findColumn(parameterName), StreamType.BINARY, inputStream, JavaType.INPUTSTREAM, length);
        loggerExternal.exiting(getClassNameLogging(), "setBlob");
    }

    @Override
    public void setTimestamp(String parameterName, java.sql.Timestamp value) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setTimestamp", new Object[] {parameterName, value});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.TIMESTAMP, value, JavaType.TIMESTAMP, false);
        loggerExternal.exiting(getClassNameLogging(), "setTimestamp");
    }

    @Override
    public void setTimestamp(String parameterName, java.sql.Timestamp value, int scale) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setTimestamp", new Object[] {parameterName, value});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.TIMESTAMP, value, JavaType.TIMESTAMP, null, scale, false);
        loggerExternal.exiting(getClassNameLogging(), "setTimestamp");
    }

    @Override
    public void setTimestamp(String parameterName, java.sql.Timestamp value, int scale,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setTimestamp",
                    new Object[] {parameterName, value, forceEncrypt});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.TIMESTAMP, value, JavaType.TIMESTAMP, null, scale, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setTimestamp");
    }

    @Override
    public void setDateTimeOffset(String parameterName, microsoft.sql.DateTimeOffset value) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setDateTimeOffset", new Object[] {parameterName, value});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.DATETIMEOFFSET, value, JavaType.DATETIMEOFFSET, false);
        loggerExternal.exiting(getClassNameLogging(), "setDateTimeOffset");
    }

    @Override
    public void setDateTimeOffset(String parameterName, microsoft.sql.DateTimeOffset value,
            int scale) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setDateTimeOffset", new Object[] {parameterName, value});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.DATETIMEOFFSET, value, JavaType.DATETIMEOFFSET, null, scale,
                false);
        loggerExternal.exiting(getClassNameLogging(), "setDateTimeOffset");
    }

    @Override
    public void setDateTimeOffset(String parameterName, microsoft.sql.DateTimeOffset value, int scale,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setDateTimeOffset",
                    new Object[] {parameterName, value, forceEncrypt});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.DATETIMEOFFSET, value, JavaType.DATETIMEOFFSET, null, scale,
                forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setDateTimeOffset");
    }

    @Override
    public void setDate(String parameterName, java.sql.Date value) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setDate", new Object[] {parameterName, value});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.DATE, value, JavaType.DATE, false);
        loggerExternal.exiting(getClassNameLogging(), "setDate");
    }

    @Override
    public void setTime(String parameterName, java.sql.Time value) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setTime", new Object[] {parameterName, value});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.TIME, value, JavaType.TIME, false);
        loggerExternal.exiting(getClassNameLogging(), "setTime");
    }

    @Override
    public void setTime(String parameterName, java.sql.Time value, int scale) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setTime", new Object[] {parameterName, value});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.TIME, value, JavaType.TIME, null, scale, false);
        loggerExternal.exiting(getClassNameLogging(), "setTime");
    }

    @Override
    public void setTime(String parameterName, java.sql.Time value, int scale,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setTime",
                    new Object[] {parameterName, value, forceEncrypt});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.TIME, value, JavaType.TIME, null, scale, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setTime");
    }

    @Override
    public void setDateTime(String parameterName, java.sql.Timestamp value) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setDateTime", new Object[] {parameterName, value});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.DATETIME, value, JavaType.TIMESTAMP, false);
        loggerExternal.exiting(getClassNameLogging(), "setDateTime");
    }

    @Override
    public void setDateTime(String parameterName, java.sql.Timestamp value,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setDateTime",
                    new Object[] {parameterName, value, forceEncrypt});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.DATETIME, value, JavaType.TIMESTAMP, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setDateTime");
    }

    @Override
    public void setSmallDateTime(String parameterName, java.sql.Timestamp value) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setSmallDateTime", new Object[] {parameterName, value});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.SMALLDATETIME, value, JavaType.TIMESTAMP, false);
        loggerExternal.exiting(getClassNameLogging(), "setSmallDateTime");
    }

    @Override
    public void setSmallDateTime(String parameterName, java.sql.Timestamp value,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setSmallDateTime",
                    new Object[] {parameterName, value, forceEncrypt});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.SMALLDATETIME, value, JavaType.TIMESTAMP, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setSmallDateTime");
    }

    @Override
    public void setUniqueIdentifier(String parameterName, String guid) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setUniqueIdentifier", new Object[] {parameterName, guid});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.GUID, guid, JavaType.STRING, false);
        loggerExternal.exiting(getClassNameLogging(), "setUniqueIdentifier");
    }

    @Override
    public void setUniqueIdentifier(String parameterName, String guid, boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setUniqueIdentifier",
                    new Object[] {parameterName, guid, forceEncrypt});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.GUID, guid, JavaType.STRING, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setUniqueIdentifier");
    }

    @Override
    public void setBytes(String parameterName, byte[] value) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBytes", new Object[] {parameterName, value});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.BINARY, value, JavaType.BYTEARRAY, false);
        loggerExternal.exiting(getClassNameLogging(), "setBytes");
    }

    @Override
    public void setBytes(String parameterName, byte[] value, boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBytes",
                    new Object[] {parameterName, value, forceEncrypt});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.BINARY, value, JavaType.BYTEARRAY, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setBytes");
    }

    @Override
    public void setByte(String parameterName, byte value) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setByte", new Object[] {parameterName, value});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.TINYINT, value, JavaType.BYTE, false);
        loggerExternal.exiting(getClassNameLogging(), "setByte");
    }

    @Override
    public void setByte(String parameterName, byte value, boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setByte",
                    new Object[] {parameterName, value, forceEncrypt});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.TINYINT, value, JavaType.BYTE, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setByte");
    }

    @Override
    public void setString(String parameterName, String value) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setString", new Object[] {parameterName, value});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.VARCHAR, value, JavaType.STRING, false);
        loggerExternal.exiting(getClassNameLogging(), "setString");
    }

    @Override
    public void setString(String parameterName, String value, boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setString",
                    new Object[] {parameterName, value, forceEncrypt});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.VARCHAR, value, JavaType.STRING, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setString");
    }

    @Override
    public void setMoney(String parameterName, BigDecimal value) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setMoney", new Object[] {parameterName, value});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.MONEY, value, JavaType.BIGDECIMAL, false);
        loggerExternal.exiting(getClassNameLogging(), "setMoney");
    }

    @Override
    public void setMoney(String parameterName, BigDecimal value, boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setMoney",
                    new Object[] {parameterName, value, forceEncrypt});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.MONEY, value, JavaType.BIGDECIMAL, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setMoney");
    }

    @Override
    public void setSmallMoney(String parameterName, BigDecimal value) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setSmallMoney", new Object[] {parameterName, value});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.SMALLMONEY, value, JavaType.BIGDECIMAL, false);
        loggerExternal.exiting(getClassNameLogging(), "setSmallMoney");
    }

    @Override
    public void setSmallMoney(String parameterName, BigDecimal value, boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setSmallMoney",
                    new Object[] {parameterName, value, forceEncrypt});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.SMALLMONEY, value, JavaType.BIGDECIMAL, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setSmallMoney");
    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal value) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBigDecimal", new Object[] {parameterName, value});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.DECIMAL, value, JavaType.BIGDECIMAL, false);
        loggerExternal.exiting(getClassNameLogging(), "setBigDecimal");
    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal value, int precision,
            int scale) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBigDecimal",
                    new Object[] {parameterName, value, precision, scale});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.DECIMAL, value, JavaType.BIGDECIMAL, precision, scale, false);
        loggerExternal.exiting(getClassNameLogging(), "setBigDecimal");
    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal value, int precision, int scale,
            boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBigDecimal",
                    new Object[] {parameterName, value, precision, scale, forceEncrypt});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.DECIMAL, value, JavaType.BIGDECIMAL, precision, scale,
                forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setBigDecimal");
    }

    @Override
    public void setDouble(String parameterName, double value) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setDouble", new Object[] {parameterName, value});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.DOUBLE, value, JavaType.DOUBLE, false);
        loggerExternal.exiting(getClassNameLogging(), "setDouble");
    }

    @Override
    public void setDouble(String parameterName, double value, boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setDouble",
                    new Object[] {parameterName, value, forceEncrypt});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.DOUBLE, value, JavaType.DOUBLE, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setDouble");
    }

    @Override
    public void setFloat(String parameterName, float value) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setFloat", new Object[] {parameterName, value});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.REAL, value, JavaType.FLOAT, false);
        loggerExternal.exiting(getClassNameLogging(), "setFloat");
    }

    @Override
    public void setFloat(String parameterName, float value, boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setFloat",
                    new Object[] {parameterName, value, forceEncrypt});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.REAL, value, JavaType.FLOAT, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setFloat");
    }

    @Override
    public void setInt(String parameterName, int value) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setInt", new Object[] {parameterName, value});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.INTEGER, value, JavaType.INTEGER, false);
        loggerExternal.exiting(getClassNameLogging(), "setInt");
    }

    @Override
    public void setInt(String parameterName, int value, boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setInt", new Object[] {parameterName, value, forceEncrypt});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.INTEGER, value, JavaType.INTEGER, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setInt");
    }

    @Override
    public void setLong(String parameterName, long value) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setLong", new Object[] {parameterName, value});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.BIGINT, value, JavaType.LONG, false);
        loggerExternal.exiting(getClassNameLogging(), "setLong");
    }

    @Override
    public void setLong(String parameterName, long value, boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setLong",
                    new Object[] {parameterName, value, forceEncrypt});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.BIGINT, value, JavaType.LONG, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setLong");
    }

    @Override
    public void setShort(String parameterName, short value) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setShort", new Object[] {parameterName, value});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.SMALLINT, value, JavaType.SHORT, false);
        loggerExternal.exiting(getClassNameLogging(), "setShort");
    }

    @Override
    public void setShort(String parameterName, short value, boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setShort",
                    new Object[] {parameterName, value, forceEncrypt});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.SMALLINT, value, JavaType.SHORT, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setShort");
    }

    @Override
    public void setBoolean(String parameterName, boolean value) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBoolean", new Object[] {parameterName, value});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.BIT, value, JavaType.BOOLEAN, false);
        loggerExternal.exiting(getClassNameLogging(), "setBoolean");
    }

    @Override
    public void setBoolean(String parameterName, boolean value, boolean forceEncrypt) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setBoolean",
                    new Object[] {parameterName, value, forceEncrypt});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.BIT, value, JavaType.BOOLEAN, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setBoolean");
    }

    @Override
    public void setNull(String parameterName, int nType) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setNull", new Object[] {parameterName, nType});
        checkClosed();
        setObject(setterGetParam(findColumn(parameterName)), null, JavaType.OBJECT, JDBCType.of(nType), null, null,
                false, findColumn(parameterName), null);
        loggerExternal.exiting(getClassNameLogging(), "setNull");
    }

    @Override
    public void setNull(String parameterName, int nType, String sTypeName) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setNull", new Object[] {parameterName, nType, sTypeName});
        checkClosed();
        setObject(setterGetParam(findColumn(parameterName)), null, JavaType.OBJECT, JDBCType.of(nType), null, null,
                false, findColumn(parameterName), sTypeName);
        loggerExternal.exiting(getClassNameLogging(), "setNull");
    }

    @Override
    public void setURL(String parameterName, URL url) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "setURL", parameterName);
        checkClosed();
        setURL(findColumn(parameterName), url);
        loggerExternal.exiting(getClassNameLogging(), "setURL");
    }

    @Override
    public final void setStructured(String parameterName, String tvpName,
            SQLServerDataTable tvpDataTable) throws SQLServerException {
        tvpName = getTVPNameIfNull(findColumn(parameterName), tvpName);
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setStructured",
                    new Object[] {parameterName, tvpName, tvpDataTable});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.TVP, tvpDataTable, JavaType.TVP, tvpName);
        loggerExternal.exiting(getClassNameLogging(), "setStructured");
    }

    @Override
    public final void setStructured(String parameterName, String tvpName,
            ResultSet tvpResultSet) throws SQLServerException {
        tvpName = getTVPNameIfNull(findColumn(parameterName), tvpName);
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setStructured",
                    new Object[] {parameterName, tvpName, tvpResultSet});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.TVP, tvpResultSet, JavaType.TVP, tvpName);
        loggerExternal.exiting(getClassNameLogging(), "setStructured");
    }

    @Override
    public final void setStructured(String parameterName, String tvpName,
            ISQLServerDataRecord tvpDataRecord) throws SQLServerException {
        tvpName = getTVPNameIfNull(findColumn(parameterName), tvpName);
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setStructured",
                    new Object[] {parameterName, tvpName, tvpDataRecord});
        checkClosed();
        setValue(findColumn(parameterName), JDBCType.TVP, tvpDataRecord, JavaType.TVP, tvpName);
        loggerExternal.exiting(getClassNameLogging(), "setStructured");
    }

    @Override
    public URL getURL(int parameterIndex) throws SQLException {
        SQLServerException.throwNotSupportedException(connection, this);
        return null;
    }

    @Override
    public URL getURL(String parameterName) throws SQLException {
        SQLServerException.throwNotSupportedException(connection, this);
        return null;
    }

    @Override
    public final void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setSQLXML", new Object[] {parameterName, xmlObject});
        checkClosed();
        setSQLXMLInternal(findColumn(parameterName), xmlObject);
        loggerExternal.exiting(getClassNameLogging(), "setSQLXML");
    }

    @Override
    public final SQLXML getSQLXML(int parameterIndex) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getSQLXML", parameterIndex);
        checkClosed();
        SQLServerSQLXML value = (SQLServerSQLXML) getSQLXMLInternal(parameterIndex);
        loggerExternal.exiting(getClassNameLogging(), "getSQLXML", value);
        return value;
    }

    @Override
    public final SQLXML getSQLXML(String parameterName) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getSQLXML", parameterName);
        checkClosed();
        SQLServerSQLXML value = (SQLServerSQLXML) getSQLXMLInternal(findColumn(parameterName));
        loggerExternal.exiting(getClassNameLogging(), "getSQLXML", value);
        return value;
    }

    @Override
    public final void setRowId(String parameterName, RowId value) throws SQLException {
        SQLServerException.throwNotSupportedException(connection, this);
    }

    @Override
    public final RowId getRowId(int parameterIndex) throws SQLException {
        SQLServerException.throwNotSupportedException(connection, this);
        return null;
    }

    @Override
    public final RowId getRowId(String parameterName) throws SQLException {
        SQLServerException.throwNotSupportedException(connection, this);
        return null;
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "registerOutParameter",
                    new Object[] {parameterName, sqlType, typeName});
        checkClosed();
        registerOutParameter(findColumn(parameterName), sqlType, typeName);
        loggerExternal.exiting(getClassNameLogging(), "registerOutParameter");
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "registerOutParameter",
                    new Object[] {parameterName, sqlType, scale});
        checkClosed();
        registerOutParameter(findColumn(parameterName), sqlType, scale);
        loggerExternal.exiting(getClassNameLogging(), "registerOutParameter");
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, int precision,
            int scale) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "registerOutParameter",
                    new Object[] {parameterName, sqlType, scale});
        checkClosed();
        registerOutParameter(findColumn(parameterName), sqlType, precision, scale);
        loggerExternal.exiting(getClassNameLogging(), "registerOutParameter");
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "registerOutParameter",
                    new Object[] {parameterName, sqlType});
        checkClosed();
        registerOutParameter(findColumn(parameterName), sqlType);
        loggerExternal.exiting(getClassNameLogging(), "registerOutParameter");
    }

    @Override
    public void registerOutParameter(int parameterIndex, SQLType sqlType) throws SQLServerException {

        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "registerOutParameter",
                    new Object[] {parameterIndex, sqlType});

        // getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
        registerOutParameter(parameterIndex, sqlType.getVendorTypeNumber());
        loggerExternal.exiting(getClassNameLogging(), "registerOutParameter");
    }

    @Override
    public void registerOutParameter(int parameterIndex, SQLType sqlType, String typeName) throws SQLServerException {

        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "registerOutParameter",
                    new Object[] {parameterIndex, sqlType, typeName});

        // getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
        registerOutParameter(parameterIndex, sqlType.getVendorTypeNumber(), typeName);
        loggerExternal.exiting(getClassNameLogging(), "registerOutParameter");
    }

    @Override
    public void registerOutParameter(int parameterIndex, SQLType sqlType, int scale) throws SQLServerException {

        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "registerOutParameter",
                    new Object[] {parameterIndex, sqlType, scale});

        // getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
        registerOutParameter(parameterIndex, sqlType.getVendorTypeNumber(), scale);
        loggerExternal.exiting(getClassNameLogging(), "registerOutParameter");
    }

    @Override
    public void registerOutParameter(int parameterIndex, SQLType sqlType, int precision,
            int scale) throws SQLServerException {

        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "registerOutParameter",
                    new Object[] {parameterIndex, sqlType, scale});

        // getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
        registerOutParameter(parameterIndex, sqlType.getVendorTypeNumber(), precision, scale);
        loggerExternal.exiting(getClassNameLogging(), "registerOutParameter");
    }

    @Override
    public void setObject(String parameterName, Object value, SQLType jdbcType) throws SQLServerException {

        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setObject", new Object[] {parameterName, value, jdbcType});

        // getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
        setObject(parameterName, value, jdbcType.getVendorTypeNumber());
        loggerExternal.exiting(getClassNameLogging(), "setObject");
    }

    @Override
    public void setObject(String parameterName, Object value, SQLType jdbcType, int scale) throws SQLServerException {

        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setObject",
                    new Object[] {parameterName, value, jdbcType, scale});

        // getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
        setObject(parameterName, value, jdbcType.getVendorTypeNumber(), scale);
        loggerExternal.exiting(getClassNameLogging(), "setObject");
    }

    @Override
    public void setObject(String parameterName, Object value, SQLType jdbcType, int scale,
            boolean forceEncrypt) throws SQLServerException {

        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setObject",
                    new Object[] {parameterName, value, jdbcType, scale, forceEncrypt});

        // getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
        setObject(parameterName, value, jdbcType.getVendorTypeNumber(), scale, forceEncrypt);
        loggerExternal.exiting(getClassNameLogging(), "setObject");
    }

    @Override
    public void registerOutParameter(String parameterName, SQLType sqlType, String typeName) throws SQLServerException {

        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "registerOutParameter",
                    new Object[] {parameterName, sqlType, typeName});

        // getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
        registerOutParameter(parameterName, sqlType.getVendorTypeNumber(), typeName);
        loggerExternal.exiting(getClassNameLogging(), "registerOutParameter");
    }

    @Override
    public void registerOutParameter(String parameterName, SQLType sqlType, int scale) throws SQLServerException {

        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "registerOutParameter",
                    new Object[] {parameterName, sqlType, scale});

        // getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
        registerOutParameter(parameterName, sqlType.getVendorTypeNumber(), scale);
        loggerExternal.exiting(getClassNameLogging(), "registerOutParameter");
    }

    @Override
    public void registerOutParameter(String parameterName, SQLType sqlType, int precision,
            int scale) throws SQLServerException {

        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "registerOutParameter",
                    new Object[] {parameterName, sqlType, scale});

        // getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
        registerOutParameter(parameterName, sqlType.getVendorTypeNumber(), precision, scale);
        loggerExternal.exiting(getClassNameLogging(), "registerOutParameter");
    }

    @Override
    public void registerOutParameter(String parameterName, SQLType sqlType) throws SQLServerException {

        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "registerOutParameter",
                    new Object[] {parameterName, sqlType});

        // getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
        registerOutParameter(parameterName, sqlType.getVendorTypeNumber());
        loggerExternal.exiting(getClassNameLogging(), "registerOutParameter");
    }
}
