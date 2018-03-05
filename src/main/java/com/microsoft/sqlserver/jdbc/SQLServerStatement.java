/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static com.microsoft.sqlserver.jdbc.SQLServerConnection.getCachedParsedSQL;
import static com.microsoft.sqlserver.jdbc.SQLServerConnection.parseAndCacheSQL;

import java.sql.BatchUpdateException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.SQLTimeoutException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.ListIterator;
import java.util.Stack;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.microsoft.sqlserver.jdbc.SQLServerConnection.Sha1HashKey;

/**
 * SQLServerStatment provides the basic implementation of JDBC statement functionality. It also provides a number of base class implementation methods
 * for the JDBC prepared statement and callable Statements. SQLServerStatement's basic role is to execute SQL statements and return update counts and
 * resultset rows to the user application.
 *
 * Documentation for specific public methods that are undocumented can be found under Sun's standard JDBC documentation for class java.sql.Statement.
 * Those methods are part of Sun's standard JDBC documentation and therefore their documentation is not duplicated here.
 * <p>
 * Implementation Notes
 * <p>
 * Fetching Result sets
 * <p>
 * The queries first rowset is available immediately after the executeQuery. The first rs.next() does not make a server round trip. For non server
 * side resultsets the entire result set is in the rowset. For server side result sets the number of rows in the rowset is set with nFetchSize
 * <p>
 * The API javadoc for JDBC API methods that this class implements are not repeated here. Please see Sun's JDBC API interfaces javadoc for those
 * details.
 */

public class SQLServerStatement implements ISQLServerStatement {
    final static char LEFT_CURLY_BRACKET = 123;
    final static char RIGHT_CURLY_BRACKET = 125;

    private boolean isResponseBufferingAdaptive = false;

    final boolean getIsResponseBufferingAdaptive() {
        return isResponseBufferingAdaptive;
    }

    private boolean wasResponseBufferingSet = false;

    final boolean wasResponseBufferingSet() {
        return wasResponseBufferingSet;
    }

    final static String identityQuery = " select SCOPE_IDENTITY() AS GENERATED_KEYS";

    /** the stored procedure name to call (if there is one) */
    String procedureName;

    /** OUT parameters associated with this statement's execution */
    private int serverCursorId;

    final int getServerCursorId() {
        return serverCursorId;
    }

    private int serverCursorRowCount;

    final int getServerCursorRowCount() {
        return serverCursorRowCount;
    }

    /** The value of the poolable state */
    boolean stmtPoolable;

    /** Streaming access to the response TDS data stream */
    private TDSReader tdsReader;

    final TDSReader resultsReader() {
        return tdsReader;
    }

    final boolean wasExecuted() {
        return null != tdsReader;
    }

    /**
     * The input and out parameters for statement execution.
     */
    Parameter[] inOutParam; // Parameters for prepared stmts and stored procedures

    /**
     * The statements connection.
     */
    final SQLServerConnection connection;

    /**
     * The user's specifed query timeout (in seconds).
     */
    int queryTimeout;

    /**
     * Is closeOnCompletion is enabled? If true statement will be closed when all of its dependent result sets are closed
     */
    boolean isCloseOnCompletion = false;

    /**
     * Currently executing or most recently executed TDSCommand (statement cmd, server cursor cmd, ...) subject to cancellation through
     * Statement.cancel.
     *
     * Note: currentCommand is declared volatile to ensure that the JVM always returns the most recently set value for currentCommand to the
     * cancelling thread.
     */
    private volatile TDSCommand currentCommand = null;
    private TDSCommand lastStmtExecCmd = null;

    final void discardLastExecutionResults() {
        if (null != lastStmtExecCmd && !bIsClosed) {
            lastStmtExecCmd.close();
            lastStmtExecCmd = null;
        }
        clearLastResult();
    }

    static final java.util.logging.Logger loggerExternal = java.util.logging.Logger.getLogger("com.microsoft.sqlserver.jdbc.Statement");
    final private String loggingClassName;
    final private String traceID;

    String getClassNameLogging() {
        return loggingClassName;
    }

    /*
     * Column Encryption Override. Defaults to the connection setting, in which case it will be Enabled if columnEncryptionSetting = true in the
     * connection setting, Disabled if false. This may also be used to set other behavior which overrides connection level setting.
     */
    protected SQLServerStatementColumnEncryptionSetting stmtColumnEncriptionSetting = SQLServerStatementColumnEncryptionSetting.UseConnectionSetting;

    /**
     * ExecuteProperties encapsulates a subset of statement property values as they were set at execution time.
     */
    final class ExecuteProperties {
        final private boolean wasResponseBufferingSet;

        final boolean wasResponseBufferingSet() {
            return wasResponseBufferingSet;
        }

        final private boolean isResponseBufferingAdaptive;

        final boolean isResponseBufferingAdaptive() {
            return isResponseBufferingAdaptive;
        }

        final private int holdability;

        final int getHoldability() {
            return holdability;
        }

        ExecuteProperties(SQLServerStatement stmt) {
            wasResponseBufferingSet = stmt.wasResponseBufferingSet();
            isResponseBufferingAdaptive = stmt.getIsResponseBufferingAdaptive();
            holdability = stmt.connection.getHoldabilityInternal();
        }
    }

    private ExecuteProperties execProps;

    final ExecuteProperties getExecProps() {
        return execProps;
    }

    /**
     * Executes this Statement using TDSCommand newStmtCmd.
     *
     * The TDSCommand is assumed to be a statement execution command (StmtExecCmd, PrepStmtExecCmd, PrepStmtBatchExecCmd).
     */
    final void executeStatement(TDSCommand newStmtCmd) throws SQLServerException, SQLTimeoutException {
        // Ensure that any response left over from a previous execution has been
        // completely processed. There may be ENVCHANGEs in that response that
        // we must acknowledge before proceeding.
        discardLastExecutionResults();

        // make sure statement hasn't been closed due to closeOnCompletion
        checkClosed();

        execProps = new ExecuteProperties(this);

        try {
            // (Re)execute this Statement with the new command
            executeCommand(newStmtCmd);
        } catch (SQLServerException e) {
        	if (e.getDriverErrorCode() == SQLServerException.ERROR_QUERY_TIMEOUT)
        		throw new SQLTimeoutException(e.getMessage(), e.getSQLState(), e.getErrorCode(), e.getCause());
        	else
        		throw e;
        }
        finally {
            lastStmtExecCmd = newStmtCmd;
        }
    }

    /**
     * Executes TDSCommand newCommand through this Statement object, allowing it to be cancelled through Statement.cancel().
     *
     * The specified command is typically the one used to execute the statement. But it could also be a server cursor command (fetch, move, close)
     * generated by a ResultSet that this statement produced.
     *
     * This method does not prevent applications from simultaneously executing commands from multiple threads. The assumption is that apps only call
     * cancel() from another thread while the command is executing.
     */
    final void executeCommand(TDSCommand newCommand) throws SQLServerException{
        // Set the new command as the current command so that
        // its execution can be cancelled from another thread
        currentCommand = newCommand;
        connection.executeCommand(newCommand);
    }

    /**
     * Flag to indicate that are potentially more results (ResultSets, update counts, or errors) to be processed in the response.
     */
    boolean moreResults = false;

    /**
     * The statement's current result set.
     */
    SQLServerResultSet resultSet;

    /**
     * The number of opened result sets in the statement.
     */
    int resultSetCount = 0;

    /**
     * Increment opened result set counter
     */
    synchronized void incrResultSetCount() {
        resultSetCount++;
    }

    /**
     * decrement opened result set counter
     */
    synchronized void decrResultSetCount() {
        resultSetCount--;
        assert resultSetCount >= 0;

        // close statement if no more result sets opened
        if (isCloseOnCompletion && !(EXECUTE_BATCH == executeMethod && moreResults) && resultSetCount == 0) {
            closeInternal();
        }
    }

    /**
     * The statement execution method (executeQuery(), executeUpdate(), execute(), or executeBatch()) that was used to execute the statement.
     */
    static final int EXECUTE_NOT_SET = 0;
    static final int EXECUTE_QUERY = 1;
    static final int EXECUTE_UPDATE = 2;
    static final int EXECUTE = 3;
    static final int EXECUTE_BATCH = 4;
    static final int EXECUTE_QUERY_INTERNAL = 5;
    int executeMethod = EXECUTE_NOT_SET;

    /**
     * The update count returned from an update.
     */
    long updateCount = -1;

    /**
     * The status of escape processing.
     */
    boolean escapeProcessing;

    /** Limit for the maximum number of rows in a ResultSet */
    int maxRows = 0; // default: 0 --> no limit

    /** Limit for the size of data (in bytes) returned for any column value */
    int maxFieldSize = 0; // default: 0 --> no limit

    /**
     * The user's specified result set concurrency.
     */
    int resultSetConcurrency;

    /**
     * The app result set type. This is the value passed to the statement's constructor (or inferred by default) when the statement was created.
     * ResultSet.getType() returns this value. It may differ from the SQL Server result set type (see below). Namely, an app result set type of
     * TYPE_FORWARD_ONLY will have an SQL Server result set type of TYPE_SS_DIRECT_FORWARD_ONLY or TYPE_SS_SERVER_CURSOR_FORWARD_ONLY depending on the
     * value of the selectMethod connection property.
     *
     * Possible values of the app result set type are:
     *
     * TYPE_FORWARD_ONLY TYPE_SCROLL_INSENSITIVE TYPE_SCROLL_SENSITIVE TYPE_SS_DIRECT_FORWARD_ONLY TYPE_SS_SERVER_CURSOR_FORWARD_ONLY
     * TYPE_SS_SCROLL_DYNAMIC TYPE_SS_SCROLL_KEYSET TYPE_SS_SCROLL_STATIC
     */
    int appResultSetType;

    /**
     * The SQL Server result set type. This is the value used everywhere EXCEPT ResultSet.getType(). This value may or may not be the same as the app
     * result set type (above).
     *
     * Possible values of the SQL Server result set type are:
     *
     * TYPE_SS_DIRECT_FORWARD_ONLY TYPE_SS_SERVER_CURSOR_FORWARD_ONLY TYPE_SS_SCROLL_DYNAMIC TYPE_SS_SCROLL_KEYSET TYPE_SS_SCROLL_STATIC
     */
    int resultSetType;

    final int getSQLResultSetType() {
        return resultSetType;
    }

    final int getCursorType() {
        return getResultSetScrollOpt() & ~TDS.SCROLLOPT_PARAMETERIZED_STMT;
    }

    /**
     * Indicates whether to request a server cursor when executing this statement.
     *
     * Executing a statement with execute() or executeQuery() requests a server cursor in all scrollability and updatability combinations except
     * direct forward-only, read-only.
     *
     * Note that when execution requests a server cursor (i.e. this method returns true), there is no guarantee that SQL Server returns one. The
     * variable executedSqlDirectly indicates whether SQL Server executed the query with a cursor or not.
     *
     * @return true if statement execution requests a server cursor, false otherwise.
     */
    final boolean isCursorable(int executeMethod) {
        return resultSetType != SQLServerResultSet.TYPE_SS_DIRECT_FORWARD_ONLY && (EXECUTE == executeMethod || EXECUTE_QUERY == executeMethod);
    }

    /**
     * Indicates whether SQL Server executed this statement with a cursor or not.
     *
     * When trying to execute a cursor-unfriendly statement with a server cursor, SQL Server may choose to execute the statement directly (i.e. as if
     * no server cursor had been requested) rather than fail to execute the statement at all. We need to know when this happens so that if no rows are
     * returned, we can tell whether the result is an empty result set or a cursored result set with rows to be fetched later.
     */
    boolean executedSqlDirectly = false;

    /**
     * Indicates whether OUT parameters (cursor ID and row count) from cursorized execution of this statement are expected in the response.
     *
     * In most cases, except for severe errors, cursor OUT parameters are returned whenever a cursor is requested for statement execution. Even if SQL
     * Server does not cursorize the statement as requested, these values are still present in the response and must be processed, even though their
     * values are meaningless in that case.
     */
    boolean expectCursorOutParams;

    class StmtExecOutParamHandler extends TDSTokenHandler {
        StmtExecOutParamHandler() {
            super("StmtExecOutParamHandler");
        }

        boolean onRetStatus(TDSReader tdsReader) throws SQLServerException {
            (new StreamRetStatus()).setFromTDS(tdsReader);
            return true;
        }

        boolean onRetValue(TDSReader tdsReader) throws SQLServerException {
            if (expectCursorOutParams) {
                Parameter param = new Parameter(Util.shouldHonorAEForParameters(stmtColumnEncriptionSetting, connection));

                // Read the cursor ID
                param.skipRetValStatus(tdsReader);
                serverCursorId = param.getInt(tdsReader);
                param.skipValue(tdsReader, true);

                param = new Parameter(Util.shouldHonorAEForParameters(stmtColumnEncriptionSetting, connection));
                // Read the row count (-1 means unknown)
                param.skipRetValStatus(tdsReader);
                if (-1 == (serverCursorRowCount = param.getInt(tdsReader)))
                    serverCursorRowCount = SQLServerResultSet.UNKNOWN_ROW_COUNT;
                param.skipValue(tdsReader, true);

                // We now have everything we need to build the result set.
                expectCursorOutParams = false;
                return true;
            }

            return false;
        }

        boolean onDone(TDSReader tdsReader) throws SQLServerException {
            return false;
        }
    }

    /**
     * The cursor name.
     */
    String cursorName;

    /**
     * The user's specified fetch size. Only used for server side result sets. Client side cursors read all rows.
     */
    int nFetchSize;
    int defaultFetchSize;

    /**
     * The users specified result set fetch direction
     */
    int nFetchDirection;

    /**
     * True is the statment is closed
     */
    boolean bIsClosed;

    /**
     * True if the user requested to driver to generate insert keys
     */
    boolean bRequestedGeneratedKeys;

    /**
     * The result set if auto generated keys were requested.
     */
    private ResultSet autoGeneratedKeys;

    /**
     * The array of objects in a batched call. Applicable to statements and prepared statements When the iterativeBatching property is turned on.
     */
    /** The buffer that accumulates batchable statements */
    private final ArrayList<String> batchStatementBuffer = new ArrayList<>();

    /** logging init at the construction */
    static final private java.util.logging.Logger stmtlogger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerStatement");

    /** The statement's id for logging info */
    public String toString() {
        return traceID;
    }

    // Internal function used in tracing
    String getClassNameInternal() {
        return "SQLServerStatement";
    }

    /** Generate the statement's logging ID */
    private static final AtomicInteger lastStatementID = new AtomicInteger(0);

    private static int nextStatementID() {
        return lastStatementID.incrementAndGet();
    }

    /**
     * The regular statement constructor
     *
     * @param con
     *            The statements connections.
     * @param nType
     *            The statement type.
     * @param nConcur
     *            The statement concurrency.
     * @param stmtColEncSetting
     *            The statement column encryption setting.
     * @exception SQLServerException
     *                The statement could not be created.
     */
    SQLServerStatement(SQLServerConnection con,
            int nType,
            int nConcur,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting)

            throws SQLServerException {
        // Return a string representation of this statement's unqualified class name
        // (e.g. "SQLServerStatement" or "SQLServerPreparedStatement"),
        // its unique ID, and its parent connection.
        int statementID = nextStatementID();
        String classN = getClassNameInternal();
        traceID = classN + ":" + statementID;
        loggingClassName = "com.microsoft.sqlserver.jdbc." + classN + ":" + statementID;

        stmtPoolable = false;
        connection = con;
        bIsClosed = false;
        final int nTypes = 5;

        // Validate result set type ...
        if (ResultSet.TYPE_FORWARD_ONLY != nType && ResultSet.TYPE_SCROLL_SENSITIVE != nType && ResultSet.TYPE_SCROLL_INSENSITIVE != nType
                && SQLServerResultSet.TYPE_SS_DIRECT_FORWARD_ONLY != nType && SQLServerResultSet.TYPE_SS_SERVER_CURSOR_FORWARD_ONLY != nType
                && SQLServerResultSet.TYPE_SS_SCROLL_DYNAMIC != nType && SQLServerResultSet.TYPE_SS_SCROLL_KEYSET != nType
                && SQLServerResultSet.TYPE_SS_SCROLL_STATIC != nType) {
            SQLServerException.makeFromDriverError(connection, this, SQLServerException.getErrString("R_unsupportedCursor"), null, true);
        }

        // ... and concurrency
        if (ResultSet.CONCUR_READ_ONLY != nConcur && ResultSet.CONCUR_UPDATABLE != nConcur && SQLServerResultSet.CONCUR_SS_SCROLL_LOCKS != nConcur
                && SQLServerResultSet.CONCUR_SS_OPTIMISTIC_CC != nConcur && SQLServerResultSet.CONCUR_SS_OPTIMISTIC_CCVAL != nConcur) {
            SQLServerException.makeFromDriverError(connection, this, SQLServerException.getErrString("R_unsupportedConcurrency"), null, true);
        }

        if (null == stmtColEncSetting) {
            SQLServerException.makeFromDriverError(connection, this, SQLServerException.getErrString("R_unsupportedStmtColEncSetting"), null, true);
        }

        stmtColumnEncriptionSetting = stmtColEncSetting;

        resultSetConcurrency = nConcur;

        // App result set type is always whatever was used to create the statement.
        // This value will be returned by ResultSet.getType() calls.
        appResultSetType = nType;

        // SQL Server result set type is used everwhere other than ResultSet.getType() and
        // may need to be inferred based on the app result set type and the value of
        // the selectMethod connection property.
        if (ResultSet.TYPE_FORWARD_ONLY == nType) {
            if (ResultSet.CONCUR_READ_ONLY == nConcur) {
                // Check selectMethod and set to TYPE_SS_DIRECT_FORWARD_ONLY or
                // TYPE_SS_SERVER_CURSOR_FORWARD_ONLY accordingly.
                String selectMethod = con.getSelectMethod();
                resultSetType = (null == selectMethod || !selectMethod.equals("cursor")) ? SQLServerResultSet.TYPE_SS_DIRECT_FORWARD_ONLY : // Default
                                                                                                                                            // forward-only,
                                                                                                                                            // read-only
                                                                                                                                            // cursor
                                                                                                                                            // type
                        SQLServerResultSet.TYPE_SS_SERVER_CURSOR_FORWARD_ONLY;
            }
            else {
                resultSetType = SQLServerResultSet.TYPE_SS_SERVER_CURSOR_FORWARD_ONLY;
            }
        }
        else if (ResultSet.TYPE_SCROLL_INSENSITIVE == nType) {
            resultSetType = SQLServerResultSet.TYPE_SS_SCROLL_STATIC;
        }
        else if (ResultSet.TYPE_SCROLL_SENSITIVE == nType) {
            resultSetType = SQLServerResultSet.TYPE_SS_SCROLL_KEYSET;
        }
        else // App specified one of the SQL Server types
        {
            resultSetType = nType;
        }

        // Figure out default fetch direction
        nFetchDirection = (SQLServerResultSet.TYPE_SS_DIRECT_FORWARD_ONLY == resultSetType
                || SQLServerResultSet.TYPE_SS_SERVER_CURSOR_FORWARD_ONLY == resultSetType) ? ResultSet.FETCH_FORWARD : ResultSet.FETCH_UNKNOWN;

        // Figure out fetch size:
        //
        // Too many scroll locks adversely impact concurrency on the server
        // and thus system performance, so default to a low value when using
        // scroll locks. Otherwise, use a larger fetch size.
        nFetchSize = (SQLServerResultSet.CONCUR_SS_SCROLL_LOCKS == resultSetConcurrency) ? 8 : 128;

        // Save off the default fetch size so it can be restored by setFetchSize(0).
        defaultFetchSize = nFetchSize;

        // Check compatibility between the result set type and concurrency.
        // Against a Yukon server, certain scrollability values are incompatible
        // with all but read only concurrency. Against a Shiloh server, such
        // combinations cause the cursor to be silently "upgraded" to one that
        // works. We enforce the more restrictive behavior of the two here.
        if (ResultSet.CONCUR_READ_ONLY != nConcur
                && (SQLServerResultSet.TYPE_SS_DIRECT_FORWARD_ONLY == resultSetType || SQLServerResultSet.TYPE_SS_SCROLL_STATIC == resultSetType)) {
            SQLServerException.makeFromDriverError(connection, this, SQLServerException.getErrString("R_unsupportedCursorAndConcurrency"), null,
                    true);
        }

        // All result set types other than firehose (SQL Server default) use server side cursors.
        setResponseBuffering(connection.getResponseBuffering());

        setDefaultQueryTimeout();

        if (stmtlogger.isLoggable(java.util.logging.Level.FINER)) {
            stmtlogger.finer("Properties for " + toString() + ":" + " Result type:" + appResultSetType + " (" + resultSetType + ")" + " Concurrency:"
                    + resultSetConcurrency + " Fetchsize:" + nFetchSize + " bIsClosed:" + bIsClosed + " useLastUpdateCount:"
                    + connection.useLastUpdateCount());
        }

        if (stmtlogger.isLoggable(java.util.logging.Level.FINE)) {
            stmtlogger.fine(toString() + " created by (" + connection.toString() + ")");
        }
    }

    // add query timeout to statement
    private void setDefaultQueryTimeout() {
        int queryTimeoutSeconds = this.connection.getQueryTimeoutSeconds();
        if (queryTimeoutSeconds > 0) {
            this.queryTimeout = queryTimeoutSeconds;
        }
    }

    final java.util.logging.Logger getStatementLogger() {
        return stmtlogger;
    }

    /**
     * Standard handler for unsupported data types.
     */
    /* L0 */ final void NotImplemented() throws SQLServerException {
        SQLServerException.makeFromDriverError(connection, this, SQLServerException.getErrString("R_notSupported"), null, false);
    }

    /**
     * Close the statement.
     *
     * Note that the public close() method performs all of the cleanup work through this internal method which cannot throw any exceptions. This is
     * done deliberately to ensure that ALL of the object's client-side and server-side state is cleaned up as best as possible, even under conditions
     * which would normally result in exceptions being thrown.
     */
    void closeInternal() {
        // Regardless what happens when cleaning up,
        // the statement is considered closed.
        assert !bIsClosed;

        discardLastExecutionResults();

        bIsClosed = true;
        autoGeneratedKeys = null;
        sqlWarnings = null;
        inOutParam = null;
    }

    public void close() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "close");

        if (!bIsClosed)
            closeInternal();

        loggerExternal.exiting(getClassNameLogging(), "close");
    }

    public void closeOnCompletion() throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "closeOnCompletion");

        checkClosed();

        // enable closeOnCompletion feature
        isCloseOnCompletion = true;

        loggerExternal.exiting(getClassNameLogging(), "closeOnCompletion");
    }

    /**
     * Execute a result set query
     * 
     * @param sql
     *            the SQL query
     * @exception SQLServerException
     *                The SQL was invalid.
     * @return a JDBC result set.
     */
    public java.sql.ResultSet executeQuery(String sql) throws SQLServerException, SQLTimeoutException {
        loggerExternal.entering(getClassNameLogging(), "executeQuery", sql);
        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
    	executeStatement(new StmtExecCmd(this, sql, EXECUTE_QUERY, NO_GENERATED_KEYS));
        loggerExternal.exiting(getClassNameLogging(), "executeQuery", resultSet);
        return resultSet;
    }

    final SQLServerResultSet executeQueryInternal(String sql) throws SQLServerException, SQLTimeoutException {
        checkClosed();
        executeStatement(new StmtExecCmd(this, sql, EXECUTE_QUERY_INTERNAL, NO_GENERATED_KEYS));
        return resultSet;
    }

    /**
     * Execute a JDBC update
     * 
     * @param sql
     *            the SQL query
     * @exception SQLServerException
     *                The SQL was invalid.
     * @return The number of rows updated.
     */
    public int executeUpdate(String sql) throws SQLServerException, SQLTimeoutException {
        loggerExternal.entering(getClassNameLogging(), "executeUpdate", sql);
        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
        executeStatement(new StmtExecCmd(this, sql, EXECUTE_UPDATE, NO_GENERATED_KEYS));

        // this shouldn't happen, caller probably meant to call executeLargeUpdate
        if (updateCount < Integer.MIN_VALUE || updateCount > Integer.MAX_VALUE)
            SQLServerException.makeFromDriverError(connection, this, SQLServerException.getErrString("R_updateCountOutofRange"), null, true);

        loggerExternal.exiting(getClassNameLogging(), "executeUpdate", updateCount);

        return (int) updateCount;
    }

    /**
     * Execute a JDBC update
     * 
     * @param sql
     *            the SQL query
     * @exception SQLServerException
     *                The SQL was invalid.
     * @return The number of rows updated.
     */
    public long executeLargeUpdate(String sql) throws SQLServerException, SQLTimeoutException {
        DriverJDBCVersion.checkSupportsJDBC42();

        loggerExternal.entering(getClassNameLogging(), "executeLargeUpdate", sql);
        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
        executeStatement(new StmtExecCmd(this, sql, EXECUTE_UPDATE, NO_GENERATED_KEYS));

        loggerExternal.exiting(getClassNameLogging(), "executeLargeUpdate", updateCount);
        return updateCount;
    }

    /**
     * Execute an update or query.
     *
     * @param sql
     *            The update or query.
     * @exception SQLServerException
     *                The SQL statement was not valid.
     * @return True if a result set was generated.
     */
    public boolean execute(String sql) throws SQLServerException, SQLTimeoutException {
        loggerExternal.entering(getClassNameLogging(), "execute", sql);
        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
        executeStatement(new StmtExecCmd(this, sql, EXECUTE, NO_GENERATED_KEYS));
        loggerExternal.exiting(getClassNameLogging(), "execute", null != resultSet);
        return null != resultSet;
    }

    private final class StmtExecCmd extends TDSCommand {
        final SQLServerStatement stmt;
        final String sql;
        final int executeMethod;
        final int autoGeneratedKeys;

        StmtExecCmd(SQLServerStatement stmt,
                String sql,
                int executeMethod,
                int autoGeneratedKeys) {
            super(stmt.toString() + " executeXXX", stmt.queryTimeout);
            this.stmt = stmt;
            this.sql = sql;
            this.executeMethod = executeMethod;
            this.autoGeneratedKeys = autoGeneratedKeys;
        }

        final boolean doExecute() throws SQLServerException {
            stmt.doExecuteStatement(this);
            return false;
        }

        final void processResponse(TDSReader tdsReader) throws SQLServerException {
            ensureExecuteResultsReader(tdsReader);
            processExecuteResults();
        }
    }

    private String ensureSQLSyntax(String sql) throws SQLServerException {
        if (sql.indexOf(LEFT_CURLY_BRACKET) >= 0) {

            Sha1HashKey cacheKey = new Sha1HashKey(sql);

            // Check for cached SQL metadata.
            ParsedSQLCacheItem cacheItem = getCachedParsedSQL(cacheKey);
            if (null == cacheItem)
                cacheItem = parseAndCacheSQL(cacheKey, sql);

            // Retrieve from cache item.
            procedureName = cacheItem.procedureName;
            return cacheItem.processedSQL;
        }

        return sql;
    }

    void startResults() {
        moreResults = true;
    }

    final void setMaxRowsAndMaxFieldSize() throws SQLServerException {
        if (EXECUTE_QUERY == executeMethod || EXECUTE == executeMethod) {
            connection.setMaxRows(maxRows);
            connection.setMaxFieldSize(maxFieldSize);
        }
        else {
            assert EXECUTE_UPDATE == executeMethod || EXECUTE_BATCH == executeMethod || EXECUTE_QUERY_INTERNAL == executeMethod;

            // If we are executing via any of the above methods then make sure any
            // previous maxRows limitation on the connection is removed.
            connection.setMaxRows(0);
        }
    }

    final void doExecuteStatement(StmtExecCmd execCmd) throws SQLServerException {
        resetForReexecute();

        // Set this command as the current command
        executeMethod = execCmd.executeMethod;

        // Apps can use JDBC call syntax to call unparameterized stored procedures
        // through regular Statement objects. We need to ensure that any such JDBC
        // call syntax is rewritten here as SQL exec syntax.
        String sql = ensureSQLSyntax(execCmd.sql);

        // If this request might be a query (as opposed to an update) then make
        // sure we set the max number of rows and max field size for any ResultSet
        // that may be returned.
        //
        // If the app uses Statement.execute to execute an UPDATE or DELETE statement
        // and has called Statement.setMaxRows to limit the number of rows from an
        // earlier query, then the number of rows updated/deleted will be limited as
        // well.
        //
        // Note: similar logic in SQLServerPreparedStatement.doExecutePreparedStatement
        setMaxRowsAndMaxFieldSize();

        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        if (isCursorable(executeMethod) && isSelect(sql)) {
            if (stmtlogger.isLoggable(java.util.logging.Level.FINE))
                stmtlogger.fine(toString() + " Executing server side cursor " + sql);

            doExecuteCursored(execCmd, sql);
        }
        else // Non-cursored execution (includes EXECUTE_QUERY_INTERNAL)
        {
            executedSqlDirectly = true;
            expectCursorOutParams = false;

            TDSWriter tdsWriter = execCmd.startRequest(TDS.PKT_QUERY);

            tdsWriter.writeString(sql);

            // If this is an INSERT statement and generated keys were requested
            // then add on the query to return them.
            if (RETURN_GENERATED_KEYS == execCmd.autoGeneratedKeys && (EXECUTE_UPDATE == executeMethod || EXECUTE == executeMethod)
                    && sql.trim().toUpperCase().startsWith("INSERT")) {
                tdsWriter.writeString(identityQuery);
            }

            if (stmtlogger.isLoggable(java.util.logging.Level.FINE))
                stmtlogger.fine(toString() + " Executing (not server cursor) " + sql);

            // Start the response
            ensureExecuteResultsReader(execCmd.startResponse(isResponseBufferingAdaptive));
            startResults();
            getNextResult();
        }

        // If execution produced no result set, then throw an exception if executeQuery() was used.
        if (null == resultSet) {
            if (EXECUTE_QUERY == executeMethod) {
                SQLServerException.makeFromDriverError(connection, this, SQLServerException.getErrString("R_noResultset"), null, true);
            }
        }
        // Otherwise, if execution produced a result set, then throw an exception
        // if executeUpdate() or executeBatch() was used.
        else {
            if (EXECUTE_UPDATE == executeMethod || EXECUTE_BATCH == executeMethod) {
                SQLServerException.makeFromDriverError(connection, this, SQLServerException.getErrString("R_resultsetGeneratedForUpdate"), null,
                        false);
            }
        }
    }

    private final class StmtBatchExecCmd extends TDSCommand {
        final SQLServerStatement stmt;

        StmtBatchExecCmd(SQLServerStatement stmt) {
            super(stmt.toString() + " executeBatch", stmt.queryTimeout);
            this.stmt = stmt;
        }

        final boolean doExecute() throws SQLServerException {
            stmt.doExecuteStatementBatch(this);
            return false;
        }

        final void processResponse(TDSReader tdsReader) throws SQLServerException {
            ensureExecuteResultsReader(tdsReader);
            processExecuteResults();
        }
    }

    private void doExecuteStatementBatch(StmtBatchExecCmd execCmd) throws SQLServerException {
        resetForReexecute();

        // Make sure any previous maxRows limitation on the connection is removed.
        connection.setMaxRows(0);

        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }

        // Batch execution is always non-cursored
        executeMethod = EXECUTE_BATCH;
        executedSqlDirectly = true;
        expectCursorOutParams = false;

        TDSWriter tdsWriter = execCmd.startRequest(TDS.PKT_QUERY);

        // Write the concatenated batch of statements, delimited by semicolons
        ListIterator<String> batchIter = batchStatementBuffer.listIterator();
        tdsWriter.writeString(batchIter.next());
        while (batchIter.hasNext()) {
            tdsWriter.writeString(" ; ");
            tdsWriter.writeString(batchIter.next());
        }

        // Start the response
        ensureExecuteResultsReader(execCmd.startResponse(isResponseBufferingAdaptive));
        startResults();
        getNextResult();

        // If execution produced a result set, then throw an exception
        if (null != resultSet) {
            SQLServerException.makeFromDriverError(connection, this, SQLServerException.getErrString("R_resultsetGeneratedForUpdate"), null, false);
        }
    }

    /**
     * Reset the state to get the statement for reexecute callable statement overrides this.
     */
    final void resetForReexecute() throws SQLServerException {
        ensureExecuteResultsReader(null);
        autoGeneratedKeys = null;
        updateCount = -1;
        sqlWarnings = null;
        executedSqlDirectly = false;
        startResults();
    }

    /**
     * Determine if the SQL is a SELECT.
     * 
     * @param sql
     *            The statment SQL.
     * @return True is the statement is a select.
     */
    /* L0 */ final boolean isSelect(String sql) throws SQLServerException {
        checkClosed();
        // Used to check just the first letter which would cause
        // "Set" commands to return true...
        String temp = sql.trim();
        char c = temp.charAt(0);
        if (c != 's' && c != 'S')
            return false;
        return temp.substring(0, 6).equalsIgnoreCase("select");
    }

    /**
     * Replace a JDBC parameter marker with the parameter's string value
     * 
     * @param str
     *            the parameter syntax
     * @param marker
     *            the parameter marker
     * @param replaceStr
     *            the param value
     * @return String
     */
    /* L0 */ static String replaceParameterWithString(String str,
            char marker,
            String replaceStr) {
        int index = 0;
        while ((index = str.indexOf("" + marker)) >= 0) {
            str = str.substring(0, index) + replaceStr + str.substring(index + 1, str.length());
        }
        return str;
    }

    /**
     * Set a JDBC parameter to null. (Used only when processing LOB column sources.)
     * 
     * @param sql
     *            the parameter syntax
     * @return the result
     */
    /* L0 */ static String replaceMarkerWithNull(String sql) {
        if (!sql.contains("'")) {
            String retStr = replaceParameterWithString(sql, '?', "null");
            return retStr;
        }
        else {
            StringTokenizer st = new StringTokenizer(sql, "'", true);
            boolean beforeColon = true;
            String retSql = "";
            while (st.hasMoreTokens()) {
                String str = st.nextToken();
                if (str.equals("'")) {
                    retSql += "'";
                    beforeColon = !beforeColon;
                    continue;
                }
                if (beforeColon) {
                    String repStr = replaceParameterWithString(str, '?', "null");
                    retSql += repStr;
                    continue;
                }
                else {
                    retSql += str;
                    continue;
                }
            }
            return retSql;
        }
    }

    /* L0 */ void checkClosed() throws SQLServerException {
        // Check the connection first so that Statement methods
        // throw a "Connection closed" exception if the reason
        // that the statement was closed is because the connection
        // was closed.
        connection.checkClosed();
        if (bIsClosed) {
            SQLServerException.makeFromDriverError(connection, this, SQLServerException.getErrString("R_statementIsClosed"), null, false);
        }
    }

    /* ---------------- JDBC API methods ------------------ */

    /* L0 */ public final int getMaxFieldSize() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getMaxFieldSize");
        checkClosed();
        loggerExternal.exiting(getClassNameLogging(), "getMaxFieldSize", maxFieldSize);
        return maxFieldSize;
    }

    /* L0 */ public final void setMaxFieldSize(int max) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "setMaxFieldSize", max);
        checkClosed();
        if (max < 0) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidLength"));
            Object[] msgArgs = {max};
            SQLServerException.makeFromDriverError(connection, this, form.format(msgArgs), null, true);
        }
        maxFieldSize = max;
        loggerExternal.exiting(getClassNameLogging(), "setMaxFieldSize");
    }

    /* L0 */ public final int getMaxRows() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getMaxRows");
        checkClosed();
        loggerExternal.exiting(getClassNameLogging(), "getMaxRows", maxRows);
        return maxRows;
    }

    public final long getLargeMaxRows() throws SQLServerException {
        DriverJDBCVersion.checkSupportsJDBC42();

        loggerExternal.entering(getClassNameLogging(), "getLargeMaxRows");

        // SQL Server only supports integer limits for setting max rows.
        // So, getLargeMaxRows() and getMaxRows() will return the same value.
        loggerExternal.exiting(getClassNameLogging(), "getLargeMaxRows", (long) maxRows);

        return (long) getMaxRows();
    }

    /* L0 */ public final void setMaxRows(int max) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setMaxRows", max);
        checkClosed();
        if (max < 0) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidRowcount"));
            Object[] msgArgs = {max};
            SQLServerException.makeFromDriverError(connection, this, form.format(msgArgs), null, true);
        }

        // DYNAMIC scrollable server cursors have no notion of a keyset. That is, the number of rows in
        // the ResultSet can change dynamically as the app scrolls around. So there is no way to support
        // maxRows with DYNAMIC scrollable cursors.
        if (SQLServerResultSet.TYPE_SS_SCROLL_DYNAMIC != resultSetType)
            maxRows = max;
        loggerExternal.exiting(getClassNameLogging(), "setMaxRows");
    }

    public final void setLargeMaxRows(long max) throws SQLServerException {
        DriverJDBCVersion.checkSupportsJDBC42();

        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setLargeMaxRows", max);

        // SQL server only supports integer limits for setting max rows.
        // If <max> is bigger than integer limits then throw an exception, otherwise call setMaxRows(int)
        if (max > Integer.MAX_VALUE) {
            throw new UnsupportedOperationException(SQLServerException.getErrString("R_invalidMaxRows"));
        }
        setMaxRows((int) max);
        loggerExternal.exiting(getClassNameLogging(), "setLargeMaxRows");
    }

    /* L0 */ public final void setEscapeProcessing(boolean enable) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setEscapeProcessing", enable);
        checkClosed();
        escapeProcessing = enable;
        loggerExternal.exiting(getClassNameLogging(), "setEscapeProcessing");
    }

    /* L0 */ public final int getQueryTimeout() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getQueryTimeout");
        checkClosed();
        loggerExternal.exiting(getClassNameLogging(), "getQueryTimeout", queryTimeout);
        return queryTimeout;
    }

    /* L0 */ public final void setQueryTimeout(int seconds) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "setQueryTimeout", seconds);
        checkClosed();
        if (seconds < 0) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidQueryTimeOutValue"));
            Object[] msgArgs = {seconds};
            SQLServerException.makeFromDriverError(connection, this, form.format(msgArgs), null, true);
        }
        queryTimeout = seconds;
        loggerExternal.exiting(getClassNameLogging(), "setQueryTimeout");
    }

    public final void cancel() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "cancel");
        checkClosed();

        // Cancel the currently executing statement.
        if (null != currentCommand)
            currentCommand.interrupt(SQLServerException.getErrString("R_queryCancelled"));
        loggerExternal.exiting(getClassNameLogging(), "cancel");
    }

    Vector<SQLWarning> sqlWarnings; // the SQL warnings chain

    /* L0 */ public final SQLWarning getWarnings() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getWarnings");
        checkClosed();
        if (sqlWarnings == null)
            return null;
        SQLWarning warn = sqlWarnings.elementAt(0);
        loggerExternal.exiting(getClassNameLogging(), "getWarnings", warn);
        return warn;
    }

    /* L0 */ public final void clearWarnings() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "clearWarnings");
        checkClosed();
        sqlWarnings = null;
        loggerExternal.exiting(getClassNameLogging(), "clearWarnings");
    }

    /* L0 */ public final void setCursorName(String name) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "setCursorName", name);
        checkClosed();
        cursorName = name;
        loggerExternal.exiting(getClassNameLogging(), "setCursorName");
    }

    final String getCursorName() {
        return cursorName;
    }

    public final java.sql.ResultSet getResultSet() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getResultSet");
        checkClosed();
        loggerExternal.exiting(getClassNameLogging(), "getResultSet", resultSet);
        return resultSet;
    }

    /* L0 */ public final int getUpdateCount() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getUpdateCount");

        checkClosed();

        // this shouldn't happen, caller probably meant to call getLargeUpdateCount
        if (updateCount < Integer.MIN_VALUE || updateCount > Integer.MAX_VALUE)
            SQLServerException.makeFromDriverError(connection, this, SQLServerException.getErrString("R_updateCountOutofRange"), null, true);

        loggerExternal.exiting(getClassNameLogging(), "getUpdateCount", updateCount);

        return (int) updateCount;
    }

    public final long getLargeUpdateCount() throws SQLServerException {
        DriverJDBCVersion.checkSupportsJDBC42();

        loggerExternal.entering(getClassNameLogging(), "getUpdateCount");
        checkClosed();
        loggerExternal.exiting(getClassNameLogging(), "getUpdateCount", updateCount);
        return updateCount;
    }

    final void ensureExecuteResultsReader(TDSReader tdsReader) {
        this.tdsReader = tdsReader;
    }

    final void processExecuteResults() throws SQLServerException {
        if (wasExecuted()) {
            processBatch();
            TDSParser.parse(resultsReader(), "batch completion");
            ensureExecuteResultsReader(null);
        }
    }

    void processBatch() throws SQLServerException {
        processResults();
    }

    final void processResults() throws SQLServerException {
        SQLServerException interruptException = null;

        while (moreResults) {
            // Get the next result
            try {
                getNextResult();
            }
            catch (SQLServerException e) {
                // If an exception is thrown while processing the results
                // then decide what to do with it:
                if (moreResults) {
                    // Silently discard database errors and continue processing the remaining results
                    if (SQLServerException.DRIVER_ERROR_FROM_DATABASE == e.getDriverErrorCode()) {
                        if (stmtlogger.isLoggable(java.util.logging.Level.FINEST)) {
                            stmtlogger.finest(this + " ignoring database error: " + e.getErrorCode() + " " + e.getMessage());
                        }

                        continue;
                    }

                    // If statement execution was canceled then continue processing the
                    // remaining results before throwing the "statement canceled" exception.
                    if (e.getSQLState().equals(SQLState.STATEMENT_CANCELED.getSQLStateCode())) {
                        interruptException = e;
                        continue;
                    }
                }

                // Propagate up more serious exceptions to the caller
                moreResults = false;
                throw e;
            }
        }

        clearLastResult();

        if (null != interruptException)
            throw interruptException;
    }

    /**
     * Check for more results in the TDS stream
     *
     * @return true if the next result is a ResultSet object; false if it is an integer (indicating that it is an update count or there are no more
     *         results).
     */
    /* L0 */ public final boolean getMoreResults() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getMoreResults");
        checkClosed();

        // Get the next result, whatever it is (ResultSet or update count).
        // Don't just return the value from the getNextResult() call, however.
        // The getMoreResults method has a subtle spec for its return value (see above).
        getNextResult();
        loggerExternal.exiting(getClassNameLogging(), "getMoreResults", null != resultSet);
        return null != resultSet;
    }

    /**
     * Clears the most recent result (ResultSet or update count) from this statement.
     *
     * This method is used to clean up before moving to the next result and after processing the last result.
     *
     * Note that errors closing the ResultSet (if the last result is a ResultSet) are caught, logged, and ignored. The reason for this is that this
     * method is only for cleanup for when the app fails to do the cleanup itself (for example by leaving a ResultSet open when closing the
     * statement). If the app wants to be able to handle errors from closing the current result set, then it should close that ResultSet itself before
     * closing the statement, moving to the next result, or whatever. This is the recommended practice with JDBC.
     */
    final void clearLastResult() {
        // Clear any update count result
        updateCount = -1;

        // Clear any ResultSet result
        if (null != resultSet) {
            // Try closing the ResultSet. If closing fails, log the error and ignore it.
            // Then just drop our reference to the ResultSet object -- we're done with
            // it anyhow.
            try {
                resultSet.close();
            }
            catch (SQLServerException e) {
                stmtlogger.finest(this + " clearing last result; ignored error closing ResultSet: " + e.getErrorCode() + " " + e.getMessage());
            }
            finally {
                resultSet = null;
            }
        }
    }

    /**
     * Get the next result in the TDS response token stream, which may be a result set, update count or exception.
     *
     * @return true if another result (ResultSet or update count) was available; false if there were no more results.
     */
    final boolean getNextResult() throws SQLServerException {
        /**
         * TDS response token stream handler used to locate the next result in the TDS response token stream.
         */
        final class NextResult extends TDSTokenHandler {
            private StreamDone stmtDoneToken = null;

            final boolean isUpdateCount() {
                return null != stmtDoneToken;
            }

            final long getUpdateCount() {
                return stmtDoneToken.getUpdateCount();
            }

            private boolean isResultSet = false;

            final boolean isResultSet() {
                return isResultSet;
            }

            private StreamRetStatus procedureRetStatToken = null;

            NextResult() {
                super("getNextResult");
            }

            boolean onColMetaData(TDSReader tdsReader) throws SQLServerException {
                // If we have an update count from a previous command that we haven't
                // acknowledged because we didn't know at the time whether it was
                // the undesired result from a trigger, we now know that it wasn't,
                // so return it.
                if (null != stmtDoneToken)
                    return false;

                // If we encountered an ERROR token before hitting this COLMETADATA token,
                // without any intervening DONE token (indicating an error result), then
                // act as if that had been the case and drop out to propagate the error
                // up as an SQLException.
                if (null != getDatabaseError())
                    return false;

                // Otherwise, column metadata indicates the start of a ResultSet
                isResultSet = true;
                return false;
            }

            boolean onDone(TDSReader tdsReader) throws SQLServerException {
                // Consume the done token and decide what to do with it...
                // Handling DONE/DONEPROC/DONEINPROC tokens is a little tricky...
                StreamDone doneToken = new StreamDone();
                doneToken.setFromTDS(tdsReader);

                // If the done token has the attention ack bit set, then record
                // it as the attention ack DONE token. We may or may not throw
                // an statement canceled/timed out exception later based on
                // whether we've seen this ack.
                if (doneToken.isAttnAck())
                    return false;

                // The meaning of other done tokens depends on several things ...
                //
                // A done token from a DML or DDL command normally contains an update
                // count that is usually returned as the next result. But processing
                // doesn't stop until we reach the end of the result, which may include
                // other tokens. And there are exceptions to that rule ...
                if (doneToken.cmdIsDMLOrDDL()) {
                    // If update counts are suppressed (i.e. SET NOCOUNT ON was used) then
                    // the DML/DDL update count is not valid, and this result should be skipped
                    // unless it's for a batch where it's ok to report a "done without count"
                    // status (Statement.SUCCESS_NO_INFO)
                    if (-1 == doneToken.getUpdateCount() && EXECUTE_BATCH != executeMethod)
                        return true;
                    
                    if ( -1 != doneToken.getUpdateCount() && EXECUTE_QUERY == executeMethod )
                        return true;

                    // Otherwise, the update count is valid. Now determine whether we should
                    // return it as a result...
                    stmtDoneToken = doneToken;

                    // Always return update counts from directly executed SQL statements.
                    // This encompases both batch execution in normal SQLServerStatement objects
                    // and compound statement execution, which are indistinguishable from one
                    // another as they are both just concatenated statements.
                    if (TDS.TDS_DONEINPROC != doneToken.getTokenType())
                        return false;

                    if (EXECUTE_BATCH != executeMethod) {
                        // Always return update counts from statements executed in stored procedure calls.
                        if (null != procedureName)
                            return false;

                        // Always return all update counts from statements executed through Statement.execute()
                        if (EXECUTE == executeMethod)
                            return false;

                        // Statement.executeUpdate() may or may not return this update count depending on the
                        // setting of the lastUpdateCount connection property:
                        //
                        // If lastUpdateCount=true (the default), then skip this update count and continue.
                        // Otherwise (lastUpdateCount=false), return this update count.
                        if (!connection.useLastUpdateCount())
                            return false;

                        // An update count from a statement that appears to have been executed in a stored
                        // procedure call, when no stored procedure was called, is assumed to be the result of
                        // a trigger firing. Skip it for now until/unless it is shown to be the last update
                        // count of a statement's execution.
                    }
                }

                // A done token without an update count usually just indicates the execution
                // of a SQL statement that isn't an INSERT, UPDATE, DELETE or DDL, and it should
                // be ignored. However, some of these done tokens have special meaning that we
                // need to consider...
                else {
                    // The final done token in the response always marks the end of the result,
                    // even if there is no update count.
                    if (doneToken.isFinal()) {
                        moreResults = false;
                        return false;
                    }

                    if (EXECUTE_BATCH == executeMethod) {
                        // The done token that marks the end of a batch always marks the end
                        // of the result, even if there is no update count.
                        if (TDS.TDS_DONEINPROC != doneToken.getTokenType() || doneToken.wasRPCInBatch()) {
                            moreResults = false;
                            return false;
                        }
                    }
                }

                // If the current command (whatever it was) produced an error then stop parsing and propagate it up.
                // In this case, the command is likely to be a RAISERROR, but it could be anything.
                if (doneToken.isError())
                    return false;

                // In all other cases, keep parsing
                return true;
            }

            boolean onRetStatus(TDSReader tdsReader) throws SQLServerException {
                // If this return status token marks the start of statement execution OUT parameters,
                // then consume those OUT parameters, setting server cursor ID, row count, and/or
                // the prepared statement handle from them.
                if (consumeExecOutParam(tdsReader)) {
                    moreResults = false;
                }

                // If this RETSTATUS token is not the one that begins the statement execution OUT
                // parameters, then it must be one from a stored procedure that was called by
                // the query itself. In that case, it should be ignored as it does not contribute
                // to any result. It may mark the start of application OUT parameters, however,
                // so remember that we've seen it so that we don't ignore the RETVALUE tokens that
                // may follow.
                else {
                    procedureRetStatToken = new StreamRetStatus();
                    procedureRetStatToken.setFromTDS(tdsReader);
                }

                return true;
            }

            boolean onRetValue(TDSReader tdsReader) throws SQLServerException {
                // We are only interested in return values that are statement OUT parameters,
                // in which case we need to stop parsing and let CallableStatement take over.
                // A RETVALUE token appearing in the execution results, but before any RETSTATUS
                // token, is a TEXTPTR return value that should be ignored.
                if (moreResults && null == procedureRetStatToken) {
                    Parameter p = new Parameter(Util.shouldHonorAEForParameters(stmtColumnEncriptionSetting, connection));
                    p.skipRetValStatus(tdsReader);
                    p.skipValue(tdsReader, true);
                    return true;
                }

                return false;
            }

            boolean onInfo(TDSReader tdsReader) throws SQLServerException {
                StreamInfo infoToken = new StreamInfo();
                infoToken.setFromTDS(tdsReader);

                // Under some circumstances the server cannot produce the cursored result set
                // that we requested, but produces a client-side (default) result set instead.
                // When this happens, there are no cursor ID or rowcount OUT parameters -- the
                // result set rows are returned directly as if this were a client-cursored
                // result set.
                //
                // ErrorNumber: 16954
                // ErrorSeverity: EX_USER
                // ErrorFormat: Executing SQL directly; no cursor.
                // ErrorCause: Server cursor is not supported on the specified SQL, falling back to default result set
                // ErrorCorrectiveAction: None required
                //
                if (16954 == infoToken.msg.getErrorNumber())
                    executedSqlDirectly = true;

                SQLWarning warning = new SQLWarning(infoToken.msg.getMessage(),
                        SQLServerException.generateStateCode(connection, infoToken.msg.getErrorNumber(), infoToken.msg.getErrorState()),
                        infoToken.msg.getErrorNumber());

                if (sqlWarnings == null) {
                    sqlWarnings = new Vector<>();
                }
                else {
                    int n = sqlWarnings.size();
                    SQLWarning w = sqlWarnings.elementAt(n - 1);
                    w.setNextWarning(warning);
                }
                sqlWarnings.add(warning);
                return true;
            }
        }

        // If the statement has no results yet, then return
        if (!wasExecuted()) {
            moreResults = false;
            return false;
        }

        // Clear out previous results
        clearLastResult();

        // If there are no more results, then we're done.
        // All we had to do was to close out the previous results.
        if (!moreResults)
            return false;

        // Figure out the next result.
        NextResult nextResult = new NextResult();
        TDSParser.parse(resultsReader(), nextResult);

        // Check for errors first.
        if (null != nextResult.getDatabaseError()) {
            SQLServerException.makeFromDatabaseError(connection, null, nextResult.getDatabaseError().getMessage(), nextResult.getDatabaseError(),
                    false);
        }

        // Not an error. Is it a result set?
        else if (nextResult.isResultSet()) {
            // Make sure SQLServerResultSet42 is used for 4.2 and above
            if (Util.use42Wrapper() || Util.use43Wrapper()) {
                resultSet = new SQLServerResultSet42(this);
            }
            else {
                resultSet = new SQLServerResultSet(this);
            }

            return true;
        }

        // Nope. Not an error or a result set. Maybe a result from a T-SQL statement?
        // That is, one of the following:
        // - a positive count of the number of rows affected (from INSERT, UPDATE, or DELETE),
        // - a zero indicating no rows affected, or the statement was DDL, or
        // - a -1 indicating the statement succeeded, but there is no update count
        // information available (translates to Statement.SUCCESS_NO_INFO in batch
        // update count arrays).
        else if (nextResult.isUpdateCount()) {
            updateCount = nextResult.getUpdateCount();
            return true;
        }

        // None of the above. Last chance here... Going into the parser above, we know
        // moreResults was initially true. If we come out with moreResults false, then
        // we hit a DONE token (either DONE (FINAL) or DONE (RPC in batch)) that indicates
        // that the batch succeeded overall, but that there is no information on individual
        // statements' update counts. This is similar to the last case above, except that
        // there is no update count. That is: we have a successful result (return true),
        // but we have no other information about it (updateCount = -1).
        updateCount = -1;
        if (!moreResults)
            return true;

        // Only way to get here (moreResults is still true, but no apparent results of any kind)
        // is if the TDSParser didn't actually parse _anything_. That is, we are at EOF in the
        // response. In that case, there truly are no more results. We're done.
        moreResults = false;
        return false;
    }

    /**
     * Consume the OUT parameter for the statement object itself.
     *
     * Normal Statement objects consume the server cursor OUT params when present. PreparedStatement and CallableStatement objects override this
     * method to consume the prepared statement handle as well.
     */
    boolean consumeExecOutParam(TDSReader tdsReader) throws SQLServerException {
        if (expectCursorOutParams) {
            TDSParser.parse(tdsReader, new StmtExecOutParamHandler());
            return true;
        }

        return false;
    }

    // --------------------------JDBC 2.0-----------------------------

    public final void setFetchDirection(int nDir) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setFetchDirection", nDir);
        checkClosed();
        if ((ResultSet.FETCH_FORWARD != nDir && ResultSet.FETCH_REVERSE != nDir && ResultSet.FETCH_UNKNOWN != nDir) ||

                (ResultSet.FETCH_FORWARD != nDir && (SQLServerResultSet.TYPE_SS_DIRECT_FORWARD_ONLY == resultSetType
                        || SQLServerResultSet.TYPE_SS_SERVER_CURSOR_FORWARD_ONLY == resultSetType))) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidFetchDirection"));
            Object[] msgArgs = {nDir};
            SQLServerException.makeFromDriverError(connection, this, form.format(msgArgs), null, false);
        }

        nFetchDirection = nDir;
        loggerExternal.exiting(getClassNameLogging(), "setFetchDirection");
    }

    public final int getFetchDirection() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getFetchDirection");
        checkClosed();
        loggerExternal.exiting(getClassNameLogging(), "getFetchDirection", nFetchDirection);
        return nFetchDirection;
    }

    /* L0 */ public final void setFetchSize(int rows) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setFetchSize", rows);
        checkClosed();
        if (rows < 0)
            SQLServerException.makeFromDriverError(connection, this, SQLServerException.getErrString("R_invalidFetchSize"), null, false);

        nFetchSize = (0 == rows) ? defaultFetchSize : rows;
        loggerExternal.exiting(getClassNameLogging(), "setFetchSize");
    }

    /* L0 */ public final int getFetchSize() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getFetchSize");
        checkClosed();
        loggerExternal.exiting(getClassNameLogging(), "getFetchSize", nFetchSize);
        return nFetchSize;
    }

    /* L0 */ public final int getResultSetConcurrency() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getResultSetConcurrency");
        checkClosed();
        loggerExternal.exiting(getClassNameLogging(), "getResultSetConcurrency", resultSetConcurrency);
        return resultSetConcurrency;
    }

    /* L0 */ public final int getResultSetType() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getResultSetType");
        checkClosed();
        loggerExternal.exiting(getClassNameLogging(), "getResultSetType", appResultSetType);
        return appResultSetType;
    }

    public void addBatch(String sql) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "addBatch", sql);
        checkClosed();

        // Apps can use JDBC call syntax to call unparameterized stored procedures
        // through regular Statement objects. We need to ensure that any such JDBC
        // call syntax is rewritten here as SQL exec syntax.
        sql = ensureSQLSyntax(sql);

        batchStatementBuffer.add(sql);
        loggerExternal.exiting(getClassNameLogging(), "addBatch");
    }

    public void clearBatch() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "clearBatch");
        checkClosed();
        batchStatementBuffer.clear();
        loggerExternal.exiting(getClassNameLogging(), "clearBatch");
    }

    /**
     * Send a batch of statements to the database.
     */
    public int[] executeBatch() throws SQLServerException, BatchUpdateException, SQLTimeoutException {
        loggerExternal.entering(getClassNameLogging(), "executeBatch");
        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
        discardLastExecutionResults();

        try {
            // Initialize all the update counts to EXECUTE_FAILED. This is so that if the server
            // returns fewer update counts than we expected (for example, due to closing the connection
            // on a severe error), we will return errors for the unexecuted batches.
            final int batchSize = batchStatementBuffer.size();
            int[] updateCounts = new int[batchSize];
            for (int batchNum = 0; batchNum < batchSize; batchNum++)
                updateCounts[batchNum] = Statement.EXECUTE_FAILED;

            // Last exception thrown. If database errors are returned, then executeBatch throws a
            // BatchUpdateException with this exception and the update counts, including errors.
            SQLServerException lastError = null;

            for (int batchNum = 0; batchNum < batchSize; batchNum++) {
                // NOTE:
                // When making changes to anything below, consider whether similar changes need
                // to be made to PreparedStatement batch execution.

                try {
                    if (0 == batchNum) {
                        // First time through, execute the entire set of batches and return the first result
                        executeStatement(new StmtBatchExecCmd(this));
                    }
                    else {
                        // Subsequent times through, just get the result from the next batch.
                        // If there are not enough results (update counts) to satisfy the number of batches,
                        // then bail, leaving EXECUTE_FAILED in the remaining slots of the update count array.
                        startResults();
                        if (!getNextResult())
                            break;
                    }

                    if (null != resultSet) {
                        SQLServerException.makeFromDriverError(connection, this, SQLServerException.getErrString("R_resultsetGeneratedForUpdate"),
                                null, true);
                    }
                    else {
                        updateCounts[batchNum] = (-1 != (int) updateCount) ? (int) updateCount : Statement.SUCCESS_NO_INFO;
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
                    lastError = e;
                }
            }

            // If we had any errors then throw a BatchUpdateException with the partial results.
            if (null != lastError) {
                throw new BatchUpdateException(lastError.getMessage(), lastError.getSQLState(), lastError.getErrorCode(), updateCounts);
            }
            loggerExternal.exiting(getClassNameLogging(), "executeBatch", updateCounts);
            return updateCounts;

        }
        finally {
            // Regardless what happens, always clear out the batch after execution.
            // Note: Don't use the clearBatch API as it checks that the statement is
            // not closed, which it might be in the event of a severe error.
            batchStatementBuffer.clear();
        }
    } // executeBatch

    public long[] executeLargeBatch() throws SQLServerException, BatchUpdateException, SQLTimeoutException {
        DriverJDBCVersion.checkSupportsJDBC42();

        loggerExternal.entering(getClassNameLogging(), "executeLargeBatch");
        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
        discardLastExecutionResults();

        try {
            // Initialize all the update counts to EXECUTE_FAILED. This is so that if the server
            // returns fewer update counts than we expected (for example, due to closing the connection
            // on a severe error), we will return errors for the unexecuted batches.
            final int batchSize = batchStatementBuffer.size();
            long[] updateCounts = new long[batchSize];
            for (int batchNum = 0; batchNum < batchSize; batchNum++)
                updateCounts[batchNum] = Statement.EXECUTE_FAILED;

            // Last exception thrown. If database errors are returned, then executeBatch throws a
            // BatchUpdateException with this exception and the update counts, including errors.
            SQLServerException lastError = null;

            for (int batchNum = 0; batchNum < batchSize; batchNum++) {
                // NOTE:
                // When making changes to anything below, consider whether similar changes need
                // to be made to PreparedStatement batch execution.

                try {
                    if (0 == batchNum) {
                        // First time through, execute the entire set of batches and return the first result
                        executeStatement(new StmtBatchExecCmd(this));
                    }
                    else {
                        // Subsequent times through, just get the result from the next batch.
                        // If there are not enough results (update counts) to satisfy the number of batches,
                        // then bail, leaving EXECUTE_FAILED in the remaining slots of the update count array.
                        startResults();
                        if (!getNextResult())
                            break;
                    }

                    if (null != resultSet) {
                        SQLServerException.makeFromDriverError(connection, this, SQLServerException.getErrString("R_resultsetGeneratedForUpdate"),
                                null, true);
                    }
                    else {
                        updateCounts[batchNum] = (-1 != updateCount) ? updateCount : Statement.SUCCESS_NO_INFO;
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
                    lastError = e;
                }
            }

            // If we had any errors then throw a BatchUpdateException with the partial results.
            if (null != lastError) {
                DriverJDBCVersion.throwBatchUpdateException(lastError, updateCounts);
            }
            loggerExternal.exiting(getClassNameLogging(), "executeLargeBatch", updateCounts);
            return updateCounts;

        }
        finally {
            // Regardless what happens, always clear out the batch after execution.
            // Note: Don't use the clearBatch API as it checks that the statement is
            // not closed, which it might be in the event of a severe error.
            batchStatementBuffer.clear();
        }
    } // executeLargeBatch

    /**
     * Return the statement's connection
     * 
     * @throws SQLServerException
     *             when an error occurs
     * @return the connection
     */
    /* L0 */ public final java.sql.Connection getConnection() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getConnection");
        if (bIsClosed) {
            SQLServerException.makeFromDriverError(connection, this, SQLServerException.getErrString("R_statementIsClosed"), null, false);
        }
        java.sql.Connection con = connection.getConnection();
        loggerExternal.exiting(getClassNameLogging(), "getConnection", con);
        return con;
    }

    /* ----------------- Server side cursor support -------------------------- */

    /**
     * Open a server side cursor.
     *
     * @param sql
     *            The SQL query.
     * @exception SQLServerException
     *                The SQL query was invalid.
     */

    final int getResultSetScrollOpt() {
        int scrollOpt = (null == inOutParam) ? 0 : TDS.SCROLLOPT_PARAMETERIZED_STMT;

        switch (resultSetType) {
            case SQLServerResultSet.TYPE_SS_SERVER_CURSOR_FORWARD_ONLY:
                return scrollOpt | ((ResultSet.CONCUR_READ_ONLY == resultSetConcurrency) ? TDS.SCROLLOPT_FAST_FORWARD : TDS.SCROLLOPT_FORWARD_ONLY);

            case SQLServerResultSet.TYPE_SS_SCROLL_DYNAMIC:
                return scrollOpt | TDS.SCROLLOPT_DYNAMIC;

            case SQLServerResultSet.TYPE_SS_SCROLL_KEYSET:
                return scrollOpt | TDS.SCROLLOPT_KEYSET;

            case SQLServerResultSet.TYPE_SS_SCROLL_STATIC:
                return scrollOpt | TDS.SCROLLOPT_STATIC;

            // Other (invalid) values were caught by the constructor.
        }

        return 0;
    }

    final int getResultSetCCOpt() {
        switch (resultSetConcurrency) {
            case ResultSet.CONCUR_READ_ONLY:
                return TDS.CCOPT_READ_ONLY | TDS.CCOPT_ALLOW_DIRECT;

            case SQLServerResultSet.CONCUR_SS_OPTIMISTIC_CC:
                // aka case ResultSet.CONCUR_UPDATABLE:
                return TDS.CCOPT_OPTIMISTIC_CC | TDS.CCOPT_UPDT_IN_PLACE | TDS.CCOPT_ALLOW_DIRECT;

            case SQLServerResultSet.CONCUR_SS_SCROLL_LOCKS:
                return TDS.CCOPT_SCROLL_LOCKS | TDS.CCOPT_UPDT_IN_PLACE | TDS.CCOPT_ALLOW_DIRECT;

            case SQLServerResultSet.CONCUR_SS_OPTIMISTIC_CCVAL:
                return TDS.CCOPT_OPTIMISTIC_CCVAL | TDS.CCOPT_UPDT_IN_PLACE | TDS.CCOPT_ALLOW_DIRECT;

            // Other (invalid) values were caught by the constructor.
        }

        return 0;
    }

    private void doExecuteCursored(StmtExecCmd execCmd,
            String sql) throws SQLServerException {
        if (stmtlogger.isLoggable(java.util.logging.Level.FINER)) {
            stmtlogger.finer(toString() + " Execute for cursor open" + " SQL:" + sql + " Scrollability:" + getResultSetScrollOpt() + " Concurrency:"
                    + getResultSetCCOpt());
        }

        executedSqlDirectly = false;
        expectCursorOutParams = true;
        TDSWriter tdsWriter = execCmd.startRequest(TDS.PKT_RPC);
        tdsWriter.writeShort((short) 0xFFFF); // procedure name length -> use ProcIDs
        tdsWriter.writeShort(TDS.PROCID_SP_CURSOROPEN);
        tdsWriter.writeByte((byte) 0);  // RPC procedure option 1
        tdsWriter.writeByte((byte) 0);  // RPC procedure option 2

        // <cursor> OUT
        tdsWriter.writeRPCInt(null, 0, true);

        // <stmt> IN
        tdsWriter.writeRPCStringUnicode(sql);

        // <scrollopt> IN
        tdsWriter.writeRPCInt(null, getResultSetScrollOpt(), false);

        // <ccopt> IN
        tdsWriter.writeRPCInt(null, getResultSetCCOpt(), false);

        // <rowcount> OUT
        tdsWriter.writeRPCInt(null, 0, true);

        ensureExecuteResultsReader(execCmd.startResponse(isResponseBufferingAdaptive));
        startResults();
        getNextResult();
    }

    /* JDBC 3.0 */

    /* L3 */ public final int getResultSetHoldability() throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getResultSetHoldability");
        checkClosed();
        int holdability = connection.getHoldability(); // For SQL Server must be the same as the connection
        loggerExternal.exiting(getClassNameLogging(), "getResultSetHoldability", holdability);
        return holdability;
    }

    public final boolean execute(java.lang.String sql,
            int autoGeneratedKeys) throws SQLServerException, SQLTimeoutException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER)) {
            loggerExternal.entering(getClassNameLogging(), "execute", new Object[] {sql, autoGeneratedKeys});
            if (Util.IsActivityTraceOn()) {
                loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
            }
        }
        checkClosed();
        if (autoGeneratedKeys != Statement.RETURN_GENERATED_KEYS && autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidAutoGeneratedKeys"));
            Object[] msgArgs = {autoGeneratedKeys};
            SQLServerException.makeFromDriverError(connection, this, form.format(msgArgs), null, false);
        }

        executeStatement(new StmtExecCmd(this, sql, EXECUTE, autoGeneratedKeys));
        loggerExternal.exiting(getClassNameLogging(), "execute", null != resultSet);
        return null != resultSet;
    }

    public final boolean execute(java.lang.String sql,
            int[] columnIndexes) throws SQLServerException, SQLTimeoutException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "execute", new Object[] {sql, columnIndexes});
        checkClosed();
        if (columnIndexes == null || columnIndexes.length != 1) {
            SQLServerException.makeFromDriverError(connection, this, SQLServerException.getErrString("R_invalidColumnArrayLength"), null, false);
        }
        boolean fSuccess = execute(sql, Statement.RETURN_GENERATED_KEYS);
        loggerExternal.exiting(getClassNameLogging(), "execute", fSuccess);
        return fSuccess;
    }

    public final boolean execute(java.lang.String sql,
            java.lang.String[] columnNames) throws SQLServerException, SQLTimeoutException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "execute", new Object[] {sql, columnNames});
        checkClosed();
        if (columnNames == null || columnNames.length != 1) {
            SQLServerException.makeFromDriverError(connection, this, SQLServerException.getErrString("R_invalidColumnArrayLength"), null, false);
        }
        boolean fSuccess = execute(sql, Statement.RETURN_GENERATED_KEYS);
        loggerExternal.exiting(getClassNameLogging(), "execute", fSuccess);
        return fSuccess;
    }

    public final int executeUpdate(String sql,
            int autoGeneratedKeys) throws SQLServerException, SQLTimeoutException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER)) {
            loggerExternal.entering(getClassNameLogging(), "executeUpdate", new Object[] {sql, autoGeneratedKeys});
            if (Util.IsActivityTraceOn()) {
                loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
            }
        }
        checkClosed();
        if (autoGeneratedKeys != Statement.RETURN_GENERATED_KEYS && autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidAutoGeneratedKeys"));
            Object[] msgArgs = {autoGeneratedKeys};
            SQLServerException.makeFromDriverError(connection, this, form.format(msgArgs), null, false);
        }
        executeStatement(new StmtExecCmd(this, sql, EXECUTE_UPDATE, autoGeneratedKeys));

        // this shouldn't happen, caller probably meant to call executeLargeUpdate
        if (updateCount < Integer.MIN_VALUE || updateCount > Integer.MAX_VALUE)
            SQLServerException.makeFromDriverError(connection, this, SQLServerException.getErrString("R_updateCountOutofRange"), null, true);

        loggerExternal.exiting(getClassNameLogging(), "executeUpdate", updateCount);

        return (int) updateCount;
    }

    public final long executeLargeUpdate(String sql,
            int autoGeneratedKeys) throws SQLServerException, SQLTimeoutException {
        DriverJDBCVersion.checkSupportsJDBC42();

        if (loggerExternal.isLoggable(java.util.logging.Level.FINER)) {
            loggerExternal.entering(getClassNameLogging(), "executeLargeUpdate", new Object[] {sql, autoGeneratedKeys});
            if (Util.IsActivityTraceOn()) {
                loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
            }
        }
        checkClosed();
        if (autoGeneratedKeys != Statement.RETURN_GENERATED_KEYS && autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidAutoGeneratedKeys"));
            Object[] msgArgs = {autoGeneratedKeys};
            SQLServerException.makeFromDriverError(connection, this, form.format(msgArgs), null, false);
        }
        executeStatement(new StmtExecCmd(this, sql, EXECUTE_UPDATE, autoGeneratedKeys));
        loggerExternal.exiting(getClassNameLogging(), "executeLargeUpdate", updateCount);
        return updateCount;
    }

    public final int executeUpdate(java.lang.String sql,
            int[] columnIndexes) throws SQLServerException, SQLTimeoutException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "executeUpdate", new Object[] {sql, columnIndexes});
        checkClosed();
        if (columnIndexes == null || columnIndexes.length != 1) {
            SQLServerException.makeFromDriverError(connection, this, SQLServerException.getErrString("R_invalidColumnArrayLength"), null, false);
        }
        int count = executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
        loggerExternal.exiting(getClassNameLogging(), "executeUpdate", count);
        return count;
    }

    public final long executeLargeUpdate(java.lang.String sql,
            int[] columnIndexes) throws SQLServerException, SQLTimeoutException {
        DriverJDBCVersion.checkSupportsJDBC42();

        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "executeLargeUpdate", new Object[] {sql, columnIndexes});
        checkClosed();
        if (columnIndexes == null || columnIndexes.length != 1) {
            SQLServerException.makeFromDriverError(connection, this, SQLServerException.getErrString("R_invalidColumnArrayLength"), null, false);
        }
        long count = executeLargeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
        loggerExternal.exiting(getClassNameLogging(), "executeLargeUpdate", count);
        return count;
    }

    public final int executeUpdate(java.lang.String sql,
            String[] columnNames) throws SQLServerException, SQLTimeoutException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "executeUpdate", new Object[] {sql, columnNames});
        checkClosed();
        if (columnNames == null || columnNames.length != 1) {
            SQLServerException.makeFromDriverError(connection, this, SQLServerException.getErrString("R_invalidColumnArrayLength"), null, false);
        }
        int count = executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
        loggerExternal.exiting(getClassNameLogging(), "executeUpdate", count);
        return count;
    }

    public final long executeLargeUpdate(java.lang.String sql,
            String[] columnNames) throws SQLServerException, SQLTimeoutException {
        DriverJDBCVersion.checkSupportsJDBC42();

        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "executeLargeUpdate", new Object[] {sql, columnNames});
        checkClosed();
        if (columnNames == null || columnNames.length != 1) {
            SQLServerException.makeFromDriverError(connection, this, SQLServerException.getErrString("R_invalidColumnArrayLength"), null, false);
        }
        long count = executeLargeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
        loggerExternal.exiting(getClassNameLogging(), "executeLargeUpdate", count);
        return count;
    }

    public final ResultSet getGeneratedKeys() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getGeneratedKeys");
        checkClosed();

        if (null == autoGeneratedKeys) {
            long orgUpd = updateCount;

            // Generated keys are returned in a ResultSet result right after the update count.
            // Try to get that ResultSet. If there are no more results after the update count,
            // or if the next result isn't a ResultSet, then something is wrong.
            if (!getNextResult() || null == resultSet) {
                SQLServerException.makeFromDriverError(connection, this, SQLServerException.getErrString("R_statementMustBeExecuted"), null, false);
            }

            autoGeneratedKeys = resultSet;
            updateCount = orgUpd;
        }
        loggerExternal.exiting(getClassNameLogging(), "getGeneratedKeys", autoGeneratedKeys);
        return autoGeneratedKeys;
    }

    /* L3 */ public final boolean getMoreResults(int mode) throws SQLServerException {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getMoreResults", mode);
        checkClosed();
        if (KEEP_CURRENT_RESULT == mode)
            NotImplemented();

        if (CLOSE_CURRENT_RESULT != mode && CLOSE_ALL_RESULTS != mode)
            SQLServerException.makeFromDriverError(connection, this, SQLServerException.getErrString("R_modeSuppliedNotValid"), null, true);

        ResultSet rsPrevious = resultSet;
        boolean fResults = getMoreResults();
        if (rsPrevious != null) {
            try {
                rsPrevious.close();
            }
            catch (SQLException e) {
                throw new SQLServerException(e.getMessage(), null, 0, e);
            }
        }

        loggerExternal.exiting(getClassNameLogging(), "getMoreResults", fResults);
        return fResults;
    }

    public boolean isClosed() throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "isClosed");
        boolean result = bIsClosed || connection.isSessionUnAvailable();
        loggerExternal.exiting(getClassNameLogging(), "isClosed", result);
        return result;
    }

    public boolean isCloseOnCompletion() throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "isCloseOnCompletion");
        checkClosed();
        loggerExternal.exiting(getClassNameLogging(), "isCloseOnCompletion", isCloseOnCompletion);
        return isCloseOnCompletion;
    }

    public boolean isPoolable() throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "isPoolable");
        checkClosed();
        loggerExternal.exiting(getClassNameLogging(), "isPoolable", stmtPoolable);
        return stmtPoolable;
    }

    public void setPoolable(boolean poolable) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "setPoolable", poolable);
        checkClosed();
        stmtPoolable = poolable;
        loggerExternal.exiting(getClassNameLogging(), "setPoolable");
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "isWrapperFor");
        boolean f = iface.isInstance(this);
        loggerExternal.exiting(getClassNameLogging(), "isWrapperFor", f);
        return f;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "unwrap");
        T t;
        try {
            t = iface.cast(this);
        }
        catch (ClassCastException e) {
            throw new SQLServerException(e.getMessage(), e);
        }
        loggerExternal.exiting(getClassNameLogging(), "unwrap", t);
        return t;
    }

    // responseBuffering controls the driver's buffering of responses from SQL Server.
    // Possible values are:
    //
    // "full" - Fully buffer the response at execution time.
    // Advantages:
    // 100% back compat with v1.1 driver
    // Maximizes concurrency on the server
    // Disadvantages:
    // Consumes more client-side memory
    // Client scalability limits with large responses
    // More execute latency
    //
    // "adaptive" - Data Pipe adaptive buffering
    // Advantages:
    // Buffers only when necessary, only as much as necessary
    // Enables handling very large responses, values
    // Disadvantages
    // Reduced concurrency on the server
    public final void setResponseBuffering(String value) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "setResponseBuffering", value);
        checkClosed();
        if ("full".equalsIgnoreCase(value)) {
            isResponseBufferingAdaptive = false;
            wasResponseBufferingSet = true;
        }
        else if ("adaptive".equalsIgnoreCase(value)) {
            isResponseBufferingAdaptive = true;
            wasResponseBufferingSet = true;
        }
        else {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidresponseBuffering"));
            Object[] msgArgs = {value};
            SQLServerException.makeFromDriverError(connection, this, form.format(msgArgs), null, false);
        }
        loggerExternal.exiting(getClassNameLogging(), "setResponseBuffering");
    }

    public final String getResponseBuffering() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getResponseBuffering");
        checkClosed();
        String responseBuff;
        if (wasResponseBufferingSet) {
            if (isResponseBufferingAdaptive)
                responseBuff = "adaptive";
            else
                responseBuff = "full";
        }
        else {
            responseBuff = connection.getResponseBuffering();
        }
        loggerExternal.exiting(getClassNameLogging(), "getResponseBuffering", responseBuff);
        return responseBuff;
    }
}

/**
 * Helper class that does some basic parsing work for SQL statements that are stored procedure calls.
 *
 * - Determines whether the SQL uses JDBC call syntax ("{[? =] call procedure_name...}") or T-SQL EXECUTE syntax ("EXEC [@p0 =] procedure_name...").
 * If JDBC call syntax is present, it gets rewritten as T-SQL EXECUTE syntax.
 *
 * - Determines whether the caller expects a return value from the stored procedure (see the optional return value syntax in [] above).
 *
 * - Extracts the stored procedure name from the call.
 *
 * SQL statements that are not stored procedure calls are passed through unchanged.
 */
final class JDBCSyntaxTranslator {
    private String procedureName = null;

    String getProcedureName() {
        return procedureName;
    }

    private boolean hasReturnValueSyntax = false;

    boolean hasReturnValueSyntax() {
        return hasReturnValueSyntax;
    }

    /*
     * SQL Identifier regex
     *
     * Loosely follows the spec'd SQL identifier syntax: - anything between escape characters (square brackets or double quotes), including escaped
     * escape characters, OR - any contiguous string of non-whitespace characters. - including multipart identifiers
     */
    private final static String sqlIdentifierPart = "(?:(?:\\[(?:[^\\]]|(?:\\]\\]))+?\\])|(?:\"(?:[^\"]|(?:\"\"))+?\")|(?:\\S+?))";

    private final static String sqlIdentifierWithoutGroups = "(" + sqlIdentifierPart + "(?:\\." + sqlIdentifierPart + "){0,3}?)";

    private final static String sqlIdentifierWithGroups = "(" + sqlIdentifierPart + ")" + "(?:\\." + "(" + sqlIdentifierPart + "))?";

    // This is used in three part name matching.
    static String getSQLIdentifierWithGroups() {
        return sqlIdentifierWithGroups;
    }

    /*
     * JDBC call syntax regex
     *
     * From the JDBC spec: {call procedure_name} {call procedure_name(?, ?, ...)} {? = call procedure_name[(?, ?, ...)]}
     *
     * allowing for arbitrary amounts of whitespace in the obvious places.
     */
    private final static Pattern jdbcCallSyntax = Pattern
            .compile("(?s)\\s*?\\{\\s*?(\\?\\s*?=)?\\s*?[cC][aA][lL][lL]\\s+?" + sqlIdentifierWithoutGroups + "(?:\\s*?\\((.*)\\))?\\s*\\}.*+");

    /*
     * T-SQL EXECUTE syntax regex
     *
     * EXEC | EXECUTE [@return_result =] procedure_name [parameters]
     *
     * allowing for arbitrary amounts of whitespace in the obvious places.
     */
    private final static Pattern sqlExecSyntax = Pattern.compile("\\s*?[eE][xX][eE][cC](?:[uU][tT][eE])??\\s+?(" + sqlIdentifierWithoutGroups
            + "\\s*?=\\s+?)??" + sqlIdentifierWithoutGroups + "(?:$|(?:\\s+?.*+))");

    /*
     * JDBC limit escape syntax
     *
     * From the JDBC spec: {LIMIT <rows> [OFFSET <row_offset>]} The driver currently does not support the OFFSET part. It will throw an exception if
     * used.
     */
    enum State {
        START,
        END,
        SUBQUERY, // also handles anything inside any (), e.g. scalar functions
        SELECT,
        OPENQUERY,
        OPENROWSET,
        LIMIT,
        OFFSET,
        QUOTE,
        PROCESS
    };

    // This pattern matches the LIMIT syntax with an OFFSET clause. The driver does not support OFFSET expression in the LIMIT clause.
    // It will throw an exception if OFFSET is present in the LIMIT escape syntax.
    private final static Pattern limitSyntaxWithOffset = Pattern
            .compile("\\{\\s*[lL][iI][mM][iI][tT]\\s+(.*)\\s+[oO][fF][fF][sS][eE][tT]\\s+(.*)\\}");
    // This pattern is used to determine if the query has LIMIT escape syntax. If so, then the query is further processed to translate the syntax.
    private final static Pattern limitSyntaxGeneric = Pattern
            .compile("\\{\\s*[lL][iI][mM][iI][tT]\\s+(.*)(\\s+[oO][fF][fF][sS][eE][tT](.*)\\}|\\s*\\})");

    private final static Pattern selectPattern = Pattern.compile("([sS][eE][lL][eE][cC][tT])\\s+");

    // OPENQUERY ( linked_server ,'query' )
    private final static Pattern openQueryPattern = Pattern.compile("[oO][pP][eE][nN][qQ][uU][eE][rR][yY]\\s*\\(.*,\\s*'(.*)'\\s*\\)");
    /*
     * OPENROWSET ( 'provider_name', { 'datasource' ; 'user_id' ; 'password' | 'provider_string' }, { [ catalog. ] [ schema. ] object | 'query' } )
     */
    private final static Pattern openRowsetPattern = Pattern.compile("[oO][pP][eE][nN][rR][oO][wW][sS][eE][tT]\\s*\\(.*,.*,\\s*'(.*)'\\s*\\)");

    /*
     * {limit 30} {limit ?} {limit (?)}
     */
    private final static Pattern limitOnlyPattern = Pattern.compile("\\{\\s*[lL][iI][mM][iI][tT]\\s+(((\\(|\\s)*)(\\d*|\\?)((\\)|\\s)*))\\s*\\}");

    /**
     * This function translates the LIMIT escape syntax, {LIMIT <row> [OFFSET <offset>]} SQL Server does not support LIMIT syntax, the LIMIT escape
     * syntax is thus translated to use "TOP" syntax The OFFSET clause is not supported, and will throw an exception if used.
     * 
     * @param sql the SQL query
     * 
     * @param indx Position in the query from where to start translation
     * 
     * @param endChar The character that marks the end of translation
     * 
     * @throws SQLServerException
     * 
     * @return the number of characters that have been translated
     * 
     */

    int translateLimit(StringBuffer sql,
            int indx,
            char endChar) throws SQLServerException {
        Matcher selectMatcher = selectPattern.matcher(sql);
        Matcher openQueryMatcher = openQueryPattern.matcher(sql);
        Matcher openRowsetMatcher = openRowsetPattern.matcher(sql);
        Matcher limitMatcher = limitOnlyPattern.matcher(sql);
        Matcher offsetMatcher = limitSyntaxWithOffset.matcher(sql);

        int startIndx = indx;
        Stack<Integer> topPosition = new Stack<>();
        State nextState = State.START;

        while (indx < sql.length()) {
            char ch = sql.charAt(indx);

            switch (nextState) {
                case START:
                    nextState = State.PROCESS;
                    break;
                case PROCESS:
                    // The search for endChar should come before the search for quote (') as openquery has quote(') as the endChar
                    if (endChar == ch) {
                        nextState = State.END;
                    }
                    else if ('\'' == ch) {
                        nextState = State.QUOTE;
                    }
                    else if ('(' == ch) {
                        nextState = State.SUBQUERY;
                    }
                    else if (limitMatcher.find(indx) && indx == limitMatcher.start()) {
                        nextState = State.LIMIT;
                    }
                    else if (offsetMatcher.find(indx) && indx == offsetMatcher.start()) {
                        nextState = State.OFFSET;
                    }
                    else if (openQueryMatcher.find(indx) && indx == openQueryMatcher.start()) {
                        nextState = State.OPENQUERY;
                    }
                    else if (openRowsetMatcher.find(indx) && indx == openRowsetMatcher.start()) {
                        nextState = State.OPENROWSET;
                    }
                    else if (selectMatcher.find(indx) && indx == selectMatcher.start()) {
                        nextState = State.SELECT;
                    }
                    else
                        indx++;
                    break;
                case OFFSET:
                    // throw exception as OFFSET is not supported
                    throw new SQLServerException(SQLServerException.getErrString("R_limitOffsetNotSupported"), null, // SQLState is null as this error
                                                                                                                     // is generated in the driver
                            0, // Use 0 instead of DriverError.NOT_SET to use the correct constructor
                            null);
                case LIMIT:
                    // Check if the number of opening/closing parentheses surrounding the digits or "?" in LIMIT match
                    // Count the number of opening parentheses.
                    int openingParentheses = 0, closingParentheses = 0;
                    int pos = -1;
                    String openingStr = limitMatcher.group(2);
                    String closingStr = limitMatcher.group(5);
                    while (-1 != (pos = openingStr.indexOf('(', pos + 1))) {
                        openingParentheses++;
                    }
                    pos = -1;
                    while (-1 != (pos = closingStr.indexOf(')', pos + 1))) {
                        closingParentheses++;
                    }
                    if (openingParentheses != closingParentheses) {
                        throw new SQLServerException(SQLServerException.getErrString("R_limitEscapeSyntaxError"), null, // SQLState is null as this
                                                                                                                        // error is generated in the
                                                                                                                        // driver
                                0, // Use 0 instead of DriverError.NOT_SET to use the correct constructor
                                null);
                    }

                    /*
                     * 'topPosition' is a stack that keeps track of the positions where the next "TOP" should be inserted. The SELECT expressions are
                     * matched with the closest LIMIT expressions unless in a subquery with explicit parentheses, that's why it needs to be a stack.
                     * To translate, we add the clause <TOP rows> after SELECT and delete the clause {LIMIT rows}.
                     */
                    if (!topPosition.empty()) {
                        Integer top = topPosition.pop();
                        String rows = limitMatcher.group(1);
                        // Delete the LIMIT clause.
                        sql.delete(limitMatcher.start() - 1, limitMatcher.end());
                        // Add the TOP clause.
                        if ('?' == rows.charAt(0)) {
                            // For parameterized queries the '?' needs to wrapped in parentheses.
                            sql.insert(top, " TOP (" + rows + ")");
                            // add the letters/spaces inserted with TOP, add the digits from LIMIT, subtract one because the
                            // current letter at the index is deleted.
                            indx += 7 + rows.length() - 1;
                        }
                        else {
                            sql.insert(top, " TOP " + rows);
                            indx += 5 + rows.length() - 1;
                        }
                    }
                    else {
                        // Could not match LIMIT with a SELECT, should never occur.
                        // But if it does, just ignore
                        // Matcher.end() returns offset after the last character of matched string
                        indx = limitMatcher.end() - 1;
                    }
                    nextState = State.PROCESS;
                    break;
                case SELECT:
                    indx = selectMatcher.end(1);
                    topPosition.push(indx);
                    nextState = State.PROCESS;
                    break;
                case QUOTE:
                    // Consume the current character
                    indx++;
                    if (sql.length() > indx && '\'' == sql.charAt(indx)) {
                        // Consume the quote.
                        // If this is part of an escaped quote, stay in QUOTE state, else go to PROCESS
                        // To escape a quote SQL Server requires two quotes
                        indx++;
                        if (sql.length() > indx && '\'' == sql.charAt(indx)) {
                            nextState = State.QUOTE;
                        }
                        else {
                            nextState = State.PROCESS;
                        }
                    }
                    else {
                        nextState = State.QUOTE;
                    }
                    break;
                case SUBQUERY:
                    // Consume the opening bracket.
                    indx++;
                    // Consume the subquery.
                    indx += translateLimit(sql, indx, ')');
                    nextState = State.PROCESS;
                    break;
                case OPENQUERY:
                    // skip the characters until query start.
                    indx = openQueryMatcher.start(1);
                    indx += translateLimit(sql, indx, '\'');
                    nextState = State.PROCESS;
                    break;
                case OPENROWSET:
                    // skip the characters until query start.
                    indx = openRowsetMatcher.start(1);
                    indx += translateLimit(sql, indx, '\'');
                    nextState = State.PROCESS;
                    break;
                case END:
                    // Consume the endChar character found
                    indx++;
                    return indx - startIndx;
                default:
                    // This should never occur.
                    // throw
                    break;
            }
        }// end of while
        return indx - startIndx;
    }

    String translate(String sql) throws SQLServerException {
        Matcher matcher;

        matcher = jdbcCallSyntax.matcher(sql);
        if (matcher.matches()) {

            // Figure out the procedure name and whether there is a return value and then
            // rewrite the JDBC call syntax as T-SQL EXEC syntax.
            hasReturnValueSyntax = (null != matcher.group(1));
            procedureName = matcher.group(2);
            String args = matcher.group(3);
            sql = "EXEC " + (hasReturnValueSyntax ? "? = " : "") + procedureName + ((null != args) ? (" " + args) : "");
        }
        else {
            matcher = sqlExecSyntax.matcher(sql);
            if (matcher.matches()) {

                // Figure out the procedure name and whether there is a return value,
                // but do not rewrite the statement as it is already in T-SQL EXEC syntax.
                hasReturnValueSyntax = (null != matcher.group(1));
                procedureName = matcher.group(3);
            }
        }

        // LIMIT escape is introduced in JDBC 4.1. Make sure versions lower than 4.1 do not have this feature.
        if (((4 == DriverJDBCVersion.major) && (1 <= DriverJDBCVersion.minor)) || (4 < DriverJDBCVersion.major)) {
            // Search for LIMIT escape syntax. Do further processing if present.
            matcher = limitSyntaxGeneric.matcher(sql);
            if (matcher.find()) {
                StringBuffer sqlbuf = new StringBuffer(sql);
                translateLimit(sqlbuf, 0, '\0');
                return sqlbuf.toString();
            }
        }

        // 'sql' is modified if CALL or LIMIT escape sequence is present, Otherwise pass it straight through.
        return sql;
    }
}
