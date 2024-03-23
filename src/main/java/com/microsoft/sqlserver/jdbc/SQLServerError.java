/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


/**
 * SQLServerError represents a TDS error or message event.
 */
public final class SQLServerError extends StreamPacket implements Serializable, ISQLServerMessage {

    /**
     * List SQL Server transient errors drivers will retry on from
     * https://docs.microsoft.com/en-us/azure/azure-sql/database/troubleshoot-common-errors-issues#transient-faults-connection-loss-and-other-temporary-errors
     * and SqlClient
     * https://github.com/dotnet/SqlClient/blob/main/src/Microsoft.Data.SqlClient/netfx/src/Microsoft/Data/SqlClient/SqlInternalConnectionTds.cs#L589
     */
    enum TransientError {
        // Cannot open database "%.*ls" requested by the login. The login failed.
        SQLSERVER_ERROR_4060(4060),

        /*
         * You will receive this error, when the service is down due to software or hardware upgrades, hardware
         * failures, or any other failover problems. The error code (%d) embedded within the message of error 40197
         * provides additional information about the kind of failure or failover that occurred. Some examples of the
         * error codes are embedded within the message of error 40197 are 40020, 40143, 40166, and 40540.
         */
        SQLSERVER_ERROR_40197(40197),
        SQLSERVER_ERROR_40143(40143),
        SQLSERVER_ERROR_40166(40166),
        SQLSERVER_ERROR_40540(40540),

        // The service is currently busy. Retry the request after 10 seconds. Incident ID: %ls. Code: %d.
        SQLSERVER_ERROR_40501(40501),

        // Database '%.*ls' on server '%.*ls' is not currently available. Please retry the connection later.
        // If the problem persists, contact customer support, and provide them the session tracing ID of '%.*ls'.
        SQLSERVER_ERROR_40613(40613),

        // Cannot process request. Not enough resources to process request
        SQLSERVER_ERROR_49918(49918),

        // Cannot process create or update request. Too many create or update operations in progress for subscription
        // "%ld".
        SQLSERVER_ERROR_49919(49919),

        // Cannot process request. Too many operations in progress for subscription "%ld".
        SQLSERVER_ERROR_49920(49920),

        // Login to read-secondary failed due to long wait on 'HADR_DATABASE_WAIT_FOR_TRANSITION_TO_VERSIONING'. The
        // replica is not available for login because row versions are missing for transactions that were in-flight when
        // the replica was recycled. The issue can be resolved by rolling back or committing the active transactions on
        // the primary replica. Occurrences of this condition can be minimized by avoiding long write transactions on
        // the primary.
        SQLSERVER_ERROR_4221(4221),

        /**
         * The following are from SqlClient
         */
        // Resource ID: %d. The %s limit for the database is %d and has been reached.
        SQLSERVER_ERROR_10928(10928),
        SQLSERVER_ERROR_40020(40020),

        // Resource ID: %d. The %s minimum guarantee is %d, maximum limit is %d and the current usage for the database
        // is %d. However, the server is currently too busy to support requests greater than %d for this database.
        SQLSERVER_ERROR_10929(10929),

        // Can not connect to the SQL pool since it is paused. Please resume the SQL pool and try again.
        SQLSERVER_ERROR_42108(42108),

        // The SQL pool is warming up. Please try again.
        SQLSERVER_ERROR_42109(42109),

        /**
         * From <a
         * href="https://docs.microsoft.com/en-us/azure/azure-sql/database/troubleshoot-common-connectivity-issues#entlib60-istransient-method-source-code"</a>
         */
        // A transport-level error has occurred when sending the request to the server.
        // (provider: TCP Provider, error: 0 - An existing connection was forcibly closed by the remote host.)
        SQLSERVER_ERROR_10053(10053),

        // A transport-level error has occurred when sending the request to the server.
        // (provider: TCP Provider, error: 0 - An existing connection was forcibly closed by the remote host.)
        SQLSERVER_ERROR_10054(10054),

        // The client was unable to establish a connection because of an error during connection initialization process
        // before login.
        // Possible causes include the following: the client tried to connect to an unsupported version of SQL Server;
        // the server was too busy
        // to accept new connections; or there was a resource limitation (insufficient memory or maximum allowed
        // connections) on the server.
        // (provider: TCP Provider, error: 0 - An existing connection was forcibly closed by the remote host.)
        SQLSERVER_ERROR_233(233),

        // A connection was successfully established with the server, but then an error occurred during the login
        // process.
        // (provider: TCP Provider, error: 0 - The specified network name is no longer available.)
        SQLSERVER_ERROR_64(64);

        private final int errNo;

        TransientError(int errNo) {
            this.errNo = errNo;
        }

        public int getErrNo() {
            return errNo;
        }

        public static boolean isTransientError(SQLServerError sqlServerError) {
            if (null == sqlServerError) {
                return false;
            }

            int errNo = sqlServerError.getErrorNumber();

            for (TransientError p : TransientError.values()) {
                if (errNo == p.errNo) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Always update serialVersionUID when prompted
     */
    private static final long serialVersionUID = -7304033613218700719L;

    /** error message string */
    private String errorMessage = "";

    /** error number */
    private int errorNumber;

    /** error state */
    private int errorState;

    /** error severity */
    private int errorSeverity;

    /** server name */
    private String serverName;

    /** procedure name */
    private String procName;

    /** line number */
    private long lineNumber;

    /**
     * Returns error message as received from SQL Server
     * 
     * @return Error Message
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Returns error number as received from SQL Server
     * 
     * @return Error Number
     */
    public int getErrorNumber() {
        return errorNumber;
    }

    /**
     * Returns error state as received from SQL Server
     * 
     * @return Error State
     */
    public int getErrorState() {
        return errorState;
    }

    /**
     * Returns Severity of error (as int value) as received from SQL Server
     * 
     * @return Error Severity
     */
    public int getErrorSeverity() {
        return errorSeverity;
    }

    /**
     * Returns name of the server where exception occurs as received from SQL Server
     * 
     * @return Server Name
     */
    public String getServerName() {
        return serverName;
    }

    /**
     * Returns name of the stored procedure where exception occurs as received from SQL Server
     * 
     * @return Procedure Name
     */
    public String getProcedureName() {
        return procName;
    }

    /**
     * Returns line number where the error occurred in Stored Procedure returned by <code>getProcedureName()</code> as
     * received from SQL Server
     * 
     * @return Line Number
     */
    public long getLineNumber() {
        return lineNumber;
    }

    SQLServerError() {
        super(TDS.TDS_ERR);
    }

    SQLServerError(SQLServerError errorMsg) {
        super(TDS.TDS_ERR);
        this.errorNumber = errorMsg.errorNumber;
        this.errorState = errorMsg.errorState;
        this.errorSeverity = errorMsg.errorSeverity;
        this.errorMessage = errorMsg.errorMessage;
        this.serverName = errorMsg.serverName;
        this.procName = errorMsg.procName;
        this.lineNumber = errorMsg.lineNumber;
    }

    @Override
    void setFromTDS(TDSReader tdsReader) throws SQLServerException {
        if (TDS.TDS_ERR != tdsReader.readUnsignedByte())
            assert false;
        setContentsFromTDS(tdsReader);
    }

    void setContentsFromTDS(TDSReader tdsReader) throws SQLServerException {
        tdsReader.readUnsignedShort(); // token length (ignored)
        errorNumber = tdsReader.readInt();
        errorState = tdsReader.readUnsignedByte();
        errorSeverity = tdsReader.readUnsignedByte(); // matches master.dbo.sysmessages
        errorMessage = tdsReader.readUnicodeString(tdsReader.readUnsignedShort());
        serverName = tdsReader.readUnicodeString(tdsReader.readUnsignedByte());
        procName = tdsReader.readUnicodeString(tdsReader.readUnsignedByte());
        lineNumber = tdsReader.readUnsignedInt();
    }

    /**
     * Holds any "overflow messages", or messages that has been added <b>after</b> the first message.
     * <p>
     * This is later on used when creating a SQLServerException.<br>
     * Where all entries in the errorChain will be added {@link java.sql.SQLException#setNextException(SQLException)}
     */
    private List<SQLServerError> errorChain;

    void addError(SQLServerError sqlServerError) {
        if (errorChain == null) {
            errorChain = new ArrayList<>();
        }
        errorChain.add(sqlServerError);
    }

    List<SQLServerError> getErrorChain() {
        return errorChain;
    }

    @Override
    public SQLServerError getSQLServerMessage() {
        return this;
    }

    /**
     * Downgrade a Error message into a Info message
     * <p>
     * This simply create a SQLServerInfoMessage from this SQLServerError,
     * without changing the message content.
     * 
     * @return ISQLServerMessage
     */
    public ISQLServerMessage toSQLServerInfoMessage() {
        return toSQLServerInfoMessage(-1, -1);
    }

    /**
     * Downgrade a Error message into a Info message
     * <p>
     * This simply create a SQLServerInfoMessage from this SQLServerError,
     * 
     * @param newErrorSeverity
     *        - The new ErrorSeverity
     * 
     * @return ISQLServerMessage
     */
    public ISQLServerMessage toSQLServerInfoMessage(int newErrorSeverity) {
        return toSQLServerInfoMessage(newErrorSeverity, -1);
    }

    /**
     * Downgrade a Error message into a Info message
     * <p>
     * This simply create a SQLServerInfoMessage from this SQLServerError,
     * 
     * @param newErrorSeverity
     *        - If you want to change the ErrorSeverity (-1: leave unchanged)
     * @param newErrorNumber
     *        - If you want to change the ErrorNumber (-1: leave unchanged)
     * 
     * @return ISQLServerMessage
     */
    public ISQLServerMessage toSQLServerInfoMessage(int newErrorSeverity, int newErrorNumber) {
        if (newErrorSeverity != -1) {
            this.setErrorSeverity(newErrorSeverity);
        }

        if (newErrorNumber != -1) {
            this.setErrorNumber(newErrorNumber);
        }

        return new SQLServerInfoMessage(this);
    }

    /**
     * Set a new ErrorSeverity for this Message
     * 
     * @param newSeverity
     *        new severity
     */
    public void setErrorSeverity(int newSeverity) {
        this.errorSeverity = newSeverity;
    }

    /**
     * Set a new ErrorNumber for this Message
     * 
     * @param newErrorNumber
     *        new error number
     */
    public void setErrorNumber(int newErrorNumber) {
        this.errorNumber = newErrorNumber;
    }

    @Override
    public SQLException toSqlExceptionOrSqlWarning() {
        return new SQLServerException(this);
    }
}
