/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.Serializable;


/**
 * SQLServerError represents a TDS error or message event.
 */
public final class SQLServerError extends StreamPacket implements Serializable {

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

        /*
         * Cannot process create or update request. Too many create or update operations in progress for subscription
         * "%ld".
         */
        SQLSERVER_ERROR_49919(49919),

        // Cannot process request. Too many operations in progress for subscription "%ld".
        SQLSERVER_ERROR_49920(49920),

        /*
         * Login to read-secondary failed due to long wait on 'HADR_DATABASE_WAIT_FOR_TRANSITION_TO_VERSIONING'. The
         * replica is not available for login because row versions are missing for transactions that were in-flight when
         * the replica was recycled. The issue can be resolved by rolling back or committing the active transactions on
         * the primary replica. Occurrences of this condition can be minimized by avoiding long write transactions on
         * the primary.
         */
        SQLSERVER_ERROR_4221(4221),

        // The follow are from SqlClient

        // Resource ID: %d. The %s limit for the database is %d and has been reached.
        SQLSERVER_ERROR_10928(10928),
        SQLSERVER_ERROR_40020(40020),

        /*
         * Resource ID: %d. The %s minimum guarantee is %d, maximum limit is %d and the current usage for the database
         * is %d. However, the server is currently too busy to support requests greater than %d for this database.
         */
        SQLSERVER_ERROR_10929(10929);

        private final int errNo;

        TransientError(int errNo) {
            this.errNo = errNo;
        };

        public int getErrNo() {
            return errNo;
        };

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
    };

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
}
