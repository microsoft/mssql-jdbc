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
     * Always update serialVersionUID when prompted
     */
    private static final long serialVersionUID = -7304033613218700719L;
    private String errorMessage = "";
    private int errorNumber;
    private int errorState;
    private int errorSeverity;
    private String serverName;
    private String procName;
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
