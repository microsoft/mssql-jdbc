/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * StreamError represents a TDS error or message event.
 */

final class StreamError extends StreamPacket {
    /** the error message */
    String errorMessage = "";
    /** the error number */
    int errorNumber;
    /** the tds error state */
    int errorState;
    /** the tds error severity */
    int errorSeverity;

    String serverName;
    String procName;
    long lineNumber;

    final String getMessage() {
        return errorMessage;
    }

    final int getErrorNumber() {
        return errorNumber;
    }

    final int getErrorState() {
        return errorState;
    }

    final int getErrorSeverity() {
        return errorSeverity;
    }

    StreamError() {
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
