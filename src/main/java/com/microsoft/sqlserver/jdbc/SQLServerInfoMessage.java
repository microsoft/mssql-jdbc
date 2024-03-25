/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.SQLException;


/**
 * Holds information about SQL Server messages that is considered as Informational Messages (normally if SQL Server Severity is at 10)
 * <p>
 * Instead of just holding the SQL Server message (like a normal SQLWarning, it also holds all the
 * SQL Servers extended information, like: ErrorSeverity, ServerName, ProcName etc
 * <p>
 * This enables client to print out extra information about the message.<br>
 * Like: In what procedure was the message produced.
 * <p>
 * A SQLServerInfoMessage is produced when reading the TDS Stream and added to the Connection as a SQLServerWarning
 */
public final class SQLServerInfoMessage extends StreamPacket implements ISQLServerMessage {
    SQLServerError msg = new SQLServerError();

    SQLServerInfoMessage() {
        super(TDS.TDS_MSG);
    }

    SQLServerInfoMessage(SQLServerError msg) {
        super(TDS.TDS_MSG);
        this.msg = msg;
    }

    @Override
    void setFromTDS(TDSReader tdsReader) throws SQLServerException {
        if (TDS.TDS_MSG != tdsReader.readUnsignedByte())
            assert false;
        msg.setContentsFromTDS(tdsReader);
    }

    @Override
    public SQLServerError getSQLServerMessage() {
        return msg;
    }

    @Override
    public String getErrorMessage() {
        return msg.getErrorMessage();
    }

    @Override
    public int getErrorNumber() {
        return msg.getErrorNumber();
    }

    @Override
    public int getErrorState() {
        return msg.getErrorState();
    }

    @Override
    public int getErrorSeverity() {
        return msg.getErrorSeverity();
    }

    @Override
    public String getServerName() {
        return msg.getServerName();
    }

    @Override
    public String getProcedureName() {
        return msg.getProcedureName();
    }

    @Override
    public long getLineNumber() {
        return msg.getLineNumber();
    }

    /**
     * Upgrade a Info message into a Error message
     * <p>
     * This simply create a SQLServerError from this SQLServerInfoMessage,
     * without changing the message content.
     * 
     * @return ISQLServerMessage
     */
    public ISQLServerMessage toSQLServerError() {
        return toSQLServerError(-1, -1);
    }

    /**
     * Upgrade a Info message into a Error message
     * <p>
     * This simply create a SQLServerError from this SQLServerInfoMessage.
     * 
     * @param newErrorSeverity
     *        - The new ErrorSeverity
     * 
     * @return ISQLServerMessage
     */
    public ISQLServerMessage toSQLServerError(int newErrorSeverity) {
        return toSQLServerError(newErrorSeverity, -1);
    }

    /**
     * Upgrade a Info message into a Error message
     * <p>
     * This simply create a SQLServerError from this SQLServerInfoMessage.
     * 
     * @param newErrorSeverity
     *        - If you want to change the ErrorSeverity (-1: leave unchanged)
     * @param newErrorNumber
     *        - If you want to change the ErrorNumber (-1: leave unchanged)
     * 
     * @return ISQLServerMessage
     */
    public ISQLServerMessage toSQLServerError(int newErrorSeverity, int newErrorNumber) {
        if (newErrorSeverity != -1) {
            this.msg.setErrorSeverity(newErrorSeverity);
        }

        if (newErrorNumber != -1) {
            this.msg.setErrorNumber(newErrorNumber);
        }

        return new SQLServerError(this.msg);
    }

    @Override
    public SQLException toSqlExceptionOrSqlWarning() {
        return new SQLServerWarning(this.msg);
    }
}
