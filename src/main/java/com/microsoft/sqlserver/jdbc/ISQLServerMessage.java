/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import java.sql.SQLException;

public interface ISQLServerMessage
{
    /**
     * Returns ISQLServerMessage containing detailed info about SQL Server Message as received from SQL Server.
     * 
     * @return ISQLServerMessage
     */
//    public ISQLServerMessage getSQLServerMessage();
    public SQLServerError getSQLServerMessage();

//    /**
//     * Set SQLServerError which holds detailed info about SQL Server Message.
//     * 
//     * @return SQLServerError
//     */
//    public void setSQLServerMessage(SQLServerError msg);

    /**
     * Returns error message as received from SQL Server
     * 
     * @return Error Message
     */
    public String getErrorMessage();

    /**
     * Returns error number as received from SQL Server
     * 
     * @return Error Number
     */
    public int getErrorNumber();

    /**
     * Returns error state as received from SQL Server
     * 
     * @return Error State
     */
    public int getErrorState();

    /**
     * Returns Severity of error (as int value) as received from SQL Server
     * 
     * @return Error Severity
     */
    public int getErrorSeverity();

    /**
     * Returns name of the server where exception occurs as received from SQL Server
     * 
     * @return Server Name
     */
    public String getServerName();

    /**
     * Returns name of the stored procedure where exception occurs as received from SQL Server
     * 
     * @return Procedure Name
     */
    public String getProcedureName();

    /**
     * Returns line number where the error occurred in Stored Procedure returned by <code>getProcedureName()</code> as
     * received from SQL Server
     * 
     * @return Line Number
     */
    public long getLineNumber();

    /**
     * Creates a SQLServerException or SQLServerWarning from this SQLServerMessage
     * @return
     */
    public SQLException toSqlExceptionOrSqlWarning();

}
