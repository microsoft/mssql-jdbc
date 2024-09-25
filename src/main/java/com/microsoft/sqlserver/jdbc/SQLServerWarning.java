/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import java.sql.SQLWarning;


/**
 * Holds information about SQL Server messages that is considered as Informational Messages (normally if SQL Server Severity is at 10)
 * <p>
 * Instead of just holding the SQL Server message (like a normal SQLWarning, it also holds all the
 * SQL Servers extended information, like: ErrorSeverity, ServerName, ProcName etc
 * <p>
 * This enables client to print out extra information about the message.<br>
 * Like: In what procedure was the message produced.
 */
public class SQLServerWarning extends SQLWarning {
    private static final long serialVersionUID = -5212432397705929142L;

    /** SQL server error */
    private SQLServerError sqlServerError;

    /**
     * Create a SQLWarning from an SQLServerError object
     * 
     * @param sqlServerError
     *        SQL Server error
     */
    public SQLServerWarning(SQLServerError sqlServerError) {
        super(sqlServerError.getErrorMessage(), SQLServerException.generateStateCode(null,
                sqlServerError.getErrorNumber(), sqlServerError.getErrorState()), sqlServerError.getErrorNumber(),
                null);

        this.sqlServerError = sqlServerError;
    }

    /**
     * Returns SQLServerError object containing detailed info about exception as received from SQL Server. This API
     * returns null if no server error has occurred.
     * 
     * @return SQLServerError
     */
    public SQLServerError getSQLServerError() {
        return sqlServerError;
    }
}
