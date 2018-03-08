/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.text.MessageFormat;
import java.util.UUID;
import java.util.logging.Level;

/**
 * SQLServerException is thrown from any point in the driver that throws a java.sql.SQLException. SQLServerException handles both SQL 92 and XOPEN
 * state codes. They are switchable via a user specified connection property. SQLServerExceptions are written to any open log files the user has
 * specified.
 */

enum SQLState {
    STATEMENT_CANCELED                      ("HY008"),
    DATA_EXCEPTION_NOT_SPECIFIC             ("22000"),
    DATA_EXCEPTION_DATETIME_FIELD_OVERFLOW  ("22008"),
    DATA_EXCEPTION_LENGTH_MISMATCH          ("22026"),
    COL_NOT_FOUND                           ("42S22");

    private final String sqlStateCode;

    final String getSQLStateCode() {
        return sqlStateCode;
    }

    SQLState(String sqlStateCode) {
        this.sqlStateCode = sqlStateCode;
    }
}

enum DriverError {
    NOT_SET(0);

    private final int errorCode;

    final int getErrorCode() {
        return errorCode;
    }

    DriverError(int errorCode) {
        this.errorCode = errorCode;
    }
}

public final class SQLServerException extends java.sql.SQLException {
    static final String EXCEPTION_XOPEN_CONNECTION_CANT_ESTABLISH = "08001";
    static final String EXCEPTION_XOPEN_CONNECTION_DOES_NOT_EXIST = "08003";
    static final String EXCEPTION_XOPEN_CONNECTION_FAILURE = "08006"; // After connection was connected OK
    static final String LOG_CLIENT_CONNECTION_ID_PREFIX = " ClientConnectionId:";

    // SQL error values (from sqlerrorcodes.h)
    static final int LOGON_FAILED = 18456;
    static final int PASSWORD_EXPIRED = 18488;
    static final int USER_ACCOUNT_LOCKED = 18486;
    static java.util.logging.Logger exLogger = java.util.logging.Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerException");

    // Facility for driver-specific error codes
    static final int DRIVER_ERROR_NONE = 0;
    static final int DRIVER_ERROR_FROM_DATABASE = 2;
    static final int DRIVER_ERROR_IO_FAILED = 3;
    static final int DRIVER_ERROR_INVALID_TDS = 4;
    static final int DRIVER_ERROR_SSL_FAILED = 5;
    static final int DRIVER_ERROR_UNSUPPORTED_CONFIG = 6;
    static final int DRIVER_ERROR_INTERMITTENT_TLS_FAILED = 7;
    static final int ERROR_SOCKET_TIMEOUT = 8;
    static final int ERROR_QUERY_TIMEOUT = 9;
    private int driverErrorCode = DRIVER_ERROR_NONE;

    final int getDriverErrorCode() {
        return driverErrorCode;
    }

    final void setDriverErrorCode(int value) {
        driverErrorCode = value;
    }

    /**
     * Log an exception to the driver log file
     * 
     * @param o
     *            the io buffer that generated the exception
     * @param errText
     *            the excception message
     * @param bStack
     *            true to generate the stack trace
     */
    /* L0 */ private void logException(Object o,
            String errText,
            boolean bStack) {
        String id = "";
        if (o != null)
            id = o.toString();

        if (exLogger.isLoggable(Level.FINE))
            exLogger.fine("*** SQLException:" + id + " " + this.toString() + " " + errText);
        if (bStack) {
            if (exLogger.isLoggable(Level.FINE)) {
                StringBuilder sb = new StringBuilder(100);
                StackTraceElement st[] = this.getStackTrace();
                for (StackTraceElement aSt : st) sb.append(aSt.toString());
                Throwable t = this.getCause();
                if (t != null) {
                    sb.append("\n caused by " + t + "\n");
                    StackTraceElement tst[] = t.getStackTrace();
                    for (StackTraceElement aTst : tst) sb.append(aTst.toString());
                }
                exLogger.fine(sb.toString());
            }
        }
        if (errText.equals(SQLServerException.getErrString("R_queryTimedOut")))
        {
        	this.setDriverErrorCode(SQLServerException.ERROR_QUERY_TIMEOUT);
        }
    }

    static String getErrString(String errCode) {
        return SQLServerResource.getResource(errCode);
    }

    /**
     * Make a new SQLException
     * 
     * @param errText
     *            the exception message
     * @param sqlState
     *            the statement
     * @param driverError
     *            the driver error object
     * @param cause
     *            The exception that caused this exception
     */
    public SQLServerException(String errText,
            SQLState sqlState,
            DriverError driverError,
            Throwable cause) {
        this(errText, sqlState.getSQLStateCode(), driverError.getErrorCode(), cause);
    }

    public SQLServerException(String errText,
            String errState,
            int errNum,
            Throwable cause) {
        super(errText, errState, errNum);
        initCause(cause);
        logException(null, errText, true);
        ActivityCorrelator.setCurrentActivityIdSentFlag(); // set the activityid flag so that we don't send the current ActivityId later.
    }

    public SQLServerException(String errText,
            Throwable cause) {
        super(errText);
        initCause(cause);
        logException(null, errText, true);
        ActivityCorrelator.setCurrentActivityIdSentFlag();
    }

    /* L0 */ public SQLServerException(Object obj,
            String errText,
            String errState,
            int errNum,
            boolean bStack) {
        super(errText, errState, errNum);
        logException(obj, errText, bStack);
        ActivityCorrelator.setCurrentActivityIdSentFlag();
    }

    /**
     * Make a new SQLException
     * 
     * @param obj
     *            the object
     * @param errText
     *            the exception message
     * @param errState
     *            the exception state
     * @param streamError
     *            the StreamError object
     * @param bStack
     *            true to generate the stack trace
     */
    /* L0 */ public SQLServerException(Object obj,
            String errText,
            String errState,
            StreamError streamError,
            boolean bStack) {
        super(errText, errState, streamError.getErrorNumber());

        // Log SQL error with info from StreamError.
        errText = "Msg " + streamError.getErrorNumber() + ", Level " + streamError.getErrorSeverity() + ", State " + streamError.getErrorState()
                + ", " + errText;
        logException(obj, errText, bStack);
    }

    /**
     * Build a new SQL Exception from an error detected by the driver.
     * 
     * @param con
     *            the connection
     * @param obj
     * @param errText
     *            the excception message
     * @param state
     *            he excpeption state
     * @param bStack
     *            true to generate the stack trace
     * @throws SQLServerException
     */
    /* L0 */static void makeFromDriverError(SQLServerConnection con,
            Object obj,
            String errText,
            String state,
            boolean bStack) throws SQLServerException {
        // The sql error code is 0 since the error was not returned from the database
        // The state code is supplied by the calling code as XOPEN compliant.
        // XOPEN is used as the primary code here since they are published free
        // The SQL 99 states must be purchased from ANSII..
        String stateCode = "";
        // close the connection on a connection failure.

        if (state != null) // Many are null since XOPEN errors do not cover internal driver errors
            stateCode = state;
        if (con == null || !con.xopenStates)
            stateCode = mapFromXopen(state);

        SQLServerException theException = new SQLServerException(obj, SQLServerException.checkAndAppendClientConnId(errText, con), stateCode, 0,
                bStack);
        if ((null != state && state.equals(EXCEPTION_XOPEN_CONNECTION_FAILURE)) && (null != con)) {
            con.notifyPooledConnection(theException);
            // note this close wont close the connection if there is an associated pooled connection.
            con.close();
        }

        throw theException;
    }

    /**
     * Build a new SQL Exception from a streamError detected by the driver.
     * 
     * @param con
     *            the connection
     * @param obj
     * @param errText
     *            the excception message
     * @param streamError
     * @param bStack
     *            true to generate the stack trace
     * @throws SQLServerException
     */
    /* L0 */ static void makeFromDatabaseError(SQLServerConnection con,
            Object obj,
            String errText,
            StreamError streamError,
            boolean bStack) throws SQLServerException {
        String state = generateStateCode(con, streamError.getErrorNumber(), streamError.getErrorState());

        SQLServerException theException = new SQLServerException(obj, SQLServerException.checkAndAppendClientConnId(errText, con), state, streamError,
                bStack);
        theException.setDriverErrorCode(DRIVER_ERROR_FROM_DATABASE);

        // Close the connection if we get a severity 20 or higher error class (nClass is severity of error).
        if ((streamError.getErrorSeverity() >= 20) && (null != con)) {
            con.notifyPooledConnection(theException);
            con.close();
        }

        throw theException;
    }

    // This code is same as the conversion logic that previously existed in connecthelper.
    static void ConvertConnectExceptionToSQLServerException(String hostName,
            int portNumber,
            SQLServerConnection conn,
            Exception ex) throws SQLServerException {
        Exception connectException = ex;
        // Throw the exception if exception was caught by code above (stored in connectException).
        if (connectException != null) {
            MessageFormat formDetail = new MessageFormat(SQLServerException.getErrString("R_tcpOpenFailed"));
            Object[] msgArgsDetail = {connectException.getMessage()};
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_tcpipConnectionFailed"));
            Object[] msgArgs = {hostName, Integer.toString(portNumber), formDetail.format(msgArgsDetail)};
            String s = form.format(msgArgs);
            SQLServerException.makeFromDriverError(conn, conn, s, SQLServerException.EXCEPTION_XOPEN_CONNECTION_CANT_ESTABLISH, false);
        }
    }

    /**
     * Map XOPEN states.
     * 
     * @param state
     *            the state
     * @return the mapped state
     */
    /* L0 */ static String mapFromXopen(String state) {
        // Exceptions generated by the driver (not the database) are instanced with an XOPEN state code
        // since the SQL99 states cant be located on the web (must pay) and the XOPEN states appear to
        // be specific. Therefore if the driver is in SQL 99 mode we must map to SQL 99 state codes.
        // SQL99 values based on previous SQLServerConnect code and some inet values..
        if (state == null)
            return null;
        if (state.equals("07009"))
            return "S1093";

        // Connection (network) failure after connection made
        if (state.equals(SQLServerException.EXCEPTION_XOPEN_CONNECTION_CANT_ESTABLISH))
            return "08S01";
        if (state.equals(SQLServerException.EXCEPTION_XOPEN_CONNECTION_FAILURE))
            return "08S01";

        // if (state.equals(SQLServerException.EXCEPTION_XOPEN_NETWORK_ERROR))
        // return "S0022"; //Previous SQL99 state code for bad column name

        return "";
    }

    /**
     * Generate the JDBC state code based on the error number returned from the database
     * 
     * @param con
     *            the connection
     * @param errNum
     *            the error number
     * @param databaseState
     *            the database state
     * @return the state code
     */
    /* L0 */ static String generateStateCode(SQLServerConnection con,
            int errNum,
            int databaseState) {
        // Generate a SQL 99 or XOPEN state from a database generated error code
        boolean xopenStates = (con != null && con.xopenStates);
        if (xopenStates) {
            switch (errNum) {
                case 4060:
                    return "08001"; // Database name undefined at loging
                case 18456:
                    return "08001"; // username password wrong at login
                case 2714:
                    return "42S01"; // Table already exists
                case 208:
                    return "42S02"; // Table not found
                case 207:
                    return "42S22"; // Column not found
                // case 156: return "42000"; //Invalid syntax
            }
            return "42000"; // Use XOPEN 'Syntax error or access violation'
            // The error code came from the db but XOPEN does not have a specific case for it.
        }
        else {
            switch (errNum) {
                // case 18456: return "08001"; //username password wrong at login
                case 8152:
                    return "22001"; // String data right truncation
                case 515: // 2.2705
                case 547:
                    return "23000";  // Integrity constraint violation
                case 2601:
                    return "23000";  // Integrity constraint violation
                case 2714:
                    return "S0001"; // table already exists
                case 208:
                    return "S0002";  // table not found
                case 1205:
                    return "40001"; // deadlock detected
                case 2627:
                    return "23000"; // DPM 4.04. Primary key violation
            }
            return "S000" + databaseState;
        }
    }

    /**
     * Append ClientConnectionId to an error message if applicable
     * 
     * @param errMsg
     *            - the orginal error message.
     * @param conn
     *            - the SQLServerConnection object
     * @return error string concated by ClientConnectionId(in string format) if applicable, otherwise, return original error string.
     */
    static String checkAndAppendClientConnId(String errMsg,
            SQLServerConnection conn) throws SQLServerException {
        if (null != conn && conn.attachConnId()) {
            UUID clientConnId = conn.getClientConIdInternal();
            assert null != clientConnId;
            StringBuilder sb = new StringBuilder(errMsg);
            // This syntex of adding connection id is matched in a retry logic. If anything changes here, make
            // necessary changes to enableSSL() function's exception handling mechanism.
            sb.append(LOG_CLIENT_CONNECTION_ID_PREFIX);
            sb.append(clientConnId.toString());
            return sb.toString();
        }
        else
            return errMsg;

    }

}
