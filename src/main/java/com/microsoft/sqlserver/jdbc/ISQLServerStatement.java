/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.Serializable;

/**
 * Provides an interface to the {@link SQLServerStatement} class.
 */
public interface ISQLServerStatement extends java.sql.Statement, Serializable {
    /**
     * Sets the response buffering mode for this SQLServerStatement object to case-insensitive String full or adaptive.
     * <p>
     * Response buffering controls the driver's buffering of responses from SQL Server.
     * <p>
     * Possible values are:
     * <p>
     * "full" - Fully buffer the response at execution time.
     * <p>
     * "adaptive" - Data Pipe adaptive buffering
     * 
     * @param value
     *        A String that contains the response buffering mode. The valid mode can be one of the following
     *        case-insensitive Strings: full or adaptive.
     * @throws SQLServerException
     *         If there are any errors in setting the response buffering mode.
     */
    void setResponseBuffering(String value) throws SQLServerException;

    /**
     * Returns the response buffering mode for this SQLServerStatement object.
     * 
     * @return A String that contains a lower-case full or adaptive.
     * @throws SQLServerException
     *         If there are any errors in retrieving the response buffering mode.
     */
    String getResponseBuffering() throws SQLServerException;

    /**
     * Returns the <code>cancelQueryTimeout</code> property set on this SQLServerStatement object.
     * 
     * @return cancelQueryTimeout Time duration in seconds.
     * @throws SQLServerException
     *         if any error occurs
     */
    int getCancelQueryTimeout() throws SQLServerException;

    /**
     * Sets the <code>cancelQueryTimeout</code> property on this SQLServerStatement object to cancel
     * <code>queryTimeout</code> set on <code>Connection</code> or <code>Statement</code> level.
     * 
     * @param seconds
     *        Time duration in seconds.
     * @throws SQLServerException
     *         if any error occurs
     */
    void setCancelQueryTimeout(int seconds) throws SQLServerException;
}
