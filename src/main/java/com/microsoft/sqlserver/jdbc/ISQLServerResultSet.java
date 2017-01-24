/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.SQLException;

public interface ISQLServerResultSet extends java.sql.ResultSet {

    public static final int TYPE_SS_DIRECT_FORWARD_ONLY = 2003; // TYPE_FORWARD_ONLY + 1000
    public static final int TYPE_SS_SERVER_CURSOR_FORWARD_ONLY = 2004; // TYPE_FORWARD_ONLY + 1001
    public static final int TYPE_SS_SCROLL_STATIC = 1004; // TYPE_SCROLL_INSENSITIVE
    public static final int TYPE_SS_SCROLL_KEYSET = 1005; // TYPE_SCROLL_SENSITIVE
    public static final int TYPE_SS_SCROLL_DYNAMIC = 1006; // TYPE_SCROLL_SENSITIVE + 1

    /* SQL Server concurrency values */
    public static final int CONCUR_SS_OPTIMISTIC_CC = 1008; // CONCUR_UPDATABLE
    public static final int CONCUR_SS_SCROLL_LOCKS = 1009; // CONCUR_UPDATABLE + 1
    public static final int CONCUR_SS_OPTIMISTIC_CCVAL = 1010; // CONCUR_UPDATABLE + 2

    /**
     * Retrieves the value of the designated column as a microsoft.sql.DateTimeOffset object, given a zero-based column ordinal.
     * 
     * @param columnIndex
     *            The zero-based ordinal of a column.
     * @return A DateTimeOffset Class object.
     * @throws SQLException
     *             when an error occurs
     */
    public microsoft.sql.DateTimeOffset getDateTimeOffset(int columnIndex) throws SQLException;

    /**
     * Retrieves the value of the column specified as a microsoft.sql.DateTimeOffset object, given a column name.
     * 
     * @param columnName
     *            The name of a column.
     * @return A DateTimeOffset Class object.
     * @throws SQLException
     *             when an error occurs
     */
    public microsoft.sql.DateTimeOffset getDateTimeOffset(String columnName) throws SQLException;

    /**
     * Updates the value of the column specified to the DateTimeOffset Class value, given a zero-based column ordinal.
     * 
     * @param index
     *            The zero-based ordinal of a column.
     * @param x
     *            A DateTimeOffset Class object.
     * @throws SQLException
     *             when an error occurs
     */
    public void updateDateTimeOffset(int index,
            microsoft.sql.DateTimeOffset x) throws SQLException;

    /**
     * Updates the value of the column specified to the DateTimeOffset Class value, given a column name.
     * 
     * @param columnName
     *            The name of a column.
     * @param x
     *            A DateTimeOffset Class object.
     * @throws SQLException
     *             when an error occurs
     */
    public void updateDateTimeOffset(String columnName,
            microsoft.sql.DateTimeOffset x) throws SQLException;

}
