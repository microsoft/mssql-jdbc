/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.Serializable;
import java.sql.ResultSetMetaData;


/**
 * Provides an interface to the{@link SQLServerResultSetMetaData} class.
 */
public interface ISQLServerResultSetMetaData extends ResultSetMetaData, Serializable {

    /**
     * Returns if the column is a SQLServer SparseColumnSet.
     * 
     * @param column
     *        The column number
     * @return true if a column in a result set is a sparse column set, otherwise false.
     * @throws SQLServerException
     *         when an error occurs
     */
    boolean isSparseColumnSet(int column) throws SQLServerException;

}
