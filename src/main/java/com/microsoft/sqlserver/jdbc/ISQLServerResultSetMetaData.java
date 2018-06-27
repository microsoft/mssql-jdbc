package com.microsoft.sqlserver.jdbc;

import java.sql.ResultSetMetaData;

public interface ISQLServerResultSetMetaData extends ResultSetMetaData {

    /**
     * Returns true if the column is a SQLServer SparseColumnSet
     * 
     * @param column
     *            The column number
     * @return true if a column in a result set is a sparse column set, otherwise false.
     * @throws SQLServerException
     *             when an error occurs
     */
    public boolean isSparseColumnSet(int column) throws SQLServerException;

}
