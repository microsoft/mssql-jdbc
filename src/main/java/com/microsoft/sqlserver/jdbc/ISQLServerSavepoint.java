/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.Savepoint;


/**
 * This interface is implemented by {@link SQLServerSavepoint} class.
 */
public interface ISQLServerSavepoint extends Savepoint {

    /**
     * Get the savepoint name
     * 
     * @return the name of savepoint
     */
    public String getSavepointName() throws SQLServerException;

    /**
     * Get the savepoint label
     * 
     * @return the label for Savepoint
     */
    public String getLabel();

    /**
     * Checks if the savepoint label is null
     * 
     * @return true is the savepoint is named. Otherwise, false.
     */
    public boolean isNamed();

}
