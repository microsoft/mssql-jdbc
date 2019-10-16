/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.Serializable;
import java.sql.Savepoint;


/**
 * Provides an interface to the {@link SQLServerSavepoint} class.
 */
public interface ISQLServerSavepoint extends Savepoint, Serializable {

    /**
     * Returns the savepoint name
     * 
     * @return the name of savepoint
     */
    String getSavepointName() throws SQLServerException;

    /**
     * Returns the savepoint label
     * 
     * @return the label for Savepoint
     */
    String getLabel();

    /**
     * Returns if the savepoint label is null
     * 
     * @return true is the savepoint is named. Otherwise, false.
     */
    boolean isNamed();

}
