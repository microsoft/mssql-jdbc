/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import java.io.Serializable;
import java.sql.SQLException;


/**
 * SQL Server Lob abstract class
 */
abstract class SQLServerLob implements Serializable {

    /**
     * Always update serialVersionUID when prompted.
     */
    private static final long serialVersionUID = -6444654924359581662L;

    /**
     * Provides functionality for the result set to maintain blobs it has created.
     * 
     * @throws SQLException
     */
    abstract void fillFromStream() throws SQLException;

    boolean delayLoadingLob = true;

    /**
     * Provides functionality for the result set to set whether to load the LOB objects fully. Setting this property to
     * TRUE will cause LOBs to be loaded into memory. The default behavior is FALSE.
     */
    void setDelayLoadingLob() {
        delayLoadingLob = false;
    }
}
