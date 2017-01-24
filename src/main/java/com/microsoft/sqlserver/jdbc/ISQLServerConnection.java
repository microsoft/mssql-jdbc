/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.SQLException;
import java.util.UUID;

/**
 *
 * This interface is implemented by SQLServerConnection Class.
 */
public interface ISQLServerConnection extends java.sql.Connection {
    // Transaction types.
    // TRANSACTION_SNAPSHOT corresponds to -> SET TRANSACTION ISOLATION LEVEL SNAPSHOT
    public final static int TRANSACTION_SNAPSHOT = 0x1000;

    /**
     * Gets the connection ID of the most recent connection attempt, regardless of whether the attempt succeeded or failed.
     * 
     * @return 16-byte GUID representing the connection ID of the most recent connection attempt. Or, NULL if there is a failure after the connection
     *         request is initiated and the pre-login handshake.
     * @throws SQLException
     *             If any errors occur.
     */
    public UUID getClientConnectionId() throws SQLException;
}
