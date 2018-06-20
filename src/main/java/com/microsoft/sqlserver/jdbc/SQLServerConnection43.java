/*
 * Microsoft JDBC Driver for SQL Server
 *
 * Copyright(c) Microsoft Corporation All rights reserved.
 *
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.ShardingKey;

public class SQLServerConnection43 extends SQLServerConnection implements ISQLServerConnection43 {

    SQLServerConnection43(String parentInfo) throws SQLServerException {
        super(parentInfo);
    }

    /**
     * Hints to the driver that a request, an independent unit of work, is beginning on this connection. It backs up the values of the connection
     * properties that are modifiable through public methods. Each request is independent of all other requests with regard to state local to the
     * connection either on the client or the server. Work done between {@code beginRequest}, {@code endRequest} pairs does not depend on any other
     * work done on the connection either as part of another request or outside of any request. A request may include multiple transactions. There may
     * be dependencies on committed database state as that is not local to the connection. {@code beginRequest} marks the beginning of the work unit.
     * <p>
     * Local state is defined as any state associated with a Connection that is local to the current Connection either in the client or the database
     * that is not transparently reproducible.
     * <p>
     * Calls to {@code beginRequest} and {@code endRequest} are not nested. Multiple calls to {@code beginRequest} without an intervening call to
     * {@code endRequest} is not an error. The first {@code beginRequest} call marks the start of the request and subsequent calls are treated as a
     * no-op It is recommended to enclose each unit of work in {@code beginRequest}, {@code endRequest} pairs such that there is no open transaction
     * at the beginning or end of the request and no dependency on local state that crosses request boundaries. Committed database state is not local.
     *
     * This method is to be used by Connection pooling managers.
     * <p>
     * The pooling manager should call {@code beginRequest} on the underlying connection prior to returning a connection to the caller.
     * <p>
     * 
     * @throws SQLException
     *             if an error occurs
     * @see endRequest
     */
    public void beginRequest() throws SQLException {
        beginRequestInternal();
    }

    /**
     * Hints to the driver that a request, an independent unit of work, has completed. It rolls back the open transactions. Resets the connection
     * properties that are modifiable through public methods back to their original values. Calls to {@code beginRequest} and {@code endRequest} are
     * not nested. Multiple calls to {@code endRequest} without an intervening call to {@code beginRequest} is not an error. The first
     * {@code endRequest} call marks the request completed and subsequent calls are treated as a no-op. If {@code endRequest} is called without an
     * initial call to {@code beginRequest} is a no-op. This method is to be used by Connection pooling managers.
     * <p>
     * 
     * @throws SQLException
     *             if an error occurs
     * @see beginRequest
     */
    public void endRequest() throws SQLException {
        endRequestInternal();
    }

    public void setShardingKey(ShardingKey shardingKey) throws SQLServerException {
        DriverJDBCVersion.checkSupportsJDBC43();
        throw new SQLServerException("setShardingKey not implemented", new SQLFeatureNotSupportedException("setShardingKey not implemented"));
    }

    public void setShardingKey(ShardingKey shardingKey,
            ShardingKey superShardingKey) throws SQLServerException {
        DriverJDBCVersion.checkSupportsJDBC43();
        throw new SQLServerException("setShardingKey not implemented", new SQLFeatureNotSupportedException("setShardingKey not implemented"));
    }

    public boolean setShardingKeyIfValid(ShardingKey shardingKey,
            int timeout) throws SQLServerException {
        DriverJDBCVersion.checkSupportsJDBC43();
        throw new SQLServerException("setShardingKeyIfValid not implemented",
                new SQLFeatureNotSupportedException("setShardingKeyIfValid not implemented"));
    }

    public boolean setShardingKeyIfValid(ShardingKey shardingKey,
            ShardingKey superShardingKey,
            int timeout) throws SQLServerException {
        DriverJDBCVersion.checkSupportsJDBC43();
        throw new SQLServerException("setShardingKeyIfValid not implemented",
                new SQLFeatureNotSupportedException("setShardingKeyIfValid not implemented"));
    }

}
