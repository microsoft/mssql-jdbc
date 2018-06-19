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

public class SQLServerConnection43 extends SQLServerConnection {

    SQLServerConnection43(String parentInfo) throws SQLServerException {
        super(parentInfo);
    }

    @Override
    public void beginRequest() throws SQLException {
        beginRequestInternal();
    }

    @Override
    public void endRequest() throws SQLException {
        endRequestInternal();
    }

    @Override
    public void setShardingKey(ShardingKey shardingKey) throws SQLServerException {
        throw new SQLServerException("setShardingKey not implemented", new SQLFeatureNotSupportedException("setShardingKey not implemented"));
    }

    @Override
    public void setShardingKey(ShardingKey shardingKey,
            ShardingKey superShardingKey) throws SQLServerException {
        throw new SQLServerException("setShardingKey not implemented", new SQLFeatureNotSupportedException("setShardingKey not implemented"));
    }

    @Override
    public boolean setShardingKeyIfValid(ShardingKey shardingKey,
            int timeout) throws SQLServerException {
        throw new SQLServerException("setShardingKeyIfValid not implemented",
                new SQLFeatureNotSupportedException("setShardingKeyIfValid not implemented"));
    }

    @Override
    public boolean setShardingKeyIfValid(ShardingKey shardingKey,
            ShardingKey superShardingKey,
            int timeout) throws SQLServerException {
        throw new SQLServerException("setShardingKeyIfValid not implemented",
                new SQLFeatureNotSupportedException("setShardingKeyIfValid not implemented"));
    }
}
