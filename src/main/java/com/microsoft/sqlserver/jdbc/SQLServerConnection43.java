/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.SQLException;
import java.sql.ShardingKey;

/**
 * SQLServerConnection43 extends {@link SQLServerConnection43} class and implements {@link ISQLServerConnection43} with methods introduced in JDBC 4.3
 * Specifications. This class is used by the drdiver when initializing a class with 43 driver version
 */
public class SQLServerConnection43 extends SQLServerConnection implements ISQLServerConnection43 {

    /**
     * Always refresh SerialVersionUID when prompted
     */
    private static final long serialVersionUID = -6904163521498951547L;

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
    public void setShardingKey(ShardingKey shardingKey) throws SQLException {
        SQLServerException.throwFeatureNotSupportedException();
    }

    @Override
    public void setShardingKey(ShardingKey shardingKey,
            ShardingKey superShardingKey) throws SQLException {
        SQLServerException.throwFeatureNotSupportedException();
    }

    @Override
    public boolean setShardingKeyIfValid(ShardingKey shardingKey,
            int timeout) throws SQLException {
        SQLServerException.throwFeatureNotSupportedException();
        return false;
    }

    @Override
    public boolean setShardingKeyIfValid(ShardingKey shardingKey,
            ShardingKey superShardingKey,
            int timeout) throws SQLException {
        SQLServerException.throwFeatureNotSupportedException();
        return false;
    }
}
