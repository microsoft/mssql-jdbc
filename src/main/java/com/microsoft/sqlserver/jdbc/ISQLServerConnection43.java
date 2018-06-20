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

public interface ISQLServerConnection43 extends ISQLServerConnection {

    public void beginRequest() throws SQLException;

    public void endRequest() throws SQLException;

    public void setShardingKey(ShardingKey shardingKey) throws SQLServerException;

    public void setShardingKey(ShardingKey shardingKey,
            ShardingKey superShardingKey) throws SQLServerException;

    public boolean setShardingKeyIfValid(ShardingKey shardingKey,
            int timeout) throws SQLServerException;

    public boolean setShardingKeyIfValid(ShardingKey shardingKey,
            ShardingKey superShardingKey,
            int timeout) throws SQLServerException;

}
