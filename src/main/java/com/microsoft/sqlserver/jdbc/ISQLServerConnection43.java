package com.microsoft.sqlserver.jdbc;

import java.sql.SQLFeatureNotSupportedException;
import java.sql.ShardingKey;

public interface ISQLServerConnection43 extends ISQLServerConnection {

    public void setShardingKey(ShardingKey shardingKey) throws SQLFeatureNotSupportedException;
    
    public void setShardingKey(ShardingKey shardingKey,
            ShardingKey superShardingKey) throws SQLFeatureNotSupportedException;
    
    public boolean setShardingKeyIfValid(ShardingKey shardingKey,
            int timeout) throws SQLFeatureNotSupportedException;
    
    public boolean setShardingKeyIfValid(ShardingKey shardingKey,
            ShardingKey superShardingKey,
            int timeout) throws SQLFeatureNotSupportedException;
    
}
