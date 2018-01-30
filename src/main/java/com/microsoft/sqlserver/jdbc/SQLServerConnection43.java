package com.microsoft.sqlserver.jdbc;

import java.sql.SQLFeatureNotSupportedException;
import java.sql.ShardingKey;

public class SQLServerConnection43 extends SQLServerConnection implements ISQLServerConnection43 {

    SQLServerConnection43(String parentInfo) throws SQLServerException {
        super(parentInfo);
    }
    
    public void setShardingKey(ShardingKey shardingKey) throws SQLServerException {
        DriverJDBCVersion.checkSupportsJDBC43();
        throw new SQLServerException("setShardingKey not implemented", new SQLFeatureNotSupportedException("setShardingKey not implemented"));
    }

    public void setShardingKey(ShardingKey shardingKey,
            ShardingKey superShardingKey) throws SQLServerException {
        DriverJDBCVersion.checkSupportsJDBC43();
        throw new SQLServerException("setShardingKey not implemented", new SQLFeatureNotSupportedException("setShardingKey not implemented")) ;
    }

    public boolean setShardingKeyIfValid(ShardingKey shardingKey,
            int timeout) throws SQLServerException {
        DriverJDBCVersion.checkSupportsJDBC43();
        throw new SQLServerException("setShardingKeyIfValid not implemented", new SQLFeatureNotSupportedException("setShardingKeyIfValid not implemented"));
    }

    public boolean setShardingKeyIfValid(ShardingKey shardingKey,
            ShardingKey superShardingKey,
            int timeout) throws SQLServerException {
        DriverJDBCVersion.checkSupportsJDBC43();
        throw new SQLServerException("setShardingKeyIfValid not implemented", new SQLFeatureNotSupportedException("setShardingKeyIfValid not implemented"));
    }

}
