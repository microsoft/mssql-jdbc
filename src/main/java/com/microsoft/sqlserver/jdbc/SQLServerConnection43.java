package com.microsoft.sqlserver.jdbc;

import java.sql.SQLFeatureNotSupportedException;
import java.sql.ShardingKey;

public class SQLServerConnection43 extends SQLServerConnection implements ISQLServerConnection43 {

    SQLServerConnection43(String parentInfo) throws SQLServerException {
        super(parentInfo);
        // TODO Auto-generated constructor stub
    }
    
    public void setShardingKey(ShardingKey shardingKey) throws SQLFeatureNotSupportedException {
        DriverJDBCVersion.checkSupportsJDBC43();
        throw new SQLFeatureNotSupportedException("createShardingKeyBuilder not implemented");
    }

    public void setShardingKey(ShardingKey shardingKey,
            ShardingKey superShardingKey) throws SQLFeatureNotSupportedException {
        DriverJDBCVersion.checkSupportsJDBC43();
        throw new SQLFeatureNotSupportedException("createShardingKeyBuilder not implemented");
    }

    public boolean setShardingKeyIfValid(ShardingKey shardingKey,
            int timeout) throws SQLFeatureNotSupportedException {
        DriverJDBCVersion.checkSupportsJDBC43();
        throw new SQLFeatureNotSupportedException("createShardingKeyBuilder not implemented");
    }

    public boolean setShardingKeyIfValid(ShardingKey shardingKey,
            ShardingKey superShardingKey,
            int timeout) throws SQLFeatureNotSupportedException {
        DriverJDBCVersion.checkSupportsJDBC43();
        throw new SQLFeatureNotSupportedException("createShardingKeyBuilder not implemented");
    }

}
