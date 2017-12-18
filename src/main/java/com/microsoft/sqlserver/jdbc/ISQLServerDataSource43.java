package com.microsoft.sqlserver.jdbc;

import java.sql.ConnectionBuilder;
import java.sql.SQLException;
import java.sql.ShardingKeyBuilder;

public interface ISQLServerDataSource43 extends ISQLServerDataSource {

    public ShardingKeyBuilder createShardingKeyBuilder() throws SQLException;
    
    public ConnectionBuilder createConnectionBuilder() throws SQLException;
}
