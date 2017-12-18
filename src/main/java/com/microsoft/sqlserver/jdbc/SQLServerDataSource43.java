package com.microsoft.sqlserver.jdbc;

import java.sql.ConnectionBuilder;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.ShardingKeyBuilder;

public class SQLServerDataSource43 extends SQLServerDataSource implements ISQLServerDataSource43{

    public SQLServerDataSource43 () throws SQLServerException {
        super();
    }
    
    public ShardingKeyBuilder createShardingKeyBuilder() throws SQLException {
        DriverJDBCVersion.checkSupportsJDBC43();
        throw new SQLFeatureNotSupportedException("createShardingKeyBuilder not implemented");
    }

    public ConnectionBuilder createConnectionBuilder() throws SQLException {
        DriverJDBCVersion.checkSupportsJDBC43();
        throw new SQLFeatureNotSupportedException("createConnectionBuilder not implemented");
    }

}
