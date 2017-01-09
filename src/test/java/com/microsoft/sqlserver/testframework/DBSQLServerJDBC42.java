package com.microsoft.sqlserver.testframework;

import java.sql.SQLFeatureNotSupportedException;

/**
 * 
 * Stubs for JDBC 4.2 test jars
 * 
 * @author Microsoft
 */
public class DBSQLServerJDBC42 {
    public static final int value = 42;

    public Class getSqlFeatureNotSupportedExceptionClass() {
        return SQLFeatureNotSupportedException.class;
    }
}
