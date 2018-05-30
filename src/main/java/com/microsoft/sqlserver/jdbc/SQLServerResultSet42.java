/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.SQLType;

/**
 * 
 * This class is separated from SQLServerResultSet class in order to resolve compiling error of missing Java 8 Types when running with Java 7.
 * 
 * This class will be initialized instead of SQLServerResultSet when Java 8 and JDBC 4.2 are used.
 * 
 */
public class SQLServerResultSet42 extends SQLServerResultSet implements ISQLServerResultSet42 {

    /**
     * Makes a new result set
     * 
     * @param stmtIn
     *            the generating statement
     * @throws SQLServerException
     *             when an error occurs
     */
    public SQLServerResultSet42(SQLServerStatement stmtIn) throws SQLServerException {
        super(stmtIn);
    }

    public void updateObject(int index,
            Object obj,
            SQLType targetSqlType) throws SQLServerException {
        DriverJDBCVersion.checkSupportsJDBC42();

        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateObject", new Object[] {index, obj, targetSqlType});

        checkClosed();
        // getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
        updateObject(index, obj, null, JDBCType.of(targetSqlType.getVendorTypeNumber()), null, false);

        loggerExternal.exiting(getClassNameLogging(), "updateObject");
    }

    public void updateObject(int index,
            Object obj,
            SQLType targetSqlType,
            int scale) throws SQLServerException {
        DriverJDBCVersion.checkSupportsJDBC42();

        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateObject", new Object[] {index, obj, targetSqlType, scale});

        checkClosed();
        // getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
        updateObject(index, obj, scale, JDBCType.of(targetSqlType.getVendorTypeNumber()), null, false);

        loggerExternal.exiting(getClassNameLogging(), "updateObject");
    }

    public void updateObject(int index,
            Object obj,
            SQLType targetSqlType,
            int scale,
            boolean forceEncrypt) throws SQLServerException {
        DriverJDBCVersion.checkSupportsJDBC42();

        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateObject", new Object[] {index, obj, targetSqlType, scale, forceEncrypt});

        checkClosed();
        // getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
        updateObject(index, obj, scale, JDBCType.of(targetSqlType.getVendorTypeNumber()), null, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateObject");
    }

    public void updateObject(String columnName,
            Object obj,
            SQLType targetSqlType,
            int scale) throws SQLServerException {
        DriverJDBCVersion.checkSupportsJDBC42();

        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateObject", new Object[] {columnName, obj, targetSqlType, scale});

        checkClosed();

        // getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
        updateObject(findColumn(columnName), obj, scale, JDBCType.of(targetSqlType.getVendorTypeNumber()), null, false);

        loggerExternal.exiting(getClassNameLogging(), "updateObject");
    }

    public void updateObject(String columnName,
            Object obj,
            SQLType targetSqlType,
            int scale,
            boolean forceEncrypt) throws SQLServerException {
        DriverJDBCVersion.checkSupportsJDBC42();

        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateObject", new Object[] {columnName, obj, targetSqlType, scale, forceEncrypt});

        checkClosed();

        // getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
        updateObject(findColumn(columnName), obj, scale, JDBCType.of(targetSqlType.getVendorTypeNumber()), null, forceEncrypt);

        loggerExternal.exiting(getClassNameLogging(), "updateObject");
    }

    public void updateObject(String columnName,
            Object obj,
            SQLType targetSqlType) throws SQLServerException {
        DriverJDBCVersion.checkSupportsJDBC42();

        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "updateObject", new Object[] {columnName, obj, targetSqlType});

        checkClosed();

        // getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
        updateObject(findColumn(columnName), obj, null, JDBCType.of(targetSqlType.getVendorTypeNumber()), null, false);

        loggerExternal.exiting(getClassNameLogging(), "updateObject");
    }

}
