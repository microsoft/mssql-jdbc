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
 * This class provides the underlying implementation of ISQLServerPreparedStatement42 interface to SQLServerPreparedStatement42 and
 * SQLServerCallableStatement42, so that SQLServerPreparedStatement42 and SQLServerCallableStatement42 have the same implementation for same methods.
 *
 */
class SQLServerPreparedStatement42Helper {

    static final void setObject(SQLServerPreparedStatement ps,
            int index,
            Object obj,
            SQLType jdbcType) throws SQLServerException {
        DriverJDBCVersion.checkSupportsJDBC42();

        if (SQLServerStatement.loggerExternal.isLoggable(java.util.logging.Level.FINER))
            SQLServerStatement.loggerExternal.entering(ps.getClassNameLogging(), "setObject", new Object[] {index, obj, jdbcType});

        // getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
        ps.setObject(index, obj, jdbcType.getVendorTypeNumber());

        SQLServerStatement.loggerExternal.exiting(ps.getClassNameLogging(), "setObject");
    }

    static final void setObject(SQLServerPreparedStatement ps,
            int parameterIndex,
            Object x,
            SQLType targetSqlType,
            int scaleOrLength) throws SQLServerException {
        DriverJDBCVersion.checkSupportsJDBC42();

        if (SQLServerStatement.loggerExternal.isLoggable(java.util.logging.Level.FINER))
            SQLServerStatement.loggerExternal.entering(ps.getClassNameLogging(), "setObject",
                    new Object[] {parameterIndex, x, targetSqlType, scaleOrLength});

        // getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
        ps.setObject(parameterIndex, x, targetSqlType.getVendorTypeNumber(), scaleOrLength);

        SQLServerStatement.loggerExternal.exiting(ps.getClassNameLogging(), "setObject");
    }

    static final void setObject(SQLServerPreparedStatement ps,
            int parameterIndex,
            Object x,
            SQLType targetSqlType,
            Integer precision,
            Integer scale) throws SQLServerException {
        DriverJDBCVersion.checkSupportsJDBC42();

        if (SQLServerStatement.loggerExternal.isLoggable(java.util.logging.Level.FINER))
            SQLServerStatement.loggerExternal.entering(ps.getClassNameLogging(), "setObject",
                    new Object[] {parameterIndex, x, targetSqlType, precision, scale});

        // getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
        ps.setObject(parameterIndex, x, targetSqlType.getVendorTypeNumber(), precision, scale, false);

        SQLServerStatement.loggerExternal.exiting(ps.getClassNameLogging(), "setObject");
    }

    static final void setObject(SQLServerPreparedStatement ps,
            int parameterIndex,
            Object x,
            SQLType targetSqlType,
            Integer precision,
            Integer scale,
            boolean forceEncrypt) throws SQLServerException {
        DriverJDBCVersion.checkSupportsJDBC42();

        if (SQLServerStatement.loggerExternal.isLoggable(java.util.logging.Level.FINER))
            SQLServerStatement.loggerExternal.entering(ps.getClassNameLogging(), "setObject",
                    new Object[] {parameterIndex, x, targetSqlType, precision, scale, forceEncrypt});

        // getVendorTypeNumber() returns the same constant integer values as in java.sql.Types
        ps.setObject(parameterIndex, x, targetSqlType.getVendorTypeNumber(), precision, scale, forceEncrypt);

        SQLServerStatement.loggerExternal.exiting(ps.getClassNameLogging(), "setObject");
    }
}
