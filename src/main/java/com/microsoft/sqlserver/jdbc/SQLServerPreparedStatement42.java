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
 * This class is separated from SQLServerPreparedStatement class in order to resolve compiling error of missing Java 8 Types when running with Java 7.
 * 
 * This class will be initialized instead of SQLServerPreparedStatement when Java 8 and JDBC 4.2 are used.
 * 
 * It shares the same PreparedStatement implementation with SQLServerCallableStatement42.
 * 
 */
public class SQLServerPreparedStatement42 extends SQLServerPreparedStatement implements ISQLServerPreparedStatement42 {

    SQLServerPreparedStatement42(SQLServerConnection conn,
            String sql,
            int nRSType,
            int nRSConcur,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLServerException {
        super(conn, sql, nRSType, nRSConcur, stmtColEncSetting);
    }

    public final void setObject(int index,
            Object obj,
            SQLType jdbcType) throws SQLServerException {
        SQLServerPreparedStatement42Helper.setObject(this, index, obj, jdbcType);
    }

    public final void setObject(int parameterIndex,
            Object x,
            SQLType targetSqlType,
            int scaleOrLength) throws SQLServerException {
        SQLServerPreparedStatement42Helper.setObject(this, parameterIndex, x, targetSqlType, scaleOrLength);
    }

    public final void setObject(int parameterIndex,
            Object x,
            SQLType targetSqlType,
            Integer precision,
            Integer scale) throws SQLServerException {
        SQLServerPreparedStatement42Helper.setObject(this, parameterIndex, x, targetSqlType, precision, scale);
    }

    public final void setObject(int parameterIndex,
            Object x,
            SQLType targetSqlType,
            Integer precision,
            Integer scale,
            boolean forceEncrypt) throws SQLServerException {
        SQLServerPreparedStatement42Helper.setObject(this, parameterIndex, x, targetSqlType, precision, scale, forceEncrypt);
    }
}
