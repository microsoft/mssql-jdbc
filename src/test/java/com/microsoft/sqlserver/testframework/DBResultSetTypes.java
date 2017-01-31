/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.testframework;

/**
 * @author Microsoft
 *
 */

public enum DBResultSetTypes {

    TYPE_FORWARD_ONLY_CONCUR_READ_ONLY(DBResultSet.TYPE_FORWARD_ONLY, DBResultSet.CONCUR_READ_ONLY),
    TYPE_SCROLL_INSENSITIVE_CONCUR_READ_ONLY(DBResultSet.TYPE_SCROLL_INSENSITIVE, DBResultSet.CONCUR_READ_ONLY),
    TYPE_SCROLL_SENSITIVE_CONCUR_READ_ONLY(DBResultSet.TYPE_SCROLL_SENSITIVE, DBResultSet.CONCUR_READ_ONLY),
    TYPE_FORWARD_ONLY_CONCUR_UPDATABLE(DBResultSet.TYPE_FORWARD_ONLY, DBResultSet.CONCUR_UPDATABLE),
    TYPE_SCROLL_SENSITIVE_CONCUR_UPDATABLE(DBResultSet.TYPE_SCROLL_SENSITIVE, DBResultSet.CONCUR_UPDATABLE),
    TYPE_DYNAMIC_CONCUR_OPTIMISTIC(DBResultSet.TYPE_DYNAMIC, DBResultSet.CONCUR_OPTIMISTIC),
    TYPE_CURSOR_FORWARDONLY_CONCUR_UPDATABLE(DBResultSet.TYPE_CURSOR_FORWARDONLY, DBResultSet.CONCUR_READ_ONLY),;

    public int resultsetCursor;
    public int resultSetConcurrency;

    DBResultSetTypes(int resultSetCursor,
            int resultSetConcurrency) {
        this.resultsetCursor = resultSetCursor;
        this.resultSetConcurrency = resultSetConcurrency;
    }

}
