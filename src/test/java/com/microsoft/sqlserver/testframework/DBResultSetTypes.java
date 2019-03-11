/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.testframework;

/**
 * @author Microsoft
 *
 */

public enum DBResultSetTypes {

    TYPE_FORWARD_ONLY_CONCUR_READ_ONLY(DBConstants.RESULTSET_TYPE_FORWARD_ONLY, DBConstants.RESULTSET_CONCUR_READ_ONLY),
    TYPE_SCROLL_INSENSITIVE_CONCUR_READ_ONLY(DBConstants.RESULTSET_TYPE_SCROLL_INSENSITIVE, DBConstants.RESULTSET_CONCUR_READ_ONLY),
    TYPE_SCROLL_SENSITIVE_CONCUR_READ_ONLY(DBConstants.RESULTSET_TYPE_SCROLL_SENSITIVE, DBConstants.RESULTSET_CONCUR_READ_ONLY),
    TYPE_FORWARD_ONLY_CONCUR_UPDATABLE(DBConstants.RESULTSET_TYPE_FORWARD_ONLY, DBConstants.RESULTSET_CONCUR_UPDATABLE),
    TYPE_SCROLL_SENSITIVE_CONCUR_UPDATABLE(DBConstants.RESULTSET_TYPE_SCROLL_SENSITIVE, DBConstants.RESULTSET_CONCUR_UPDATABLE),
    TYPE_DYNAMIC_CONCUR_OPTIMISTIC(DBConstants.RESULTSET_TYPE_DYNAMIC, DBConstants.RESULTSET_CONCUR_OPTIMISTIC),
    TYPE_CURSOR_FORWARDONLY_CONCUR_UPDATABLE(DBConstants.RESULTSET_TYPE_CURSOR_FORWARDONLY, DBConstants.RESULTSET_CONCUR_READ_ONLY),;

    public int resultsetCursor;
    public int resultSetConcurrency;

    DBResultSetTypes(int resultSetCursor, int resultSetConcurrency) {
        this.resultsetCursor = resultSetCursor;
        this.resultSetConcurrency = resultSetConcurrency;
    }

}
