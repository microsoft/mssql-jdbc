/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.statemachinetest.resultset;

import com.microsoft.sqlserver.jdbc.statemachinetest.core.StateKey;


/**
 * State keys for ResultSet state machine testing.
 * 
 * Defines all states tracked during ResultSet operations:
 * - ResultSet object
 * - Closed status
 * - Valid row position
 */
public enum ResultSetState implements StateKey {

    RS("rs"),
    CLOSED("closed"),
    ON_VALID_ROW("onValidRow"),
    STMT("stmt"),
    CONN("conn"),
    ON_INSERT_ROW("onInsertRow"),
    ROW_DELETED("rowDeleted"),
    ROW_COUNT("rowCount"),
    TABLE_NAME("tableName"),
    IS_UPDATABLE("isUpdatable"),
    IS_SCROLLABLE("isScrollable");

    private final String key;

    ResultSetState(String key) {
        this.key = key;
    }

    @Override
    public String key() {
        return key;
    }
}
