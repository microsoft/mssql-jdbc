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

    /** The JDBC ResultSet object. */
    RS("rs"),

    /** ResultSet closed status (boolean). True = ResultSet is closed. */
    CLOSED("closed"),

    /** Whether cursor is on a valid row (boolean). True = on valid row. */
    ON_VALID_ROW("onValidRow");

    private final String key;

    ResultSetState(String key) {
        this.key = key;
    }

    @Override
    public String key() {
        return key;
    }
}
