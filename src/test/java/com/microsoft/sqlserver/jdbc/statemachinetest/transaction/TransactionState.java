/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.statemachinetest.transaction;

import com.microsoft.sqlserver.jdbc.statemachinetest.core.StateKey;


/**
 * State keys for Transaction state machine testing.
 * 
 * Defines all states tracked during transaction operations:
 * - Connection object
 * - Auto-commit mode
 * - Connection closed status
 * - Table name for queries
 */
public enum TransactionState implements StateKey {

    /** The JDBC Connection object. */
    CONN("conn"),

    /** Auto-commit mode (boolean). True = auto-commit enabled. */
    AUTO_COMMIT("autoCommit"),

    /** Connection closed status (boolean). True = connection is closed. */
    CLOSED("closed"),

    /** Table name used in queries (String). */
    TABLE_NAME("tableName");

    private final String key;

    TransactionState(String key) {
        this.key = key;
    }

    @Override
    public String key() {
        return key;
    }
}
