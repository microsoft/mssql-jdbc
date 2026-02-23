/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.statemachinetest.transaction;

import com.microsoft.sqlserver.jdbc.statemachinetest.core.StateKey;


/**
 * State keys for Transaction state machine.
 * 
 * Represents the state of a JDBC Connection's transaction behavior,
 * including test validation state for tracking expected vs pending values.
 */
public enum TransactionState implements StateKey {

    /** The JDBC Connection object. */
    CONN("conn"),

    /** Auto-commit mode (boolean). True = auto-commit enabled. */
    AUTO_COMMIT("autoCommit"),

    /** Connection closed status (boolean). True = connection is closed. */
    CLOSED("closed"),

    /** Last committed value (Integer) - test artifact for validation. */
    EXPECTED_VALUE("expectedValue"),

    /** Uncommitted value after UPDATE (Integer) - test artifact for validation. */
    PENDING_VALUE("pendingValue");

    private final String key;

    TransactionState(String key) {
        this.key = key;
    }

    @Override
    public String key() {
        return key;
    }
}
