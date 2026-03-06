/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.statemachinetest.statement;

import com.microsoft.sqlserver.jdbc.statemachinetest.core.StateKey;


/**
 * State keys for Statement Execution state machine testing. Tracks all states
 * during statement execution operations modeled after FX fxStatement model variables.
 */
public enum StatementExecutionState implements StateKey {

    CONN("conn"),
    STMT("stmt"),
    CLOSED("closed"),
    EXECUTED("executed"),
    HAS_RESULT_SET("hasResultSet"),
    LAST_EXECUTE_WAS_BATCH("lastExecuteWasBatch"),
    LAST_EXECUTE_WAS_UPDATE("lastExecuteWasUpdate"),
    LAST_EXECUTE_GENERATED_KEYS("lastExecuteGeneratedKeys"),
    MAX_ROWS("maxRows"),
    MAX_FIELD_SIZE("maxFieldSize"),
    HAS_WARNINGS("hasWarnings"),
    FETCH_SIZE("fetchSize"),
    CURSOR_TYPE("cursorType"),
    CONCURRENCY("concurrency"),
    HOLDABILITY("holdability"),
    QUERY_INDEX("queryIndex"),
    TABLE_NAME("tableName"),
    ROW_COUNT("rowCount");

    private final String key;

    StatementExecutionState(String key) {
        this.key = key;
    }

    @Override
    public String key() {
        return key;
    }
}
