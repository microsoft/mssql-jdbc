/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.statemachinetest.core;

import java.util.List;


/**
 * Immutable result of a state machine execution.
 * Use {@code seed} to reproduce the exact action sequence.
 */
public class Result {
    /** True if execution completed without errors. */
    public boolean success;

    /** Number of actions executed. */
    public int actionCount;

    /** Total execution time in milliseconds. */
    public long durationMs;

    /** Random seed used - save this to reproduce the test run. */
    public long seed;

    /** Log of action names executed in order. */
    public List<String> log;

    /** Exception if an error occurred, null otherwise. */
    public Exception error;

    public Result(boolean success, int actionCount, long durationMs, long seed, List<String> log,
            Exception error) {
        this.success = success;
        this.actionCount = actionCount;
        this.durationMs = durationMs;
        this.seed = seed;
        this.log = log;
        this.error = error;
    }

    public boolean isSuccess() {
        return success;
    }

    public int getActionCount() {
        return actionCount;
    }

    public long getSeed() {
        return seed;
    }

    @Override
    public String toString() {
        return "Result{success=" + success + ", actions=" + actionCount + ", seed=" + seed + "}";
    }
}
