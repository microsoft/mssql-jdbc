/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.statemachinetest.core;

import java.util.Collections;
import java.util.List;


/**
 * Immutable result of a state machine execution.
 * Use {@code seed} to reproduce the exact action sequence.
 */
public class Result {
    /** True if execution completed without errors. */
    public final boolean success;

    /** Number of actions executed. */
    public final int actionCount;

    /** Total execution time in milliseconds. */
    public final long durationMs;

    /** Random seed used - save this to reproduce the test run. */
    public final long seed;

    /** Log of action names executed in order. */
    public final List<String> log;

    /** Throwable if an error occurred, null otherwise. */
    public final Throwable error;

    public Result(boolean success, int actionCount, long durationMs, long seed, List<String> log,
            Throwable error) {
        this.success = success;
        this.actionCount = actionCount;
        this.durationMs = durationMs;
        this.seed = seed;
        this.log = log != null ? Collections.unmodifiableList(log) : Collections.emptyList();
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
