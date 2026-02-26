/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.statemachinetest.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Objects;
import java.util.Random;

/**
 * Abstract base class for state machine actions with validation support.
 *
 * <p>
 * Template Method Pattern — lifecycle:
 * {@code canRun() -> execute() -> run() -> validate()}.
 * Subclasses implement {@code canRun()} and {@code run()}; override
 * {@code validate()} for
 * data verification. The framework calls {@code execute()} (final) which
 * invokes
 * {@code run()} then {@code validate()}.
 *
 * <p>
 * State management: Actions access state through their {@link DataCache}
 * (row 0) using convenience methods {@link #setState}, {@link #getState},
 * {@link #isState}, {@link #getStateInt}. Both {@code dataCache} and
 * {@code sm} are auto-linked when the action is added to a
 * {@link StateMachineTest} via {@link StateMachineTest#addAction}.
 *
 * @see Engine
 * @see StateMachineTest
 */
public abstract class Action {
    /** Name of this action, used for logging. */
    public final String name;

    /** Weight for random selection. Higher weight = more likely to be selected. */
    public final int weight;

    /**
     * Shared DataCache — single source of truth for all state and test data.
     * Row 0 holds state variables (keyed by {@link StateKey#key()}).
     * Rows 1+ hold expected test data (if any).
     * Auto-linked by {@link StateMachineTest#addAction}.
     */
    protected DataCache dataCache;

    /**
     * Parent state machine. Provides access to {@link #getRandom()} and
     * other framework features. Auto-linked by {@link StateMachineTest#addAction}.
     */
    protected StateMachineTest sm;

    public Action(String name, int weight) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        if (weight < 0) {
            throw new IllegalArgumentException("weight must be >= 0, got: " + weight);
        }
        this.weight = weight;
    }

    // ==================== State Convenience Methods ====================

    /**
     * Sets a state variable in DataCache row 0.
     *
     * @param key   the state key
     * @param value the value to store
     */
    public void setState(StateKey key, Object value) {
        dataCache.updateValue(0, key.key(), value);
    }

    /**
     * Gets a state variable from DataCache row 0.
     *
     * @param key the state key
     * @return the stored value, or {@code null} if not set
     */
    public Object getState(StateKey key) {
        return dataCache.getValue(0, key.key());
    }

    /**
     * Gets a boolean state variable from DataCache row 0.
     * Returns {@code false} if not set or not a Boolean.
     *
     * @param key the state key
     * @return the boolean value
     */
    public boolean isState(StateKey key) {
        Object v = dataCache.getValue(0, key.key());
        return v instanceof Boolean && (Boolean) v;
    }

    /**
     * Gets an integer state variable from DataCache row 0.
     * Returns {@code 0} if not set or not a Number.
     *
     * @param key the state key
     * @return the integer value
     */
    public int getStateInt(StateKey key) {
        Object v = dataCache.getValue(0, key.key());
        return v instanceof Number ? ((Number) v).intValue() : 0;
    }

    /**
     * Returns the seeded random number generator from the parent state machine.
     * Use this for any random choices to ensure reproducibility.
     *
     * @return the seeded Random instance
     */
    public Random getRandom() {
        return sm.getRandom();
    }

    // ==================== Lifecycle Methods ====================

    /**
     * Precondition check - returns true if this action can be executed.
     * Override to check state variables like !isState(CLOSED), etc.
     */
    public abstract boolean canRun();

    /**
     * Executes the action and updates state.
     * Override to call the actual JDBC method and update domain state variables.
     * Focus on operation logic; validation happens in validate() hook.
     */
    public abstract void run() throws Exception;

    /**
     * Validation hook called after {@code run()}. Override to verify results.
     * Default is no-op.
     */
    public void validate() throws Exception {
        // Default: no validation
    }

    /**
     * Framework-controlled execution: calls {@code run()} then {@code validate()}.
     * Called by {@link Engine}. Do not override.
     *
     * @throws Exception if operation or validation fails
     */
    public final void execute() throws Exception {
        run();
        validate();
    }

    // ==================== DataCache Access ====================

    /** @return the DataCache, or {@code null} if not yet linked */
    public DataCache getDataCache() {
        return dataCache;
    }

    /** @return {@code true} if dataCache has data rows beyond state (row 0) */
    protected boolean hasDataCache() {
        return dataCache != null && dataCache.getRowCount() > 1;
    }

    /**
     * Asserts that {@code actual} equals {@code expected}.
     *
     * @throws AssertionFailedError if values don't match
     */
    public final void assertExpected(Object actual, Object expected, String message) {
        assertEquals(expected, actual, message);
    }
}
