/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.statemachinetest.core;

import java.util.Objects;

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
 * @see Engine
 * @see StateMachineTest
 */
public abstract class Action {
    /** Name of this action, used for logging. */
    public final String name;

    /** Weight for random selection. Higher weight = more likely to be selected. */
    public final int weight;

    /** Expected row data for validation. */
    protected DataCache dataCache;

    public Action(String name, int weight) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        if (weight < 0) {
            throw new IllegalArgumentException("weight must be >= 0, got: " + weight);
        }
        this.weight = weight;
    }

    /**
     * Precondition check - returns true if this action can be executed.
     * Override to check state variables like !closed, isValidRow, etc.
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

    /** @return the DataCache, or {@code null} if not set */
    public DataCache getDataCache() {
        return dataCache;
    }

    /** Sets the expected data cache for validation. */
    public void setDataCache(DataCache cache) {
        this.dataCache = cache;
    }

    /** @return {@code true} if dataCache exists and is not empty */
    protected boolean hasDataCache() {
        return dataCache != null && !dataCache.isEmpty();
    }

    /**
     * Asserts that {@code actual} equals {@code expected}.
     *
     * @throws ValidationException if values don't match
     */
    public final void assertExpected(Object actual, Object expected, String message) {
        if (!Objects.equals(expected, actual)) {
            throw new ValidationException(message, actual, expected);
        }
    }

    /** Asserts that {@code actual} equals {@code expected} (int overload). */
    public final void assertExpected(int actual, int expected, String message) {
        if (actual != expected) {
            throw new ValidationException(message, actual, expected);
        }
    }

    /**
     * Thrown when a validation assertion fails. Carries actual and expected values.
     */
    public static class ValidationException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private final Object actual;
        private final Object expected;

        public ValidationException(String message, Object actual, Object expected) {
            super(formatMessage(message, actual, expected));
            this.actual = actual;
            this.expected = expected;
        }

        public Object getActual() {
            return actual;
        }

        public Object getExpected() {
            return expected;
        }

        private static String formatMessage(String message, Object actual, Object expected) {
            return message + "\n  Expected: " + expected + "\n  Actual:   " + actual;
        }
    }
}
