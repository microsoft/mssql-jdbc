/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.statemachinetest.core;

import java.util.Objects;

/**
 * Abstract base class for actions with test validation support.
 * 
 * Extends {@link Action} to add test behavior capabilities:
 * - DataCache for expected row data (ResultSet validation)
 * - Validation context for test artifacts (table names, expected values)
 * - Assertion methods for validation
 * - ValidationException for failures
 * 
 * This separates:
 * - Driver behavior (pure JDBC operations in Action)
 * - Test behavior (validation infrastructure in ValidatedAction)
 * 
 * Example with validation:
 * <pre>
 * public class NextAction extends ValidatedAction {
 *     private final StateMachineTest sm;
 * 
 *     public NextAction(StateMachineTest sm) {
 *         super("next", 10);
 *         this.sm = sm;
 *     }
 * 
 *     public void run() throws Exception {
 *         // Driver behavior: actual JDBC operation
 *         ResultSet rs = (ResultSet) sm.getStateValue(RS);
 *         boolean valid = rs.next();
 *         sm.setState(ON_VALID_ROW, valid);
 *         
 *         // Update test tracking (CURRENT_ROW is test artifact)
 *         if (valid) {
 *             int row = sm.getStateInt(CURRENT_ROW);
 *             sm.setState(CURRENT_ROW, row + 1);
 *         }
 *     }
 * 
 *     public void validate() throws Exception {
 *         // Test behavior: verify against expected data
 *         if (sm.isState(ON_VALID_ROW) && hasValidationData()) {
 *             verifyCurrentRow();
 *         }
 *     }
 * }
 * </pre>
 * 
 * Test setup:
 * <pre>
 * // Create validation data
 * DataCache cache = new DataCache();
 * cache.addColumn("name", "VARCHAR");
 * cache.addRow(Map.of("name", "Alice"));
 * 
 * // Preload into action
 * sm.addAction(new NextAction(sm).preload(cache));
 * </pre>
 */
public abstract class ValidatedAction extends Action {
    
    /** 
     * Validation data cache - stores expected row data for ResultSet validation.
     * Test artifact: not part of driver domain.
     */
    protected DataCache dataCache;

    /** 
     * Additional validation context - stores test artifacts like:
     * - Table names (transaction tests)
     * - Expected/pending values (commit/rollback validation)
     * - Custom test tracking data
     * 
     * Test artifact: not part of driver domain.
     */
    protected Object validationContext;

    public ValidatedAction(String name, int weight) {
        super(name, weight);
    }

    // ==================== Validation Data Management ====================

    /**
     * Preloads validation data into this action.
     * Call this before executing the action to set up expected values.
     * 
     * @param cache the DataCache with expected data
     * @return this action (for fluent chaining)
     */
    public ValidatedAction preload(DataCache cache) {
        this.dataCache = cache;
        return this;
    }

    /**
     * Loads validation data from a source.
     * Convenience method for setting up expected values.
     * 
     * @param cache the DataCache with expected data
     * @return this action (for fluent chaining)
     */
    public ValidatedAction load(DataCache cache) {
        return preload(cache);
    }

    /**
     * Populates validation context (table names, expected values, etc.).
     * Use this for validation data that's not in DataCache format.
     * 
     * NOTE: Domain-specific base classes (TransactionValidatedAction, ResultSetValidatedAction)
     * provide built-in fields and setters, eliminating the need for separate context objects.
     * Prefer extending domain-specific base classes over using this generic method.
     * 
     * Example with domain-specific base class:
     * <pre>
     * CommitAction action = new CommitAction();
     * action.setTableName(tableName);
     * action.setExpectedValue(100);
     * </pre>
     * 
     * @param context validation context object (deprecated pattern)
     * @return this action (for fluent chaining)
     */
    public ValidatedAction populate(Object context) {
        this.validationContext = context;
        return this;
    }

    /**
     * Gets the DataCache for validation.
     * 
     * @return the DataCache or null if not set
     */
    protected DataCache getDataCache() {
        return dataCache;
    }

    /**
     * Gets the validation context.
     * 
     * @return the validation context or null if not set
     */
    protected Object getValidationContext() {
        return validationContext;
    }

    /**
     * Checks if validation data is available.
     * 
     * @return true if validation data exists
     */
    protected boolean hasValidationData() {
        return dataCache != null || validationContext != null;
    }

    // ==================== Validation Framework ====================

    /**
     * Asserts expected value matches actual within validation.
     * Use this in your validate() method to check results.
     * 
     * @param actual   the actual value obtained from operation
     * @param expected the expected value
     * @param message  context message for validation failure
     * @throws ValidationException if values don't match
     */
    public final void assertExpected(Object actual, Object expected, String message) {
        if (!Objects.equals(expected, actual)) {
            throw new ValidationException(message, actual, expected);
        }
    }

    /**
     * Asserts expected int value matches actual within validation.
     */
    public final void assertExpected(int actual, int expected, String message) {
        if (actual != expected) {
            throw new ValidationException(message, actual, expected);
        }
    }

    /**
     * Exception thrown when validation fails.
     * 
     * Contains actual and expected values for debugging.
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
