/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.statemachinetest.transaction;

import com.microsoft.sqlserver.jdbc.statemachinetest.core.ValidatedAction;

/**
 * Base class for transaction-specific validated actions.
 * 
 * Domain-specific validation support for transaction actions.
 * Provides built-in test infrastructure without separate context class.
 * 
 * Test artifacts (not driver domain):
 * - tableName: Database table for testing
 * - expectedValue: Last committed value
 * - pendingValue: Uncommitted value
 * 
 * All transaction actions share these fields through inheritance.
 * Actions can read/write these fields to coordinate validation.
 * 
 * Example:
 * <pre>
 * public class CommitAction extends TransactionValidatedAction {
 *     public void run() {
 *         conn.commit();
 *         // Coordinate through shared fields
 *         expectedValue = pendingValue;
 *         pendingValue = null;
 *     }
 *     
 *     public void validate() {
 *         int actual = queryDatabase(tableName);
 *         assertExpected(actual, expectedValue, "Commit validation");
 *     }
 * }
 * </pre>
 * 
 * Setup:
 * <pre>
 * CommitAction commit = new CommitAction(sm);
 * commit.tableName = TABLE_NAME;
 * commit.expectedValue = 100;
 * sm.addAction(commit);
 * </pre>
 */
public abstract class TransactionValidatedAction extends ValidatedAction {
    
    /** Database table being tested (test artifact). */
    protected String tableName;
    
    /** Last committed value - what SELECT should return (test artifact). */
    protected Integer expectedValue;
    
    /** Value after UPDATE but before COMMIT (test artifact). */
    protected Integer pendingValue;
    
    public TransactionValidatedAction(String name, int weight) {
        super(name, weight);
    }
    
    // Simple accessors for test setup
    
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
    public void setExpectedValue(Integer expectedValue) {
        this.expectedValue = expectedValue;
    }
    
    public void setPendingValue(Integer pendingValue) {
        this.pendingValue = pendingValue;
    }
}
