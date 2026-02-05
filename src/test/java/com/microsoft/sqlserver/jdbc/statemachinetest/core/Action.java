/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.statemachinetest.core;


/**
 * Abstract base class for state machine actions.
 * 
 * Each action represents an operation on the system under test (e.g., ResultSet.next()).
 * Subclasses must implement:
 * - canRun(): Returns true if the action can be executed in the current state
 * - run(): Executes the action and updates state accordingly
 * 
 * Using abstract class instead of lambdas allows:
 * - Easier debugging with breakpoints in canRun() and run()
 * - Clear stack traces when errors occur
 * - Better code organization for complex preconditions
 */
public abstract class Action {
    /** Name of this action, used for logging. */
    public String name;

    /** 
     * Weight for random selection. Higher weight = more likely to be selected.
     * Use higher weights for common operations (e.g., next=10) and lower for rare ones (e.g., close=1).
     */
    public int weight;

    public Action(String name, int weight) {
        this.name = name;
        this.weight = weight;
    }

    /**
     * Precondition check - returns true if this action can be executed.
     * Override to check state variables like !closed, isValidRow, etc.
     */
    public abstract boolean canRun();

    /**
     * Executes the action and updates state.
     * Override to call the actual method and update state variables.
     */
    public abstract void run() throws Exception;
}
