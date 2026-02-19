/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.statemachinetest.core;

/**
 * Abstract base class for state machine actions.
 * 
 * Represents the framework interface for all actions in the state machine.
 * Pure domain actions (driver behavior) can extend this directly.
 * Test actions with validation should extend {@link ValidatedAction}.
 * 
 * Template Method Pattern: The framework controls the execution lifecycle:
 * 1. canRun() - Check if action can execute
 * 2. execute() - Framework-controlled execution (calls run() then validate())
 * 3. run() - Concrete action implementation (actual JDBC operation)
 * 4. validate() - Optional validation hook (override if needed)
 * 
 * Example of pure driver action:
 * 
 * <pre>
 * public class CloseAction extends Action {
 *     public CloseAction(StateMachineTest sm) {
 *         super("close", 1);
 *     }
 * 
 *     public boolean canRun() {
 *         return !sm.isState(CLOSED);
 *     }
 * 
 *     public void run() throws Exception {
 *         rs.close();
 *         sm.setState(CLOSED, true);
 *     }
 *     // No validation needed - pure driver behavior
 * }
 * </pre>
 * 
 * For actions that need validation, extend {@link ValidatedAction} instead.
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
     * Override to call the actual JDBC method and update domain state variables.
     * Focus on operation logic; validation happens in validate() hook.
     */
    public abstract void run() throws Exception;

    /**
     * Validation hook called by framework after run().
     * Override to validate operation results against expected data.
     * Default implementation does nothing (no validation).
     * 
     * For validation support with assertions and data caching,
     * extend {@link ValidatedAction} instead.
     */
    public void validate() throws Exception {
        // Default: no validation
    }

    /**
     * Framework-controlled execution: run operation then validate.
     * Called by Engine. Don't override this - override run() and validate()
     * instead.
     * 
     * @throws Exception if operation or validation fails
     */
    public final void execute() throws Exception {
        run();
        validate();
    }
}
