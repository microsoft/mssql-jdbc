/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.statemachinetest.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;


/**
 * Simple State Machine Testing Framework for JDBC Driver testing.
 * 
 * This framework enables Model-Based Testing (MBT) by randomly exploring valid state transitions.
 * It replaces the complex FX/KoKoMo framework with a simpler, more debuggable approach.
 * 
 * Key design principles:
 * - Single file, no external dependencies
 * - No lambdas or reflection - all actions are plain class implementations
 * - Seed-based reproducibility for debugging failed test runs
 * - Weighted random selection for realistic usage patterns
 */
public class StateMachineTest {

    /** 
     * State storage - holds all variables that define the current state of the system under test.
     * Example: row position, connection status, dirty flags, etc.
     */
    private Map<String, Object> state = new HashMap<>();

    /**
     * List of all possible actions that can be executed on the system under test.
     * Each action has preconditions (canRun) and execution logic (run).
     */
    private List<Action> actions = new ArrayList<>();

    /** Name of this state machine instance, used for logging and debugging. */
    private String name;

    public StateMachineTest(String name) {
        this.name = name;
    }

    public StateMachineTest() {
        this.name = "StateMachineTest";
    }

    // ==================== State Management ====================

    /** Sets a state variable. Used to track system state like row position, flags, etc. */
    public void set(String key, Object value) {
        state.put(key, value);
    }

    /** Gets a state variable. Returns null if not found. */
    public Object get(String key) {
        return state.get(key);
    }

    /** Gets an integer state variable. Returns 0 if not found or not a number. */
    public int getInt(String key) {
        Object v = state.get(key);
        return v instanceof Number ? ((Number) v).intValue() : 0;
    }

    /** Gets a boolean state variable. Returns false if not found or not a boolean. */
    public boolean is(String key) {
        Object v = state.get(key);
        return v instanceof Boolean && (Boolean) v;
    }

    /** Returns a copy of the current state for inspection. */
    public Map<String, Object> getState() {
        return new HashMap<>(state);
    }

    /** Returns the name of this state machine. */
    public String getName() {
        return name;
    }

    // ==================== Action Management ====================

    /** Registers an action that can be executed during exploration. */
    public void addAction(Action a) {
        actions.add(a);
    }

    /** Returns all registered actions. */
    public List<Action> getActions() {
        return actions;
    }

    /** 
     * Returns only actions whose preconditions are currently satisfied.
     * This is called by the Engine to determine which actions can be executed.
     */
    public List<Action> getValidActions() {
        List<Action> valid = new ArrayList<>();
        for (Action a : actions) {
            try {
                if (a.canRun()) {
                    valid.add(a);
                }
            } catch (Exception e) {
                // Skip actions that throw during precondition check
            }
        }
        return valid;
    }

    // ==================== Action Base Class ====================

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
    public static abstract class Action {
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

    // ==================== Execution Engine ====================

    /**
     * Engine that executes state machine exploration.
     * 
     * The engine randomly selects and executes valid actions until:
     * - Maximum action count is reached (configurable)
     * - Timeout occurs (configurable)
     * - No valid actions remain
     * - An error occurs
     * 
     * Uses weighted random selection to simulate realistic usage patterns.
     * Seed-based randomness ensures reproducibility for debugging.
     */
    public static class Engine {
        private StateMachineTest sm;
        private long seed = System.currentTimeMillis();
        private int maxActions = 500;
        private int timeout = 30;

        private Engine(StateMachineTest sm) {
            this.sm = sm;
        }

        /** Creates an engine for the given state machine. */
        public static Engine run(StateMachineTest sm) {
            return new Engine(sm);
        }

        /** Sets the random seed for reproducibility. Use same seed to reproduce a test run. */
        public Engine withSeed(long s) {
            seed = s;
            return this;
        }

        /** Sets maximum number of actions to execute. Default is 500. */
        public Engine withMaxActions(int n) {
            maxActions = n;
            return this;
        }

        /** Sets timeout in seconds. Default is 30. */
        public Engine withTimeout(int s) {
            timeout = s;
            return this;
        }

        /** Executes the state machine exploration and returns the result. */
        public Result execute() {
            Random rand = new Random(seed);
            long start = System.currentTimeMillis();
            long end = start + timeout * 1000L;
            List<String> log = new ArrayList<>();
            Exception error = null;
            int count = 0;

            System.out.println("═══════════════════════════════════════════════════════");
            System.out.println(sm.name + " | Seed:" + seed + " | Max:" + maxActions + " | Timeout:" + timeout + "s");
            System.out.println("═══════════════════════════════════════════════════════");

            try {
                while (count < maxActions && System.currentTimeMillis() < end) {
                    List<Action> valid = sm.getValidActions();
                    if (valid.isEmpty()) {
                        System.out.println("→ No valid actions");
                        break;
                    }

                    // Weighted random selection - higher weight actions are more likely
                    int total = 0;
                    for (Action a : valid) {
                        total += a.weight;
                    }
                    int pick = rand.nextInt(total);
                    int sum = 0;
                    Action selected = valid.get(0);
                    for (Action a : valid) {
                        sum += a.weight;
                        if (pick < sum) {
                            selected = a;
                            break;
                        }
                    }

                    count++;
                    selected.run();
                    log.add(selected.name);
                    System.out.printf("[%3d] %-20s | %s%n", count, selected.name, sm.state);
                }
            } catch (Exception e) {
                error = e;
                System.out.println("ERROR: " + e.getMessage());
            }

            long duration = System.currentTimeMillis() - start;
            System.out.println("═══════════════════════════════════════════════════════");
            System.out.println("Done: " + count + " actions in " + duration + "ms");
            System.out.println("═══════════════════════════════════════════════════════");

            return new Result(error == null, count, duration, seed, log, error);
        }
    }

    // ==================== Execution Result ====================

    /**
     * Result of a state machine execution.
     * 
     * Contains execution statistics and the seed for reproducibility.
     * If a test fails, use the same seed to reproduce the exact sequence.
     */
    public static class Result {
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
}
