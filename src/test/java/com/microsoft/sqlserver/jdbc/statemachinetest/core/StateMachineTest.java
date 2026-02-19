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
 * Central state whiteboard for Model-Based Testing (MBT).
 * Holds state variables (key-value map), registered {@link Action}s, and a
 * seeded {@link java.util.Random}.
 *
 * @see Action
 * @see Engine
 * @see Result
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

    /**
     * Seeded random number generator for reproducibility.
     * Actions should use this via getRandom() instead of creating their own Random
     * or using Math.random().
     * This is set by the Engine before execution to ensure all random choices use
     * the same seed.
     */
    private Random random = new Random();

    public StateMachineTest(String name) {
        this.name = name;
    }

    public StateMachineTest() {
        this.name = "StateMachineTest";
    }

    // ==================== State Management ====================

    /** Sets a state variable using enum key. */
    public void setState(StateKey key, Object value) {
        state.put(key.key(), value);
    }

    /** Gets a state variable using enum key. Returns null if not found. */
    public Object getStateValue(StateKey key) {
        return state.get(key.key());
    }

    /**
     * Gets an integer state variable using enum key. Returns 0 if not found or not
     * a number.
     */
    public int getStateInt(StateKey key) {
        Object v = state.get(key.key());
        return v instanceof Number ? ((Number) v).intValue() : 0;
    }

    /**
     * Gets a boolean state variable using enum key. Returns false if not found or
     * not a boolean.
     */
    public boolean isState(StateKey key) {
        Object v = state.get(key.key());
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

    /**
     * Returns the seeded random number generator.
     * Actions should use this for any random choices to ensure reproducibility.
     * Example: sm.getRandom().nextInt(10) instead of (int)(Math.random() * 10)
     */
    public Random getRandom() {
        return random;
    }

    /**
     * Sets the random number generator. Called by Engine with a seeded Random.
     * 
     * @param random the seeded Random instance
     */
    void setRandom(Random random) {
        this.random = random;
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

    /** Returns actions whose {@code canRun()} is currently {@code true}. */
    public List<Action> getValidActions() {
        List<Action> valid = new ArrayList<>();
        for (Action a : actions) {
            try {
                if (a.canRun()) {
                    valid.add(a);
                }
            } catch (Exception e) {
                System.err.println("WARNING: Action '" + a.name + "' canRun() threw exception: "
                        + e.getClass().getSimpleName() + ": " + e.getMessage());
                e.printStackTrace(System.err);
            }
        }
        return valid;
    }
}
