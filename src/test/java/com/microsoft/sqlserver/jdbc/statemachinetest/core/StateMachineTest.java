/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.statemachinetest.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;


/**
 * Central container for Model-Based Testing (MBT).
 * Internally owns a {@link DataCache}, registered {@link Action}s,
 * and a seeded {@link java.util.Random}.
 *
 * <p>
 * The DataCache is created internally with an empty state row (row 0).
 * Tests populate state via {@code getDataCache().updateValue(0, key, value)}
 * and add expected data rows via {@code getDataCache().addRow(row)}.
 *
 * <p>
 * State management is handled by {@link Action} convenience methods
 * ({@code setState}, {@code getState}, {@code isState}, {@code getStateInt})
 * which read/write DataCache row 0. {@code StateMachineTest} itself does not
 * provide state mutation — it is simply the container that links actions
 * to their shared DataCache.
 *
 * @see Action
 * @see Engine
 * @see Result
 */
public class StateMachineTest {

    /**
     * Internally owned DataCache — single source of truth for all state and test
     * data.
     * Row 0 holds state variables (keyed by {@link StateKey#key()}).
     * Rows 1+ hold expected test data (if any).
     * Created at construction with an empty state row; auto-linked to all
     * actions via {@link #addAction}.
     */
    private final DataCache dataCache;

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

    /**
     * Creates a state machine with an internally-owned DataCache.
     * Row 0 (state row) is pre-initialized as an empty map.
     * Use {@link #getDataCache()} to populate state and add data rows.
     *
     * @param name display name for logging
     */
    public StateMachineTest(String name) {
        this.name = name;
        this.dataCache = new DataCache();
        this.dataCache.addRow(new HashMap<>()); // Row 0: empty state row
    }

    /** Returns the underlying DataCache backing this state machine. */
    public DataCache getDataCache() {
        return dataCache;
    }

    /** Returns the name of this state machine. */
    public String getName() {
        return name;
    }

    /**
     * Returns the seeded random number generator.
     * Actions should use this for any random choices to ensure reproducibility.
     * Example: getRandom().nextInt(10) instead of (int)(Math.random() * 10)
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

    /**
     * Registers an action and auto-links it to this state machine's DataCache
     * and Random. After this call, the action can use {@code setState()},
     * {@code getState()}, {@code isState()}, {@code getStateInt()}, and
     * {@code getRandom()}.
     */
    public void addAction(Action a) {
        a.dataCache = dataCache;
        a.sm = this;
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
