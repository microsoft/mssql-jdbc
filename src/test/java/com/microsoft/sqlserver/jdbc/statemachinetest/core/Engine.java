/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.statemachinetest.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;


/**
 * Execution engine for state machine exploration.
 * Performs weighted random action selection with seed-based reproducibility.
 * Stops when max actions, timeout, no valid actions, or an error occurs.
 */
public class Engine {
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

    /** Sets the random seed for reproducibility. */
    public Engine withSeed(long s) {
        seed = s;
        return this;
    }

    /** Sets maximum actions to execute. Default 500. */
    public Engine withMaxActions(int n) {
        if (n < 1) {
            throw new IllegalArgumentException("maxActions must be at least 1, got: " + n);
        }
        maxActions = n;
        return this;
    }

    /** Sets timeout in seconds. Default 30. */
    public Engine withTimeout(int s) {
        if (s < 1) {
            throw new IllegalArgumentException("timeout must be at least 1 second, got: " + s);
        }
        timeout = s;
        return this;
    }

    /** Executes the state machine exploration and returns the result. */
    public Result execute() {
        Random rand = new Random(seed);
        // Set the seeded Random on StateMachineTest so actions can use it for reproducibility
        sm.setRandom(rand);
        long start = System.currentTimeMillis();
        long end = start + timeout * 1000L;
        List<String> log = new ArrayList<>();
        Exception error = null;
        int count = 0;

        System.out.println(sm.getName() + " | Seed:" + seed + " | Max:" + maxActions + " | Timeout:" + timeout + "s");

        try {
            while (count < maxActions && System.currentTimeMillis() < end) {
                List<Action> valid = sm.getValidActions();
                if (valid.isEmpty()) {
                    System.out.println("No valid actions");
                    break;
                }

                // Weighted random selection - higher weight actions are more likely
                int total = 0;
                for (Action a : valid) {
                    total += a.weight;
                }

                Action selected;
                if (total == 0) {
                    // All actions have zero weight - use uniform random selection
                    selected = valid.get(rand.nextInt(valid.size()));
                } else {
                    int pick = rand.nextInt(total);
                    int sum = 0;
                    selected = valid.get(0);
                    for (Action a : valid) {
                        sum += a.weight;
                        if (pick < sum) {
                            selected = a;
                            break;
                        }
                    }
                }

                count++;
                selected.execute();  // Framework calls execute() which runs then validates
                log.add(selected.name);
                System.out.println(String.format("[%3d] %-20s | %s", count, selected.name, sm.getState()));
            }
        } catch (Exception e) {
            error = e;
            String msg = e.getMessage() != null ? e.getMessage() : "(no message)";
            System.out.println("ERROR: " + e.getClass().getSimpleName() + ": " + msg);
            e.printStackTrace(System.err);
        }

        long duration = System.currentTimeMillis() - start;
        System.out.println("Done: " + count + " actions in " + duration + "ms");

        return new Result(error == null, count, duration, seed, log, error);
    }
}
