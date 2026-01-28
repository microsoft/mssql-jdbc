/*
 * Microsoft JDBC Driver for SQL Server
 * Model-Based Testing Framework for JUnit 5
 */
package com.microsoft.sqlserver.jdbc.mbt.core;

/**
 * Weight schemes for action selection during model exploration.
 */
public enum WeightScheme {
    
    /**
     * All actions have equal probability (ignores weight values).
     * Best for uniform random exploration.
     */
    WEIGHT_SCHEME_FLAT,
    
    /**
     * Actions are selected based on their weight values.
     * Higher weight = higher probability of selection.
     * Good for prioritizing important state transitions.
     */
    WEIGHT_SCHEME_WEIGHTED,
    
    /**
     * Prioritizes actions that have been called least.
     * Ensures all actions get exercised.
     */
    WEIGHT_SCHEME_CALL_COUNT_INVERSE,
    
    /**
     * Combines weighted selection with call count inverse.
     * Balances between priority and coverage.
     */
    WEIGHT_SCHEME_WEIGHTED_CALL_COUNT,
    
    /**
     * Round-robin through available actions.
     * Deterministic but comprehensive coverage.
     */
    WEIGHT_SCHEME_ROUND_ROBIN,
    
    /**
     * Favors actions that lead to unexplored states.
     * Best for maximizing state space coverage.
     */
    WEIGHT_SCHEME_STATE_COVERAGE,
    
    /**
     * Favors actions that increase edge coverage.
     * Best for maximizing transition coverage.
     */
    WEIGHT_SCHEME_EDGE_COVERAGE
}
