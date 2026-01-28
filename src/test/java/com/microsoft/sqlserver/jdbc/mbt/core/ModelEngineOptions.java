/*
 * Microsoft JDBC Driver for SQL Server
 * Model-Based Testing Framework for JUnit 5
 */
package com.microsoft.sqlserver.jdbc.mbt.core;

/**
 * Configuration options for the ModelEngine.
 * Controls seed, timeout, max actions, and weight schemes.
 */
public class ModelEngineOptions {
    
    private long seed;
    private int timeout;
    private int maxActions;
    private WeightScheme weightScheme;
    private boolean verbose;
    private boolean failFast;
    private int retryCount;
    
    /**
     * Default constructor with sensible defaults.
     */
    public ModelEngineOptions() {
        this.seed = System.currentTimeMillis();
        this.timeout = 30;  // 30 seconds default
        this.maxActions = 500;  // 500 actions default
        this.weightScheme = WeightScheme.WEIGHT_SCHEME_FLAT;
        this.verbose = false;
        this.failFast = true;
        this.retryCount = 0;
    }
    
    /**
     * Copy constructor.
     */
    public ModelEngineOptions(ModelEngineOptions other) {
        this.seed = other.seed;
        this.timeout = other.timeout;
        this.maxActions = other.maxActions;
        this.weightScheme = other.weightScheme;
        this.verbose = other.verbose;
        this.failFast = other.failFast;
        this.retryCount = other.retryCount;
    }
    
    // Builder-style setters
    
    /**
     * Sets the random seed for reproducible test paths.
     * When a test fails, rerun with same seed to reproduce exact sequence.
     * 
     * @param seed the random seed (typically a timestamp)
     * @return this options object for chaining
     */
    public ModelEngineOptions withSeed(long seed) {
        this.seed = seed;
        return this;
    }
    
    /**
     * Sets the maximum time in seconds before stopping exploration.
     * 
     * @param timeoutSeconds timeout in seconds
     * @return this options object for chaining
     */
    public ModelEngineOptions withTimeout(int timeoutSeconds) {
        this.timeout = timeoutSeconds;
        return this;
    }
    
    /**
     * Sets the maximum number of actions to execute.
     * 
     * @param maxActions maximum action count
     * @return this options object for chaining
     */
    public ModelEngineOptions withMaxActions(int maxActions) {
        this.maxActions = maxActions;
        return this;
    }
    
    /**
     * Sets the weight scheme for action selection.
     * 
     * @param scheme the weight scheme to use
     * @return this options object for chaining
     */
    public ModelEngineOptions withWeightScheme(WeightScheme scheme) {
        this.weightScheme = scheme;
        return this;
    }
    
    /**
     * Enables verbose logging of actions and state changes.
     * 
     * @param verbose whether to enable verbose mode
     * @return this options object for chaining
     */
    public ModelEngineOptions withVerbose(boolean verbose) {
        this.verbose = verbose;
        return this;
    }
    
    /**
     * Sets whether to stop on first failure.
     * 
     * @param failFast whether to fail fast
     * @return this options object for chaining
     */
    public ModelEngineOptions withFailFast(boolean failFast) {
        this.failFast = failFast;
        return this;
    }
    
    /**
     * Sets number of retries for failed actions.
     * 
     * @param retryCount number of retries
     * @return this options object for chaining
     */
    public ModelEngineOptions withRetryCount(int retryCount) {
        this.retryCount = retryCount;
        return this;
    }
    
    // Getters
    
    public long getSeed() {
        return seed;
    }
    
    public int getTimeout() {
        return timeout;
    }
    
    public int getMaxActions() {
        return maxActions;
    }
    
    public WeightScheme getWeightScheme() {
        return weightScheme;
    }
    
    public boolean isVerbose() {
        return verbose;
    }
    
    public boolean isFailFast() {
        return failFast;
    }
    
    public int getRetryCount() {
        return retryCount;
    }
    
    @Override
    public String toString() {
        return String.format(
            "ModelEngineOptions{seed=%d, timeout=%ds, maxActions=%d, weightScheme=%s, verbose=%s, failFast=%s}",
            seed, timeout, maxActions, weightScheme, verbose, failFast
        );
    }
}
