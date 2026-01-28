/*
 * Microsoft JDBC Driver for SQL Server
 * Model-Based Testing Framework for JUnit 5
 */
package com.microsoft.sqlserver.jdbc.mbt.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Model Engine - executes state machine exploration.
 * It auto-explores valid execution paths through a Model's state space.
 * 
 * Key Features:
 * - Reproducible via seed (same seed = same execution path)
 * - Respects @ModelRequirement preconditions
 * - Supports multiple weight schemes for action selection
 * - Timeout and max action limits
 * - Detailed logging for debugging
 * 
 * Usage:
 * <pre>
 * &#64;Test
 * void testConnectionModel() throws Exception {
 *     Connection conn = DriverManager.getConnection(url, user, password);
 *     ConnectionModel model = new ConnectionModel(conn);
 *     
 *     ModelEngineOptions options = new ModelEngineOptions()
 *         .withTimeout(30)
 *         .withMaxActions(100)
 *         .withSeed(12345);  // For reproducibility
 *     
 *     ModelEngine engine = new ModelEngine(model, options);
 *     ModelEngineResult result = engine.run();
 *     
 *     assertTrue(result.isSuccess());
 * }
 * </pre>
 */
public class ModelEngine {
    
    private static final Logger LOGGER = Logger.getLogger(ModelEngine.class.getName());
    
    private final Model model;
    private final ModelEngineOptions options;
    private final Random random;
    
    // Execution state
    private long startTime;
    private int actionCount;
    private List<String> executionLog;
    private boolean running;
    private Exception lastException;
    
    /**
     * Creates a ModelEngine with default options.
     * 
     * @param model the model to explore
     */
    public ModelEngine(Model model) {
        this(model, new ModelEngineOptions());
    }
    
    /**
     * Creates a ModelEngine with specified options.
     * 
     * @param model the model to explore
     * @param options configuration options
     */
    public ModelEngine(Model model, ModelEngineOptions options) {
        this.model = model;
        this.options = options;
        this.random = new Random(options.getSeed());
        this.executionLog = new ArrayList<>();
    }
    
    /**
     * Runs the model exploration.
     * 
     * @return result of the exploration
     */
    public ModelEngineResult run() {
        return runUntil(() -> false);  // Run until timeout or max actions
    }
    
    /**
     * Runs until a condition is met.
     * 
     * @param stopCondition returns true when exploration should stop
     * @return result of the exploration
     */
    public ModelEngineResult runUntil(StopCondition stopCondition) {
        startTime = System.currentTimeMillis();
        actionCount = 0;
        executionLog.clear();
        lastException = null;
        running = true;
        
        // Log seed for reproducibility
        log(String.format("Seed: %d", options.getSeed()));
        log(String.format("Model.Timeout: %d", options.getTimeout()));
        log(String.format("Model.MaxActions: %d", options.getMaxActions()));
        
        try {
            model.initialize();
            model.setup();
            
            // Execute callFirst actions first
            executeCallFirstActions();
            
            // Main exploration loop
            while (running && !shouldStop(stopCondition)) {
                List<Model.ActionInfo> validActions = getExecutableActions();
                
                if (validActions.isEmpty()) {
                    log("No valid actions available - ending exploration");
                    break;
                }
                
                Model.ActionInfo selectedAction = selectAction(validActions);
                executeAction(selectedAction);
            }
            
            // Execute callLast actions
            executeCallLastActions();
            
            model.teardown();
            
        } catch (Exception e) {
            lastException = e;
            LOGGER.log(Level.SEVERE, "Model execution failed", e);
            log("EXCEPTION: " + e.getMessage());
            
            if (options.isFailFast()) {
                running = false;
            }
        }
        
        long duration = System.currentTimeMillis() - startTime;
        return new ModelEngineResult(
            lastException == null,
            actionCount,
            duration,
            options.getSeed(),
            executionLog,
            lastException
        );
    }
    
    /**
     * Checks if exploration should stop.
     */
    private boolean shouldStop(StopCondition stopCondition) {
        // Check timeout
        long elapsed = System.currentTimeMillis() - startTime;
        if (elapsed > options.getTimeout() * 1000L) {
            log("Timeout reached (" + options.getTimeout() + "s)");
            return true;
        }
        
        // Check max actions
        if (actionCount >= options.getMaxActions()) {
            log("Max actions reached (" + options.getMaxActions() + ")");
            return true;
        }
        
        // Check custom condition
        return stopCondition.shouldStop();
    }
    
    /**
     * Gets list of actions that can be executed (requirements met, call limits not exceeded).
     */
    private List<Model.ActionInfo> getExecutableActions() {
        List<Model.ActionInfo> executable = new ArrayList<>();
        
        for (Model.ActionInfo action : model.getValidActions()) {
            // Skip callFirst/callLast actions in main loop
            if (action.callFirst || action.callLast) {
                continue;
            }
            
            // Check call limits
            if (action.callOnce && action.callCount > 0) {
                continue;
            }
            if (action.callLimit > 0 && action.callCount >= action.callLimit) {
                continue;
            }
            
            executable.add(action);
        }
        
        return executable;
    }
    
    /**
     * Selects an action based on weight scheme.
     */
    private Model.ActionInfo selectAction(List<Model.ActionInfo> actions) {
        switch (options.getWeightScheme()) {
            case WEIGHT_SCHEME_FLAT:
                return selectFlat(actions);
            case WEIGHT_SCHEME_WEIGHTED:
                return selectWeighted(actions);
            case WEIGHT_SCHEME_CALL_COUNT_INVERSE:
                return selectCallCountInverse(actions);
            case WEIGHT_SCHEME_ROUND_ROBIN:
                return selectRoundRobin(actions);
            case WEIGHT_SCHEME_WEIGHTED_CALL_COUNT:
                return selectWeightedCallCount(actions);
            default:
                return selectFlat(actions);
        }
    }
    
    /**
     * Flat selection - equal probability.
     */
    private Model.ActionInfo selectFlat(List<Model.ActionInfo> actions) {
        return actions.get(random.nextInt(actions.size()));
    }
    
    /**
     * Weighted selection - probability proportional to weight.
     */
    private Model.ActionInfo selectWeighted(List<Model.ActionInfo> actions) {
        int totalWeight = actions.stream().mapToInt(a -> a.weight).sum();
        int target = random.nextInt(totalWeight);
        
        int cumulative = 0;
        for (Model.ActionInfo action : actions) {
            cumulative += action.weight;
            if (target < cumulative) {
                return action;
            }
        }
        return actions.get(actions.size() - 1);
    }
    
    /**
     * Call count inverse - favors least-called actions.
     */
    private Model.ActionInfo selectCallCountInverse(List<Model.ActionInfo> actions) {
        // Find min call count
        int minCalls = actions.stream().mapToInt(a -> a.callCount).min().orElse(0);
        
        // Filter to only least-called
        List<Model.ActionInfo> leastCalled = new ArrayList<>();
        for (Model.ActionInfo action : actions) {
            if (action.callCount == minCalls) {
                leastCalled.add(action);
            }
        }
        
        return leastCalled.get(random.nextInt(leastCalled.size()));
    }
    
    /**
     * Round robin selection.
     */
    private Model.ActionInfo selectRoundRobin(List<Model.ActionInfo> actions) {
        return selectCallCountInverse(actions);  // Same behavior
    }
    
    /**
     * Combined weighted and call count.
     */
    private Model.ActionInfo selectWeightedCallCount(List<Model.ActionInfo> actions) {
        // Weight inversely by call count
        int maxCalls = actions.stream().mapToInt(a -> a.callCount).max().orElse(0) + 1;
        
        int totalWeight = 0;
        int[] adjustedWeights = new int[actions.size()];
        for (int i = 0; i < actions.size(); i++) {
            Model.ActionInfo action = actions.get(i);
            adjustedWeights[i] = action.weight * (maxCalls - action.callCount);
            totalWeight += adjustedWeights[i];
        }
        
        int target = random.nextInt(Math.max(1, totalWeight));
        int cumulative = 0;
        for (int i = 0; i < actions.size(); i++) {
            cumulative += adjustedWeights[i];
            if (target < cumulative) {
                return actions.get(i);
            }
        }
        return actions.get(actions.size() - 1);
    }
    
    /**
     * Executes actions marked with callFirst=true.
     */
    private void executeCallFirstActions() throws Exception {
        for (Model.ActionInfo action : model.getActions()) {
            if (action.callFirst && model.areRequirementsMet(action)) {
                executeAction(action);
            }
        }
    }
    
    /**
     * Executes actions marked with callLast=true.
     */
    private void executeCallLastActions() throws Exception {
        for (Model.ActionInfo action : model.getActions()) {
            if (action.callLast && model.areRequirementsMet(action)) {
                executeAction(action);
            }
        }
    }
    
    /**
     * Executes a single action.
     */
    private void executeAction(Model.ActionInfo action) throws Exception {
        actionCount++;
        
        if (options.isVerbose()) {
            log(String.format("[%d] Executing: %s (weight=%d, calls=%d)", 
                actionCount, action.name, action.weight, action.callCount + 1));
            log("  State: " + model.getStateSnapshot());
        } else {
            log(String.format("[%d] %s", actionCount, action.name));
        }
        
        model.executeAction(action);
    }
    
    /**
     * Logs a message.
     */
    private void log(String message) {
        executionLog.add(message);
        LOGGER.fine(message);
    }
    
    /**
     * Stops the engine gracefully.
     */
    public void stop() {
        running = false;
    }
    
    /**
     * Functional interface for custom stop conditions.
     */
    @FunctionalInterface
    public interface StopCondition {
        boolean shouldStop();
    }
    
    /**
     * Result of a model exploration run.
     */
    public static class ModelEngineResult {
        private final boolean success;
        private final int actionCount;
        private final long durationMs;
        private final long seed;
        private final List<String> executionLog;
        private final Exception exception;
        
        public ModelEngineResult(boolean success, int actionCount, long durationMs, 
                                long seed, List<String> executionLog, Exception exception) {
            this.success = success;
            this.actionCount = actionCount;
            this.durationMs = durationMs;
            this.seed = seed;
            this.executionLog = new ArrayList<>(executionLog);
            this.exception = exception;
        }
        
        public boolean isSuccess() {
            return success;
        }
        
        public int getActionCount() {
            return actionCount;
        }
        
        public long getDurationMs() {
            return durationMs;
        }
        
        public long getSeed() {
            return seed;
        }
        
        public List<String> getExecutionLog() {
            return executionLog;
        }
        
        public Exception getException() {
            return exception;
        }
        
        @Override
        public String toString() {
            return String.format(
                "ModelEngineResult{success=%s, actions=%d, duration=%dms, seed=%d}",
                success, actionCount, durationMs, seed
            );
        }
        
        /**
         * Prints execution log to stdout.
         */
        public void printLog() {
            System.out.println("=== Model Execution Log ===");
            System.out.println("Seed: " + seed + " (use this to reproduce)");
            System.out.println("Success: " + success);
            System.out.println("Actions: " + actionCount);
            System.out.println("Duration: " + durationMs + "ms");
            System.out.println("--- Actions ---");
            for (String entry : executionLog) {
                System.out.println(entry);
            }
            if (exception != null) {
                System.out.println("--- Exception ---");
                exception.printStackTrace(System.out);
            }
        }
    }
}
