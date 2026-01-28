/*
 * Microsoft JDBC Driver for SQL Server
 * Model-Based Testing Framework for JUnit 5
 */
package com.microsoft.sqlserver.jdbc.mbt.core;

import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;

import java.util.logging.Logger;

/**
 * JUnit 5 Extension for Model-Based Testing.
 * 
 * This extension:
 * 1. Reads @ModelTest annotation settings
 * 2. Creates ModelEngineOptions from annotation
 * 3. Stores options in ExtensionContext for test use
 * 4. Handles exceptions with seed logging for reproducibility
 */
public class ModelTestExtension implements BeforeEachCallback, TestExecutionExceptionHandler {
    
    private static final Logger LOGGER = Logger.getLogger(ModelTestExtension.class.getName());
    
    private static final String OPTIONS_KEY = "modelEngineOptions";
    private static final String LAST_SEED_KEY = "lastSeed";
    
    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        // Check for @ModelTest annotation
        ModelTest annotation = context.getRequiredTestMethod()
            .getAnnotation(ModelTest.class);
        
        if (annotation != null) {
            // Build options from annotation
            long seed = annotation.seed() == -1 
                ? System.currentTimeMillis() 
                : annotation.seed();
            
            ModelEngineOptions options = new ModelEngineOptions()
                .withSeed(seed)
                .withTimeout(annotation.timeout())
                .withMaxActions(annotation.maxActions())
                .withWeightScheme(annotation.weightScheme())
                .withVerbose(annotation.verbose())
                .withFailFast(annotation.failFast());
            
            // Store in context
            context.getStore(ExtensionContext.Namespace.GLOBAL)
                .put(OPTIONS_KEY, options);
            
            context.getStore(ExtensionContext.Namespace.GLOBAL)
                .put(LAST_SEED_KEY, seed);
            
            LOGGER.fine(() -> "ModelTest configured with seed: " + seed);
        }
    }
    
    @Override
    public void handleTestExecutionException(ExtensionContext context, Throwable throwable) 
            throws Throwable {
        
        // Get the seed for reproducibility message
        Long seed = context.getStore(ExtensionContext.Namespace.GLOBAL)
            .get(LAST_SEED_KEY, Long.class);
        
        ModelTest annotation = context.getRequiredTestMethod()
            .getAnnotation(ModelTest.class);
        
        if (seed != null && annotation != null && annotation.printLogOnFailure()) {
            System.err.println("========================================");
            System.err.println("MODEL TEST FAILURE");
            System.err.println("To reproduce, use seed: " + seed);
            System.err.println("Add @ModelTest(seed = " + seed + "L) to rerun exact sequence");
            System.err.println("========================================");
        }
        
        throw throwable;
    }
    
    /**
     * Gets ModelEngineOptions from current test context.
     * Call this from your test method to get configured options.
     * 
     * @param context the extension context
     * @return configured options, or default if not found
     */
    public static ModelEngineOptions getOptions(ExtensionContext context) {
        ModelEngineOptions options = context.getStore(ExtensionContext.Namespace.GLOBAL)
            .get(OPTIONS_KEY, ModelEngineOptions.class);
        return options != null ? options : new ModelEngineOptions();
    }
    
    /**
     * Helper to run a model in a test with current context options.
     * 
     * @param model the model to run
     * @param options engine options
     * @return the result
     * @throws AssertionError if model execution fails
     */
    public static ModelEngine.ModelEngineResult runModel(Model model, ModelEngineOptions options) {
        ModelEngine engine = new ModelEngine(model, options);
        ModelEngine.ModelEngineResult result = engine.run();
        
        if (!result.isSuccess()) {
            System.err.println("Model execution failed. Seed: " + result.getSeed());
            result.printLog();
            throw new AssertionError(
                "Model test failed. To reproduce, use seed: " + result.getSeed(),
                result.getException()
            );
        }
        
        return result;
    }
}
