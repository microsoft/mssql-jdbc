/*
 * Microsoft JDBC Driver for SQL Server
 * Model-Based Testing Framework for JUnit 5
 */
package com.microsoft.sqlserver.jdbc.mbt.core;

import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a test method as a Model-Based Test.
 * 
 * This annotation triggers the MBT framework to run the model
 * with specified options.
 * 
 * Usage:
 * <pre>
 * &#64;ModelTest(
 *     timeout = 30,
 *     maxActions = 100,
 *     weightScheme = WeightScheme.WEIGHT_SCHEME_WEIGHTED,
 *     verbose = true
 * )
 * void testConnectionModel(ConnectionModel model) {
 *     // Model is automatically run and validated
 * }
 * </pre>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(ModelTestExtension.class)
public @interface ModelTest {
    
    /**
     * Fixed seed for reproducible runs.
     * Use -1 (default) for random seed based on timestamp.
     */
    long seed() default -1;
    
    /**
     * Timeout in seconds.
     */
    int timeout() default 30;
    
    /**
     * Maximum number of actions to execute.
     */
    int maxActions() default 500;
    
    /**
     * Weight scheme for action selection.
     */
    WeightScheme weightScheme() default WeightScheme.WEIGHT_SCHEME_FLAT;
    
    /**
     * Enable verbose logging.
     */
    boolean verbose() default false;
    
    /**
     * Stop on first failure.
     */
    boolean failFast() default true;
    
    /**
     * Number of times to run the model.
     * Each run uses a different seed (if seed is -1).
     */
    int runs() default 1;
    
    /**
     * Print execution log on failure.
     */
    boolean printLogOnFailure() default true;
}
