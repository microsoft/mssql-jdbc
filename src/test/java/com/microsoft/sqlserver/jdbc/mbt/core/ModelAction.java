/*
 * Microsoft JDBC Driver for SQL Server
 * Model-Based Testing Framework for JUnit 5
 */
package com.microsoft.sqlserver.jdbc.mbt.core;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as a Model Action that can be executed by the ModelEngine.
 * 
 * Actions are the transitions in the state machine. The engine will randomly
 * select and execute actions based on their weight and requirements.
 * 
 * Usage:
 * <pre>
 * &#64;ModelAction(weight = 10)
 * public void createStatement() throws SQLException {
 *     Statement stmt = connection.createStatement();
 *     // ... verification
 * }
 * </pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ModelAction {
    
    /**
     * Weight determines the probability of this action being selected.
     * Higher weight = more likely to be chosen.
     * Default is 10.
     */
    int weight() default 10;
    
    /**
     * If true, this action will be called before any other actions.
     * Useful for setup operations.
     */
    boolean callFirst() default false;
    
    /**
     * If true, this action will be called after all other actions.
     * Useful for cleanup operations like close().
     */
    boolean callLast() default false;
    
    /**
     * Maximum number of times this action can be called.
     * Default is unlimited (Integer.MAX_VALUE).
     */
    int callLimit() default Integer.MAX_VALUE;
    
    /**
     * If true, this action can only be called once.
     * Shorthand for callLimit = 1.
     */
    boolean callOnce() default false;
}
