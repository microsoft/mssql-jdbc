/*
 * Microsoft JDBC Driver for SQL Server
 * Model-Based Testing Framework for JUnit 5
 */
package com.microsoft.sqlserver.jdbc.mbt.core;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Defines a requirement that must be met for an action to be executable.
 * 
 * Requirements are preconditions based on model state variables.
 * An action can only be executed when ALL its requirements are satisfied.
 * 
 * Usage:
 * <pre>
 * &#64;ModelAction(weight = 10)
 * &#64;ModelRequirement(variable = "closed", value = "false")
 * &#64;ModelRequirement(variable = "autoCommit", value = "false")
 * public void commit() throws SQLException {
 *     connection.commit();
 * }
 * </pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Repeatable(ModelRequirements.class)
public @interface ModelRequirement {
    
    /**
     * Name of the state variable to check.
     * Must match a field annotated with @ModelVariable.
     */
    String variable();
    
    /**
     * Expected value of the variable (as a String).
     * Supports: "true", "false", numeric values, enum names.
     */
    String value();
    
    /**
     * Comparison operator.
     * Default is EQUALS.
     */
    Operator operator() default Operator.EQUALS;
    
    /**
     * Weight for this requirement when used in invalid scenario testing.
     */
    int weight() default 1;
    
    /**
     * If true and requirement is NOT met, the action expects an exception.
     * This enables testing of invalid scenarios.
     */
    boolean throwsOnFailure() default false;
    
    /**
     * Expected exception class when throwsOnFailure is true.
     */
    Class<? extends Exception> expectedException() default Exception.class;
    
    /**
     * Comparison operators for requirements.
     */
    enum Operator {
        EQUALS,
        NOT_EQUALS,
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL,
        LESS_THAN,
        LESS_THAN_OR_EQUAL
    }
}
