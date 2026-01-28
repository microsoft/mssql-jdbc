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
 * Marks a field as a state variable that can be used in model requirements.
 * 
 * State variables track the current state of the system under test.
 * The ModelEngine uses these to determine which actions are valid.
 * 
 * Usage:
 * <pre>
 * &#64;ModelVariable
 * private boolean closed = false;
 * 
 * &#64;ModelVariable
 * private boolean autoCommit = true;
 * 
 * &#64;ModelVariable(name = "statementCount", initial = "0")
 * private int openStatements = 0;
 * </pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ModelVariable {
    
    /**
     * Name of the variable for use in @ModelRequirement.
     * Defaults to the field name if not specified.
     */
    String name() default "";
    
    /**
     * Initial value as a String.
     * If not specified, uses field's default value.
     */
    String initial() default "";
    
    /**
     * Human-readable description of this variable.
     */
    String description() default "";
    
    /**
     * If true, this variable's value changes are logged during execution.
     */
    boolean trace() default false;
}
