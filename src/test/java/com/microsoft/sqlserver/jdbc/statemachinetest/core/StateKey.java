/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.statemachinetest.core;


/**
 * Interface for state keys used in StateMachineTest.
 * 
 * Implement this interface as an enum to define domain-specific state keys
 * with compile-time safety and easy visualization of all states.
 * 
 * Example:
 * <pre>
 * public enum MyState implements StateKey {
 *     CONNECTION("conn"),
 *     IS_OPEN("isOpen");
 *     
 *     private final String key;
 *     MyState(String key) { this.key = key; }
 *     public String key() { return key; }
 * }
 * </pre>
 */
public interface StateKey {
    
    /**
     * Returns the string key used for state storage.
     * This allows enums to define their own key names.
     */
    String key();
}
