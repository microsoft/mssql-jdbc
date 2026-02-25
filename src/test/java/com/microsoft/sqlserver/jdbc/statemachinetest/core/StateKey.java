/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.statemachinetest.core;


/**
 * Type-safe key for state variables. Implement as an enum per domain.
 * State is stored in {@link DataCache} row 0 and accessed via
 * {@link Action#setState}, {@link Action#getState}, {@link Action#isState},
 * {@link Action#getStateInt}.
 */
public interface StateKey {
    
    /** Returns the string key used for state storage. */
    String key();
}
