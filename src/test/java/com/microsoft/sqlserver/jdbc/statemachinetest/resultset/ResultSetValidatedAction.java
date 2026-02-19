/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.statemachinetest.resultset;

import com.microsoft.sqlserver.jdbc.statemachinetest.core.DataCache;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.ValidatedAction;

/**
 * Base class for ResultSet-specific validated actions.
 * 
 * Domain-specific validation support for ResultSet actions.
 * Provides built-in DataCache without needing separate preload() calls.
 * 
 * Test artifact (not driver domain):
 * - dataCache: Expected row data for validation
 * 
 * All ResultSet actions share this field through inheritance.
 * Actions can read dataCache to verify actual results match expected values.
 * 
 * Example:
 * <pre>
 * public class NextAction extends ResultSetValidatedAction {
 *     public void run() {
 *         boolean valid = rs.next();
 *         sm.setState(ON_VALID_ROW, valid);
 *     }
 *     
 *     public void validate() {
 *         if (hasDataCache()) {
 *             Map<String, Object> expectedRow = dataCache.getRow(currentRow);
 *             String actualName = rs.getString("name");
 *             assertExpected(actualName, expectedRow.get("name"), "Name mismatch");
 *         }
 *     }
 * }
 * </pre>
 * 
 * Setup:
 * <pre>
 * DataCache cache = new DataCache();
 * cache.addRow(Map.of("name", "Alice", "value", 100));
 * 
 * NextAction next = new NextAction(sm);
 * next.setDataCache(cache);
 * sm.addAction(next);
 * </pre>
 */
public abstract class ResultSetValidatedAction extends ValidatedAction {
    
    /** Expected row data for validation (test artifact). */
    protected DataCache dataCache;
    
    public ResultSetValidatedAction(String name, int weight) {
        super(name, weight);
    }
    
    /**
     * Sets the DataCache for validation.
     * 
     * @param cache the expected data
     */
    public void setDataCache(DataCache cache) {
        this.dataCache = cache;
    }
    
    /**
     * Checks if validation data is available.
     * 
     * @return true if dataCache exists and is not empty
     */
    protected boolean hasDataCache() {
        return dataCache != null && !dataCache.isEmpty();
    }
}
