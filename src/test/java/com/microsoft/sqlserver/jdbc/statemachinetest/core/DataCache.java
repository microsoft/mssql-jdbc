/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.statemachinetest.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Cache of expected row data used for validation.
 * Stores rows (as column-name->value maps).
 * Actions compare actual JDBC results against cached expected values.
 */
public class DataCache {

    /** Expected rows, indexed by row number (0-based) */
    private final List<Map<String, Object>> rows = new ArrayList<>();

    /**
     * Adds a new row of expected data.
     * 
     * @param row map of column name to expected value
     */
    public void addRow(Map<String, Object> row) {
        rows.add(new HashMap<>(row));
    }

    /**
     * Gets the expected data for a specific row.
     * 
     * @param rowIndex 0-based row index
     * @return map of column name to expected value, or null if row doesn't exist
     */
    public Map<String, Object> getRow(int rowIndex) {
        if (rowIndex < 0 || rowIndex >= rows.size()) {
            return null;
        }
        return Collections.unmodifiableMap(rows.get(rowIndex));
    }

    /**
     * Gets the expected value for a specific column in a specific row.
     * 
     * @param rowIndex 0-based row index
     * @param columnName name of the column
     * @return expected value, or null if row/column doesn't exist
     */
    public Object getValue(int rowIndex, String columnName) {
        if (rowIndex < 0 || rowIndex >= rows.size()) {
            return null;
        }
        Map<String, Object> row = rows.get(rowIndex);
        return row != null ? row.get(columnName) : null;
    }

    /**
     * Returns the total number of expected rows.
     * 
     * @return row count
     */
    public int getRowCount() {
        return rows.size();
    }

    /**
     * Checks if cache is empty (no expected rows).
     * 
     * @return true if empty, false otherwise
     */
    public boolean isEmpty() {
        return rows.isEmpty();
    }

    /**
     * Clears all cached data.
     */
    public void clear() {
        rows.clear();
    }

    /**
     * Updates an expected value in the cache.
     * Used to track expected state changes (e.g., after UPDATE operations).
     * 
     * @param rowIndex 0-based row index
     * @param columnName name of the column
     * @param newValue new expected value
     */
    public void updateValue(int rowIndex, String columnName, Object newValue) {
        if (rowIndex < 0 || rowIndex >= rows.size()) {
            throw new IndexOutOfBoundsException(
                    "Row index " + rowIndex + " out of bounds for DataCache with " + rows.size() + " rows");
        }
        rows.get(rowIndex).put(columnName, newValue);
    }

    @Override
    public String toString() {
        return "DataCache{rows=" + rows.size() + "}";
    }
}
