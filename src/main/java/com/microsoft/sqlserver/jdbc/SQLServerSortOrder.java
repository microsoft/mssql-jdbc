/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * 
 * Specifies the sorting order
 *
 */
public enum SQLServerSortOrder {
    ASCENDING(0),
    DESCENDING(1),
    UNSPECIFIED(-1);

    final int value;

    SQLServerSortOrder(int sortOrderVal) {
        value = sortOrderVal;
    }
}
