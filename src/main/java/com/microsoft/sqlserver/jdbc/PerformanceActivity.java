/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * Enum representing different performance activities.
 */
public enum PerformanceActivity {
    CONNECTION("Connection"),
    TOKEN_ACQUISITION("Token acquisition"),
    LOGIN("Login"),
    PRELOGIN("Prelogin");

    private final String activity;

    PerformanceActivity(String activity) {
        this.activity = activity;
    }

    public String activity() {
        return activity;
    }

    @Override
    public String toString() {
        return activity;
    }
}
