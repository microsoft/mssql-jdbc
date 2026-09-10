/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * Enum representing the type of JDBC statement being executed.
 */
public enum StatementType {

    /**
     * A plain {@link java.sql.Statement}.
     */
    STATEMENT("Statement"),

    /**
     * A {@link java.sql.PreparedStatement}.
     */
    PREPARED_STATEMENT("PreparedStatement"),

    /**
     * A {@link java.sql.CallableStatement}.
     */
    CALLABLE_STATEMENT("CallableStatement");

    private final String type;

    StatementType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return type;
    }
}
