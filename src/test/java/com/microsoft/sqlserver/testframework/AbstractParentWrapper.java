/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework;

/**
 * Stores the parent class. For connection parent is null; for Statement, Connection is parent; for ResultSet, Statement is parent
 */
public abstract class AbstractParentWrapper {
    static final int ENGINE_EDITION_FOR_SQL_AZURE = 5;

    AbstractParentWrapper parent = null;
    Object internal = null;
    String name = null;

    AbstractParentWrapper(AbstractParentWrapper parent,
            Object internal,
            String name) {
        this.parent = parent;
        this.internal = internal;
        this.name = name;
    }

    void setInternal(Object internal) {
        this.internal = internal;
    }

    public Object product() {
        return internal;
    }

    public AbstractParentWrapper parent() {
        return parent;
    }
}
