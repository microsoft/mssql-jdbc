/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) 2016 Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.testframework;

import java.util.ArrayList;

/**
 * DBCoercions
 *
 */
public class DBCoercions extends DBItems {

    /**
     * constructor
     */
    public DBCoercions() {
        coercionsList = new ArrayList<>();
    }

    public DBCoercions(DBCoercion coercion) {
        this.add(coercion);
    }

}