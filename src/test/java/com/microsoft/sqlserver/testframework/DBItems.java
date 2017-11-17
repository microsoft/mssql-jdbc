/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework;

import java.util.ArrayList;

/**
 * Each sqlType has a list of coercions associate with it
 *
 */
public class DBItems {
    protected ArrayList<DBCoercion> coercionsList;

    public DBItems() {

    }

    public DBCoercion find(Class type) {
        for (DBCoercion item : coercionsList) {
            if (item.type() == type)
                return item;
        }
        return null;
    }

    /**
     * 
     * @param item
     * @return
     */
    public boolean add(DBCoercion item) {
        return coercionsList.add(item);
    }
}