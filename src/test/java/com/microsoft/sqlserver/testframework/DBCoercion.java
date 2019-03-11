/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) 2016 Microsoft Corporation All rights reserved. This program is
 * made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.testframework;

import java.util.BitSet;


public class DBCoercion {
    Class<?> type = null;
    protected BitSet flags = new BitSet();
    protected String name = null;

    /**
     * 
     * @param type
     */
    public DBCoercion(Class<?> type) {
        this(type, new int[] {DBConstants.GET_COERCION});
    }

    /**
     * 
     * @param type
     * @param tempflags
     */
    public DBCoercion(Class<?> type, int[] tempflags) {
        name = type.toString();
        this.type = type;
        for (int tempflag : tempflags)
            flags.set(tempflag);
    }

    /**
     * @return type
     */
    public Class<?> type() {
        return type;
    }

    /**
     * @return
     */
    public BitSet flags() {
        return flags;
    }
}
