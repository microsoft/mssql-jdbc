/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) 2016 Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.testframework;

import java.util.BitSet;

public class DBCoercion {
    Class type = null;
    protected BitSet flags = new BitSet();
    protected String name = null;
    // Flags

    public static final int GET = 1;
    public static final int UPDATE = 2;
    public static final int SET = 3;
    public static final int SETOBJECT = 4;
    public static final int REG = 5;
    public static final int GETPARAM = 6;
    public static final int UPDATEOBJECT = 7;
    public static final int ALL = 8;

    public static final int STREAM = 9;
    public static final int CHAR = 10;
    public static final int NCHAR = 11;
    public static final int ASCII = 12;

    /**
     * 
     * @param type
     */
    public DBCoercion(Class type) {
        this(type, new int[] {GET});
    }

    /**
     * 
     * @param type
     * @param tempflags
     */
    public DBCoercion(Class type,
            int[] tempflags) {
        name = type.toString();
        this.type = type;
        for (int tempflag : tempflags) flags.set(tempflag);
    }

    /**
     * @return type
     */
    public Class type() {
        return type;
    }

    /**
     * @return
     */
    public BitSet flags() {
        return flags;
    }
}