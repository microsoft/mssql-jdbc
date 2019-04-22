package com.microsoft.sqlserver.testframework;

import java.sql.ResultSet;


public class DBConstants {

    // Coercion Flags
    public static final int GET_COERCION = 1;
    public static final int UPDATE_COERCION = 2;
    public static final int SET_COERCION = 3;
    public static final int SETOBJECT_COERCION = 4;
    public static final int REG_COERCION = 5;
    public static final int GETPARAM_COERCION = 6;
    public static final int UPDATEOBJECT_COERCION = 7;
    public static final int ALL_COERCION = 8;
    public static final int STREAM_COERCION = 9;
    public static final int CHAR_COERCION = 10;
    public static final int NCHAR_COERCION = 11;
    public static final int ASCII_COERCION = 12;

    // ResultSet Flags
    public static final int RESULTSET_TYPE_DYNAMIC = ResultSet.TYPE_SCROLL_SENSITIVE + 1;
    public static final int RESULTSET_CONCUR_OPTIMISTIC = ResultSet.CONCUR_UPDATABLE + 2;
    public static final int RESULTSET_TYPE_CURSOR_FORWARDONLY = ResultSet.TYPE_FORWARD_ONLY + 1001;
    public static final int RESULTSET_TYPE_FORWARD_ONLY = ResultSet.TYPE_FORWARD_ONLY;
    public static final int RESULTSET_CONCUR_READ_ONLY = ResultSet.CONCUR_READ_ONLY;
    public static final int RESULTSET_TYPE_SCROLL_INSENSITIVE = ResultSet.TYPE_SCROLL_INSENSITIVE;
    public static final int RESULTSET_TYPE_SCROLL_SENSITIVE = ResultSet.TYPE_SCROLL_SENSITIVE;
    public static final int RESULTSET_CONCUR_UPDATABLE = ResultSet.CONCUR_UPDATABLE;
    public static final int RESULTSET_TYPE_DIRECT_FORWARDONLY = ResultSet.TYPE_FORWARD_ONLY + 1000;

    // Statement Flags
    public static final int STATEMENT = 0;
    public static final int PREPAREDSTATEMENT = 1;
    public static final int CALLABLESTATEMENT = 2;
    public static final int ALL_STATEMENTS = 3;
}
