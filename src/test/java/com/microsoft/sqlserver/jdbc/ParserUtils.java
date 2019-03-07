package com.microsoft.sqlserver.jdbc;

public class ParserUtils {
    
    public static String getTableName(String s) {
        try {
            String s2 = SQLServerParser.getTableName(s);
            System.out.println(s2);
            return s2;
        } catch (SQLServerException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }
}
