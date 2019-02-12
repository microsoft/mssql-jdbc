package com.microsoft.sqlserver.jdbc;

import java.util.regex.Pattern;

public class SQLServerParser {
    /*
     * Takes in a String object and returns the string without any comments or white-spaces at the beginning
     */
    public static String skipWSComments(String s) {
        /*
         * Remove leading whitespace first.
         */
        String resultString = s.stripLeading();
        if (resultString.startsWith("/*")) {
            /*
             * Remove block comments.
             */
            final String[] tokens = resultString.split(Pattern.quote("*/"), 2);
            if (tokens.length == 2) {
                return skipWSComments(tokens[1]);
            } else {
                /*
                 * May need to throw Missing end comment mark '* /' like SSMS
                 */
                return "";
            }
        } else if (resultString.startsWith("--")) {            
            /*
             * Remove single line comments.
             */
            final String[] tokens = resultString.split("\n", 2);
            if (tokens.length == 2) {
                return skipWSComments(tokens[1]);
            } else {
                return "";
            }
        } else {
            return resultString;
        }
    }
    
    public static void peekNextSQLWord(String s) {
        String resultString = skipWSComments(s).toUpperCase();
        switch (resultString.charAt(0)) {
            case 'S':
                if (resultString.startsWith("SELECT ")) {
                    
                }
                break;
            case 'I':
                if (resultString.startsWith("INSERT ")) {
                    
                }
                break;
            case 'U':
                if (resultString.startsWith("UPDATE ")) {
                    
                }
                break;
            case 'D':
                if (resultString.startsWith("DELETE ")) {
                    
                }
                break;
            case '(':
                break;
            default:
                break;
        }
    }
}
