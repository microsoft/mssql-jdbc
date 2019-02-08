package com.microsoft.sqlserver.jdbc;

public class SQLServerParser {
    
    /*
     * Takes in a String object and returns the string without any comments or whitespaces at the beginning
     */
    public static String skipWSComments(String s) {
        boolean keepSearching = false;
        String cleanedString = s;
        do {
            keepSearching = false;
            cleanedString = cleanedString.stripLeading();
            if (cleanedString.startsWith("/*")) {
                boolean endFound = false;
                while (!endFound) {
                    String[] parts = cleanedString.split("*/", 2);
                    if (parts[1] == "") {
                        /*
                         * Throw comment end not found error
                         */
                    } else {
                        
                    }
                }
            } else if (cleanedString.startsWith("--")) {
                String[] parts = cleanedString.split("\n", 2);
                if (parts[1] == "") {
                    //Didn't find a new line, the entire string is a comment
                    return "";
                } else {
                    cleanedString = parts[1];
                    keepSearching = true;
                }
            }
        } while (keepSearching);
        return cleanedString;
    }
}
