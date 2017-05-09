package com.microsoft.sqlserver.jdbc;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

class ThreePartName {
    /*
     * Three part names parsing For metdata calls we parse the procedure name into parts so we can use it in sp_sproc_columns sp_sproc_columns
     * [[@procedure_name =] 'name'] [,[@procedure_owner =] 'owner'] [,[@procedure_qualifier =] 'qualifier']
     *
     */
    private static final Pattern THREE_PART_NAME = Pattern.compile(JDBCSyntaxTranslator.getSQLIdentifierWithGroups());

    private final String databasePart;
    private final String ownerPart;
    private final String procedurePart;

    private ThreePartName(String databasePart, String ownerPart, String procedurePart) {
        this.databasePart = databasePart;
        this.ownerPart = ownerPart;
        this.procedurePart = procedurePart;
    }

    String getDatabasePart() {
        return databasePart;
    }

    String getOwnerPart() {
        return ownerPart;
    }

    String getProcedurePart() {
        return procedurePart;
    }

    static ThreePartName parse(String theProcName) {
        String procedurePart = null;
        String ownerPart = null;
        String databasePart = null;
        Matcher matcher;
        if (null != theProcName) {
            matcher = THREE_PART_NAME.matcher(theProcName);
            if (matcher.matches()) {
                if (matcher.group(2) != null) {
                    databasePart = matcher.group(1);

                    // if we have two parts look to see if the last part can be broken even more
                    matcher = THREE_PART_NAME.matcher(matcher.group(2));
                    if (matcher.matches()) {
                        if (null != matcher.group(2)) {
                            ownerPart = matcher.group(1);
                            procedurePart = matcher.group(2);
                        }
                        else {
                            ownerPart = databasePart;
                            databasePart = null;
                            procedurePart = matcher.group(1);
                        }
                    }
                }
                else {
                    procedurePart = matcher.group(1);
                }
            }
            else {
                procedurePart = theProcName;
            }
        }
        return new ThreePartName(databasePart, ownerPart, procedurePart);
    }
}
