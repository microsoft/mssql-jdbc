package com.microsoft.sqlserver.jdbc;

public enum ErrorNumberEnum {
        ERR_22001("22001", "String data right truncation"),
        //ERR_22001("22001", "String data right truncation"),
        ERR_23000("23000", "Integrity constraint violation"),
        //ERR_23000("23000", "DPM 4.04. Primary key violation"),
        ERR_S0001("S0001", "table already exists"),
        ERR_S0002("S0002", "table not found"),
        ERR_40001("40001", "deadlock detected"),
        ERR_08001("08001", "Database name undefined at logging"),
        //ERR_08001("08001", "username password wrong at login"),
        ERR_42S01("42S01", "Table already exists"),
        ERR_42S02("42S02", "Table not found"),
        ERR_42S22("42S22", "Column not found"),
        /*ERR_("S1093";
        ERR_("08S01";
        ERR_("08S01";*/
        ERR_42000("42000", "Use XOPEN 'Syntax error or access violation'");

    ;

    private String errorNumber;
    private String description;

    ErrorNumberEnum(String errorNumber, String description) {
        this.errorNumber = errorNumber;
        this.description = description;
    }

    public String getErrorNumber() {
        return errorNumber;
    }

    public String getDescription() {
        return description;
    }
}
