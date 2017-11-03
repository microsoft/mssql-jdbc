/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import java.util.LinkedList;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;

/**
 * Wrapper class that has all the data/values needed to execute BulkCopy test case
 */
class BulkCopyTestWrapper {
    /**
     * Test case name
     */
    String testName;

    /**
     * <code>true</code> if SQLServerBulkCopy should use connection object
     */
    private boolean isUsingConnection;

    /**
     * <code>true</code> if SQLServerBulkCopy should include SQLServerBulkCopyOptions
     */
    private boolean useBulkCopyOptions;

    /**
     * <code>true</code> if SQLServerBulkCopy should use column mapping
     */
    private boolean isUsingColumnMapping = false;

    public LinkedList<ColumnMap> cm = new LinkedList<>();

    private SQLServerBulkCopyOptions bulkOptions;

    private String connectionString;

    BulkCopyTestWrapper(String connectionString) {
        this.connectionString = connectionString;
    }

    /**
     * 
     * @return
     */
    public boolean isUsingConnection() {
        return isUsingConnection;
    }

    /**
     * 
     * @param isUsingConnection
     *            <code>true</code> if connection object should be passed in BulkCopy constructor <code>false</code> if connection string is to be
     *            passed to constructor
     */
    public void setUsingConnection(boolean isUsingConnection) {
        this.isUsingConnection = isUsingConnection;
        testName += "isUsingConnection=" + isUsingConnection + ";";
    }

    public boolean isUsingBulkCopyOptions() {
        return useBulkCopyOptions;
    }

    public void useBulkCopyOptions(boolean useBulkCopyOptions) {
        this.useBulkCopyOptions = useBulkCopyOptions;
        testName += "useBulkCopyOptions=" + useBulkCopyOptions + ";";
    }

    public SQLServerBulkCopyOptions getBulkOptions() {
        return bulkOptions;
    }

    public void setBulkOptions(SQLServerBulkCopyOptions bulkOptions) {
        this.bulkOptions = bulkOptions;
    }

    public String getConnectionString() {
        return connectionString;
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    public void setUsingColumnMapping() {
        this.isUsingColumnMapping = true;
    }

    public boolean isUsingColumnMapping() {
        return isUsingColumnMapping;
    }

    public void setColumnMapping(int sourceColOrdinal,
            int destColOrdinal) {
        setUsingColumnMapping();
        cm.add(new ColumnMap(sourceColOrdinal, destColOrdinal));
    }

    public void setColumnMapping(int sourceColOrdinal,
            String destColName) {
        setUsingColumnMapping();
        cm.add(new ColumnMap(sourceColOrdinal, destColName));
    }

    public void setColumnMapping(String sourceColName,
            String destColName) {
        setUsingColumnMapping();
        cm.add(new ColumnMap(sourceColName, destColName));
    }

    public void setColumnMapping(String sourceColName,
            int destColOrdinal) {
        setUsingColumnMapping();
        cm.add(new ColumnMap(sourceColName, destColOrdinal));
    }

    class ColumnMap {
        boolean sourceIsInt = false;
        boolean destIsInt = false;

        int srcInt = -1;
        String srcString = null;
        int destInt = -1;
        String destString = null;

        ColumnMap(int src,
                int dest) {
            this.sourceIsInt = true;
            this.destIsInt = true;

            this.srcInt = src;
            this.destInt = dest;
        }

        ColumnMap(String src,
                int dest) {
            this.sourceIsInt = false;
            this.destIsInt = true;

            this.srcString = src;
            this.destInt = dest;
        }

        ColumnMap(int src,
                String dest) {
            this.sourceIsInt = true;
            this.destIsInt = false;

            this.srcInt = src;
            this.destString = dest;
        }

        ColumnMap(String src,
                String dest) {
            this.sourceIsInt = false;
            this.destIsInt = false;

            this.srcString = src;
            this.destString = dest;
        }
    }
}
