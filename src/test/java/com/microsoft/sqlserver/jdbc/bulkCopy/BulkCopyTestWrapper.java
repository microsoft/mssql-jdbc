/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) 2016 Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

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

}


