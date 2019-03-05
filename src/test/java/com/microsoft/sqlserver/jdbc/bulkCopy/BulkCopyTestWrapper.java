/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import java.util.LinkedList;

import com.microsoft.sqlserver.jdbc.ISQLServerDataSource;
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
    private boolean isUsingConnection = false;

    /**
     * <code>true</code> if SQLServerBulkCopy should use XA connection object
     */
    private boolean isUsingXAConnection = false;

    /**
     * <code>true</code> if SQLServerBulkCopy should use pooled connection object
     */
    private boolean isUsingPooledConnection = false;

    /**
     * <code>true</code> if SQLServerBulkCopy should include SQLServerBulkCopyOptions
     */
    private boolean useBulkCopyOptions = false;

    /**
     * <code>true</code> if SQLServerBulkCopy should use column mapping
     */
    private boolean isUsingColumnMapping = false;

    public LinkedList<ColumnMap> cm = new LinkedList<>();

    private SQLServerBulkCopyOptions bulkOptions;

    private String connectionString;

    private ISQLServerDataSource dataSource;

    BulkCopyTestWrapper(String connectionString) {
        this.connectionString = connectionString;
    }

    /**
     * Returns boolean value <code>true</code> if connection object is used for testing bulk copy
     * 
     * @return isUsingConnection
     */
    public boolean isUsingConnection() {
        return isUsingConnection;
    }

    /**
     * Returns boolean value <code>true</code> if XA connection object is used for testing bulk copy
     * 
     * @return isUsingXAConnection
     */
    public boolean isUsingXAConnection() {
        return isUsingXAConnection;
    }

    /**
     * Returns boolean value <code>true</code> if pooled connection object is used for testing bulk copy
     * 
     * @return isUsingPooledConnection
     */
    public boolean isUsingPooledConnection() {
        return isUsingPooledConnection;
    }

    /**
     * @param isUsingConnection
     *        <code>true</code> if connection object should be passed in BulkCopy constructor <code>false</code> if
     *        connection string is to be passed to constructor
     */
    public void setUsingConnection(boolean isUsingConnection, ISQLServerDataSource ds) {
        this.isUsingConnection = isUsingConnection;
        testName += "isUsingConnection=" + isUsingConnection + ";";
        setDataSource(ds);
    }

    /**
     * Sets boolean property if XA Connection must be used to test bulk Copy
     * 
     * @param isUsingXAConnection
     *        <code>true</code> if XA connection object should be passed in BulkCopy constructor <code>false</code> if
     *        otherwise
     */
    public void setUsingXAConnection(boolean isUsingXAConnection, ISQLServerDataSource ds) {
        if (!isUsingConnection) {
            // Cannot use XA Connection if connection itself is not in use.
            isUsingXAConnection = false;
        }
        this.isUsingXAConnection = isUsingXAConnection;
        testName += "isUsingXAConnection=" + isUsingXAConnection + ";";
        if (isUsingXAConnection)
            setDataSource(ds);
    }

    /**
     * Sets boolean property if Pooled Connection must be used to test bulk Copy
     * 
     * @param isUsingPooledConnection
     *        <code>true</code> if Pooled connection object should be passed in BulkCopy constructor <code>false</code>
     *        if otherwise
     */
    public void setUsingPooledConnection(boolean isUsingPooledConnection, ISQLServerDataSource ds) {
        if (!isUsingConnection || isUsingXAConnection) {
            // Cannot use Pooled connection if connection itself is not in use or we are already using XA Connection
            isUsingPooledConnection = false;
        }
        this.isUsingPooledConnection = isUsingPooledConnection;
        testName += "isUsingPooledConnection=" + isUsingPooledConnection + ";";
        if (isUsingPooledConnection)
            setDataSource(ds);
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

    public ISQLServerDataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(ISQLServerDataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void setUsingColumnMapping() {
        this.isUsingColumnMapping = true;
    }

    public boolean isUsingColumnMapping() {
        return isUsingColumnMapping;
    }

    public void setColumnMapping(int sourceColOrdinal, int destColOrdinal) {
        setUsingColumnMapping();
        cm.add(new ColumnMap(sourceColOrdinal, destColOrdinal));
    }

    public void setColumnMapping(int sourceColOrdinal, String destColName) {
        setUsingColumnMapping();
        cm.add(new ColumnMap(sourceColOrdinal, destColName));
    }

    public void setColumnMapping(String sourceColName, String destColName) {
        setUsingColumnMapping();
        cm.add(new ColumnMap(sourceColName, destColName));
    }

    public void setColumnMapping(String sourceColName, int destColOrdinal) {
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

        ColumnMap(int src, int dest) {
            this.sourceIsInt = true;
            this.destIsInt = true;

            this.srcInt = src;
            this.destInt = dest;
        }

        ColumnMap(String src, int dest) {
            this.sourceIsInt = false;
            this.destIsInt = true;

            this.srcString = src;
            this.destInt = dest;
        }

        ColumnMap(int src, String dest) {
            this.sourceIsInt = true;
            this.destIsInt = false;

            this.srcInt = src;
            this.destString = dest;
        }

        ColumnMap(String src, String dest) {
            this.sourceIsInt = false;
            this.destIsInt = false;

            this.srcString = src;
            this.destString = dest;
        }
    }
}
