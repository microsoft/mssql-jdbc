/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.DBStatement;
import com.microsoft.sqlserver.testframework.DBTable;;

/**
 * Create and drop source table needed for testing bulk copy
 */
@RunWith(JUnitPlatform.class)
public class BulkCopyTestSetUp extends AbstractTest {

    static DBTable sourceTable;

    /**
     * Create source table needed for testing bulk copy
     */
    @BeforeAll
    static void setUpSourceTable() {
        DBConnection con = null;
        DBStatement stmt = null;
        try {
            con = new DBConnection(connectionString);
            stmt = con.createStatement();
            sourceTable = new DBTable(true);
            stmt.createTable(sourceTable);
            stmt.populateTable(sourceTable);
        }
        finally {
            con.close();
        }
    }

    /**
     * drop source table after testing bulk copy
     */
    @AfterAll
    static void dropSourceTable() {
        DBConnection con = null;
        DBStatement stmt = null;
        try {
            con = new DBConnection(connectionString);
            stmt = con.createStatement();
            stmt.dropTable(sourceTable);
        }
        finally {
            con.close();
        }
    }

}
