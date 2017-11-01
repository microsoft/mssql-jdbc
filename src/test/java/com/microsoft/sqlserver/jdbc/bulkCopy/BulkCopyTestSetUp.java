/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import java.sql.SQLException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.DBPreparedStatement;
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
     * @throws SQLException 
     */
    @BeforeAll
    static void setUpSourceTable() throws SQLException {
        try (DBConnection con = new DBConnection(connectionString);
        	 DBStatement stmt = con.createStatement();
        	 DBPreparedStatement pstmt = new DBPreparedStatement(con);) {
            sourceTable = new DBTable(true);
            stmt.createTable(sourceTable);
            pstmt.populateTable(sourceTable);
        }
    }

    /**
     * drop source table after testing bulk copy
     * @throws SQLException 
     */
    @AfterAll
    static void dropSourceTable() throws SQLException {
        try (DBConnection con = new DBConnection(connectionString);
        	 DBStatement stmt = con.createStatement()) {
            stmt.dropTable(sourceTable);
        }
    }
}
