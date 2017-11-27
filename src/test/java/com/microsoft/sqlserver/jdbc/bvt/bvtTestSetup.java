/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bvt;

import java.sql.SQLException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.DBStatement;
import com.microsoft.sqlserver.testframework.DBTable;

/**
 * 
 * Setting up the test
 */
@RunWith(JUnitPlatform.class)
public class bvtTestSetup extends AbstractTest {

    static DBTable table1;
    static DBTable table2;

    @BeforeAll
    public static void init() throws SQLException {
        try (DBConnection conn = new DBConnection(connectionString);
             DBStatement stmt = conn.createStatement()) {
            // create tables
            table1 = new DBTable(true);
            stmt.createTable(table1);
            stmt.populateTable(table1);
            table2 = new DBTable(true);
            stmt.createTable(table2);
            stmt.populateTable(table2);
        }
    }
}
