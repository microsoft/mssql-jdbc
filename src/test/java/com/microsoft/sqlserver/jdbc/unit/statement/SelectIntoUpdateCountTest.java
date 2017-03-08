/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBConnection;

@RunWith(JUnitPlatform.class)
public class SelectIntoUpdateCountTest extends AbstractTest {
    private static String tableName = "[#SourceTableForSelectInto]";

    /**
     * Tests update count returned by SELECT INTO
     * 
     * @throws SQLException
     */
    @Test
    public void testSelectIntoUpdateCount() throws SQLException {
        SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(connectionString);
        
        // Azure does not do SELECT INTO
        if (!DBConnection.isSqlAzure(con)) {
            Statement stmt = con.createStatement();
            
            // expected values
            int numRowsToCopy = 2;
    
            stmt.executeUpdate("CREATE TABLE " + tableName + " (col1 int primary key, col2 varchar(3), col3 varchar(128))");
            stmt.executeUpdate("INSERT INTO " + tableName + " VALUES (1, 'CAN', 'Canada')");
            stmt.executeUpdate("INSERT INTO " + tableName + " VALUES (2, 'USA', 'United States of America')");
            stmt.executeUpdate("INSERT INTO " + tableName + " VALUES (3, 'JPN', 'Japan')");

            int updateCount = stmt.executeUpdate("SELECT * INTO #TMPTABLE FROM " + tableName + " WHERE col1 <= 2");
            assertEquals(numRowsToCopy, updateCount, "Incorrect update count");
            
            if (null != stmt)
                stmt.close();
        }
        if (null != con)
            con.close();
    }

}
