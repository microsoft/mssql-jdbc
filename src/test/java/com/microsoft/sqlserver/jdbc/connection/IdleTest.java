/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.testframework.AbstractTest;

/*
 * This test is for testing idle connection detection
 */
@RunWith(JUnitPlatform.class)
public class IdleTest extends AbstractTest {

    @Test
    public void testIdle() {
        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setURL(connectionString);

            String sqlSelect = "SELECT 'result'";

            try (Connection con = ds.getConnection(); Statement stmt = con.createStatement()) {
                try (ResultSet rs = stmt.executeQuery(sqlSelect)) {
                    if (rs.next()) {
                        assertEquals(rs.getString(1), "result");
                    } else {
                        assertTrue(false, "Expected row of data was not found.");
                    }
                }

                // Sleep for an idle period to ensure we exercise the poll() path
                Thread.sleep(35000);

                Thread t1 = new Thread() {
                    public void run() {
                        try {
                            stmt.execute("WAITFOR DELAY '00:01:00'");
                            assertTrue(false, "stmt should have been cancelled but wasn't.");
                        } catch (SQLException e) {
                            assertEquals("The query was canceled.", e.getMessage());
                        }
                    };
                };
                t1.start();
                // Make sure the query has time to start
                Thread.sleep(1000);
                stmt.cancel();
                t1.join();

                try (ResultSet rs = stmt.executeQuery(sqlSelect)) {
                    if (rs.next()) {
                        assertEquals(rs.getString(1), "result");
                    } else {
                        assertTrue(false, "Expected row of data was not found.");
                    }
                }
            } catch (InterruptedException e) {
                assertTrue(false, "Test was interrupted.");
            }
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }
}
