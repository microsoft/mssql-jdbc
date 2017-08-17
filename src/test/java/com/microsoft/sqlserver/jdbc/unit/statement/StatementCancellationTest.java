/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.testframework.AbstractTest;

@RunWith(JUnitPlatform.class)
public class StatementCancellationTest extends AbstractTest {
    private static final long DELAY_WAIT_MILLISECONDS = 10000;
    private static final long CANCEL_WAIT_MILLISECONDS = 5000;

    /**
     * Tests Statement Cancellation works when MultiSubnetFailover is set to true
     */
    @Test
    public void setMultiSubnetFailoverToTrue() throws SQLException {
        long timeStart = 0;
        long timeEnd = 0;

        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setURL(connectionString);
            ds.setMultiSubnetFailover(true);

            try (Connection conn = ds.getConnection();) {
                try (final Statement stmt = conn.createStatement()) {
                    final Thread cancellationThread = new Thread() {
                        public void run() {
                            try {
                                Thread.sleep(CANCEL_WAIT_MILLISECONDS);
                                stmt.cancel();
                            }
                            catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    };
                    cancellationThread.setName("stmtCancel");
                    cancellationThread.start();

                    try {
                        timeStart = System.currentTimeMillis();
                        stmt.execute("WAITFOR DELAY '00:00:" + (DELAY_WAIT_MILLISECONDS / 1000) + "'");
                    }
                    catch (SQLException e) {
                        assertTrue(e.getMessage().startsWith("The query was canceled"), "Unexpected error message.");
                    }
                }
            }
        }
        finally {
            timeEnd = System.currentTimeMillis();
            long timeDifference = timeEnd - timeStart;
            assertTrue(timeDifference >= CANCEL_WAIT_MILLISECONDS, "Cancellation failed.");
            assertTrue(timeDifference < DELAY_WAIT_MILLISECONDS, "Cancellation failed.");
            assertTrue((timeDifference - CANCEL_WAIT_MILLISECONDS) < 1000, "Cancellation failed.");
        }
    }
}
