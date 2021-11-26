/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.microsoft.sqlserver.jdbc.TestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDW)
public class StatementCancellationTest extends AbstractTest {
    private static final long DELAY_WAIT_MILLISECONDS = 10000;
    private static final long CANCEL_WAIT_MILLISECONDS = 5000;

    @BeforeAll
    public static void setupTests() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"trustServerCertificate", "true");
        setConnection();
    }

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
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    };
                    cancellationThread.setName("stmtCancel");
                    cancellationThread.start();

                    try {
                        timeStart = System.currentTimeMillis();
                        stmt.execute("WAITFOR DELAY '00:00:" + (DELAY_WAIT_MILLISECONDS / 1000) + "'");
                    } catch (SQLException e) {
                        // The query was canceled"), "Unexpected error message
                        assertTrue(e.getMessage().startsWith(TestResource.getResource("R_queryCancelled")),
                                TestResource.getResource("R_unexpectedExceptionContent"));
                    }
                }
            }
        } finally {
            timeEnd = System.currentTimeMillis();
            long timeDifference = timeEnd - timeStart;
            assertTrue(timeDifference >= CANCEL_WAIT_MILLISECONDS, TestResource.getResource("R_cancellationFailed"));
            assertTrue(timeDifference < DELAY_WAIT_MILLISECONDS, TestResource.getResource("R_cancellationFailed"));
            assertTrue((timeDifference - CANCEL_WAIT_MILLISECONDS) < 1000,
                    TestResource.getResource("R_cancellationFailed"));
        }
    }
}
