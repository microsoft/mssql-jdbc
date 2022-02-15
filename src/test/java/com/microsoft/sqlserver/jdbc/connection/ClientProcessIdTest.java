/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.microsoft.sqlserver.jdbc.TestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


/*
 * This test is for validating that client process ID gets registered with the server when available to the driver.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDW)
public class ClientProcessIdTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"trustServerCertificate", "true");
        setConnection();
    }

    private static int pid = 0;

    static {
        long pidLong = 0;
        try {
            pidLong = ProcessHandle.current().pid();
        } catch (NoClassDefFoundError e) { // ProcessHandle is Java 9+
        }
        pid = (pidLong > Integer.MAX_VALUE) ? 0 : (int) pidLong;
    }

    @Test
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.xJDBC42)
    public void testClientProcessId() throws SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setURL(connectionString);
        String sqlSelect = "select host_process_id from sys.dm_exec_sessions where session_id = @@SPID";

        try (Connection con = ds.getConnection(); Statement stmt = con.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(sqlSelect)) {
                if (rs.next()) {
                    assertEquals(pid, rs.getInt(1));
                } else {
                    assertTrue(false, "Expected row of data was not found.");
                }
            }
        }
    }
}
