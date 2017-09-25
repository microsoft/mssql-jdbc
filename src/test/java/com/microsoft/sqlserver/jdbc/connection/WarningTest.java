/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.testframework.AbstractTest;

@RunWith(JUnitPlatform.class)
public class WarningTest extends AbstractTest {
    @Test
    public void testWarnings() throws SQLException {
        SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString);

        Properties info = conn.getClientInfo();
        conn.setClientInfo(info);
        SQLWarning warn = conn.getWarnings();
        assertEquals(null, warn, "Warnings found.");

        Properties info2 = new Properties();
        String[] infoArray = {"prp1", "prp2", "prp3", "prp4", "prp5"};
        for (int i = 0; i < 5; i++) {
            info2.put(infoArray[i], "");
        }
        conn.setClientInfo(info2);
        warn = conn.getWarnings();
        for (int i = 0; i < 5; i++) {
            boolean found = false;
            List<String> list = Arrays.asList(infoArray);
            for (String word : list) {
                if (warn.toString().contains(word)) {
                    found = true;
                    break;
                }
            }
            assertTrue(found, "warning : '" + warn.toString() + "' not found!");
            warn = warn.getNextWarning();
            found = false;
        }
        conn.clearWarnings();

        conn.setClientInfo("prop7", "");
        warn = conn.getWarnings();
        assertTrue(warn.toString().contains("prop7"), "Warnings not found!");
        warn = warn.getNextWarning();
        assertEquals(null, warn, "Warnings found!");
    }
}
