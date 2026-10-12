/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.PrepUtil;


public class StringParameterDefaultLengthConnectionTest extends AbstractTest {
    private static final String PROPERTY = "stringParameterDefaultLength";

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0, 1, 5000, 8001, Integer.MAX_VALUE})
    public void testConnectionPropertyRoutes(int value) throws Exception {
        int expected = Math.max(0, value);
        String url = TestUtils.addOrOverrideProperty(connectionString, PROPERTY, Integer.toString(value));
        try (SQLServerConnection connection = (SQLServerConnection) PrepUtil.getConnection(url)) {
            assertEquals(expected, connection.getStringParameterDefaultLength());
            assertUsable(connection);
        }

        Properties properties = new Properties();
        properties.setProperty(PROPERTY, Integer.toString(value));
        String overriddenUrl = TestUtils.addOrOverrideProperty(connectionString, PROPERTY, "23");
        try (SQLServerConnection connection = (SQLServerConnection) DriverManager.getConnection(overriddenUrl,
                properties)) {
            assertEquals(expected, connection.getStringParameterDefaultLength());
            assertUsable(connection);
        }

        SQLServerDataSource source = new SQLServerDataSource();
        source.setURL(overriddenUrl);
        source.setStringParameterDefaultLength(value);
        try (SQLServerConnection connection = (SQLServerConnection) source.getConnection()) {
            assertEquals(expected, connection.getStringParameterDefaultLength());
            assertUsable(connection);
        }
    }

    @Test
    public void testConnectionSettingsAreIndependent() throws Exception {
        SQLServerDataSource source = new SQLServerDataSource();
        source.setURL(connectionString);
        source.setStringParameterDefaultLength(5000);
        try (SQLServerConnection first = (SQLServerConnection) source.getConnection()) {
            source.setStringParameterDefaultLength(100);
            try (SQLServerConnection second = (SQLServerConnection) source.getConnection()) {
                assertEquals(5000, first.getStringParameterDefaultLength());
                assertEquals(100, second.getStringParameterDefaultLength());
            }
        }
    }

    private static void assertUsable(SQLServerConnection connection) throws Exception {
        try (Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery("SELECT 1")) {
            assertTrue(result.next());
            assertEquals(1, result.getInt(1));
        }
    }
}
