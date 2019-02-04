/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.unit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * A class for testing the UTF8 support changes.
 */
@RunWith(JUnitPlatform.class)
public class UTF8SupportTest extends AbstractTest {
    private static Connection connection;
    private static String databaseName;
    private static String tableName;

    /**
     * Test against UTF8 CHAR type.
     * 
     * @throws SQLException
     */
    @Test
    public void testChar() throws SQLException {
        if (TestUtils.serverSupportsUTF8(connection)) {
            createTable("char(10)");
            validate("teststring");
            // This is 10 UTF-8 bytes. D1 82 D0 B5 D1 81 D1 82 31 32
            validate("тест12");
            // E2 95 A1 E2 95 A4 E2 88 9E 2D
            validate("╡╤∞-");

            createTable("char(4000)");
            validate(String.join("", Collections.nCopies(400, "teststring")));
            validate(String.join("", Collections.nCopies(400, "тест12")));
            validate(String.join("", Collections.nCopies(400, "╡╤∞-")));

            createTable("char(4001)");
            validate(String.join("", Collections.nCopies(400, "teststring")) + "1");
            validate(String.join("", Collections.nCopies(400, "тест12")) + "1");
            validate(String.join("", Collections.nCopies(400, "╡╤∞-")) + "1");

            createTable("char(8000)");
            validate(String.join("", Collections.nCopies(800, "teststring")));
            validate(String.join("", Collections.nCopies(800, "тест12")));
            validate(String.join("", Collections.nCopies(800, "╡╤∞-")));
        }
    }

    /**
     * Test against UTF8 VARCHAR type.
     * 
     * @throws SQLException
     */
    @Test
    public void testVarchar() throws SQLException {
        if (TestUtils.serverSupportsUTF8(connection)) {
            createTable("varchar(10)");
            validate("teststring");
            validate("тест12");
            validate("╡╤∞-");

            createTable("varchar(4000)");
            validate(String.join("", Collections.nCopies(400, "teststring")));
            validate(String.join("", Collections.nCopies(400, "тест12")));
            validate(String.join("", Collections.nCopies(400, "╡╤∞-")));

            createTable("varchar(4001)");
            validate(String.join("", Collections.nCopies(400, "teststring")) + "1");
            validate(String.join("", Collections.nCopies(400, "тест12")) + "1");
            validate(String.join("", Collections.nCopies(400, "╡╤∞-")) + "1");

            createTable("varchar(8000)");
            validate(String.join("", Collections.nCopies(800, "teststring")));
            validate(String.join("", Collections.nCopies(800, "тест12")));
            validate(String.join("", Collections.nCopies(800, "╡╤∞-")));

            createTable("varchar(MAX)");
            validate(String.join("", Collections.nCopies(800, "teststring")));
            validate(String.join("", Collections.nCopies(800, "тест12")));
            validate(String.join("", Collections.nCopies(800, "╡╤∞-")));
        }
    }

    @BeforeAll
    public static void setUp() throws ClassNotFoundException, SQLException {
        connection = PrepUtil.getConnection(getConnectionString());
        if (TestUtils.serverSupportsUTF8(connection)) {
            databaseName = RandomUtil.getIdentifier("UTF8Database");
            tableName = RandomUtil.getIdentifier("RequestBoundaryTable");
            createDatabaseWithUTF8Collation();
            connection.setCatalog(databaseName);
        }
    }

    @AfterAll
    public static void cleanUp() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            if (TestUtils.serverSupportsUTF8(connection)) {
                TestUtils.dropDatabaseIfExists(databaseName, stmt);
            }
        }
        if (null != connection) {
            connection.close();
        }
    }

    private static void createDatabaseWithUTF8Collation() throws SQLException {
        try (Statement stmt = connection.createStatement();) {
            stmt.executeUpdate("CREATE DATABASE " + AbstractSQLGenerator.escapeIdentifier(databaseName)
                    + " COLLATE Cyrillic_General_100_CS_AS_SC_UTF8");
        }
    }

    private static void createTable(String columnType) throws SQLException {
        try (Statement stmt = connection.createStatement();) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            stmt.executeUpdate(
                    "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (c " + columnType + ")");
        }
    }

    public void clearTable() throws SQLException {
        try (Statement stmt = connection.createStatement();) {
            stmt.executeUpdate("DELETE FROM " + AbstractSQLGenerator.escapeIdentifier(tableName));
        }
    }

    public void validate(String value) throws SQLException {
        try (PreparedStatement psInsert = connection
                .prepareStatement("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " VALUES(?)");
                PreparedStatement psFetch = connection
                        .prepareStatement("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName));
                Statement stmt = connection.createStatement();) {
            clearTable();
            // Used for exact byte comparison.
            byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

            psInsert.setString(1, value);
            psInsert.executeUpdate();

            // Fetch using Statement.
            try (ResultSet rs = stmt
                    .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                // Compare Strings.
                assertEquals(value, rs.getString(1));
                // Test UTF8 sequence returned from getBytes().
                assertArrayEquals(valueBytes, rs.getBytes(1));
            }

            // Fetch using PreparedStatement.
            try (ResultSet rsPreparedStatement = psFetch.executeQuery()) {
                rsPreparedStatement.next();
                assertEquals(value, rsPreparedStatement.getString(1));
                assertArrayEquals(valueBytes, rsPreparedStatement.getBytes(1));
            }
        }
    }
}
