/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.datatypes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.junit.jupiter.api.Tag;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;

@RunWith(JUnitPlatform.class)
@DisplayName("Test Json Functions")
public class JSONFunctionTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }
    
    private static final String JSON_FILE_PATH = "large_json.json";

    /**
     * Test ISJSON function with JSON.
     * ISJSON -> Tests whether a string contains valid JSON.
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testISJSON() throws SQLException {
        String dstTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTable")));

        try (Connection conn = DriverManager.getConnection(connectionString);) {
            try (Statement dstStmt = conn.createStatement()) {

                dstStmt.executeUpdate(
                        "CREATE TABLE " + dstTable + " (testCol JSON);");

                String validJson = "{\"key1\":\"value1\"}";
                dstStmt.executeUpdate(
                        "INSERT INTO " + dstTable + " (testCol) VALUES ('" + validJson + "')");

                String select = "SELECT testCol, " +
                        "ISJSON(testCol) AS isJsonValid " +
                        "FROM " + dstTable;
                try (ResultSet rs = dstStmt.executeQuery(select)) {
                    assertTrue(rs.next());
                    assertEquals(validJson, rs.getString("testCol"));
                    assertEquals(1, rs.getInt("isJsonValid"));
                }

            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                try (Statement stmt = conn.createStatement();) {
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }
    }

    /**
     * Test ISJSON function with valid and invalid JSON value.
     * ISJSON -> Tests whether a string contains valid JSON.
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testISJSONWithVariousInputs() throws SQLException {
        String dstTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier("dstTable"));

        try (Connection conn = DriverManager.getConnection(connectionString);) {
            try (Statement stmt = conn.createStatement()) {

                stmt.executeUpdate(
                        "CREATE TABLE " + dstTable + " (testCol NVARCHAR(MAX));");

                String validJson = "{\"key1\":\"value1\"}";
                String invalidJson = "Not a JSON string";
                String jsonScalar = "123";

                stmt.executeUpdate("INSERT INTO " + dstTable + " (testCol) VALUES ('" + validJson + "')");
                stmt.executeUpdate("INSERT INTO " + dstTable + " (testCol) VALUES ('" + invalidJson + "')");
                stmt.executeUpdate("INSERT INTO " + dstTable + " (testCol) VALUES ('" + jsonScalar + "')");

                String select = "SELECT testCol, " +
                        "ISJSON(testCol) AS isJsonValid " +
                        "FROM " + dstTable;
                try (ResultSet rs = stmt.executeQuery(select)) {
                    assertTrue(rs.next());
                    assertEquals(validJson, rs.getString("testCol"));
                    assertEquals(1, rs.getInt("isJsonValid"));

                    assertTrue(rs.next());
                    assertEquals(invalidJson, rs.getString("testCol"));
                    assertEquals(0, rs.getInt("isJsonValid"));

                    assertTrue(rs.next());
                    assertEquals(jsonScalar, rs.getString("testCol"));
                    assertEquals(0, rs.getInt("isJsonValid"));
                }

            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                try (Statement stmt = conn.createStatement();) {
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }
    }

    /**
     * Test JSON_ARRAY function without NULL values.
     * JSON_ARRAY -> Constructs JSON array text from zero or more expressions.
     * input: JSON_ARRAY('value1', 123, NULL) ->
     * output: ["value1",123]
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJSONArrayWithoutNulls() throws SQLException {
        String dstTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTable")));

        try (Connection conn = DriverManager.getConnection(connectionString);) {
            try (Statement dstStmt = conn.createStatement()) {
                dstStmt.executeUpdate(
                        "CREATE TABLE " + dstTable + " (testCol JSON);");

                String data1 = "SELECT JSON_ARRAY('value1', 123, NULL) AS jsonArray";
                dstStmt.executeUpdate("INSERT INTO " + dstTable + " (testCol) " + data1);

                String select = "SELECT testCol FROM " + dstTable;
                try (ResultSet rs = dstStmt.executeQuery(select)) {
                    assertTrue(rs.next());
                    assertEquals("[\"value1\",123]", rs.getString("testCol"));
                }
            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                try (Statement stmt = conn.createStatement();) {
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }
    }

    /**
     * Test JSON_ARRAY function with NULL values included.
     * JSON_ARRAY -> Constructs JSON array text from zero or more expressions.
     * input: JSON_ARRAY('value1', 123, NULL, 'value2' NULL ON NULL) ->
     * output: ["value1",123,null,"value2"]
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJSONArrayWithNulls() throws SQLException {
        String dstTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTable")));

        try (Connection conn = DriverManager.getConnection(connectionString);) {
            try (Statement dstStmt = conn.createStatement()) {
                dstStmt.executeUpdate(
                        "CREATE TABLE " + dstTable + " (testCol JSON);");

                String data2 = "SELECT JSON_ARRAY('value1', 123, NULL, 'value2' NULL ON NULL) AS jsonArray";
                dstStmt.executeUpdate("INSERT INTO " + dstTable + " (testCol) " + data2);

                String select = "SELECT testCol FROM " + dstTable;
                try (ResultSet rs = dstStmt.executeQuery(select)) {
                    assertTrue(rs.next());
                    assertEquals("[\"value1\",123,null,\"value2\"]", rs.getString("testCol"));
                }
            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                try (Statement stmt = conn.createStatement();) {
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }
    }

    /**
     * Test JSON_ARRAY with string, JSON object, and JSON array.
     * JSON_ARRAY -> Constructs JSON array text from zero or more expressions.
     * input: JSON_ARRAY('a', JSON_OBJECT('name':'value', 'type':1), JSON_ARRAY(1,
     * null, 2 NULL ON NULL)) ->
     * output: ["a",{"name":"value","type":1},[1,null,2]]
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJSONArrayWithMixedElements() throws SQLException {
        String dstTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTable")));

        try (Connection conn = DriverManager.getConnection(connectionString);) {
            try (Statement dstStmt = conn.createStatement()) {
                dstStmt.executeUpdate(
                        "CREATE TABLE " + dstTable + " (testCol JSON);");

                String data = "SELECT JSON_ARRAY('a', JSON_OBJECT('name':'value', 'type':1), JSON_ARRAY(1, null, 2 NULL ON NULL)) AS jsonArray";
                dstStmt.executeUpdate("INSERT INTO " + dstTable + " (testCol) " + data);

                String select = "SELECT testCol FROM " + dstTable;
                try (ResultSet rs = dstStmt.executeQuery(select)) {
                    assertTrue(rs.next());
                    assertEquals("[\"a\",{\"name\":\"value\",\"type\":1},[1,null,2]]", rs.getString("testCol"));
                }
            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                try (Statement stmt = conn.createStatement();) {
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }
    }

    /**
     * Test JSON_ARRAY with variables and SQL expressions.
     * JSON_ARRAY -> Constructs JSON array text from zero or more expressions.
     * input: JSON_ARRAY(1, @id_value, (SELECT @@SPID)) ->
     * output: [1,"<GUID>","<SPID>"]
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJSONArrayWithVariablesAndExpressions() throws SQLException {
        String dstTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTable")));

        try (Connection conn = DriverManager.getConnection(connectionString);) {
            try (Statement dstStmt = conn.createStatement()) {
                dstStmt.executeUpdate(
                        "CREATE TABLE " + dstTable + " (testCol NVARCHAR(MAX));");

                String data = "DECLARE @id_value nvarchar(64) = NEWID(); " +
                        "INSERT INTO " + dstTable + " (testCol) " +
                        "SELECT JSON_ARRAY(1, @id_value, (SELECT @@SPID)) AS jsonArray";

                dstStmt.execute(data);

                String select = "SELECT testCol FROM " + dstTable;
                try (ResultSet rs = dstStmt.executeQuery(select)) {
                    assertTrue(rs.next());
                    String jsonArray = rs.getString("testCol");
                    assertTrue(jsonArray.startsWith("[1,\""));
                    assertTrue(jsonArray.contains("\","));
                    assertTrue(jsonArray.endsWith("]"));
                }
            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                try (Statement stmt = conn.createStatement();) {
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }
    }

    /**
     * Test JSON_ARRAY per row in the query.
     * JSON_ARRAY -> Constructs JSON array text from zero or more expressions.
     * input: JSON_ARRAY(s.host_name, s.program_name, s.client_interface_name) ->
     * output: ["<host_name>","<program_name>","<client_interface_name>"]
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJSONArrayPerRow() throws SQLException {
        String dstTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTable")));

        try (Connection conn = DriverManager.getConnection(connectionString);) {
            try (Statement dstStmt = conn.createStatement()) {
                dstStmt.executeUpdate(
                        "CREATE TABLE " + dstTable + " (session_id INT, info JSON);");

                String data = "SELECT s.session_id, JSON_ARRAY(s.host_name, s.program_name, s.client_interface_name) AS info "
                        +
                        "FROM sys.dm_exec_sessions AS s " +
                        "WHERE s.is_user_process = 1";
                dstStmt.executeUpdate("INSERT INTO " + dstTable + " (session_id, info) " + data);

                String select = "SELECT session_id, info FROM " + dstTable;
                try (ResultSet rs = dstStmt.executeQuery(select)) {
                    while (rs.next()) {
                        int sessionId = rs.getInt("session_id");
                        String info = rs.getString("info");
                        assertTrue(info.startsWith("[\""));
                        assertTrue(info.endsWith("\"]"));
                    }
                }
            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                try (Statement stmt = conn.createStatement();) {
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }
    }

    /**
     * Test JSON_ARRAYAGG function.
     * JSON_ARRAYAGG -> Constructs a JSON array from an aggregation of SQL data or
     * columns.
     * input: JSON_ARRAYAGG(testCol) ->
     * output: ["<value1>","<value2>"]
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJSONArrayAgg() throws SQLException {
        String dstTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTable")));

        try (Connection conn = DriverManager.getConnection(connectionString);) {
            try (Statement dstStmt = conn.createStatement()) {

                dstStmt.executeUpdate(
                        "CREATE TABLE " + dstTable + " (testCol JSON);");

                dstStmt.executeUpdate("INSERT INTO " + dstTable + " VALUES ('{\"key\":\"value1\"}');");
                dstStmt.executeUpdate("INSERT INTO " + dstTable + " VALUES ('{\"key\":\"value2\"}');");

                String select = "SELECT JSON_ARRAYAGG(testCol) AS jsonArrayAgg FROM " + dstTable;
                try (ResultSet rs = dstStmt.executeQuery(select)) {
                    assertTrue(rs.next());
                    assertEquals("[{\"key\":\"value1\"},{\"key\":\"value2\"}]", rs.getString("jsonArrayAgg"));
                }

            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                try (Statement stmt = conn.createStatement();) {
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }
    }

    /**
     * Test JSON_ARRAYAGG with three elements from a result set.
     * JSON_ARRAYAGG -> Constructs a JSON array from an aggregation of SQL data or
     * columns.
     * input: JSON_ARRAYAGG(c1) ->
     * output: ["c","b","a"]
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJSONArrayAggWithThreeElements() throws SQLException {
        String select = "SELECT JSON_ARRAYAGG(c1) AS jsonArrayAgg FROM (VALUES ('c'), ('b'), ('a')) AS t(c1)";
        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(select)) {
            assertTrue(rs.next());
            assertEquals("[\"c\",\"b\",\"a\"]", rs.getString("jsonArrayAgg"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test JSON_ARRAYAGG with three elements ordered by the value of the column.
     * JSON_ARRAYAGG -> Constructs a JSON array from an aggregation of SQL data or
     * columns.
     * input: JSON_ARRAYAGG(c1 ORDER BY c1) ->
     * output: ["a","b","c"]
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJSONArrayAggWithOrderedElements() throws SQLException {
        String select = "SELECT JSON_ARRAYAGG(c1 ORDER BY c1) AS jsonArrayAgg FROM (VALUES ('c'), ('b'), ('a')) AS t(c1)";
        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(select)) {
            assertTrue(rs.next());
            assertEquals("[\"a\",\"b\",\"c\"]", rs.getString("jsonArrayAgg"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test JSON_ARRAYAGG with two columns.
     * JSON_ARRAYAGG -> Constructs a JSON array from an aggregation of SQL data or
     * columns.
     * input: JSON_ARRAYAGG(c.name ORDER BY c.column_id) ->
     * output: ["column1","column2"]
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJSONArrayAggWithTwoColumns() throws SQLException {
        String dstTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTable")));

        try (Connection conn = DriverManager.getConnection(connectionString);) {
            try (Statement stmt = conn.createStatement()) {
                // Create table and insert data
                stmt.executeUpdate(
                        "CREATE TABLE " + dstTable + " (object_id INT, name NVARCHAR(50), column_id INT);");

                stmt.executeUpdate(
                        "INSERT INTO " + dstTable + " (object_id, name, column_id) VALUES (1, 'column1', 1);");
                stmt.executeUpdate(
                        "INSERT INTO " + dstTable + " (object_id, name, column_id) VALUES (1, 'column2', 2);");
                stmt.executeUpdate(
                        "INSERT INTO " + dstTable + " (object_id, name, column_id) VALUES (2, 'column3', 1);");
                stmt.executeUpdate(
                        "INSERT INTO " + dstTable + " (object_id, name, column_id) VALUES (2, 'column4', 2);");

                String select = "SELECT object_id, JSON_ARRAYAGG(name ORDER BY column_id) AS column_list " +
                        "FROM " + dstTable + " " +
                        "GROUP BY object_id";
                try (ResultSet rs = stmt.executeQuery(select)) {
                    while (rs.next()) {
                        int objectId = rs.getInt("object_id");
                        String columnList = rs.getString("column_list");
                        if (objectId == 1) {
                            assertEquals("[\"column1\",\"column2\"]", columnList);
                        } else if (objectId == 2) {
                            assertEquals("[\"column3\",\"column4\"]", columnList);
                        } else {
                            fail("Unexpected object_id: " + objectId);
                        }
                    }
                }
            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                try (Statement stmt = conn.createStatement();) {
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }
    }

    /**
     * Test JSON_MODIFY function with various operations.
     * JSON_MODIFY -> Updates the value of a property in a JSON string and returns
     * the updated JSON string.
     * input: JSON_MODIFY(testCol, '$.key', 'value2') ->
     * output: {"key":"value2"}
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJSONModify() throws SQLException {
        String dstTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTable")));

        try (Connection conn = DriverManager.getConnection(connectionString);) {
            try (Statement dstStmt = conn.createStatement()) {

                dstStmt.executeUpdate(
                        "CREATE TABLE " + dstTable + " (testCol JSON);");

                String data = "{\"key\":\"value1\"}";
                dstStmt.executeUpdate(
                        "INSERT INTO " + dstTable + " (testCol) VALUES ('" + data + "')");

                String update = "UPDATE " + dstTable + " SET testCol = JSON_MODIFY(testCol, '$.key', 'value2')";
                dstStmt.executeUpdate(update);

                String select = "SELECT testCol FROM " + dstTable;
                try (ResultSet rs = dstStmt.executeQuery(select)) {
                    assertTrue(rs.next());
                    assertEquals("{\"key\":\"value2\"}", rs.getString("testCol"));
                }

                String newRow = "{\"name\":\"John\",\"skills\":[\"C#\",\"SQL\"]}";
                dstStmt.executeUpdate("INSERT INTO " + dstTable + " (testCol) VALUES ('" + newRow + "')");

                select = "SELECT testCol FROM " + dstTable + " WHERE JSON_VALUE(testCol, '$.name') = 'John'";
                try (ResultSet rs = dstStmt.executeQuery(select)) {
                    assertTrue(rs.next());
                    assertEquals("{\"name\":\"John\",\"skills\":[\"C#\",\"SQL\"]}", rs.getString("testCol"));
                }

                String delete = "DELETE FROM " + dstTable + " WHERE JSON_VALUE(testCol, '$.key') = 'value2'";
                dstStmt.executeUpdate(delete);

                select = "SELECT testCol FROM " + dstTable;
                try (ResultSet rs = dstStmt.executeQuery(select)) {
                    assertTrue(rs.next());
                    assertEquals("{\"name\":\"John\",\"skills\":[\"C#\",\"SQL\"]}", rs.getString("testCol"));
                }

                String addKeyValue = "UPDATE " + dstTable + " SET testCol = JSON_MODIFY(testCol, '$.surname', 'Smith')";
                dstStmt.executeUpdate(addKeyValue);

                select = "SELECT testCol FROM " + dstTable;
                try (ResultSet rs = dstStmt.executeQuery(select)) {
                    assertTrue(rs.next());
                    assertEquals("{\"name\":\"John\",\"skills\":[\"C#\",\"SQL\"],\"surname\":\"Smith\"}",
                            rs.getString("testCol"));
                }

                String addSkill = "UPDATE " + dstTable
                        + " SET testCol = JSON_MODIFY(testCol, 'append $.skills', 'Azure')";
                dstStmt.executeUpdate(addSkill);

                select = "SELECT testCol FROM " + dstTable;
                try (ResultSet rs = dstStmt.executeQuery(select)) {
                    assertTrue(rs.next());
                    assertEquals("{\"name\":\"John\",\"skills\":[\"C#\",\"SQL\",\"Azure\"],\"surname\":\"Smith\"}",
                            rs.getString("testCol"));
                }
            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                try (Statement stmt = conn.createStatement();) {
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }
    }

    /**
     * Test JSON_MODIFY with multiple updates.
     * JSON_MODIFY -> Updates the value of a property in a JSON string and returns
     * the updated JSON string.
     * input: JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(@info, '$.name', 'Mike'),
     * '$.surname', 'Smith'), 'append $.skills', 'Azure') ->
     * output: {"name":"Mike","skills":["C#","SQL","Azure"],"surname":"Smith"}
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJSONModifyMultipleUpdates() throws SQLException {
        String json = "{\"name\":\"John\",\"skills\":[\"C#\",\"SQL\"]}";
        String expectedJson = "{\"name\":\"Mike\",\"skills\":[\"C#\",\"SQL\",\"Azure\"],\"surname\":\"Smith\"}";

        String update = "DECLARE @info JSON = '" + json + "'; " +
                "SET @info = JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(@info, '$.name', 'Mike'), '$.surname', 'Smith'), 'append $.skills', 'Azure'); "
                +
                "SELECT @info AS info;";
        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(update)) {
            assertTrue(rs.next());
            assertEquals(expectedJson, rs.getString("info"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test JSON_MODIFY to rename a key.
     * JSON_MODIFY -> Updates the value of a property in a JSON string and returns
     * the updated JSON string.
     * input: JSON_MODIFY(JSON_MODIFY(@product, '$.Price', CAST(JSON_VALUE(@product,
     * '$.price') AS NUMERIC(4, 2))), '$.price', NULL) ->
     * output: {"Price":49.99}
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJSONModifyRenameKey() throws SQLException {
        String json = "{\"price\":49.99}";
        String expectedJson = "{\"Price\":49.99}";

        String update = "DECLARE @product JSON = '" + json + "'; " +
                "SET @product = JSON_MODIFY(JSON_MODIFY(@product, '$.Price', CAST(JSON_VALUE(@product, '$.price') AS NUMERIC(4, 2))), '$.price', NULL); "
                +
                "SELECT @product AS product;";
        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(update)) {
            assertTrue(rs.next());
            assertEquals(expectedJson, rs.getString("product"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test JSON_MODIFY to increment a value.
     * JSON_MODIFY -> Updates the value of a property in a JSON string and returns
     * the updated JSON string.
     * input: JSON_MODIFY(@stats, '$.click_count', CAST(JSON_VALUE(@stats,
     * '$.click_count') AS INT) + 1) ->
     * output: {"click_count":174}
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJSONModifyIncrementValue() throws SQLException {
        String json = "{\"click_count\":173}";
        String expectedJson = "{\"click_count\":174}";

        String update = "DECLARE @stats JSON = '" + json + "'; " +
                "SET @stats = JSON_MODIFY(@stats, '$.click_count', CAST(JSON_VALUE(@stats, '$.click_count') AS INT) + 1); "
                +
                "SELECT @stats AS stats;";
        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(update)) {
            assertTrue(rs.next());
            assertEquals(expectedJson, rs.getString("stats"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test JSON_MODIFY to update a JSON column.
     * JSON_MODIFY -> Updates the value of a property in a JSON string and returns
     * the updated JSON string.
     * input: JSON_MODIFY(jsonCol, '$.info.address.town', 'London') ->
     * output: {"info":{"address":{"town":"London"}}}
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJSONModifyUpdateJsonColumn() throws SQLException {
        String dstTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTable")));

        try (Connection conn = DriverManager.getConnection(connectionString);) {
            try (Statement dstStmt = conn.createStatement()) {

                dstStmt.executeUpdate(
                        "CREATE TABLE " + dstTable + " (EmployeeID INT, jsonCol JSON);");

                String data = "{\"info\":{\"address\":{\"town\":\"OldTown\"}}}";
                dstStmt.executeUpdate(
                        "INSERT INTO " + dstTable + " (EmployeeID, jsonCol) VALUES (17, '" + data + "')");

                String update = "UPDATE " + dstTable
                        + " SET jsonCol = JSON_MODIFY(jsonCol, '$.info.address.town', 'London') WHERE EmployeeID = 17";
                dstStmt.executeUpdate(update);

                String select = "SELECT jsonCol FROM " + dstTable + " WHERE EmployeeID = 17";
                try (ResultSet rs = dstStmt.executeQuery(select)) {
                    assertTrue(rs.next());
                    assertEquals("{\"info\":{\"address\":{\"town\":\"London\"}}}", rs.getString("jsonCol"));
                }
            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                try (Statement stmt = conn.createStatement();) {
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }
    }

    /**
     * Test JSON_OBJECT function to return an empty JSON object.
     * JSON_OBJECT() -> Constructs JSON object text from zero or more expressions.
     * input: JSON_OBJECT() ->
     * output: {}
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJSONObjectEmpty() throws SQLException {
        String select = "SELECT JSON_OBJECT() AS jsonObject";
        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(select)) {
            assertTrue(rs.next());
            assertEquals("{}", rs.getString("jsonObject"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test JSON_OBJECT function to return a JSON object with one key since the
     * value for one of the keys is NULL and the ABSENT ON NULL option is specified.
     * JSON_OBJECT() -> Constructs JSON object text from zero or more expressions.
     * input: JSON_OBJECT('name':'value', 'type':NULL ABSENT ON NULL) ->
     * output: {"name":"value"}
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJSONObjectWithMultipleKeys() throws SQLException {
        String select = "SELECT JSON_OBJECT('name':'value', 'type':NULL ABSENT ON NULL) AS jsonObject";
        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(select)) {
            assertTrue(rs.next());
            assertEquals("{\"name\":\"value\"}", rs.getString("jsonObject"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test JSON_OBJECT function to return a JSON object with two keys. One key
     * contains a JSON string and another key contains a JSON array.
     * JSON_OBJECT() -> Constructs JSON object text from zero or more expressions.
     * input: JSON_OBJECT('name':'value', 'type':JSON_ARRAY(1, 2)) ->
     * output: {"name":"value","type":[1,2]}
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJSONObjectWithJsonArray() throws SQLException {
        String select = "SELECT JSON_OBJECT('name':'value', 'type':JSON_ARRAY(1, 2)) AS jsonObject";
        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(select)) {
            assertTrue(rs.next());
            assertEquals("{\"name\":\"value\",\"type\":[1,2]}", rs.getString("jsonObject"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test JSON_OBJECT function to return a JSON object with two keys. One key
     * contains a JSON string and another key contains a JSON object.
     * JSON_OBJECT() -> Constructs JSON object text from zero or more expressions.
     * input: JSON_OBJECT('name':'value', 'type':JSON_OBJECT('type_id':1,
     * 'name':'a')) ->
     * output: {"name":"value","type":{"type_id":1,"name":"a"}}
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJSONObjectWithNestedJsonObject() throws SQLException {
        String select = "SELECT JSON_OBJECT('name':'value', 'type':JSON_OBJECT('type_id':1, 'name':'a')) AS jsonObject";
        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(select)) {
            assertTrue(rs.next());
            assertEquals("{\"name\":\"value\",\"type\":{\"type_id\":1,\"name\":\"a\"}}", rs.getString("jsonObject"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test JSON_OBJECT function to return a JSON object per row in the query.
     * JSON_OBJECT() -> Constructs a JSON object per row in the query.
     * input: JSON_OBJECT('security_id':s.security_id, 'login':s.login_name,
     * 'status':s.status) ->
     * output:
     * {"security_id":"<security_id>","login":"<login_name>","status":"<status>"}
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJSONObjectPerRow() throws SQLException {
        String select = "SELECT s.session_id, JSON_OBJECT('security_id':s.security_id, 'login':s.login_name, 'status':s.status) AS info "
                +
                "FROM sys.dm_exec_sessions AS s " +
                "WHERE s.is_user_process = 1";
        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(select)) {
            while (rs.next()) {
                int sessionId = rs.getInt("session_id");
                String info = rs.getString("info");
                assertTrue(info.contains("\"security_id\":\""));
                assertTrue(info.contains("\"login\":\""));
                assertTrue(info.contains("\"status\":\""));
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test JSON_OBJECTAGG function to construct a JSON object with three properties
     * from a result set.
     * JSON_OBJECTAGG() -> Constructs a JSON object with three properties.
     * input: JSON_OBJECTAGG(c1:c2) ->
     * output: {"key1":"c","key2":"b","key3":"a"}
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJSONObjectAggWithThreeProperties() throws SQLException {
        String select = "SELECT JSON_OBJECTAGG(c1:c2) AS jsonObjectAgg FROM (VALUES('key1', 'c'), ('key2', 'b'), ('key3','a')) AS t(c1, c2)";
        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(select)) {
            assertTrue(rs.next());
            assertEquals("{\"key1\":\"c\",\"key2\":\"b\",\"key3\":\"a\"}", rs.getString("jsonObjectAgg"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test JSON_OBJECTAGG function to return a result with two columns. The first
     * column contains the object_id value.
     * The second column contains a JSON object where the key is the column name and
     * value is the column_id.
     * JSON_OBJECTAGG() -> Constructs a JSON object with column names and column
     * IDs.
     * input: JSON_OBJECTAGG(c.name:c.column_id) ->
     * output: {"column1":1,"column2":2}
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJSONObjectAggWithColumnNamesAndIDs() throws SQLException {
        String dstTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTable")));

        try (Connection conn = DriverManager.getConnection(connectionString);) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(
                        "CREATE TABLE " + dstTable + " (object_id INT, name NVARCHAR(50), column_id INT);");

                stmt.executeUpdate(
                        "INSERT INTO " + dstTable + " (object_id, name, column_id) VALUES (1, 'column1', 1);");
                stmt.executeUpdate(
                        "INSERT INTO " + dstTable + " (object_id, name, column_id) VALUES (1, 'column2', 2);");
                stmt.executeUpdate(
                        "INSERT INTO " + dstTable + " (object_id, name, column_id) VALUES (2, 'column3', 1);");
                stmt.executeUpdate(
                        "INSERT INTO " + dstTable + " (object_id, name, column_id) VALUES (2, 'column4', 2);");

                String select = "SELECT object_id, JSON_OBJECTAGG(name:column_id) AS columns " +
                        "FROM " + dstTable + " " +
                        "GROUP BY object_id";
                try (ResultSet rs = stmt.executeQuery(select)) {
                    while (rs.next()) {
                        int objectId = rs.getInt("object_id");
                        String columns = rs.getString("columns");
                        if (objectId == 1) {
                            assertEquals("{\"column1\":1,\"column2\":2}", columns);
                        } else if (objectId == 2) {
                            assertEquals("{\"column3\":1,\"column4\":2}", columns);
                        } else {
                            fail("Unexpected object_id: " + objectId);
                        }
                    }
                }
            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                try (Statement stmt = conn.createStatement();) {
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }
    }

    /**
     * Test JSON_PATH_EXISTS function to return 1 since the input JSON string
     * contains the specified SQL/JSON path.
     * JSON_PATH_EXISTS() -> Checks if a specified SQL/JSON path exists in the input
     * JSON string.
     * input: JSON_PATH_EXISTS(@jsonInfo, '$.info.address') ->
     * output: 1
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJSONPathExistsTrue() throws SQLException {
        String jsonInfo = "{\"info\":{\"address\":[{\"town\":\"Paris\"},{\"town\":\"London\"}]}}";
        String select = "DECLARE @jsonInfo AS JSON = N'" + jsonInfo + "'; " +
                "SELECT JSON_PATH_EXISTS(@jsonInfo, '$.info.address') AS pathExists";
        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(select)) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("pathExists"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test JSON_PATH_EXISTS function to return 0 since the input JSON string
     * doesn't contain the specified SQL/JSON path.
     * JSON_PATH_EXISTS() -> Checks if a specified SQL/JSON path exists in the input
     * JSON string.
     * input: JSON_PATH_EXISTS(@jsonInfo, '$.info.addresses') ->
     * output: 0
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJSONPathExistsFalse() throws SQLException {
        String jsonInfo = "{\"info\":{\"address\":[{\"town\":\"Paris\"},{\"town\":\"London\"}]}}";
        String select = "DECLARE @jsonInfo AS JSON = N'" + jsonInfo + "'; " +
                "SELECT JSON_PATH_EXISTS(@jsonInfo, '$.info.addresses') AS pathExists";
        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(select)) {
            assertTrue(rs.next());
            assertEquals(0, rs.getInt("pathExists"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test JSON_QUERY function to return a JSON fragment from a CustomFields column
     * in query results.
     * JSON_QUERY() -> Extracts a JSON fragment from a JSON string.
     * input: JSON_QUERY(CustomFields,'$.OtherLanguages') ->
     * output: JSON fragment of OtherLanguages
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJSONQueryFragment() throws SQLException {
        String dstTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTable")));

        try (Connection conn = DriverManager.getConnection(connectionString);) {
            try (Statement dstStmt = conn.createStatement()) {

                dstStmt.executeUpdate(
                        "CREATE TABLE " + dstTable + " (PersonID INT, FullName NVARCHAR(100), CustomFields JSON);");

                String insert = "INSERT INTO " + dstTable + " (PersonID, FullName, CustomFields) VALUES " +
                        "(1, 'John Doe', '{\"OtherLanguages\":[\"French\",\"Spanish\"]}'), " +
                        "(2, 'Jane Smith', '{\"OtherLanguages\":[\"German\",\"Italian\"]}')";
                dstStmt.executeUpdate(insert);

                String select = "SELECT PersonID, FullName, JSON_QUERY(CustomFields, '$.OtherLanguages') AS Languages FROM "
                        + dstTable;
                try (ResultSet rs = dstStmt.executeQuery(select)) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt("PersonID"));
                    assertEquals("John Doe", rs.getString("FullName"));
                    assertEquals("[\"French\",\"Spanish\"]", rs.getString("Languages"));

                    assertTrue(rs.next());
                    assertEquals(2, rs.getInt("PersonID"));
                    assertEquals("Jane Smith", rs.getString("FullName"));
                    assertEquals("[\"German\",\"Italian\"]", rs.getString("Languages"));
                }

            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                try (Statement stmt = conn.createStatement();) {
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }
    }

    /**
     * Test JSON_QUERY function to include JSON fragments in the output of the FOR
     * JSON clause.
     * JSON_QUERY() -> Extracts a JSON fragment from a JSON string.
     * input: JSON_QUERY(Tags),
     * JSON_QUERY(CONCAT('[\"',ValidFrom,'\",\"',ValidTo,'\"]')) ValidityPeriod ->
     * output: JSON fragments in the output of the FOR JSON clause
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJSONQueryForJSONClause() throws SQLException {
        String dstTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTable")));

        try (Connection conn = DriverManager.getConnection(connectionString);) {
            try (Statement dstStmt = conn.createStatement()) {

                dstStmt.executeUpdate(
                        "CREATE TABLE " + dstTable
                                + " (StockItemID INT, StockItemName NVARCHAR(100), Tags JSON, ValidFrom DATE, ValidTo DATE);");

                String insert = "INSERT INTO " + dstTable
                        + " (StockItemID, StockItemName, Tags, ValidFrom, ValidTo) VALUES " +
                        "(1, 'Item1', '[\"Tag1\",\"Tag2\"]', '2023-01-01', '2023-12-31'), " +
                        "(2, 'Item2', '[\"Tag3\",\"Tag4\"]', '2023-02-01', '2023-11-30')";
                dstStmt.executeUpdate(insert);

                String select = "SELECT StockItemID, StockItemName, JSON_QUERY(Tags) AS Tags, " +
                        "JSON_QUERY(CONCAT('[\"', ValidFrom, '\",\"', ValidTo, '\"]')) AS ValidityPeriod " +
                        "FROM " + dstTable + " FOR JSON PATH";
                try (ResultSet rs = dstStmt.executeQuery(select)) {
                    assertTrue(rs.next());
                    String jsonResult = rs.getString(1);
                    assertTrue(jsonResult.contains("\"StockItemID\":1"));
                    assertTrue(jsonResult.contains("\"StockItemName\":\"Item1\""));
                    assertTrue(jsonResult.contains("\"Tags\":[\"Tag1\",\"Tag2\"]"));
                    assertTrue(jsonResult.contains("\"ValidityPeriod\":[\"2023-01-01\",\"2023-12-31\"]"));

                    assertTrue(jsonResult.contains("\"StockItemID\":2"));
                    assertTrue(jsonResult.contains("\"StockItemName\":\"Item2\""));
                    assertTrue(jsonResult.contains("\"Tags\":[\"Tag3\",\"Tag4\"]"));
                    assertTrue(jsonResult.contains("\"ValidityPeriod\":[\"2023-02-01\",\"2023-11-30\"]"));
                }

            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                try (Statement stmt = conn.createStatement();) {
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }
    }

    /**
     * Test JSON_VALUE function to use JSON properties in query results.
     * JSON_VALUE() -> Extracts a scalar value from a JSON string.
     * input: JSON_VALUE(jsonInfo,'$.info.address.town') ->
     * output: JSON property value of town
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJSONValueInQueryResults() throws SQLException {
        String dstTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTable")));

        try (Connection conn = DriverManager.getConnection(connectionString);) {
            try (Statement dstStmt = conn.createStatement()) {

                dstStmt.executeUpdate(
                        "CREATE TABLE " + dstTable
                                + " (FirstName NVARCHAR(50), LastName NVARCHAR(50), jsonInfo JSON);");

                String insert = "INSERT INTO " + dstTable + " (FirstName, LastName, jsonInfo) VALUES " +
                        "('John', 'Doe', '{\"info\":{\"address\":{\"town\":\"New York\",\"state\":\"US-NY\"}}}'), " +
                        "('Jane', 'Smith', '{\"info\":{\"address\":{\"town\":\"Los Angeles\",\"state\":\"US-CA\"}}}')";
                dstStmt.executeUpdate(insert);

                String select = "SELECT FirstName, LastName, JSON_VALUE(jsonInfo,'$.info.address.town') AS Town " +
                        "FROM " + dstTable + " " +
                        "WHERE JSON_VALUE(jsonInfo,'$.info.address.state') LIKE 'US%' " +
                        "ORDER BY JSON_VALUE(jsonInfo,'$.info.address.town')";
                try (ResultSet rs = dstStmt.executeQuery(select)) {
                    assertTrue(rs.next());
                    assertEquals("Jane", rs.getString("FirstName"));
                    assertEquals("Smith", rs.getString("LastName"));
                    assertEquals("Los Angeles", rs.getString("Town"));

                    assertTrue(rs.next());
                    assertEquals("John", rs.getString("FirstName"));
                    assertEquals("Doe", rs.getString("LastName"));
                    assertEquals("New York", rs.getString("Town"));
                }

            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                try (Statement stmt = conn.createStatement();) {
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }
    }

    /**
     * Test JSON_VALUE function to create computed columns based on the values of
     * JSON properties.
     * JSON_VALUE() -> Extracts a scalar value from a JSON string.
     * input: JSON_VALUE(jsonContent, '$.address[0].longitude') ->
     * output: JSON property value of longitude
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJSONValueComputedColumns() throws SQLException {
        String dstTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTable")));

        try (Connection conn = DriverManager.getConnection(connectionString);) {
            try (Statement dstStmt = conn.createStatement()) {

                dstStmt.executeUpdate(
                        "CREATE TABLE " + dstTable
                                + " (StoreID INT IDENTITY(1,1) NOT NULL, Address VARCHAR(500), jsonContent NVARCHAR(4000), "
                                +
                                "Longitude AS JSON_VALUE(jsonContent, '$.address[0].longitude'), " +
                                "Latitude AS JSON_VALUE(jsonContent, '$.address[0].latitude'));");

                String insert = "INSERT INTO " + dstTable + " (Address, jsonContent) VALUES " +
                        "('123 Main St', '{\"address\":[{\"longitude\":\"-73.935242\",\"latitude\":\"40.730610\"}]}'), "
                        +
                        "('456 Elm St', '{\"address\":[{\"longitude\":\"-118.243683\",\"latitude\":\"34.052235\"}]}')";
                dstStmt.executeUpdate(insert);

                String select = "SELECT StoreID, Address, Longitude, Latitude FROM " + dstTable;
                try (ResultSet rs = dstStmt.executeQuery(select)) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt("StoreID"));
                    assertEquals("123 Main St", rs.getString("Address"));
                    assertEquals("-73.935242", rs.getString("Longitude"));
                    assertEquals("40.730610", rs.getString("Latitude"));

                    assertTrue(rs.next());
                    assertEquals(2, rs.getInt("StoreID"));
                    assertEquals("456 Elm St", rs.getString("Address"));
                    assertEquals("-118.243683", rs.getString("Longitude"));
                    assertEquals("34.052235", rs.getString("Latitude"));
                }

            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                try (Statement stmt = conn.createStatement();) {
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }
    }

    /**
     * Test OPENJSON function to parse JSON data.
     * OPENJSON -> Parses JSON data and returns a set of rows.
     * input: OPENJSON((SELECT jsonCol FROM dstTable WHERE EmployeeID = 17)) ->
     * output: Parsed JSON data
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testOpenJsonParseJson() throws SQLException {
        String dstTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTable")));

        try (Connection conn = DriverManager.getConnection(connectionString)) {
            try (Statement dstStmt = conn.createStatement()) {

                dstStmt.executeUpdate("CREATE TABLE " + dstTable + " (EmployeeID INT, jsonCol NVARCHAR(MAX));");

                String jsonData = "{\"id\":1, \"name\":\"John\"}";
                dstStmt.executeUpdate(
                        "INSERT INTO " + dstTable + " (EmployeeID, jsonCol) VALUES (17, '" + jsonData + "')");

                String select = "SELECT [key], [value] FROM OPENJSON((SELECT jsonCol FROM " + dstTable
                        + " WHERE EmployeeID = 17))";
                try (ResultSet rs = dstStmt.executeQuery(select)) {
                    assertTrue(rs.next());
                    assertEquals("id", rs.getString("key"));
                    assertEquals("1", rs.getString("value"));
                    assertTrue(rs.next());
                    assertEquals("name", rs.getString("key"));
                    assertEquals("John", rs.getString("value"));
                }
            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                try (Statement stmt = conn.createStatement()) {
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }
    }

    /**
     * Test OPENJSON function to parse nested JSON data.
     * OPENJSON -> Parses JSON data and returns a set of rows.
     * input: OPENJSON((SELECT jsonCol FROM dstTable WHERE EmployeeID = 18),
     * '$.person') ->
     * output: Parsed nested JSON data
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testOpenJsonParseNestedJson() throws SQLException {
        String dstTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTable")));

        try (Connection conn = DriverManager.getConnection(connectionString)) {
            try (Statement dstStmt = conn.createStatement()) {
             
                dstStmt.executeUpdate("CREATE TABLE " + dstTable + " (EmployeeID INT, jsonCol NVARCHAR(MAX));");

                String jsonData = "{\"person\": {\"name\": \"John\", \"age\": 30}}";
                dstStmt.executeUpdate(
                        "INSERT INTO " + dstTable + " (EmployeeID, jsonCol) VALUES (18, '" + jsonData + "')");

                String select = "SELECT [key], [value] FROM OPENJSON((SELECT jsonCol FROM " + dstTable
                        + " WHERE EmployeeID = 18), '$.person')";
                try (ResultSet rs = dstStmt.executeQuery(select)) {
                    assertTrue(rs.next());
                    assertEquals("name", rs.getString("key"));
                    assertEquals("John", rs.getString("value"));
                    assertTrue(rs.next());
                    assertEquals("age", rs.getString("key"));
                    assertEquals("30", rs.getString("value"));
                }
            } finally {
                try (Statement stmt = conn.createStatement()) {
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }
    }

    /**
     * Test OPENJSON function to parse JSON array.
     * OPENJSON -> Parses JSON data and returns a set of rows.
     * input: OPENJSON((SELECT jsonCol FROM dstTable WHERE EmployeeID = 19),
     * '$.colors') ->
     * output: Parsed JSON array data
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testOpenJsonParseArray() throws SQLException {
        String dstTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTable")));

        try (Connection conn = DriverManager.getConnection(connectionString)) {
            try (Statement dstStmt = conn.createStatement()) {
        
                dstStmt.executeUpdate("CREATE TABLE " + dstTable + " (EmployeeID INT, jsonCol NVARCHAR(MAX));");

                String jsonData = "{\"colors\": [\"red\", \"green\", \"blue\"]}";
                dstStmt.executeUpdate(
                        "INSERT INTO " + dstTable + " (EmployeeID, jsonCol) VALUES (19, '" + jsonData + "')");

                String select = "SELECT [key], [value] FROM OPENJSON((SELECT jsonCol FROM " + dstTable
                        + " WHERE EmployeeID = 19), '$.colors')";
                try (ResultSet rs = dstStmt.executeQuery(select)) {
                    assertTrue(rs.next());
                    assertEquals("0", rs.getString("key"));
                    assertEquals("red", rs.getString("value"));
                    assertTrue(rs.next());
                    assertEquals("1", rs.getString("key"));
                    assertEquals("green", rs.getString("value"));
                    assertTrue(rs.next());
                    assertEquals("2", rs.getString("key"));
                    assertEquals("blue", rs.getString("value"));
                }
            } finally {
                try (Statement stmt = conn.createStatement()) {
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }
    }

    /**
     * Test JSON insertion and retrieval in a global temporary table.
     * Global temporary tables (##TempJson) are shared across sessions and persist
     * until all sessions using them close.
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJsonInsertionInGlobalTempTable() throws SQLException {
        String dstTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("##TempJson")));
        try (Connection conn = getConnection()) {
            String createTableSQL = "CREATE TABLE " + dstTable + "  (id INT PRIMARY KEY, data JSON)";
            String insertSQL = "INSERT INTO " + dstTable + "  VALUES (?, ?)";
            String selectSQL = "SELECT data FROM " + dstTable + "  WHERE id = ?";

            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(dstTable, stmt);
                stmt.execute(createTableSQL);
            }

            try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                pstmt.setInt(1, 1);
                pstmt.setString(2, "{\"status\": \"success\", \"code\": 200}");
                pstmt.executeUpdate();
            }

            try (PreparedStatement pstmt = conn.prepareStatement(selectSQL)) {
                pstmt.setInt(1, 1);
                try (ResultSet rs = pstmt.executeQuery()) {
                    assertTrue(rs.next());
                    String jsonData = rs.getString(1);
                    assertEquals("{\"status\":\"success\",\"code\":200}", jsonData);
                }
            }
        } finally {
            // Ensure cleanup of the global temporary table
            try (Connection conn = getConnection();
                    Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(dstTable, stmt);
            }
        }
    }

    /**
     * Test JSON insertion and retrieval in a local temporary table.
     * Local temporary tables (#TempJson) are session-bound and deleted
     * automatically when the session ends.
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJsonInsertionInLocalTempTable() throws SQLException {
        try (Connection conn = getConnection()) {
            String dstTable = TestUtils
                    .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("#TempJson")));
            String createTableSQL = "CREATE TABLE " + dstTable + " (id INT PRIMARY KEY, data JSON)";
            String insertSQL = "INSERT INTO " + dstTable + "  VALUES (?, ?)";
            String selectSQL = "SELECT data FROM " + dstTable + " WHERE id = ?";

            try (Statement stmt = conn.createStatement()) {
                stmt.execute(createTableSQL);
            }

            try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                pstmt.setInt(1, 1);
                pstmt.setString(2, "{\"status\": \"success\", \"code\": 200}");
                pstmt.executeUpdate();
            }

            try (PreparedStatement pstmt = conn.prepareStatement(selectSQL)) {
                pstmt.setInt(1, 1);
                try (ResultSet rs = pstmt.executeQuery()) {
                    assertTrue(rs.next());
                    String jsonData = rs.getString(1);
                    assertEquals("{\"status\":\"success\",\"code\":200}", jsonData);
                }
            }
        } // Connection auto-closes here, so #TempJson is automatically dropped
    }

    /**
     * Test `SELECT INTO` query to copy JSON data into a new table.
     * `SELECT INTO` creates a new table and inserts the result of the select
     * statement.
     * input: `SELECT id, data INTO TargetJsonTable FROM SourceJsonTable`
     * output: A new table `TargetJsonTable` with copied JSON data.
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testSelectIntoWithJsonType() throws SQLException {
        String sourceTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SourceJsonTable")));
        String targetTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TargetJsonTable")));

        try (Connection conn = getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(targetTable, stmt);
                TestUtils.dropTableIfExists(sourceTable, stmt);

                String createSourceTableSQL = "CREATE TABLE " + sourceTable + " (id INT PRIMARY KEY, data JSON)";
                stmt.execute(createSourceTableSQL);

                String insertSQL = "INSERT INTO " + sourceTable + " VALUES (?, ?)";
                try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                    pstmt.setInt(1, 1);
                    pstmt.setString(2, "{\"name\": \"Alice\", \"age\": 25}");
                    pstmt.executeUpdate();

                    pstmt.setInt(1, 2);
                    pstmt.setString(2, "{\"name\": \"Bob\", \"age\": 30}");
                    pstmt.executeUpdate();
                }

                // Perform `SELECT INTO` to copy data into TargetJsonTable
                String selectIntoSQL = "SELECT id, data INTO " + targetTable + " FROM " + sourceTable;
                stmt.execute(selectIntoSQL);

                String selectSQL = "SELECT id, data FROM " + targetTable + " ORDER BY id";
                try (ResultSet rs = stmt.executeQuery(selectSQL)) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt("id"));
                    assertEquals("{\"name\":\"Alice\",\"age\":25}", rs.getString("data"));

                    assertTrue(rs.next());
                    assertEquals(2, rs.getInt("id"));
                    assertEquals("{\"name\":\"Bob\",\"age\":30}", rs.getString("data"));
                }
            }
        } finally {
            try (Connection conn = getConnection();
                    Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(targetTable, stmt);
                TestUtils.dropTableIfExists(sourceTable, stmt);
            }
        }
    }

    /**
     * Test `JOIN` query to validate JSON support.
     * This test checks if a `JOIN` operation correctly retrieves JSON data
     * from multiple tables using a foreign key relationship.
     * input: `SELECT u.id, JSON_VALUE(u.data, '$.name'), o.orderDetails FROM
     * UsersTable u JOIN OrdersTable o ON u.id = o.userId`
     * output: Joined data with extracted JSON fields.
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJoinQueryWithJsonType() throws SQLException {
        String usersTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("UsersTable")));
        String ordersTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("OrdersTable")));

        try (Connection conn = getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(ordersTable, stmt);
                TestUtils.dropTableIfExists(usersTable, stmt);

                String createUsersTableSQL = "CREATE TABLE " + usersTable + " (id INT PRIMARY KEY, data JSON)";
                stmt.execute(createUsersTableSQL);

                String createOrdersTableSQL = "CREATE TABLE " + ordersTable
                        + " (orderId INT PRIMARY KEY, userId INT, orderDetails JSON, FOREIGN KEY (userId) REFERENCES "
                        + usersTable + "(id))";
                stmt.execute(createOrdersTableSQL);

                String insertUserSQL = "INSERT INTO " + usersTable + " VALUES (?, ?)";
                try (PreparedStatement pstmt = conn.prepareStatement(insertUserSQL)) {
                    pstmt.setInt(1, 1);
                    pstmt.setString(2, "{\"name\": \"Alice\", \"age\": 25}");
                    pstmt.executeUpdate();

                    pstmt.setInt(1, 2);
                    pstmt.setString(2, "{\"name\": \"Bob\", \"age\": 30}");
                    pstmt.executeUpdate();
                }
                String insertOrderSQL = "INSERT INTO " + ordersTable + " VALUES (?, ?, ?)";
                try (PreparedStatement pstmt = conn.prepareStatement(insertOrderSQL)) {
                    pstmt.setInt(1, 101);
                    pstmt.setInt(2, 1);
                    pstmt.setString(3, "{\"product\": \"Laptop\", \"price\": 1200}");
                    pstmt.executeUpdate();

                    pstmt.setInt(1, 102);
                    pstmt.setInt(2, 2);
                    pstmt.setString(3, "{\"product\": \"Phone\", \"price\": 800}");
                    pstmt.executeUpdate();
                }

                // Perform `JOIN` to extract JSON values
                String joinQuery = "SELECT u.id, JSON_VALUE(u.data, '$.name') AS userName, JSON_VALUE(o.orderDetails, '$.product') AS product "
                        +
                        "FROM " + usersTable + " u " +
                        "JOIN " + ordersTable + " o ON u.id = o.userId " +
                        "ORDER BY u.id";

                try (ResultSet rs = stmt.executeQuery(joinQuery)) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt("id"));
                    assertEquals("Alice", rs.getString("userName"));
                    assertEquals("Laptop", rs.getString("product"));

                    assertTrue(rs.next());
                    assertEquals(2, rs.getInt("id"));
                    assertEquals("Bob", rs.getString("userName"));
                    assertEquals("Phone", rs.getString("product"));
                }
            }
        } finally {
            try (Connection conn = getConnection();
                    Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(ordersTable, stmt);
                TestUtils.dropTableIfExists(usersTable, stmt);
            }
        }
    }

    /**
     * Test JSON input and output with a User-Defined Function (UDF).
     * This test ensures that JSON data can be processed via UDFs
     * in SELECT, WHERE, and FROM clauses.
     * input: UDF `GetAgeFromJson(JSON) RETURNS INT`
     * output: Extracted JSON age field in various queries.
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testJsonInputOutputWithUdf() throws SQLException {
        String personsTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("Persons")));
        String udfName = "dbo.GetAgeFromJson";

        try (Connection conn = getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(personsTable, stmt);
                String dropUdfSQL = "IF OBJECT_ID('" + udfName + "', 'FN') IS NOT NULL DROP FUNCTION " + udfName;
                stmt.execute(dropUdfSQL);
                String createUdfSQL = "CREATE FUNCTION " + udfName + " (@json JSON) " +
                        "RETURNS INT " +
                        "AS BEGIN " +
                        "RETURN CAST(JSON_VALUE(@json, '$.age') AS INT) " +
                        "END";
                stmt.execute(createUdfSQL);

                String createTableSQL = "CREATE TABLE " + personsTable + " (id INT PRIMARY KEY, data JSON)";
                stmt.execute(createTableSQL);
                String insertSQL = "INSERT INTO " + personsTable + " VALUES (?, ?)";
                try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                    pstmt.setInt(1, 1);
                    pstmt.setString(2, "{\"name\": \"Alice\", \"age\": 25}");
                    pstmt.executeUpdate();

                    pstmt.setInt(1, 2);
                    pstmt.setString(2, "{\"name\": \"Bob\", \"age\": 30}");
                    pstmt.executeUpdate();
                }

                // Test JSON UDF in SELECT clause
                String selectSQL = "SELECT id, " + udfName + "(data) AS extractedAge FROM " + personsTable
                        + " ORDER BY id";
                try (ResultSet rs = stmt.executeQuery(selectSQL)) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt("id"));
                    assertEquals(25, rs.getInt("extractedAge"));

                    assertTrue(rs.next());
                    assertEquals(2, rs.getInt("id"));
                    assertEquals(30, rs.getInt("extractedAge"));
                }

                // Test JSON UDF in WHERE clause
                String whereSQL = "SELECT id FROM " + personsTable + " WHERE " + udfName + "(data) > 25 ORDER BY id";
                try (ResultSet rs = stmt.executeQuery(whereSQL)) {
                    assertTrue(rs.next());
                    assertEquals(2, rs.getInt("id"));
                }

                // Test JSON UDF in FROM clause (as part of a subquery)
                String fromSQL = "SELECT extractedAge FROM (SELECT " + udfName + "(data) AS extractedAge FROM "
                        + personsTable + ") AS AgeTable";
                try (ResultSet rs = stmt.executeQuery(fromSQL)) {
                    assertTrue(rs.next());
                    assertEquals(25, rs.getInt("extractedAge"));

                    assertTrue(rs.next());
                    assertEquals(30, rs.getInt("extractedAge"));
                }
            }
        } finally {
            try (Connection conn = getConnection();
                    Statement stmt = conn.createStatement()) {
                TestUtils.dropFunctionIfExists(udfName, stmt);
                TestUtils.dropTableIfExists(personsTable, stmt);
            }
        }
    }

    /**
     * Test a User-Defined Function (UDF) that returns JSON data.
     * This test ensures that the UDF can be used in SELECT queries
     * to return JSON-formatted results.
     * input: UDF `GetPersonJson(INT, NVARCHAR(100)) RETURNS JSON`
     * output: JSON object with id and name fields.
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testUdfReturningJson() throws SQLException {
        String personsTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("Persons")));
        String udfName = "dbo.GetPersonJson";

        try (Connection conn = getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(personsTable, stmt);
                String dropUdfSQL = "IF OBJECT_ID('" + udfName + "', 'FN') IS NOT NULL DROP FUNCTION " + udfName;
                stmt.execute(dropUdfSQL);

                String createUdfSQL = "CREATE FUNCTION " + udfName + " (@id INT, @name NVARCHAR(100)) " +
                        "RETURNS JSON " +
                        "AS BEGIN " +
                        "RETURN JSON_QUERY('{\"id\": ' + CAST(@id AS NVARCHAR) + ', \"name\": \"' + @name + '\"}') " +
                        "END";
                stmt.execute(createUdfSQL);
                String createTableSQL = "CREATE TABLE " + personsTable + " (id INT PRIMARY KEY, name NVARCHAR(100))";
                stmt.execute(createTableSQL);

                String insertSQL = "INSERT INTO " + personsTable + " VALUES (?, ?)";
                try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                    pstmt.setInt(1, 1);
                    pstmt.setString(2, "Alice");
                    pstmt.executeUpdate();

                    pstmt.setInt(1, 2);
                    pstmt.setString(2, "Bob");
                    pstmt.executeUpdate();
                }

                String selectSQL = "SELECT id, name, " + udfName + "(id, name) AS personJson FROM " + personsTable
                        + " ORDER BY id";
                try (ResultSet rs = stmt.executeQuery(selectSQL)) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt("id"));
                    assertEquals("Alice", rs.getString("name"));
                    assertEquals("{\"id\":1,\"name\":\"Alice\"}", rs.getString("personJson"));

                    assertTrue(rs.next());
                    assertEquals(2, rs.getInt("id"));
                    assertEquals("Bob", rs.getString("name"));
                    assertEquals("{\"id\":2,\"name\":\"Bob\"}", rs.getString("personJson"));
                }
            }
        } finally {
            try (Connection conn = getConnection();
                    Statement stmt = conn.createStatement()) {
                TestUtils.dropFunctionIfExists(udfName, stmt);
                TestUtils.dropTableIfExists(personsTable, stmt);
            }
        }
    }

    /*
     * Test inserting a 1 GB JSON file into a table.
     * And verify there is no data loss.
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testInsert1GBJson() throws SQLException, IOException {
        String dstTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTable")));
        
        Path tempFile = Files.createTempFile("json_output", ".json");
        
        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = conn.createStatement()) {

            stmt.executeUpdate("CREATE TABLE " + dstTable + " (jsonColumn JSON);");
            
            generateHugeJsonFile(1L * 1024 * 1024 * 1024); // 1GB JSON file
            
            try (PreparedStatement pstmt = conn
                    .prepareStatement("INSERT INTO " + dstTable + " (jsonColumn) VALUES (?)");
                    FileReader reader = new FileReader(JSON_FILE_PATH)) {
                pstmt.setCharacterStream(1, reader);
                pstmt.executeUpdate();
            }
            
            try (PreparedStatement pstmt = conn.prepareStatement("SELECT jsonColumn FROM " + dstTable);
                    ResultSet rs = pstmt.executeQuery()) {
                
                assertTrue(rs.next());
                Clob jsonClob = rs.getClob(1);
                
                try (Reader clobReader = jsonClob.getCharacterStream();
                     BufferedWriter writer = Files.newBufferedWriter(tempFile, StandardCharsets.UTF_8)) {
                    
                    char[] buffer = new char[1024];
                    int charsRead;
                    while ((charsRead = clobReader.read(buffer)) != -1) {
                        writer.write(buffer, 0, charsRead);
                    }
                }
            }
            
            assertTrue(Files.mismatch(Path.of(JSON_FILE_PATH), tempFile) == -1);
        
        } catch (Exception e) {
            fail("Test failed due to: " + e.getMessage());
        } finally {
            try (Connection conn = DriverManager.getConnection(connectionString);
                    Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(dstTable, stmt);
            }
            Files.deleteIfExists(tempFile);
        }
    }

    /*
     * Test inserting a 1.98 GB JSON file into a table.
     * And verify there is no data loss.
     * Note: This test took around 4 mins to run
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testInsertHugeJsonData() throws SQLException, IOException {
        String dstTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTable")));
        
        Path tempFile = Files.createTempFile("json_output", ".json");
        
        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = conn.createStatement()) {

            stmt.executeUpdate("CREATE TABLE " + dstTable + " (jsonColumn JSON);");
            
            generateHugeJsonFile(2L * 1024 * 1024 * 1015); // 1.98GB JSON file
            
            try (PreparedStatement pstmt = conn
                    .prepareStatement("INSERT INTO " + dstTable + " (jsonColumn) VALUES (?)");
                    FileReader reader = new FileReader(JSON_FILE_PATH)) {
                pstmt.setCharacterStream(1, reader);
                pstmt.executeUpdate();
            }
             
            try (PreparedStatement pstmt = conn.prepareStatement("SELECT jsonColumn FROM " + dstTable);
                    ResultSet rs = pstmt.executeQuery()) {
                
                assertTrue(rs.next());
                Clob jsonClob = rs.getClob(1);
                
                try (Reader clobReader = jsonClob.getCharacterStream();
                     BufferedWriter writer = Files.newBufferedWriter(tempFile, StandardCharsets.UTF_8)) {
                    
                    char[] buffer = new char[1024];
                    int charsRead;
                    while ((charsRead = clobReader.read(buffer)) != -1) {
                        writer.write(buffer, 0, charsRead);
                    }
                }
            }
            
            assertTrue(Files.mismatch(Path.of(JSON_FILE_PATH), tempFile) == -1);
        
        } catch (Exception e) {
            fail("Test failed due to: " + e.getMessage());
        } finally {
            try (Connection conn = DriverManager.getConnection(connectionString);
                    Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(dstTable, stmt);
            }
            Files.deleteIfExists(tempFile);
        }
    }

    /*
     * Test inserting around 2 GB JSON file into a table.
     * Note: This test is expected to fail due to the maximum allowed size for a LOB.
     * The test is designed to validate the error handling for large JSON data.
     * Expected error -> org.opentest4j.AssertionFailedError: Test failed due to: Attempting to grow LOB beyond maximum allowed size of 216895848447 bytes.
     */
    @Test
    @Tag(Constants.JSONTest)
    public void testInsert2GBData() throws SQLException, FileNotFoundException, IOException {
        String dstTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTable")));

        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = conn.createStatement()) {

            stmt.executeUpdate("CREATE TABLE " + dstTable + " (jsonColumn JSON);");

            generateHugeJsonFile(2L * 1024 * 1024 * 1022); // ~2 GB JSON file

            try (PreparedStatement pstmt = conn
                    .prepareStatement("INSERT INTO " + dstTable + " (jsonColumn) VALUES (?)");
                    FileReader reader = new FileReader(JSON_FILE_PATH)) {

                pstmt.setCharacterStream(1, reader);
                pstmt.executeUpdate();
                fail("Expected an exception due to exceeding the maximum allowed size for a LOB.");
            } catch (SQLException e) {
                assertTrue(e.getMessage().contains("Attempting to grow LOB beyond maximum allowed size"));
            }

        } finally {
            try (Connection conn = DriverManager.getConnection(connectionString);
                    Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(dstTable, stmt);
            }
        }
    }

    private void generateHugeJsonFile(long targetSize) {
        File file = new File(JSON_FILE_PATH);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            writer.write("{\"data\":[");

            long currentSize = 10;
            boolean firstGroup = true;

            while (currentSize < targetSize - 10) {
                if (!firstGroup) {
                    writer.write(",");
                }
                writer.write("{\"group\":[");

                boolean firstElement = true;
                for (int i = 0; i < 500; i++) {
                    if (!firstElement) {
                        writer.write(",");
                    }
                    String jsonChunk = "{\"value\":\"" + "a".repeat(1000) + "\"}";
                    writer.write(jsonChunk);
                    currentSize += jsonChunk.length();
                    firstElement = false;
                }

                writer.write("]}");
                firstGroup = false;
            }

            writer.write("]}");
        } catch (IOException e) {
            fail("Failed to create large JSON file: " + e.getMessage());
        }
    }
}