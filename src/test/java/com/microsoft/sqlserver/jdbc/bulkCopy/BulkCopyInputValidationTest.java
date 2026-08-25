/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * Verifies that the destination table name is treated as an object name on both bulk copy paths, so that a name
 * carrying extra SQL identifies one non-existent object instead of being executed, and that ordinary multi-part names
 * keep working.
 */
@Tag(Constants.bulkCopy)
public class BulkCopyInputValidationTest extends AbstractTest {

    /** Server error for a name that does not resolve to an object. */
    private static final int INVALID_OBJECT_NAME = 208;

    private static final String TEMP_MARKER = "##bcInputVal";

    private static String sourceTable;
    private static String catalog;

    @BeforeAll
    public static void setUp() throws Exception {
        setConnection();
        sourceTable = "[BulkCopyInputVal_src_" + RandomUtil.getIdentifier("tbl") + "]";
        catalog = connection.getCatalog();

        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + sourceTable + " (id INT)");
            stmt.executeUpdate("INSERT INTO " + sourceTable + " VALUES (1)");
        }
    }

    @AfterAll
    public static void tearDown() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(sourceTable, stmt);
        }
    }

    /**
     * Payloads that try to run SQL through the destination name. Each one first creates a marker object, so the marker
     * existing is direct proof the payload executed rather than being read as an object name.
     */
    private static Stream<Arguments> injectionPayloads() {
        return Stream.of(
                Arguments.of("semicolon", "(SELECT 1 a) t; SET FMTONLY OFF; SELECT 1 INTO " + TEMP_MARKER + "--"),
                Arguments.of("comment terminated", "dbo.x; SET FMTONLY OFF; SELECT 1 INTO " + TEMP_MARKER + " --"),
                Arguments.of("closing bracket",
                        "[dbo].[x]; SET FMTONLY OFF; SELECT 1 INTO " + TEMP_MARKER + "--"),
                Arguments.of("quoted argument",
                        "(SELECT 1 a) t; SET FMTONLY OFF; SELECT 'x' INTO " + TEMP_MARKER + "--"),
                Arguments.of("remote code execution", "(SELECT 1 a) t; SET FMTONLY OFF; SELECT 1 INTO " + TEMP_MARKER
                        + "; EXEC xp_cmdshell 'echo pwned'--"),
                Arguments.of("credential theft",
                        "(SELECT 1 a) t; SET FMTONLY OFF; SELECT name, CONVERT(NVARCHAR(4000), password_hash, 1) h INTO "
                                + TEMP_MARKER + " FROM sys.sql_logins--"),
                Arguments.of("privilege escalation", "(SELECT 1 a) t; SET FMTONLY OFF; SELECT 1 INTO " + TEMP_MARKER
                        + "; CREATE LOGIN [bcInputVal] WITH PASSWORD='Test!123'--"));
    }

    /**
     * The payload names one object that does not exist, so bulk copy fails with "invalid object name" and nothing the
     * payload asked for is executed.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("injectionPayloads")
    public void testDestinationTableNamePayloadIsNotExecuted(String name, String payload) throws Exception {
        try (Connection conn = PrepUtil.getConnection(connectionString)) {
            try {
                SQLException e = assertThrows(SQLException.class, () -> {
                    try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn);
                            Statement srcStmt = conn.createStatement();
                            ResultSet sourceData = srcStmt.executeQuery("SELECT * FROM " + sourceTable)) {
                        bulkCopy.setDestinationTableName(payload);
                        bulkCopy.writeToServer(sourceData);
                    }
                });
                assertInvalidObjectName(e);
                assertPayloadDidNotRun(conn);
            } finally {
                cleanUpMarkers(conn);
            }
        }
    }

    /**
     * Same payloads on the PreparedStatement batch path, where the destination name is parsed out of the INSERT text.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("injectionPayloads")
    public void testBatchInsertPayloadIsNotExecuted(String name, String payload) throws Exception {
        try (Connection conn = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;")) {
            try {
                assertThrows(SQLException.class, () -> {
                    try (PreparedStatement pstmt = conn
                            .prepareStatement("INSERT INTO " + payload + " VALUES (?)")) {
                        pstmt.setInt(1, 1);
                        pstmt.addBatch();
                        pstmt.executeBatch();
                    }
                });
                assertPayloadDidNotRun(conn);
            } finally {
                cleanUpMarkers(conn);
            }
        }
    }

    /**
     * Names an application may legitimately use. An empty middle part defers to the default schema and has to keep
     * working, and a name the caller already delimited must not be quoted a second time.
     */
    private static Stream<Arguments> validTableNameForms() {
        return Stream.of(Arguments.of("unquoted"), Arguments.of("delimited"), Arguments.of("schema qualified"),
                Arguments.of("database qualified"), Arguments.of("default schema"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("validTableNameForms")
    public void testValidTableNameFormsStillWork(String form) throws Exception {
        String table = "BulkCopyInputVal_ok_" + RandomUtil.getIdentifier("tbl");
        String created = "[dbo].[" + table + "]";

        try (Connection conn = PrepUtil.getConnection(connectionString);
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + created + " (id INT)");
            try {
                String destination;
                switch (form) {
                    case "unquoted":
                        destination = table;
                        break;
                    case "delimited":
                        destination = "[dbo].[" + table + "]";
                        break;
                    case "schema qualified":
                        destination = "dbo." + table;
                        break;
                    case "database qualified":
                        destination = "[" + catalog + "].[dbo].[" + table + "]";
                        break;
                    case "default schema":
                        destination = "[" + catalog + "]..[" + table + "]";
                        break;
                    default:
                        throw new IllegalArgumentException(form);
                }

                try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn);
                        Statement srcStmt = conn.createStatement();
                        ResultSet sourceData = srcStmt.executeQuery("SELECT * FROM " + sourceTable)) {
                    bulkCopy.setDestinationTableName(destination);
                    bulkCopy.writeToServer(sourceData);
                }

                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + created)) {
                    rs.next();
                    assertEquals(1, rs.getInt(1), form + " destination name should copy one row");
                }
            } finally {
                TestUtils.dropTableIfExists(created, stmt);
            }
        }
    }

    /**
     * A name whose delimiters are part of the name itself has to survive the round trip, since the driver quotes it
     * rather than rejecting it.
     */
    @Test
    public void testTableNameContainingDelimiters() throws Exception {
        String table = "BulkCopyInputVal]weird.name " + RandomUtil.getIdentifier("tbl");
        String created = "[dbo].[" + table.replace("]", "]]") + "]";

        try (Connection conn = PrepUtil.getConnection(connectionString);
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + created + " (id INT)");
            try {
                try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn);
                        Statement srcStmt = conn.createStatement();
                        ResultSet sourceData = srcStmt.executeQuery("SELECT * FROM " + sourceTable)) {
                    bulkCopy.setDestinationTableName(created);
                    bulkCopy.writeToServer(sourceData);
                }

                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + created)) {
                    rs.next();
                    assertEquals(1, rs.getInt(1), "delimited name containing ] and . should copy one row");
                }
            } finally {
                stmt.execute("DROP TABLE " + created);
            }
        }
    }

    @Test
    public void testValidBatchInsertStillWorks() throws Exception {
        String table = "BulkCopyInputVal_batch_" + RandomUtil.getIdentifier("tbl");
        String created = "[dbo].[" + table + "]";

        try (Connection conn = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + created + " (id INT)");
            try {
                try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + created + " VALUES (?)")) {
                    pstmt.setInt(1, 42);
                    pstmt.addBatch();
                    pstmt.executeBatch();
                }

                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + created)) {
                    rs.next();
                    assertEquals(1, rs.getInt(1), "batch insert should still reach the destination table");
                }
            } finally {
                TestUtils.dropTableIfExists(created, stmt);
            }
        }
    }

    /**
     * A name with more parts than [server].[database].[schema].[object] cannot identify an object, so it is
     * rejected before anything is sent to the server.
     */
    @Test
    public void testTooManyPartsRejected() throws Exception {
        try (Connection conn = PrepUtil.getConnection(connectionString);
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn);
                Statement srcStmt = conn.createStatement();
                ResultSet sourceData = srcStmt.executeQuery("SELECT * FROM " + sourceTable)) {
            bulkCopy.setDestinationTableName("a.b.c.d.e");
            assertThrows(SQLServerException.class, () -> bulkCopy.writeToServer(sourceData));
        }
    }

    /** Asserts the failure is the server reporting an unknown object, not some unrelated error. */
    private static void assertInvalidObjectName(SQLException thrown) {
        for (Throwable t = thrown; t != null; t = t.getCause()) {
            if (t instanceof SQLServerException) {
                SQLServerException e = (SQLServerException) t;
                if (e.getSQLServerError() != null && INVALID_OBJECT_NAME == e.getSQLServerError().getErrorNumber()) {
                    return;
                }
                // metadata query may fail before INSERT BULK reaches the server
                if (e.getMessage() != null && e.getMessage().contains("Unable to retrieve column metadata")) {
                    return;
                }
            }
        }
        fail("expected error " + INVALID_OBJECT_NAME + " (invalid object name) but got: " + thrown);
    }

    /** The marker object only exists if the payload ran as SQL. */
    private static void assertPayloadDidNotRun(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT OBJECT_ID('tempdb.." + TEMP_MARKER + "'), "
                        + "(SELECT COUNT(*) FROM sys.server_principals WHERE name = 'bcInputVal')")) {
            rs.next();
            if (rs.getObject(1) != null) {
                fail("the destination table name executed as SQL: it created " + TEMP_MARKER);
            }
            assertEquals(0, rs.getInt(2), "the destination table name executed as SQL: it created a login");
        }
    }

    private static void cleanUpMarkers(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("IF OBJECT_ID('tempdb.." + TEMP_MARKER + "') IS NOT NULL DROP TABLE " + TEMP_MARKER);
            stmt.execute("IF EXISTS (SELECT 1 FROM sys.server_principals WHERE name = 'bcInputVal') "
                    + "DROP LOGIN [bcInputVal]");
        }
    }
}
