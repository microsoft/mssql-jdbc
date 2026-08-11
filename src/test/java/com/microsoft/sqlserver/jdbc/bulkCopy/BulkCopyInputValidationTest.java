/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * Tests that SQLServerBulkCopy.setDestinationTableName() properly validates input
 * and rejects payloads that could lead to unintended SQL execution.
 */
@Tag(Constants.bulkCopy)
public class BulkCopyInputValidationTest extends AbstractTest {

    private static String sourceTable;

    @BeforeAll
    public static void setUp() throws Exception {
        setConnection();
        sourceTable = "[BulkCopyInputVal_src_" + RandomUtil.getIdentifier("tbl") + "]";
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + sourceTable + " (id INT)");
            stmt.executeUpdate("INSERT INTO " + sourceTable + " VALUES (1)");
        }
    }

    @AfterAll
    public static void tearDown() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(sourceTable, stmt);
            // Clean up any temp tables that injection may have created
            stmt.execute("IF OBJECT_ID('tempdb..##bulkcopy_injection_test') IS NOT NULL DROP TABLE ##bulkcopy_injection_test");
        }
    }

    /**
     * Tests that a semicolon-based injection payload in setDestinationTableName() is neutralized
     * via identifier quoting so it cannot execute as SQL.
     */
    @Test
    public void testSemicolonInjectionInDestinationTableName() throws Exception {
        // Payload: valid FROM clause + injected statement that creates a temp table
        String payload = "(SELECT 1 a) t; SET FMTONLY OFF; "
                + "SELECT 1 INTO ##bulkcopy_injection_test--";

        try (Connection conn = DriverManager.getConnection(connectionString)) {
            try (Statement srcStmt = conn.createStatement();
                    ResultSet sourceData = srcStmt.executeQuery("SELECT * FROM " + sourceTable);
                    SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn)) {
                bulkCopy.setDestinationTableName(payload);
                try {
                    bulkCopy.writeToServer(sourceData);
                } catch (SQLServerException e) {
                    // Sanitized name won't resolve to a real table — error is expected
                    assertTrue(e.getMessage() != null && !e.getMessage().isEmpty(),
                            "Expected a meaningful error message from the server");
                }
            }

            // Verify: if injection succeeded, the temp table now exists
            try (ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT OBJECT_ID('tempdb..##bulkcopy_injection_test')")) {
                rs.next();
                Object objectId = rs.getObject(1);
                if (objectId != null) {
                    fail("SQL injection succeeded — arbitrary SQL was executed via setDestinationTableName(). "
                            + "The temp table ##bulkcopy_injection_test was created by the injected payload.");
                }
            }
        }
    }

    /**
     * Tests that an xp_cmdshell-style RCE payload is rejected.
     */
    @Test
    public void testRcePayloadRejected() throws Exception {
        String payload = "(SELECT 1 a) t; SET FMTONLY OFF; EXEC xp_cmdshell 'echo pwned'--";

        try (Connection conn = DriverManager.getConnection(connectionString);
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn)) {
            try {
                bulkCopy.setDestinationTableName(payload);
                try (Statement srcStmt = conn.createStatement();
                        ResultSet sourceData = srcStmt.executeQuery("SELECT * FROM " + sourceTable)) {
                    bulkCopy.writeToServer(sourceData);
                }
                fail("Expected exception for injection payload containing xp_cmdshell");
            } catch (SQLServerException e) {
                assertTrue(e.getMessage() != null && !e.getMessage().isEmpty(),
                        "Should get an error for invalid table name");
            }
        }
    }

    /**
     * Tests that a privilege escalation payload (CREATE LOGIN) is rejected.
     */
    @Test
    public void testPrivilegeEscalationPayloadRejected() throws Exception {
        String payload = "(SELECT 1 a) t; SET FMTONLY OFF; "
                + "CREATE LOGIN [test_injection_user] WITH PASSWORD='Test!123'--";

        try (Connection conn = DriverManager.getConnection(connectionString);
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn)) {
            try {
                bulkCopy.setDestinationTableName(payload);
                try (Statement srcStmt = conn.createStatement();
                        ResultSet sourceData = srcStmt.executeQuery("SELECT * FROM " + sourceTable)) {
                    bulkCopy.writeToServer(sourceData);
                }
                fail("Expected exception for injection payload containing CREATE LOGIN");
            } catch (SQLServerException e) {
                assertTrue(e.getMessage() != null && !e.getMessage().isEmpty(),
                        "Should get an error for invalid table name");
            }
        }

        // Verify the login was NOT created
        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM sys.server_principals WHERE name = 'test_injection_user'");
            rs.next();
            assertEquals(0, rs.getInt(1),
                    "Injection succeeded — backdoor login 'test_injection_user' was created!");
            // Cleanup just in case
            stmt.execute("IF EXISTS (SELECT 1 FROM sys.server_principals WHERE name='test_injection_user') "
                    + "DROP LOGIN [test_injection_user]");
        }
    }

    /**
     * Tests that a credential theft payload (SELECT INTO from sys.sql_logins) is rejected.
     */
    @Test
    public void testCredentialTheftPayloadRejected() throws Exception {
        String payload = "(SELECT 1 a) t; SET FMTONLY OFF; "
                + "SELECT name, CONVERT(NVARCHAR(4000), password_hash, 1) as hash "
                + "INTO ##bulkcopy_cred_test FROM sys.sql_logins--";

        try (Connection conn = DriverManager.getConnection(connectionString);
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn)) {
            try {
                bulkCopy.setDestinationTableName(payload);
                try (Statement srcStmt = conn.createStatement();
                        ResultSet sourceData = srcStmt.executeQuery("SELECT * FROM " + sourceTable)) {
                    bulkCopy.writeToServer(sourceData);
                }
                fail("Expected exception for injection payload containing credential theft");
            } catch (SQLServerException e) {
                assertTrue(e.getMessage() != null && !e.getMessage().isEmpty(),
                        "Should get an error for invalid table name");
            }

            // Verify: if injection succeeded, the temp table with credentials would exist
            try (Statement verifyStmt = conn.createStatement();
                    ResultSet rs = verifyStmt.executeQuery(
                            "SELECT OBJECT_ID('tempdb..##bulkcopy_cred_test')")) {
                rs.next();
                Object objectId = rs.getObject(1);
                if (objectId != null) {
                    verifyStmt.execute("DROP TABLE ##bulkcopy_cred_test");
                    fail("SQL injection succeeded — credential theft payload executed. "
                            + "sys.sql_logins data was exfiltrated into ##bulkcopy_cred_test.");
                }
            }
        }
    }

    /**
     * Tests that a valid multi-part table name still works after the fix.
     */
    @Test
    public void testValidMultiPartTableNameAccepted() throws Exception {
        String tableName = "[dbo].[BulkCopyInputVal_valid_" + RandomUtil.getIdentifier("tbl") + "]";
        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + tableName + " (id INT)");
            try {
                ResultSet sourceData = stmt.executeQuery("SELECT * FROM " + sourceTable);
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn);
                bulkCopy.setDestinationTableName(tableName);
                bulkCopy.writeToServer(sourceData);
                bulkCopy.close();

                // Verify data was inserted
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName);
                rs.next();
                assertEquals(1, rs.getInt(1), "Valid table name should allow bulk copy to succeed");
            } finally {
                TestUtils.dropTableIfExists(tableName, stmt);
            }
        }
    }

    /**
     * Tests that a simple unquoted table name still works.
     */
    @Test
    public void testValidSimpleTableNameAccepted() throws Exception {
        String tableName = "BulkCopyInputVal_simple_" + System.currentTimeMillis();
        String escapedName = "[" + tableName + "]";
        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + escapedName + " (id INT)");
            try {
                ResultSet sourceData = stmt.executeQuery("SELECT * FROM " + sourceTable);
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn);
                bulkCopy.setDestinationTableName(tableName);
                bulkCopy.writeToServer(sourceData);
                bulkCopy.close();

                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + escapedName);
                rs.next();
                assertEquals(1, rs.getInt(1), "Simple table name should allow bulk copy to succeed");
            } finally {
                TestUtils.dropTableIfExists(escapedName, stmt);
            }
        }
    }

    /**
     * Tests that the PreparedStatement executeBatch() path with useBulkCopyForBatchInsert=true
     * sanitizes the table name parsed from the INSERT SQL, preventing injection via sp_executesql.
     */
    @Test
    public void testBatchInsertInjectionViaPreparedStatement() throws Exception {
        String tableName = "BulkCopyInputVal_batch_" + RandomUtil.getIdentifier("tbl");
        String escapedName = "[" + tableName + "]";
        try (Connection conn = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + escapedName + " (id INT)");
            try {
                // Valid batch insert should succeed
                try (PreparedStatement pstmt = conn.prepareStatement(
                        "INSERT INTO " + escapedName + " VALUES (?)")) {
                    pstmt.setInt(1, 42);
                    pstmt.addBatch();
                    pstmt.executeBatch();
                }

                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + escapedName);
                rs.next();
                assertEquals(1, rs.getInt(1),
                        "Valid table name should allow batch insert to succeed via useBulkCopyForBatchInsert");
            } finally {
                TestUtils.dropTableIfExists(escapedName, stmt);
            }
        }
    }

    /**
     * Tests that a semicolon injection payload in the INSERT SQL table name is neutralized
     * when using useBulkCopyForBatchInsert=true.
     */
    @Test
    public void testBatchInsertSemicolonInjectionRejected() throws Exception {
        // Payload embeds injection in the table name portion of the INSERT SQL
        String payload = "(SELECT 1 a) t; SET FMTONLY OFF; "
                + "SELECT 1 INTO ##batch_inject_test--";

        try (Connection conn = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;")) {
            try (PreparedStatement pstmt = conn.prepareStatement(
                    "INSERT INTO " + payload + " VALUES (?)")) {
                pstmt.setInt(1, 1);
                pstmt.addBatch();
                pstmt.executeBatch();
            } catch (Exception e) {
                assertTrue(e.getMessage() != null && !e.getMessage().isEmpty(),
                        "Expected an error because sanitized table name is not a real table");
            }

            // Verify the injected temp table was NOT created
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "SELECT OBJECT_ID('tempdb..##batch_inject_test')")) {
                rs.next();
                Object objectId = rs.getObject(1);
                if (objectId != null) {
                    stmt.execute("DROP TABLE ##batch_inject_test");
                    fail("SQL injection succeeded via executeBatch() — "
                            + "injected SQL created ##batch_inject_test");
                }
            }
        }
    }

    /**
     * Tests that an xp_cmdshell RCE payload in the INSERT SQL table name is neutralized
     * when using useBulkCopyForBatchInsert=true.
     */
    @Test
    public void testBatchInsertRcePayloadRejected() throws Exception {
        String payload = "(SELECT 1 a) t; SET FMTONLY OFF; EXEC xp_cmdshell 'echo pwned'--";

        try (Connection conn = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;")) {
            try (PreparedStatement pstmt = conn.prepareStatement(
                    "INSERT INTO " + payload + " VALUES (?)")) {
                pstmt.setInt(1, 1);
                pstmt.addBatch();
                pstmt.executeBatch();
                fail("Expected exception for injection payload in batch insert table name");
            } catch (Exception e) {
                assertTrue(e.getMessage() != null && !e.getMessage().isEmpty(),
                        "Should get an error for invalid table name");
            }
        }
    }
}
