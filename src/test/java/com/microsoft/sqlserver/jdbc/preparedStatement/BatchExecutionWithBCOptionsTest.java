/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.preparedStatement;

import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.PrepUtil;

@RunWith(JUnitPlatform.class)
public class BatchExecutionWithBCOptionsTest extends AbstractTest {

    private static final String tableName = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("BatchInsertWithBCOptions"));

    /**
     * Test with useBulkCopyBatchInsert=true without passing
     * bulkCopyForBatchInsertCheckConstraints
     *
     * @throws SQLException
     */
    @Test
    public void testBulkInsertNoConnStrOptions() throws Exception {
        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;")) {
            try (PreparedStatement pstmt = connection.prepareStatement("insert into " + tableName + " values(?, ?)")) {
                pstmt.setInt(1, 1);
                pstmt.setInt(2, 0);
                pstmt.addBatch();

                pstmt.setInt(1, 2);
                pstmt.setInt(2, 2);
                pstmt.addBatch();

                pstmt.setInt(1, 3);
                pstmt.setInt(2, 0);
                pstmt.addBatch();

                pstmt.setInt(1, 4);
                pstmt.setInt(2, 4);
                pstmt.addBatch();

                pstmt.executeBatch();

                try (Statement stmt = connection.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery("select count(*) from " + tableName)) {
                        if (rs.next()) {
                            int cnt = rs.getInt(1);
                            assertEquals(cnt, 4, "row count should have been 4");
                        }
                    }
                }
            }
        } catch (SQLException e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }
    }

    /**
     * Test with useBulkCopyBatchInsert=true and
     * bulkCopyForBatchInsertCheckConstraints=true
     *
     * @throws SQLException
     */
    @Test
    public void testBulkInsertWithConnStrConstraintCheckEnabled() throws Exception {
        try (Connection connection = PrepUtil.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyForBatchInsertCheckConstraints=true")) {
            try (PreparedStatement pstmt = connection.prepareStatement("insert into " + tableName + " values(?, ?)")) {

                pstmt.setInt(1, 1);
                pstmt.setInt(2, 0);
                pstmt.addBatch();

                pstmt.setInt(1, 2);
                pstmt.setInt(2, 2);
                pstmt.addBatch();

                pstmt.setInt(1, 3);
                pstmt.setInt(2, 0);
                pstmt.addBatch();

                pstmt.setInt(1, 4);
                pstmt.setInt(2, 4);
                pstmt.addBatch();

                pstmt.executeBatch();

                fail(TestResource.getResource("R_expectedExceptionNotThrown"));

            }
        } catch (SQLException e) {
            if (!e.getMessage().contains("CHECK")) {
                fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
            }
        }
    }

    /**
     * Test with useBulkCopyBatchInsert=true and
     * bulkCopyForBatchInsertCheckConstraints=false
     *
     * @throws SQLException
     */
    @Test
    public void testBulkInsertWithConnStrCheckConstraintsDisabled() throws Exception {
        try (Connection connection = PrepUtil.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyForBatchInsertCheckConstraints=false")) {
            try (PreparedStatement pstmt = connection.prepareStatement("insert into " + tableName + " values(?, ?)")) {

                pstmt.setInt(1, 1);
                pstmt.setInt(2, 0);
                pstmt.addBatch();

                pstmt.setInt(1, 2);
                pstmt.setInt(2, 2);
                pstmt.addBatch();

                pstmt.setInt(1, 3);
                pstmt.setInt(2, 0);
                pstmt.addBatch();

                pstmt.setInt(1, 4);
                pstmt.setInt(2, 4);
                pstmt.addBatch();

                pstmt.executeBatch();

                try (Statement stmt = connection.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery("select count(*) from " + tableName)) {
                        if (rs.next()) {
                            int cnt = rs.getInt(1);
                            assertEquals(cnt, 4, "row count should have been 4");
                        }
                    }
                }
            }
        } catch (SQLException e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }
    }

    /**
     * Test with useBulkCopyBatchInsert=true and bulkCopyForBatchInsertBatchSize set
     *
     * @throws SQLException
     */
    @Test
    public void testBulkInsertWithBatchSize() throws Exception {
        try (Connection connection = PrepUtil.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyForBatchInsertBatchSize=2")) {
            try (PreparedStatement pstmt = connection.prepareStatement("insert into " + tableName + " values(?, ?)")) {
                pstmt.setInt(1, 1);
                pstmt.setInt(2, 1);
                pstmt.addBatch();

                pstmt.setInt(1, 2);
                pstmt.setInt(2, 2);
                pstmt.addBatch();

                pstmt.setInt(1, 3);
                pstmt.setInt(2, 3);
                pstmt.addBatch();

                pstmt.setInt(1, 4);
                pstmt.setInt(2, 4);
                pstmt.addBatch();

                pstmt.executeBatch();

                try (Statement stmt = connection.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery("select count(*) from " + tableName)) {
                        if (rs.next()) {
                            int cnt = rs.getInt(1);
                            assertEquals(cnt, 4, "row count should have been 4");
                        }
                    }
                }
            }
        } catch (SQLException e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }
    }

    /**
     * Test with useBulkCopyBatchInsert=true and
     * bulkCopyForBatchInsertKeepIdentity=true
     *
     * @throws SQLException
     */
    @Test
    public void testBulkInsertWithKeepIdentity() throws Exception {
        try (Connection connection = PrepUtil.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyForBatchInsertKeepIdentity=true")) {
            try (PreparedStatement pstmt = connection.prepareStatement("insert into " + tableName + " values(?, ?)")) {
                pstmt.setInt(1, 1);
                pstmt.setInt(2, 1);
                pstmt.addBatch();

                pstmt.setInt(1, 2);
                pstmt.setInt(2, 2);
                pstmt.addBatch();

                pstmt.setInt(1, 3);
                pstmt.setInt(2, 3);
                pstmt.addBatch();

                pstmt.setInt(1, 4);
                pstmt.setInt(2, 4);
                pstmt.addBatch();

                pstmt.executeBatch();

                try (Statement stmt = connection.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery("select count(*) from " + tableName)) {
                        if (rs.next()) {
                            int cnt = rs.getInt(1);
                            assertEquals(cnt, 4, "row count should have been 4");
                        }
                    }
                }
            }
        } catch (SQLException e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }
    }

    /**
     * Test with useBulkCopyBatchInsert=true and
     * bulkCopyForBatchInsertKeepIdentity=true where identity insert fails
     *
     * @throws SQLException
     */
    @Test
    public void testBulkInsertWithKeepIdentityFailure() throws Exception {
        try (Connection connection = PrepUtil.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyForBatchInsertKeepIdentity=true")) {
            try (PreparedStatement pstmt = connection.prepareStatement("insert into " + tableName + " values(?, ?)")) {
                pstmt.setInt(1, 1);
                pstmt.setInt(2, 1);
                pstmt.addBatch();

                pstmt.setInt(1, 1);
                pstmt.setInt(2, 2);
                pstmt.addBatch();

                pstmt.executeBatch();

                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            }
        } catch (SQLException e) {
            if (!e.getMessage().contains("Violation of PRIMARY KEY constraint")) {
                fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
            }
        }
    }

    /**
     * Test with useBulkCopyBatchInsert=true without passing
     * SQLServerBulkCopyOptions
     *
     * @throws SQLException
     */
    @Test
    public void testBulkInsertNoOptions() throws Exception {
        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;")) {
            try (PreparedStatement pstmt = connection.prepareStatement("insert into " + tableName + " values(?, ?)")) {
                pstmt.setInt(1, 1);
                pstmt.setInt(2, 0);
                pstmt.addBatch();

                pstmt.setInt(1, 2);
                pstmt.setInt(2, 2);
                pstmt.addBatch();

                pstmt.setInt(1, 3);
                pstmt.setInt(2, 0);
                pstmt.addBatch();

                pstmt.setInt(1, 4);
                pstmt.setInt(2, 4);
                pstmt.addBatch();

                pstmt.executeBatch();

                try (Statement stmt = connection.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery("select count(*) from " + tableName)) {
                        if (rs.next()) {
                            int cnt = rs.getInt(1);
                            assertEquals(cnt, 4, "row count should have been 4");
                        }
                    }
                }
            }
        } catch (SQLException e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }
    }

    /**
     * Test with useBulkCopyBatchInsert=true and
     * bulkCopyForBatchInsertTableLock=true
     *
     * @throws SQLException
     */
    @Test
    public void testBulkInsertWithTableLock() throws Exception {
        try (Connection connection = PrepUtil.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyForBatchInsertTableLock=true")) {
            try (PreparedStatement pstmt = connection.prepareStatement("insert into " + tableName + " values(?, ?)")) {
                pstmt.setInt(1, 1);
                pstmt.setInt(2, 1);
                pstmt.addBatch();

                pstmt.setInt(1, 2);
                pstmt.setInt(2, 2);
                pstmt.addBatch();

                pstmt.setInt(1, 3);
                pstmt.setInt(2, 3);
                pstmt.addBatch();

                pstmt.setInt(1, 4);
                pstmt.setInt(2, 4);
                pstmt.addBatch();

                pstmt.executeBatch();

                try (Statement stmt = connection.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery("select count(*) from " + tableName)) {
                        if (rs.next()) {
                            int cnt = rs.getInt(1);
                            assertEquals(cnt, 4, "row count should have been 4");
                        }
                    }
                }
            }
        } catch (SQLException e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }
    }

    /**
     * Test with useBulkCopyBatchInsert=true and
     * bulkCopyForBatchInsertTableLock=true where insert fails
     *
     * @throws SQLException
     */
    @Test
    public void testBulkInsertWithTableLockFailure() throws Exception {
        try (Connection connection = PrepUtil.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyForBatchInsertTableLock=true")) {
            try (PreparedStatement pstmt = connection.prepareStatement("insert into " + tableName + " values(?, ?)")) {
                pstmt.setInt(1, 1);
                pstmt.setInt(2, 1);
                pstmt.addBatch();

                pstmt.setInt(1, 2);
                pstmt.setInt(2, 2);
                pstmt.addBatch();

                pstmt.setInt(1, 3);
                pstmt.setInt(2, 3);
                pstmt.addBatch();

                pstmt.setInt(1, 4);
                pstmt.setInt(2, 4);
                pstmt.addBatch();

                // Start a transaction and acquire a table lock
                connection.setAutoCommit(false);
                try (Statement stmt = connection.createStatement()) {
                    String lockTableSQL = "SELECT * FROM " + tableName + " WITH (TABLOCKX)";
                    stmt.execute(lockTableSQL);

                    try (Connection connection2 = PrepUtil.getConnection(
                            connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyForBatchInsertTableLock=true");
                         PreparedStatement pstmt2 = connection2
                                 .prepareStatement("insert into " + tableName + " values(?, ?)")) {

                        pstmt2.setInt(1, 5);
                        pstmt2.setInt(2, 5);
                        pstmt2.addBatch();

                        // Set a query timeout to prevent the test from running indefinitely
                        pstmt2.setQueryTimeout(5);

                        pstmt2.executeBatch(); // This should fail due to the table lock
                        fail("Expected exception due to table lock was not thrown");
                    } catch (SQLException e) {
                        System.out.println("Bulk insert failed as expected: " + e.getMessage());
                    }
                    // Release the lock
                    connection.rollback();
                }
            }
        } catch (SQLException e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }
    }

    /**
     * Test with useBulkCopyBatchInsert=true and
     * bulkCopyForBatchInsertFireTriggers=true
     *
     * @throws SQLException
     */
    @Test
    public void testBulkCopyOptionDefaultsFireTriggers() throws Exception {
        try (Connection connection = PrepUtil.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyForBatchInsertFireTriggers=true")) {
            try (PreparedStatement pstmt = connection.prepareStatement("insert into " + tableName + " values(?, ?)")) {
                pstmt.setInt(1, 1);
                pstmt.setInt(2, 1);
                pstmt.addBatch();

                pstmt.setInt(1, 2);
                pstmt.setInt(2, 2);
                pstmt.addBatch();

                pstmt.setInt(1, 3);
                pstmt.setInt(2, 3);
                pstmt.addBatch();

                pstmt.setInt(1, 4);
                pstmt.setInt(2, 4);
                pstmt.addBatch();

                pstmt.executeBatch();
                try (Statement stmt = connection.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery("select count(*) from " + tableName)) {
                        if (rs.next()) {
                            int cnt = rs.getInt(1);
                            assertEquals(cnt, 4, "Row count should have been 4");
                        }
                    }
                }
            }
        } catch (SQLException e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }
    }

    /**
     * Test with useBulkCopyBatchInsert=true and
     * bulkCopyForBatchInsertFireTriggers=true where insert fails
     *
     * @throws SQLException
     */
    @Test
    public void testBulkCopyOptionDefaultsFireTriggersFailure() throws Exception {
        try (Connection connection = PrepUtil.getConnection(
            connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyForBatchInsertFireTriggers=true")) {
                try (PreparedStatement pstmt = connection.prepareStatement("insert into " + tableName + " values(?, ?)")) {
                    pstmt.setInt(1, 1);
                    pstmt.setInt(2, 1);
                    pstmt.addBatch();

                    pstmt.setInt(1, 2);
                    pstmt.setInt(2, 2);
                    pstmt.addBatch();

                    pstmt.setInt(1, 3);
                    pstmt.setInt(2, 3);
                    pstmt.addBatch();

                    pstmt.setInt(1, 4);
                    pstmt.setInt(2, 4);
                    pstmt.addBatch();
                    
                    // Created a trigger that will cause the batch insert to fail
                    try (Statement stmt = connection.createStatement()) {
                    String createTriggerSQL = "CREATE TRIGGER trgFailInsert ON " + tableName +
                    " AFTER INSERT AS BEGIN " +
                    "RAISERROR('Trigger failure', 16, 1); " +
                    "ROLLBACK TRANSACTION; END";
                    stmt.execute(createTriggerSQL);
                }
                
                try {
                    pstmt.executeBatch();
                    fail("Expected trigger failure exception was not thrown");
                } catch (SQLException e) {
                    System.out.println("Batch execution failed as expected: " + e.getMessage());
                }
                
                // Cleaning up by dropping the trigger
                try (Statement stmt = connection.createStatement()) {
                    String dropTriggerSQL = "DROP TRIGGER trgFailInsert";
                    stmt.execute(dropTriggerSQL);
                }
            }
        } catch (SQLException e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }
    }

    /**
     * Test with useBulkCopyBatchInsert=true and
     * bulkCopyForBatchInsertKeepNulls=true
     *
     * @throws SQLException
     */
    @Test
    public void testBulkCopyOptionDefaultsKeepNulls() throws Exception {
        try (Connection connection = PrepUtil.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyForBatchInsertKeepNulls=true")) {
            try (PreparedStatement pstmt = connection.prepareStatement("insert into " + tableName + " values(?, ?)")) {
                pstmt.setInt(1, 1);
                pstmt.setNull(2, java.sql.Types.INTEGER);
                pstmt.addBatch();

                pstmt.setInt(1, 2);
                pstmt.setNull(2, java.sql.Types.INTEGER);
                pstmt.addBatch();

                pstmt.setInt(1, 3);
                pstmt.setNull(2, java.sql.Types.INTEGER);
                pstmt.addBatch();

                pstmt.setInt(1, 4);
                pstmt.setNull(2, java.sql.Types.INTEGER);
                pstmt.addBatch();

                pstmt.executeBatch();

                try (Statement stmt = connection.createStatement()) {
                    try (ResultSet rs = stmt
                            .executeQuery("select count(*) from " + tableName + " where b is null")) {
                        if (rs.next()) {
                            int cnt = rs.getInt(1);
                            assertEquals(cnt, 4, "Row count with null values should have been 4");
                        }
                    }
                }
            }
        } catch (SQLException e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }
    }

    /**
     * Test with useBulkCopyBatchInsert=true and
     * bulkCopyForBatchInsertKeepNulls=false
     *
     * @throws SQLException
     */
    @Test
    public void testBulkCopyOptionDefaultsKeepNullsFalse() throws Exception {
        try (Connection connection = PrepUtil.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyForBatchInsertKeepNulls=false")) {
            try (PreparedStatement pstmt = connection.prepareStatement("insert into " + tableName + " values(?, ?)")) {
                pstmt.setInt(1, 1);
                pstmt.setNull(2, java.sql.Types.INTEGER);
                pstmt.addBatch();

                pstmt.setInt(1, 2);
                pstmt.setNull(2, java.sql.Types.INTEGER);
                pstmt.addBatch();

                pstmt.setInt(1, 3);
                pstmt.setNull(2, java.sql.Types.INTEGER);
                pstmt.addBatch();

                pstmt.setInt(1, 4);
                pstmt.setNull(2, java.sql.Types.INTEGER);
                pstmt.addBatch();

                pstmt.executeBatch();

                try (Statement stmt = connection.createStatement()) {
                    try (ResultSet rs = stmt
                            .executeQuery("select count(*) from " + tableName + " where b is not null")) {
                        if (rs.next()) {
                            int cnt = rs.getInt(1);
                            assertEquals(cnt, 0, "Row count with non-null values should have been 0");
                        }
                    }
                }
            }
        } catch (SQLException e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }
    }

    /**
     * Test with useBulkCopyBatchInsert=true and
     * bulkCopyForBatchInsertAllowEncryptedValueModifications=true
     *
     * @throws SQLException
     */
    @Test
    public void testBulkInsertWithEncryptedValueModifications() throws Exception {
        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyForBatchInsertAllowEncryptedValueModifications=true")) {
            try (PreparedStatement pstmt = connection.prepareStatement("insert into " + tableName + " values(?, ?)")) {
                pstmt.setInt(1, 1);
                pstmt.setInt(2, 0);
                pstmt.addBatch();

                pstmt.setInt(1, 2);
                pstmt.setInt(2, 2);
                pstmt.addBatch();

                pstmt.setInt(1, 3);
                pstmt.setInt(2, 0);
                pstmt.addBatch();

                pstmt.setInt(1, 4);
                pstmt.setInt(2, 4);
                pstmt.addBatch();

                pstmt.executeBatch();

                try (Statement stmt = connection.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery("select count(*) from " + tableName)) {
                        if (rs.next()) {
                            int cnt = rs.getInt(1);
                            assertEquals(cnt, 4, "row count should have been 4");
                        }
                    }
                }
            }
        } catch (SQLException e) {
            if (e.getMessage().contains("Invalid column type from bcp client for colid 1")) {
                return;
            } else {
                fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
            }
        }
        fail("Expected exception 'Invalid column type from bcp client for colid 1' was not thrown.");
    }

    @BeforeEach
    public void init() throws Exception {
        try (Connection con = getConnection()) {
            con.setAutoCommit(false);
            try (Statement stmt = con.createStatement()) {
                TestUtils.dropTableIfExists(tableName, stmt);
                String sql1 = "create table " + tableName + "(a INT PRIMARY KEY, b INT CHECK (b > 0))";
                stmt.executeUpdate(sql1);
            }
            con.commit();
        }
    }

    @AfterEach
    public void terminate() throws Exception {
        try (Connection con = getConnection()) {
            try (Statement stmt = con.createStatement()) {
                TestUtils.dropTableIfExists(tableName, stmt);
            }
        }
    }

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

}
