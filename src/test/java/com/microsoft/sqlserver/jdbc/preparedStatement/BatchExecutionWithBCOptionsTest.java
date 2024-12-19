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
     * bulkCopyOptionDefaultsCheckConstraints
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
     * bulkCopyOptionDefaultsCheckConstraints=true
     *
     * @throws SQLException
     */
    @Test
    public void testBulkInsertWithConnStrConstraintCheckEnabled() throws Exception {
        try (Connection connection = PrepUtil.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyOptionDefaultsCheckConstraints=true")) {
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
     * bulkCopyOptionDefaultsCheckConstraints=false
     *
     * @throws SQLException
     */
    @Test
    public void testBulkInsertWithConnStrCheckConstraintsDisabled() throws Exception {
        try (Connection connection = PrepUtil.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyOptionDefaultsCheckConstraints=false")) {
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
     * Test with useBulkCopyBatchInsert=true and bulkCopyOptionDefaultsBatchSize set
     *
     * @throws SQLException
     */
    @Test
    public void testBulkInsertWithBatchSize() throws Exception {
        try (Connection connection = PrepUtil.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyOptionDefaultsBatchSize=2")) {
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
     * bulkCopyOptionDefaultsKeepIdentity=true
     *
     * @throws SQLException
     */
    @Test
    public void testBulkInsertWithKeepIdentity() throws Exception {
        try (Connection connection = PrepUtil.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyOptionDefaultsKeepIdentity=true")) {
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
     * bulkCopyOptionDefaultsKeepIdentity=true where identity insert fails
     *
     * @throws SQLException
     */
    @Test
    public void testBulkInsertWithKeepIdentityFailure() throws Exception {
        try (Connection connection = PrepUtil.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyOptionDefaultsKeepIdentity=true")) {
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
     * Test with useBulkCopyBatchInsert=true passing SQLServerBulkCopyOptions with
     * constraint check enabled
     *
     * @throws SQLException
     */
    @Test
    public void testBulkInsertWithConstraintCheckEnabled() throws Exception {
        // Set BulkCopy options
        SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
        // options.setKeepIdentity(true); // Preserve identity values from the source
        options.setCheckConstraints(true); // enable constraint checks
        // options.setTableLock(true); // Lock the destination table for faster insert
        // options.setBatchSize(1000); // Batch size for the bulk copy
        // options.setTimeout(60); // Timeout in seconds

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;")) {
            try (PreparedStatement pstmt = connection.prepareStatement("insert into " + tableName + " values(?, ?)")) {

                ((SQLServerPreparedStatement) pstmt).setBulkCopyOptions(options);

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
     * Test with useBulkCopyBatchInsert=true passing SQLServerBulkCopyOptions with
     * constraint check disabled
     *
     * @throws SQLException
     */
    @Test
    public void testBulkInsertWithConstraintCheckDisabled() throws Exception {
        // Set BulkCopy options
        SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
        // options.setKeepIdentity(true); // Preserve identity values from the source
        options.setCheckConstraints(false); // enable constraint checks
        // options.setTableLock(true); // Lock the destination table for faster insert
        // options.setBatchSize(1000); // Batch size for the bulk copy
        // options.setTimeout(60); // Timeout in seconds

        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;")) {
            try (PreparedStatement pstmt = connection.prepareStatement("insert into " + tableName + " values(?, ?)")) {

                ((SQLServerPreparedStatement) pstmt).setBulkCopyOptions(options);

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
     * bulkCopyOptionDefaultsTableLock=true
     *
     * @throws SQLException
     */
    @Test
    public void testBulkInsertWithTableLock() throws Exception {
        try (Connection connection = PrepUtil.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyOptionDefaultsTableLock=true")) {
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
     * bulkCopyOptionDefaultsTableLock=true where insert fails
     *
     * @throws SQLException
     */
    @Test
    public void testBulkInsertWithTableLockFailure() throws Exception {
        try (Connection connection = PrepUtil.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyOptionDefaultsTableLock=true")) {
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
                            connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyOptionDefaultsTableLock=true");
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
     * bulkCopyOptionDefaultsTimeout=30
     *
     * @throws SQLException
     */
    @Test
    public void testBulkCopyOptionDefaultsTimeout() throws Exception {
        try (Connection connection = PrepUtil
                .getConnection(connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyOptionDefaultsTimeout=30")) {
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
     * Test with useBulkCopyBatchInsert=true and bulkCopyOptionDefaultsTimeout set to a lower value
     *
     * @throws SQLException
     */
    @Test
    public void testBulkCopyOptionDefaultsTimeoutLowerValue() throws Exception {
        try (Connection connection = PrepUtil.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyOptionDefaultsTimeout=1")) {
            try (PreparedStatement pstmt = connection.prepareStatement("WAITFOR DELAY '00:00:02'; insert into " + tableName + " values(?, ?)")) {
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

                // Set a query timeout to a lower value to simulate timeout
                pstmt.setQueryTimeout(1);

                try {
                    pstmt.executeBatch();
                    fail("Expected timeout exception was not thrown");
                } catch (SQLException e) {
                    assertTrue(e.getMessage().contains("The query has timed out") || e.getMessage().contains("timeout"), "Expected timeout exception");
                }
            }
        } catch (SQLException e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }
    }

    /**
     * Test with useBulkCopyBatchInsert=true and
     * bulkCopyOptionDefaultsFireTriggers=true
     *
     * @throws SQLException
     */
    @Test
    public void testBulkCopyOptionDefaultsFireTriggers() throws Exception {
        try (Connection connection = PrepUtil.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyOptionDefaultsFireTriggers=true")) {
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
     * bulkCopyOptionDefaultsFireTriggers=true where insert fails
     *
     * @throws SQLException
     */
    @Test
    public void testBulkCopyOptionDefaultsFireTriggersFailure() throws Exception {
        try (Connection connection = PrepUtil.getConnection(
            connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyOptionDefaultsFireTriggers=true")) {
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
     * bulkCopyOptionDefaultsKeepNulls=true
     *
     * @throws SQLException
     */
    @Test
    public void testBulkCopyOptionDefaultsKeepNulls() throws Exception {
        try (Connection connection = PrepUtil.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyOptionDefaultsKeepNulls=true")) {
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
     * bulkCopyOptionDefaultsKeepNulls=false
     *
     * @throws SQLException
     */
    @Test
    public void testBulkCopyOptionDefaultsKeepNullsFalse() throws Exception {
        try (Connection connection = PrepUtil.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyOptionDefaultsKeepNulls=false")) {
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
     * bulkCopyOptionDefaultsUseInternalTransaction=true
     *
     * @throws SQLException
     */
    @Test
    public void testBulkCopyOptionDefaultsUseInternalTransaction() throws Exception {
        try (Connection connection = PrepUtil.getConnection(connectionString
                + ";useBulkCopyForBatchInsert=true;bulkCopyOptionDefaultsUseInternalTransaction=true")) {
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
            if (!e.getMessage().contains("UseInternalTransaction option cannot be set to TRUE")) {
                fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
            }
        }
    }

    /**
     * Verifies that the existing functionality remains intact when
     * useBulkCopyBatchInsert=true and bulkCopyOptionDefaultsAllowEncryptedValueModifications=true
     *
     * @throws SQLException
     */
    @Test
    public void testBulkCopyOptionDefaultsAllowEncryptedValueModifications() throws Exception {
        try (Connection connection = PrepUtil.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyOptionDefaultsAllowEncryptedValueModifications=true")) {
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
