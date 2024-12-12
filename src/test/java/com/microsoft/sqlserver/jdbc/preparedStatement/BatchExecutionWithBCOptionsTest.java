/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.preparedStatement;

import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
     * Test with useBulkCopyBatchInsert=true and bulkCopyOptionDefaultsCheckConstraints=true
     *
     * @throws SQLException
     */
    @Test
    public void testBulkInsertWithConnStrConstraintCheckEnabled() throws Exception {
        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyOptionDefaultsCheckConstraints=true")) {
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
     * Test with useBulkCopyBatchInsert=true and bulkCopyOptionDefaultsCheckConstraints=false
     *
     * @throws SQLException
     */
    @Test
    public void testBulkInsertWithConnStrCheckConstraintsDisabled() throws Exception {
        try (Connection connection = PrepUtil.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;bulkCopyOptionDefaultsCheckConstraints=false")) {
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
