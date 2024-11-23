/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.preparedStatement;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.opentest4j.TestAbortedException;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDW)
public class BatchExecutionWithNullTest extends AbstractTest {

    private static final String tableName = RandomUtil.getIdentifier("batchNull");
    private static final String primaryKeyConstraintName = "pk_" + tableName;

    /**
     * Test with combination of setString and setNull which cause the "Violation of PRIMARY KEY constraint and
     * internally "Could not find prepared statement with handle X" error.
     * 
     * @throws SQLException
     */
    public void testAddBatch2(Connection conn) throws SQLException {
        // try {
        String sPrepStmt = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName)
                + " (id, name) values (?, ?)";
        int updateCountlen = 0;
        int key = 42;

        // this is the minimum sequence, I've found to trigger the error\
        try (PreparedStatement pstmt = conn.prepareStatement(sPrepStmt)) {
            pstmt.setInt(1, key++);
            pstmt.setNull(2, Types.VARCHAR);
            pstmt.addBatch();

            pstmt.setInt(1, key++);
            pstmt.setString(2, "FOO");
            pstmt.addBatch();

            pstmt.setInt(1, key++);
            pstmt.setNull(2, Types.VARCHAR);
            pstmt.addBatch();

            int[] updateCount = pstmt.executeBatch();
            updateCountlen += updateCount.length;

            pstmt.setInt(1, key++);
            pstmt.setString(2, "BAR");
            pstmt.addBatch();

            pstmt.setInt(1, key++);
            pstmt.setNull(2, Types.VARCHAR);
            pstmt.addBatch();

            updateCount = pstmt.executeBatch();
            updateCountlen += updateCount.length;

            assertTrue(updateCountlen == 5, TestResource.getResource("R_addBatchFailed"));
        }

        String sPrepStmt1 = "select count(*) from " + AbstractSQLGenerator.escapeIdentifier(tableName);
        try (PreparedStatement pstmt1 = conn.prepareStatement(sPrepStmt1); ResultSet rs = pstmt1.executeQuery()) {
            rs.next();
            assertTrue(rs.getInt(1) == 5, TestResource.getResource("R_insertBatchFailed"));
        }
    }

    /**
     * Tests with AE enabled on the connection
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xSQLv11)
    @Tag(Constants.xSQLv12)
    public void testAddbatch2AEOnConnection() throws SQLException {
        String cs = TestUtils.addOrOverrideProperty(connectionString, "columnEncryptionSetting", "Enabled");
        cs = TestUtils.addOrOverrideProperty(cs, "sendStringParametersAsUnicode", "false");
        try (Connection connection = PrepUtil.getConnection(cs)) {
            testAddBatch2(connection);
        }
    }

    /**
     * Tests the same as testAddbatch2AEOnConnection, with AE disabled
     * 
     * @throws SQLException
     */
    @Test
    public void testAddbatch2() throws SQLException {
        try (Connection connection = getConnection()) {
            testAddBatch2(connection);
        }
    }

    /**
     * TestClearBatch with AE enabled on the connection
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xSQLv11)
    @Tag(Constants.xSQLv12)
    public void testClearBatchAEOnConnection() throws SQLException {
        try (Connection connection = PrepUtil.getConnection(connectionString + ";columnEncryptionSetting=Enabled;")) {
            testClearBatch(connection);
        }
    }

    /**
     * Test the same as testClearBatchAEOnConnection, with AE disabled
     * 
     * @throws SQLException
     */
    @Test
    public void testClearBatch() throws SQLException {
        try (Connection connection = getConnection()) {
            testClearBatch(connection);
        }
    }

    private void testClearBatch(Connection conn) throws SQLException {
        // Use specific table for this testing
        String batchTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("batchTable")));
        String CREATE_TABLE_SQL = "create table " + batchTable
                + " (KEY1 numeric(19,0) not null, KEY2 numeric(19,0) not null, primary key (KEY1, KEY2))";
        String INSERT_ROW_SQL = "INSERT INTO " + batchTable + "(KEY1, KEY2) VALUES(?, ?)";

        try (Statement s = conn.createStatement()) {
            try (PreparedStatement pstmt = conn.prepareStatement(INSERT_ROW_SQL)) {
                s.execute(CREATE_TABLE_SQL);
                // Set auto-commit to false
                conn.setAutoCommit(false);
                executeBatch(pstmt, 10, "foo".hashCode() + 1);
                pstmt.clearParameters();
                executeBatch(pstmt, 10, "bar".hashCode() + 2);
            } catch (Exception e) {
                conn.rollback();
                throw e;
            } finally {
                TestUtils.dropTableIfExists(batchTable, s);
                conn.commit();
            }
        }
    }

    private void executeBatch(PreparedStatement pstmt, int count, int run) throws SQLException {
        long base = System.currentTimeMillis();

        for (int idx = 1; idx <= count; idx++) {
            pstmt.setLong(1, base + idx);
            pstmt.setLong(2, run);
            pstmt.addBatch();
        }

        int[] rowCounts = pstmt.executeBatch();
        for (int idx = 0; idx < rowCounts.length; idx++) {
            assertTrue(rowCounts[idx] == 1, "Row " + idx + " was not successfully inserted.");
        }
    }

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @BeforeEach
    @Tag(Constants.xSQLv11)
    @Tag(Constants.xSQLv12)
    public void testSetup() throws TestAbortedException, Exception {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            String sql1 = "create table " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (id integer not null, name varchar(255), constraint "
                    + AbstractSQLGenerator.escapeIdentifier(primaryKeyConstraintName) + " primary key (id))";
            stmt.execute(sql1);
        }
    }

    @AfterAll
    public static void terminateVariation() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
        }
    }
}
