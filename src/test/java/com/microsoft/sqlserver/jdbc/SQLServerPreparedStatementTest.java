/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * Tests for prepared statement batches that re-use a pooled prepared statement handle.
 */
public class SQLServerPreparedStatementTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    private static void executeSQL(SQLServerConnection con, String sql) throws SQLException {
        try (Statement stmt = con.createStatement()) {
            stmt.execute(sql);
        }
    }

    /**
     * Test that a batch is resent with the parameter values of its own entries after the server rejected the cached
     * prepared statement handle.
     *
     * With statement pooling, a new prepared statement re-uses the handle cached for the same SQL text and parameter
     * types. The server rejects that handle with error 8179 when it has been unprepared, and with error 586 when the
     * execution context (here the database) has changed since it was prepared. The driver then prepares the statement
     * again and resends the entries that have not been executed. Every entry has to be written exactly once, with its
     * own values.
     *
     * @param error
     *        the error the server raises for the cached handle, 8179 or 586
     * @param prepareMethod
     *        the prepareMethod connection property
     * @param largeBatch
     *        true to execute the batch with executeLargeBatch, false for executeBatch
     * @throws SQLException
     */
    @ParameterizedTest(name = "error {0}, prepareMethod={1}, largeBatch={2}")
    @CsvSource({"8179, prepexec, false", "8179, prepexec, true", "8179, prepare, false", "8179, prepare, true",
            "586, prepexec, false", "586, prepexec, true", "586, prepare, false", "586, prepare, true"})
    @Tag(Constants.xAzureSQLDW)
    public void testBatchRetryAfterRejectedCachedHandle(int error, String prepareMethod,
            boolean largeBatch) throws SQLException {
        String tableName = RandomUtil.getIdentifier("tableTestBatchRetryAfterRejectedHandle");

        try (SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            // Error 586 is caused by preparing in another database, which Azure SQL Database does not allow.
            Assumptions.assumeFalse(586 == error && TestUtils.isAzure(con) && !TestUtils.isAzureMI(con),
                    "USE cannot switch databases on Azure SQL Database");

            con.setDisableStatementPooling(false);
            con.setStatementPoolingCacheSize(10);
            con.setPrepareMethod(prepareMethod);

            String database = con.getCatalog();
            String schema;
            try (Statement stmt = con.createStatement(); ResultSet rs = stmt.executeQuery("SELECT SCHEMA_NAME()")) {
                rs.next();
                schema = rs.getString(1);
            }
            // The three-part name allows the statement to be prepared in another database for error 586.
            String table = AbstractSQLGenerator.escapeIdentifier(database) + "."
                    + AbstractSQLGenerator.escapeIdentifier(schema) + "."
                    + AbstractSQLGenerator.escapeIdentifier(tableName);
            // A unique SQL text per run keeps the driver-wide parsed SQL cache of earlier runs out of this test, so
            // the first execution below uses sp_executesql.
            String sql = String.format("/*batchretry_%s*/INSERT INTO %s (id, val) VALUES (?, ?)", UUID.randomUUID(),
                    table);

            executeSQL(con, "CREATE TABLE " + table + " (id int, val nvarchar(10))");
            try {
                if (586 == error) {
                    con.setCatalog("tempdb");
                }

                int handle;
                try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(sql)) {
                    pstmt.setInt(1, 0);
                    pstmt.setString(2, "prepare");
                    // executeUpdate reads the whole response, which includes the handle returned by sp_prepexec or
                    // sp_prepare.
                    pstmt.executeUpdate(); // sp_executesql
                    pstmt.executeUpdate(); // sp_prepexec, or sp_prepare and sp_execute; handle created and cached
                    handle = pstmt.getPreparedStatementHandle();
                }
                assertTrue(0 < handle, "a prepared statement handle should have been cached");

                if (586 == error) {
                    con.setCatalog(database);
                } else {
                    executeSQL(con, "EXEC sp_unprepare " + handle);
                }
                executeSQL(con, "DELETE FROM " + table);

                // The batch re-uses the cached handle, and the server rejects it.
                try (PreparedStatement pstmt = con.prepareStatement(sql)) {
                    for (int id = 1; id <= 4; id++) {
                        pstmt.setInt(1, id);
                        pstmt.setString(2, "v" + id);
                        pstmt.addBatch();
                    }
                    if (largeBatch) {
                        assertArrayEquals(new long[] {1, 1, 1, 1}, pstmt.executeLargeBatch());
                    } else {
                        assertArrayEquals(new int[] {1, 1, 1, 1}, pstmt.executeBatch());
                    }
                }

                StringBuilder rows = new StringBuilder();
                try (Statement stmt = con.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT id, val FROM " + table + " ORDER BY id")) {
                    while (rs.next()) {
                        rows.append(rs.getInt(1)).append('=').append(rs.getString(2)).append(' ');
                    }
                }
                assertEquals("1=v1 2=v2 3=v3 4=v4 ", rows.toString(),
                        "every batch entry should be written once, with its own values");
            } finally {
                con.setCatalog(database);
                try (Statement stmt = con.createStatement()) {
                    TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
                }
            }
        }
    }
}
