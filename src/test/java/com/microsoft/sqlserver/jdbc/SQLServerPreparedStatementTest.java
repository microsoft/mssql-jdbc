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
import java.util.Arrays;
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

    // A Thai and a CJK character, which no non-Unicode code page represents together
    private static final String UNICODE_VALUE = new String(new int[] {0x0e01, 0x4e2d}, 0, 2);

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
        String rows = executeBatchWithRejectedHandle("", error, prepareMethod, largeBatch, "nvarchar(10)", "v1", "v2",
                "v3", "v4");
        assertEquals("1=v1 2=v2 3=v3 4=v4 ", rows, "every batch entry should be written once, with its own values");
    }

    /**
     * Test that the first entry resent after the server rejected the cached prepared statement handle is prepared with
     * its own parameter types, not with those of a later entry of the failed request.
     *
     * The first entry binds an int and re-uses the cached handle. The second entry binds a bit, so the driver prepares
     * the statement again for it, which sends the request. If the first entry were resent with the parameter types of
     * the second one, its value 5 would be converted to bit and stored as 1.
     *
     * @param error
     *        the error the server raises for the cached handle, 8179 or 586
     * @param prepareMethod
     *        the prepareMethod connection property
     * @throws SQLException
     */
    @ParameterizedTest(name = "error {0}, prepareMethod={1}")
    @CsvSource({"8179, prepexec", "8179, prepare", "586, prepexec", "586, prepare"})
    @Tag(Constants.xAzureSQLDW)
    public void testBatchRetryAfterRejectedCachedHandleWithTypeChange(int error,
            String prepareMethod) throws SQLException {
        assertEquals("1=5 2=1 ", executeBatchWithRejectedHandle("", error, prepareMethod, false, "int", 5, true),
                "every batch entry should be written once, with its own values and parameter types");
    }

    /**
     * Test that, with Always Encrypted enabled on the connection, an entry resent after the server rejected the cached
     * prepared statement handle keeps the Unicode type of a plaintext character parameter bound with setString.
     *
     * Such a parameter is declared as varchar to retrieve the encryption metadata, and as nvarchar once the metadata
     * shows that its column is not encrypted. Declared as varchar, the resent value would lose the characters that the
     * code page of the database cannot represent. The test needs no column encryption key.
     *
     * @param error
     *        the error the server raises for the cached handle, 8179 or 586
     * @param prepareMethod
     *        the prepareMethod connection property
     * @throws SQLException
     */
    @ParameterizedTest(name = "error {0}, prepareMethod={1}")
    @CsvSource({"8179, prepexec", "8179, prepare", "586, prepexec", "586, prepare"})
    @Tag(Constants.xSQLv11)
    @Tag(Constants.xSQLv12)
    @Tag(Constants.xAzureSQLDW)
    public void testBatchRetryAfterRejectedCachedHandleAEOnConnection(int error,
            String prepareMethod) throws SQLException {
        String rows = executeBatchWithRejectedHandle(";columnEncryptionSetting=Enabled;", error, prepareMethod, false,
                "nvarchar(10)", UNICODE_VALUE);
        assertEquals("1=" + UNICODE_VALUE + " ", rows, "the batch entry should be written with all of its characters");
    }

    /**
     * Prepares an INSERT statement so that its handle is cached, makes the server reject the cached handle, and then
     * executes a batch that re-uses the handle.
     *
     * @param connectionProperties
     *        connection properties appended to the connection string
     * @param error
     *        the error the server raises for the cached handle: 8179 after the handle has been unprepared, 586 after
     *        the statement has been prepared in another database
     * @param prepareMethod
     *        the prepareMethod connection property
     * @param largeBatch
     *        true to execute the batch with executeLargeBatch, false for executeBatch
     * @param valueType
     *        the data type of the value column
     * @param values
     *        the values of the batch entries, bound with setString, setInt or setBoolean. The value of the first entry
     *        is also bound when the handle is prepared.
     * @return the rows of the table after the batch, as id=value pairs ordered by id
     * @throws SQLException
     */
    private static String executeBatchWithRejectedHandle(String connectionProperties, int error, String prepareMethod,
            boolean largeBatch, String valueType, Object... values) throws SQLException {
        String tableName = RandomUtil.getIdentifier("tableTestBatchRetryAfterRejectedHandle");

        try (SQLServerConnection con = (SQLServerConnection) PrepUtil
                .getConnection(connectionString + connectionProperties)) {
            // Error 586 is caused by preparing in another database, which Azure SQL Database does not allow.
            Assumptions.assumeFalse(586 == error && TestUtils.isAzure(con) && !TestUtils.isAzureMI(con),
                    "USE cannot switch databases on Azure SQL Database");

            con.setDisableStatementPooling(false);
            con.setStatementPoolingCacheSize(10);
            con.setPrepareMethod(prepareMethod);
            // Bulk copy would insert the batch without executing the cached handle.
            con.setUseBulkCopyForBatchInsert(false);

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

            executeSQL(con, "CREATE TABLE " + table + " (id int, val " + valueType + ")");
            try {
                if (586 == error) {
                    con.setCatalog("tempdb");
                }

                int handle;
                try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(sql)) {
                    pstmt.setInt(1, 0);
                    setValue(pstmt, values[0]);
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
                    for (int i = 0; i < values.length; i++) {
                        pstmt.setInt(1, i + 1);
                        setValue(pstmt, values[i]);
                        pstmt.addBatch();
                    }
                    if (largeBatch) {
                        long[] expectedUpdateCounts = new long[values.length];
                        Arrays.fill(expectedUpdateCounts, 1);
                        assertArrayEquals(expectedUpdateCounts, pstmt.executeLargeBatch());
                    } else {
                        int[] expectedUpdateCounts = new int[values.length];
                        Arrays.fill(expectedUpdateCounts, 1);
                        assertArrayEquals(expectedUpdateCounts, pstmt.executeBatch());
                    }
                }

                StringBuilder rows = new StringBuilder();
                try (Statement stmt = con.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT id, val FROM " + table + " ORDER BY id")) {
                    while (rs.next()) {
                        rows.append(rs.getInt(1)).append('=').append(rs.getString(2)).append(' ');
                    }
                }
                return rows.toString();
            } finally {
                con.setCatalog(database);
                try (Statement stmt = con.createStatement()) {
                    TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
                }
            }
        }
    }

    private static void setValue(PreparedStatement pstmt, Object value) throws SQLException {
        if (value instanceof Boolean) {
            pstmt.setBoolean(2, (Boolean) value);
        } else if (value instanceof Integer) {
            pstmt.setInt(2, (Integer) value);
        } else {
            pstmt.setString(2, (String) value);
        }
    }
}
