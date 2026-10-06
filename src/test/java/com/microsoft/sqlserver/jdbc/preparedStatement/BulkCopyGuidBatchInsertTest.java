/*
 * Microsoft JDBC Driver for SQL Server
 * Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.preparedStatement;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.BulkCopyCommandCapture;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDW)
@ResourceLock(BulkCopyCommandCapture.LOGGER_NAME)
public class BulkCopyGuidBatchInsertTest extends AbstractTest {
    private static final String GUID = "00112233-4455-6677-8899-aabbccddeeff";
    private static final String OTHER_GUID = "ffeeddcc-bbaa-9988-7766-554433221100";

    private enum Binding {
        STRING_LOWER,
        STRING_UPPER,
        UNIQUE_IDENTIFIER,
        UUID_INFERRED,
        UUID_EXPLICIT,
        NULL_STRING,
        NULL_GUID,
        NULL_UNIQUE_IDENTIFIER,
        NULL_OBJECT
    }

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @ParameterizedTest(name = "bulk={0}, largeBatch={1}, batchSize={2}")
    @CsvSource({"false,false,0", "false,true,0", "true,false,0", "true,true,0", "false,false,2", "false,true,2",
            "true,false,2", "true,true,2"})
    public void testBindingsAndMixedColumns(boolean bulk, boolean largeBatch, int batchSize) throws SQLException {
        String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("BulkGuidBatch"));
        try (SQLServerConnection con = (SQLServerConnection) getConnection(); Statement stmt = con.createStatement()) {
            con.setUseBulkCopyForBatchInsert(bulk);
            con.setBulkCopyForBatchInsertBatchSize(batchSize);
            con.setBulkCopyForBatchInsertKeepNulls(true);
            con.setBulkCopyForBatchInsertTableLock(batchSize > 0);
            String guidColumn = "guid UNIQUEIDENTIFIER NULL DEFAULT '" + OTHER_GUID + "'";
            // executeLargeBatch currently requires destination-order columns; executeBatch also covers reordering.
            String columns = largeBatch ? "payload NVARCHAR(40), otherGuid UNIQUEIDENTIFIER NULL, id INT NOT NULL, "
                    + guidColumn : "id INT NOT NULL, " + guidColumn + ", payload NVARCHAR(40), otherGuid UNIQUEIDENTIFIER NULL";
            stmt.executeUpdate("CREATE TABLE " + table + " (" + columns + ")");
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con
                    .prepareStatement("INSERT INTO " + table + " (payload, otherGuid, id, guid) VALUES (?, ?, ?, ?)");
                    BulkCopyCommandCapture capture = new BulkCopyCommandCapture(table)) {
                Binding[] bindings = Binding.values();
                for (int batch = 0; batch < 2; batch++) {
                    for (int row = 0; row < bindings.length; row++) {
                        int id = batch * bindings.length + row;
                        pstmt.setNString(1, "row-" + id + "-\u03bb");
                        pstmt.setString(2, 0 == row % 2 ? OTHER_GUID : null);
                        pstmt.setInt(3, id);
                        bindGuid(pstmt, bindings[row]);
                        pstmt.addBatch();
                    }
                    assertUpdateCounts(pstmt, largeBatch, bindings.length);
                    pstmt.clearBatch();
                }
                assertNativeDeclaration(capture, bulk, true);
                try (ResultSet rs = stmt
                        .executeQuery("SELECT id, guid, payload, otherGuid FROM " + table + " ORDER BY id")) {
                    for (int id = 0; id < 2 * bindings.length; id++) {
                        assertTrue(rs.next());
                        assertEquals(id, rs.getInt(1));
                        boolean nullGuid = id % bindings.length >= Binding.NULL_STRING.ordinal();
                        assertGuid(rs, 2, nullGuid ? null : GUID);
                        assertEquals("row-" + id + "-\u03bb", rs.getString(3));
                        assertGuid(rs, 4, 0 == (id % bindings.length) % 2 ? OTHER_GUID : null);
                    }
                    assertFalse(rs.next());
                }
            }
        } finally {
            dropTable(table);
        }
    }

    private static void bindGuid(SQLServerPreparedStatement pstmt, Binding binding) throws SQLException {
        switch (binding) {
            case STRING_LOWER:
                pstmt.setString(4, GUID);
                break;
            case STRING_UPPER:
                pstmt.setString(4, GUID.toUpperCase(Locale.ROOT));
                break;
            case UNIQUE_IDENTIFIER:
                pstmt.setUniqueIdentifier(4, GUID);
                break;
            case UUID_INFERRED:
                pstmt.setObject(4, UUID.fromString(GUID));
                break;
            case UUID_EXPLICIT:
                pstmt.setObject(4, UUID.fromString(GUID), microsoft.sql.Types.GUID);
                break;
            case NULL_STRING:
                pstmt.setString(4, null);
                break;
            case NULL_GUID:
                pstmt.setNull(4, microsoft.sql.Types.GUID);
                break;
            case NULL_UNIQUE_IDENTIFIER:
                pstmt.setUniqueIdentifier(4, null);
                break;
            case NULL_OBJECT:
                pstmt.setObject(4, null, microsoft.sql.Types.GUID);
                break;
            default:
                throw new AssertionError(binding);
        }
    }

    @ParameterizedTest(name = "bulk={0}, largeBatch={1}, invalid={2}")
    @MethodSource("malformedGuids")
    public void testMalformedGuidStrings(boolean bulk, boolean largeBatch, String value) throws SQLException {
        assertEquals(36, value.length());
        assertRejectedBatch(bulk, largeBatch, value);
    }

    static Stream<Arguments> malformedGuids() {
        List<Arguments> arguments = new ArrayList<>();
        for (boolean bulk : new boolean[] {false, true}) {
            for (boolean largeBatch : new boolean[] {false, true}) {
                for (String value : new String[] {"0011223-44556-6677-8899-aabbccddeeff",
                        "+0112233-4455-6677-8899-aabbccddeeff", "00112233-+455-6677-8899-aabbccddeeff",
                        "00112233-4455-6677-8899-aabbccddeefg"}) {
                    arguments.add(Arguments.of(bulk, largeBatch, value));
                }
            }
        }
        return arguments.stream();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBracedGuidExceedsBulkSourcePrecision(boolean largeBatch) throws SQLException {
        // Destination-derived bulk metadata has precision 36; ordinary parameter conversion can accept braces.
        assertRejectedBatch(true, largeBatch, "{" + GUID + "}");
    }

    private static void assertRejectedBatch(boolean bulk, boolean largeBatch, String value) throws SQLException {
        String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("BulkGuidInvalid"));
        try (SQLServerConnection con = (SQLServerConnection) getConnection(); Statement stmt = con.createStatement()) {
            con.setUseBulkCopyForBatchInsert(bulk);
            stmt.executeUpdate("CREATE TABLE " + table + " (guid UNIQUEIDENTIFIER NULL)");
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con
                    .prepareStatement("INSERT INTO " + table + " VALUES (?)");
                    BulkCopyCommandCapture capture = new BulkCopyCommandCapture(table)) {
                pstmt.setString(1, value);
                pstmt.addBatch();
                if (largeBatch) {
                    assertThrows(BatchUpdateException.class, pstmt::executeLargeBatch);
                } else {
                    assertThrows(BatchUpdateException.class, pstmt::executeBatch);
                }
                assertNativeDeclaration(capture, bulk, false);
            }
        } finally {
            // Close the failed bulk connection before cleaning up on an independent connection.
            dropTable(table);
        }
    }

    private static void assertUpdateCounts(SQLServerPreparedStatement pstmt, boolean largeBatch,
            int rows) throws SQLException {
        if (largeBatch) {
            long[] expected = new long[rows];
            Arrays.fill(expected, 1L);
            assertArrayEquals(expected, pstmt.executeLargeBatch());
        } else {
            int[] expected = new int[rows];
            Arrays.fill(expected, 1);
            assertArrayEquals(expected, pstmt.executeBatch());
        }
    }

    private static void assertGuid(ResultSet rs, int column, String expected) throws SQLException {
        String actual = rs.getString(column);
        if (null == expected) {
            assertNull(actual);
            assertTrue(rs.wasNull());
        } else {
            assertEquals(UUID.fromString(expected), UUID.fromString(actual));
            assertFalse(rs.wasNull());
        }
    }

    private static void dropTable(String table) throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            TestUtils.dropTableIfExists(table, stmt);
        }
    }

    private static void assertNativeDeclaration(BulkCopyCommandCapture capture, boolean bulk, boolean mixedColumns) {
        List<String> declarations = capture.getCommands();
        if (bulk) {
            assertFalse(declarations.isEmpty(), "The batch must use INSERT BULK rather than fall back to RPC.");
            for (String declaration : declarations) {
                assertTrue(declaration.contains("[guid] UNIQUEIDENTIFIER"), declaration);
                if (mixedColumns) {
                    assertTrue(declaration.contains("[otherGuid] UNIQUEIDENTIFIER"), declaration);
                }
            }
        } else {
            assertTrue(declarations.isEmpty(), "Ordinary batch execution must not use INSERT BULK.");
        }
    }
}
