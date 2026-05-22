/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.preparedStatement;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Field;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ISQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * Tests for ISQLServerPreparedStatement.defineParameterType()
 *
 * Tests verify that the caller-supplied max-length hint is declared on the TDS wire for variable-length
 * types (VARCHAR, CHAR, NVARCHAR, NCHAR, VARBINARY, BINARY), and that out-of-contract usage (negative
 * length, unsupported type, hint smaller than value) is rejected with appropriate errors.
 */
@RunWith(JUnitPlatform.class)
public class DefineParameterTypeTest extends AbstractTest {

    static String tableName = RandomUtil.getIdentifier("DefineParamTypeTest");
    static String escapedTable = AbstractSQLGenerator.escapeIdentifier(tableName);

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();

        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(escapedTable, stmt);
            stmt.execute("CREATE TABLE " + escapedTable + " ("
                    + "id          INT IDENTITY PRIMARY KEY,"
                    + "vcol        VARCHAR(200),"
                    + "nvcol       NVARCHAR(200),"
                    + "bincol      VARBINARY(200)"
                    + ")");
        }
    }

    @AfterAll
    public static void cleanup() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(escapedTable, stmt);
        }
    }

    // -------------------------------------------------------------------------
    // T01 – VARCHAR with hint equal to value length
    // -------------------------------------------------------------------------
    @Test
    public void testT01_VarcharHintEqualToValue() throws Exception {
        String value = "hello";
        try (PreparedStatement pstmt = connection
                .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {
            ISQLServerPreparedStatement sp = (ISQLServerPreparedStatement) pstmt;
            sp.defineParameterType(1, Types.VARCHAR, 10);
            pstmt.setString(1, value);
            pstmt.executeUpdate();
            assertEquals("varchar(10)", getTypeDefinition(pstmt, 1),
                    "Type definition on wire must reflect the hint");
        }
        assertEquals(value, readLastVarchar());
    }

    // -------------------------------------------------------------------------
    // T02 – VARCHAR with hint larger than value length
    // -------------------------------------------------------------------------
    @Test
    public void testT02_VarcharHintLargerThanValue() throws Exception {
        String value = "hi";
        try (PreparedStatement pstmt = connection
                .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {
            ISQLServerPreparedStatement sp = (ISQLServerPreparedStatement) pstmt;
            sp.defineParameterType(1, Types.VARCHAR, 100);
            pstmt.setString(1, value);
            pstmt.executeUpdate();
            assertEquals("varchar(100)", getTypeDefinition(pstmt, 1),
                    "Type definition on wire must reflect the hint");
        }
        assertEquals(value, readLastVarchar());
    }

    // -------------------------------------------------------------------------
    // T03 – CHAR type accepted (routes through VARCHAR branch on wire)
    // -------------------------------------------------------------------------
    @Test
    public void testT03_CharTypeAccepted() throws Exception {
        String value = "abc";
        try (PreparedStatement pstmt = connection
                .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {
            ISQLServerPreparedStatement sp = (ISQLServerPreparedStatement) pstmt;
            sp.defineParameterType(1, Types.CHAR, 10);
            pstmt.setString(1, value);
            pstmt.executeUpdate();
            // CHAR is validated at the API level but the wire type is still varchar
            assertEquals("varchar(10)", getTypeDefinition(pstmt, 1),
                    "CHAR hint must produce varchar(N) on the wire");
        }
        assertEquals(value, readLastVarchar());
    }

    // -------------------------------------------------------------------------
    // T04 – NVARCHAR with hint
    // -------------------------------------------------------------------------
    @Test
    public void testT04_NVarcharHint() throws Exception {
        String value = "ntest";
        try (PreparedStatement pstmt = connection
                .prepareStatement("INSERT INTO " + escapedTable + " (nvcol) VALUES (?)")) {
            ISQLServerPreparedStatement sp = (ISQLServerPreparedStatement) pstmt;
            sp.defineParameterType(1, Types.NVARCHAR, 20);
            pstmt.setNString(1, value);
            pstmt.executeUpdate();
            assertEquals("nvarchar(20)", getTypeDefinition(pstmt, 1),
                    "Type definition on wire must reflect the hint");
        }
        assertEquals(value, readLastNvarchar());
    }

    // -------------------------------------------------------------------------
    // T05 – NCHAR type accepted (routes through NVARCHAR branch on wire)
    // -------------------------------------------------------------------------
    @Test
    public void testT05_NCharTypeAccepted() throws Exception {
        String value = "nc";
        try (PreparedStatement pstmt = connection
                .prepareStatement("INSERT INTO " + escapedTable + " (nvcol) VALUES (?)")) {
            ISQLServerPreparedStatement sp = (ISQLServerPreparedStatement) pstmt;
            sp.defineParameterType(1, Types.NCHAR, 10);
            pstmt.setNString(1, value);
            pstmt.executeUpdate();
            // NCHAR is validated at the API level but the wire type is still nvarchar
            assertEquals("nvarchar(10)", getTypeDefinition(pstmt, 1),
                    "NCHAR hint must produce nvarchar(N) on the wire");
        }
        assertEquals(value, readLastNvarchar());
    }

    // -------------------------------------------------------------------------
    // T06 – VARBINARY with hint
    // -------------------------------------------------------------------------
    @Test
    public void testT06_VarbinaryHint() throws Exception {
        byte[] value = {0x01, 0x02, 0x03};
        try (PreparedStatement pstmt = connection
                .prepareStatement("INSERT INTO " + escapedTable + " (bincol) VALUES (?)")) {
            ISQLServerPreparedStatement sp = (ISQLServerPreparedStatement) pstmt;
            sp.defineParameterType(1, Types.VARBINARY, 10);
            pstmt.setBytes(1, value);
            pstmt.executeUpdate();
            assertEquals("varbinary(10)", getTypeDefinition(pstmt, 1),
                    "Type definition on wire must reflect the hint");
        }
        assertArrayEquals(value, readLastVarbinary());
    }

    // -------------------------------------------------------------------------
    // T07 – BINARY type accepted (routes through VARBINARY branch on wire)
    // -------------------------------------------------------------------------
    @Test
    public void testT07_BinaryTypeAccepted() throws Exception {
        byte[] value = {0x0A, 0x0B};
        try (PreparedStatement pstmt = connection
                .prepareStatement("INSERT INTO " + escapedTable + " (bincol) VALUES (?)")) {
            ISQLServerPreparedStatement sp = (ISQLServerPreparedStatement) pstmt;
            sp.defineParameterType(1, Types.BINARY, 10);
            pstmt.setBytes(1, value);
            pstmt.executeUpdate();
            // BINARY is validated at the API level but the wire type is still varbinary
            assertEquals("varbinary(10)", getTypeDefinition(pstmt, 1),
                    "BINARY hint must produce varbinary(N) on the wire");
        }
        assertArrayEquals(value, readLastVarbinary());
    }

    // -------------------------------------------------------------------------
    // T08 – hint=0 is accepted (driver sends varchar(1) on the wire, MIN guard)
    // -------------------------------------------------------------------------
    @Test
    public void testT08_HintZeroAccepted() throws Exception {
        // maxLength=0 is valid API input; driver will declare varchar(1) on wire (TDS protocol minimum).
        // Inserting empty string should succeed because the value is 0 bytes.
        try (PreparedStatement pstmt = connection
                .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {
            ISQLServerPreparedStatement sp = (ISQLServerPreparedStatement) pstmt;
            sp.defineParameterType(1, Types.VARCHAR, 0);
            pstmt.setString(1, "");
            pstmt.executeUpdate();
            // Math.max(0, 1) = 1 — TDS does not allow varchar(0)
            assertEquals("varchar(1)", getTypeDefinition(pstmt, 1),
                    "hint=0 must be promoted to varchar(1) on the wire");
        }
        assertEquals("", readLastVarchar());
    }

    // -------------------------------------------------------------------------
    // T09 – NULL value with hint
    // -------------------------------------------------------------------------
    @Test
    public void testT09_NullValueWithHint() throws SQLException {
        try (PreparedStatement pstmt = connection
                .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {
            ISQLServerPreparedStatement sp = (ISQLServerPreparedStatement) pstmt;
            sp.defineParameterType(1, Types.VARCHAR, 50);
            pstmt.setNull(1, Types.VARCHAR);
            pstmt.executeUpdate();
        }
        assertNull(readLastVarchar());
    }

    // -------------------------------------------------------------------------
    // T10 – NULL value without hint (baseline: no defineParameterType)
    // -------------------------------------------------------------------------
    @Test
    public void testT10_NullValueWithoutHint() throws SQLException {
        try (PreparedStatement pstmt = connection
                .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {
            pstmt.setNull(1, Types.VARCHAR);
            pstmt.executeUpdate();
        }
        assertNull(readLastVarchar());
    }

    // -------------------------------------------------------------------------
    // T11 – Empty string with hint
    // -------------------------------------------------------------------------
    @Test
    public void testT11_EmptyStringWithHint() throws SQLException {
        try (PreparedStatement pstmt = connection
                .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {
            ISQLServerPreparedStatement sp = (ISQLServerPreparedStatement) pstmt;
            sp.defineParameterType(1, Types.VARCHAR, 10);
            pstmt.setString(1, "");
            pstmt.executeUpdate();
        }
        assertEquals("", readLastVarchar());
    }

    // -------------------------------------------------------------------------
    // T12 – Hint at boundary 8000 → driver sends varchar(max) on wire (≥ threshold)
    // -------------------------------------------------------------------------
    @Test
    public void testT12_VarcharHintAt8000() throws Exception {
        String value = "boundary";
        try (PreparedStatement pstmt = connection
                .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {
            ISQLServerPreparedStatement sp = (ISQLServerPreparedStatement) pstmt;
            sp.defineParameterType(1, Types.VARCHAR, 8000);
            pstmt.setString(1, value);
            pstmt.executeUpdate();
            // 8000 >= SHORT_VARTYPE_MAX_BYTES (8000) — promotes to varchar(max)
            assertEquals("varchar(max)", getTypeDefinition(pstmt, 1),
                    "hint=8000 must promote to varchar(max) on the wire");
        }
        assertEquals(value, readLastVarchar());
    }

    // -------------------------------------------------------------------------
    // T13 – Hint at 8001 → driver sends varchar(max) on wire
    // -------------------------------------------------------------------------
    @Test
    public void testT13_VarcharHintAt8001GoesToMax() throws Exception {
        String value = "maxtest";
        try (PreparedStatement pstmt = connection
                .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {
            ISQLServerPreparedStatement sp = (ISQLServerPreparedStatement) pstmt;
            sp.defineParameterType(1, Types.VARCHAR, 8001);
            pstmt.setString(1, value);
            pstmt.executeUpdate();
            assertEquals("varchar(max)", getTypeDefinition(pstmt, 1),
                    "hint > 8000 must produce varchar(max) on the wire");
        }
        assertEquals(value, readLastVarchar());
    }

    // -------------------------------------------------------------------------
    // T14 – NVARCHAR hint at 4000 → driver sends nvarchar(max) on wire (≥ threshold)
    // -------------------------------------------------------------------------
    @Test
    public void testT14_NVarcharHintAt4000() throws Exception {
        String value = "nboundary";
        try (PreparedStatement pstmt = connection
                .prepareStatement("INSERT INTO " + escapedTable + " (nvcol) VALUES (?)")) {
            ISQLServerPreparedStatement sp = (ISQLServerPreparedStatement) pstmt;
            sp.defineParameterType(1, Types.NVARCHAR, 4000);
            pstmt.setNString(1, value);
            pstmt.executeUpdate();
            // 4000 >= SHORT_VARTYPE_MAX_CHARS (4000) — promotes to nvarchar(max)
            assertEquals("nvarchar(max)", getTypeDefinition(pstmt, 1),
                    "hint=4000 must promote to nvarchar(max) on the wire");
        }
        assertEquals(value, readLastNvarchar());
    }

    // -------------------------------------------------------------------------
    // T15 – NVARCHAR hint at 4001 → driver sends nvarchar(max) on wire
    // -------------------------------------------------------------------------
    @Test
    public void testT15_NVarcharHintAt4001GoesToMax() throws Exception {
        String value = "nmaxtest";
        try (PreparedStatement pstmt = connection
                .prepareStatement("INSERT INTO " + escapedTable + " (nvcol) VALUES (?)")) {
            ISQLServerPreparedStatement sp = (ISQLServerPreparedStatement) pstmt;
            sp.defineParameterType(1, Types.NVARCHAR, 4001);
            pstmt.setNString(1, value);
            pstmt.executeUpdate();
            assertEquals("nvarchar(max)", getTypeDefinition(pstmt, 1),
                    "hint > 4000 must produce nvarchar(max) on the wire");
        }
        assertEquals(value, readLastNvarchar());
    }

    // -------------------------------------------------------------------------
    // T16 – Hint at 1 (minimum meaningful size)
    // -------------------------------------------------------------------------
    @Test
    public void testT16_HintAt1() throws Exception {
        String value = "A";
        try (PreparedStatement pstmt = connection
                .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {
            ISQLServerPreparedStatement sp = (ISQLServerPreparedStatement) pstmt;
            sp.defineParameterType(1, Types.VARCHAR, 1);
            pstmt.setString(1, value);
            pstmt.executeUpdate();
            assertEquals("varchar(1)", getTypeDefinition(pstmt, 1),
                    "Type definition on wire must reflect the hint");
        }
        assertEquals(value, readLastVarchar());
    }

    // -------------------------------------------------------------------------
    // T17 – Large value without hint (baseline: driver uses default varchar(8000))
    // -------------------------------------------------------------------------
    @Test
    public void testT17_LargeValueWithoutHint() throws Exception {
        // Build a 200-char string — fits in the default varchar(8000) the driver sends
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 200; i++) sb.append('X');
        String value = sb.toString();

        try (PreparedStatement pstmt = connection
                .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {
            pstmt.setString(1, value);
            pstmt.executeUpdate();
            // Without defineParameterType the driver falls back to the 8000-byte default
            assertEquals("varchar(8000)", getTypeDefinition(pstmt, 1),
                    "Without a hint the driver must use the default varchar(8000)");
        }
        assertEquals(value, readLastVarchar());
    }

    // -------------------------------------------------------------------------
    // T18 – Hint too small for value → server returns truncation/overflow error
    // -------------------------------------------------------------------------
    @Test
    public void testT18_HintTooSmallServerError() throws SQLException {
        boolean exceptionThrown = false;
        try (PreparedStatement pstmt = connection
                .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {
            ISQLServerPreparedStatement sp = (ISQLServerPreparedStatement) pstmt;
            // hint=3, value is 10 chars — server should error
            sp.defineParameterType(1, Types.VARCHAR, 3);
            pstmt.setString(1, "0123456789");
            try {
                pstmt.executeUpdate();
                fail("Expected server error for value exceeding defined parameter length");
            } catch (SQLServerException e) {
                exceptionThrown = true;
                // SQL Server reports truncation — message must contain "truncat" (case-insensitive)
                assertTrue(e.getMessage().toLowerCase().contains("truncat"),
                        "Expected truncation error but got: " + e.getMessage());
            }
        }
        assertTrue(exceptionThrown, "SQLServerException must be thrown when value exceeds the hint");
        // Verify the oversized value was not persisted
        String last = readLastVarchar();
        assertTrue(last == null || !"0123456789".equals(last),
                "The value exceeding the hint must not have been persisted");
    }

    // -------------------------------------------------------------------------
    // T19 – Negative maxLength → driver rejects with R_invalidParameterLength
    // -------------------------------------------------------------------------
    @Test
    public void testT19_NegativeMaxLength() throws SQLException {
        try (PreparedStatement pstmt = connection
                .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {
            ISQLServerPreparedStatement sp = (ISQLServerPreparedStatement) pstmt;
            try {
                sp.defineParameterType(1, Types.VARCHAR, -1);
                fail("Expected SQLServerException for negative maxLength");
            } catch (SQLServerException e) {
                assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_invalidParameterLength")),
                        "Unexpected error message: " + e.getMessage());
            }
        }
    }

    // -------------------------------------------------------------------------
    // T20 – Unsupported SQL type INTEGER → driver rejects
    // -------------------------------------------------------------------------
    @Test
    public void testT20_UnsupportedTypeInteger() throws SQLException {
        try (PreparedStatement pstmt = connection
                .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {
            ISQLServerPreparedStatement sp = (ISQLServerPreparedStatement) pstmt;
            try {
                sp.defineParameterType(1, Types.INTEGER, 10);
                fail("Expected SQLServerException for unsupported type INTEGER");
            } catch (SQLServerException e) {
                assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_unsupportedTypeForDefineParamType")),
                        "Unexpected error message: " + e.getMessage());
            }
        }
    }

    // -------------------------------------------------------------------------
    // T21 – Unsupported SQL type CLOB → driver rejects
    // -------------------------------------------------------------------------
    @Test
    public void testT21_UnsupportedTypeClob() throws SQLException {
        try (PreparedStatement pstmt = connection
                .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {
            ISQLServerPreparedStatement sp = (ISQLServerPreparedStatement) pstmt;
            try {
                sp.defineParameterType(1, Types.CLOB, 10);
                fail("Expected SQLServerException for unsupported type CLOB");
            } catch (SQLServerException e) {
                assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_unsupportedTypeForDefineParamType")),
                        "Unexpected error message: " + e.getMessage());
            }
        }
    }

    // -------------------------------------------------------------------------
    // T22 – Parameter index out of range (too high)
    // -------------------------------------------------------------------------
    @Test
    public void testT22_OutOfRangeParameterIndex() throws SQLException {
        try (PreparedStatement pstmt = connection
                .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {
            ISQLServerPreparedStatement sp = (ISQLServerPreparedStatement) pstmt;
            try {
                sp.defineParameterType(99, Types.VARCHAR, 10);
                fail("Expected exception for out-of-range parameter index");
            } catch (SQLServerException e) {
                assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_indexOutOfRange")),
                        "Unexpected error message: " + e.getMessage());
            }
        }
    }

    // -------------------------------------------------------------------------
    // T23 – Calling defineParameterType on a closed statement
    // -------------------------------------------------------------------------
    @Test
    public void testT23_ClosedStatement() throws SQLException {
        PreparedStatement pstmt = connection
                .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)");
        ISQLServerPreparedStatement sp = (ISQLServerPreparedStatement) pstmt;
        pstmt.close();
        try {
            sp.defineParameterType(1, Types.VARCHAR, 10);
            fail("Expected exception when calling defineParameterType on closed statement");
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_statementIsClosed")),
                    "Unexpected error message: " + e.getMessage());
        }
    }

    // -------------------------------------------------------------------------
    // T24 – Batch: hint persists across 1000 rows
    // -------------------------------------------------------------------------
    @Test
    public void testT24_BatchHintPersistsAcrossRows() throws SQLException {
        final int BATCH_SIZE = 1000;
        int maxIdBefore = getMaxId();
        try (PreparedStatement pstmt = connection
                .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {
            ISQLServerPreparedStatement sp = (ISQLServerPreparedStatement) pstmt;
            sp.defineParameterType(1, Types.VARCHAR, 50);
            for (int i = 0; i < BATCH_SIZE; i++) {
                pstmt.setString(1, "t24row" + i);
                pstmt.addBatch();
            }
            int[] counts = pstmt.executeBatch();
            assertEquals(BATCH_SIZE, counts.length);
        }
        // Verify all 1000 rows were actually persisted
        int insertedCount = countRowsSince(maxIdBefore);
        assertEquals(BATCH_SIZE, insertedCount,
                "Expected " + BATCH_SIZE + " rows persisted but found " + insertedCount);
    }

    // -------------------------------------------------------------------------
    // T25 – Batch: varying values all within hint
    // -------------------------------------------------------------------------
    @Test
    public void testT25_BatchVaryingValuesWithinHint() throws SQLException {
        String[] values = {"t25a", "t25bb", "t25ccc", "t25dddd", "t25eeeee"};
        int maxIdBefore = getMaxId();
        try (PreparedStatement pstmt = connection
                .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {
            ISQLServerPreparedStatement sp = (ISQLServerPreparedStatement) pstmt;
            sp.defineParameterType(1, Types.VARCHAR, 100);
            for (String v : values) {
                pstmt.setString(1, v);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
        // Verify every value was persisted in insertion order
        List<String> persisted = readVarcharsSince(maxIdBefore);
        assertEquals(Arrays.asList(values), persisted,
                "Persisted rows do not match the inserted values");
    }

    // -------------------------------------------------------------------------
    // T26 – Batch: hint too small for one row → server error
    // -------------------------------------------------------------------------
    @Test
    public void testT26_BatchHintTooSmallForOneRow() throws SQLException {
        try (PreparedStatement pstmt = connection
                .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {
            ISQLServerPreparedStatement sp = (ISQLServerPreparedStatement) pstmt;
            sp.defineParameterType(1, Types.VARCHAR, 5);
            pstmt.setString(1, "ok");
            pstmt.addBatch();
            pstmt.setString(1, "this_is_way_too_long_for_hint_5");
            pstmt.addBatch();
            try {
                pstmt.executeBatch();
                fail("Expected server error for batch row exceeding defined parameter length");
            } catch (BatchUpdateException e) {
                // Verify at least one row failed (Statement.EXECUTE_FAILED = -3)
                boolean anyFailed = false;
                for (int c : e.getUpdateCounts()) {
                    if (c == Statement.EXECUTE_FAILED) {
                        anyFailed = true;
                        break;
                    }
                }
                assertTrue(anyFailed, "Expected at least one EXECUTE_FAILED update count");
            } catch (SQLServerException e) {
                // Some driver configurations throw SQLServerException directly for batch errors
                assertTrue(e.getMessage().toLowerCase().contains("truncat"),
                        "Expected truncation error but got: " + e.getMessage());
            }
        }
    }

    // -------------------------------------------------------------------------
    // T27 – No-hint baseline batch
    // -------------------------------------------------------------------------
    @Test
    public void testT27_NoHintBatch() throws SQLException {
        String[] values = {"t27val0", "t27val1", "t27val2", "t27val3", "t27val4"};
        int maxIdBefore = getMaxId();
        try (PreparedStatement pstmt = connection
                .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {
            for (String v : values) {
                pstmt.setString(1, v);
                pstmt.addBatch();
            }
            int[] counts = pstmt.executeBatch();
            assertEquals(values.length, counts.length);
        }
        // Verify every value was actually persisted
        List<String> persisted = readVarcharsSince(maxIdBefore);
        assertEquals(Arrays.asList(values), persisted,
                "Persisted rows do not match the inserted values");
    }

    // -------------------------------------------------------------------------
    // T28 – sendStringParametersAsUnicode=true: hint applied to NVARCHAR branch
    // -------------------------------------------------------------------------
    @Test
    public void testT28_SendStringParametersAsUnicodeWithHint() throws SQLException {
        String connStr = connectionString + ";sendStringParametersAsUnicode=true;";
        try (Connection con = PrepUtil.getConnection(connStr);
             PreparedStatement pstmt = con
                     .prepareStatement("INSERT INTO " + escapedTable + " (nvcol) VALUES (?)")) {
            ISQLServerPreparedStatement sp = (ISQLServerPreparedStatement) pstmt;
            sp.defineParameterType(1, Types.NVARCHAR, 50);
            pstmt.setString(1, "unicode");
            pstmt.executeUpdate();
        }
        assertEquals("unicode", readLastNvarchar());
    }

    // -------------------------------------------------------------------------
    // T31 – clearParameters() preserves the hint for the next execute
    // -------------------------------------------------------------------------
    @Test
    public void testT31_ClearParametersPreservesHint() throws Exception {
        try (PreparedStatement pstmt = connection
                .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {
            ISQLServerPreparedStatement sp = (ISQLServerPreparedStatement) pstmt;
            sp.defineParameterType(1, Types.VARCHAR, 50);
            pstmt.setString(1, "first");
            pstmt.executeUpdate();
            assertEquals("varchar(50)", getTypeDefinition(pstmt, 1),
                    "Type definition must reflect the hint before clearParameters");

            // clearParameters and re-bind — hint should still be active
            pstmt.clearParameters();
            pstmt.setString(1, "second");
            pstmt.executeUpdate();
            assertEquals("varchar(50)", getTypeDefinition(pstmt, 1),
                    "Type definition must still reflect the hint after clearParameters");
        }
        assertEquals("second", readLastVarchar());
    }

    // -------------------------------------------------------------------------
    // T32 – New PreparedStatement does not inherit hint from another statement
    // -------------------------------------------------------------------------
    @Test
    public void testT32_NewStatementNoInheritedHint() throws Exception {
        try (PreparedStatement pstmt1 = connection
                .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)");
             PreparedStatement pstmt2 = connection
                     .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {
            ISQLServerPreparedStatement sp1 = (ISQLServerPreparedStatement) pstmt1;
            sp1.defineParameterType(1, Types.VARCHAR, 20);
            pstmt1.setString(1, "ps1value");
            pstmt1.executeUpdate();
            assertEquals("varchar(20)", getTypeDefinition(pstmt1, 1),
                    "pstmt1 must use the hint");

            // pstmt2 should use default size, not the hint set on pstmt1
            pstmt2.setString(1, "ps2value");
            pstmt2.executeUpdate();
            assertEquals("varchar(8000)", getTypeDefinition(pstmt2, 1),
                    "pstmt2 must use the default (no hint was set)");
        }
        assertEquals("ps2value", readLastVarchar());
    }

    // -------------------------------------------------------------------------
    // T33 – Per-parameter hints: different hints for different parameters
    // -------------------------------------------------------------------------
    @Test
    public void testT33_PerParameterHints() throws SQLException {
        try (PreparedStatement pstmt = connection
                .prepareStatement("INSERT INTO " + escapedTable + " (vcol, nvcol) VALUES (?, ?)")) {
            ISQLServerPreparedStatement sp = (ISQLServerPreparedStatement) pstmt;
            sp.defineParameterType(1, Types.VARCHAR, 30);
            sp.defineParameterType(2, Types.NVARCHAR, 60);
            pstmt.setString(1, "vval");
            pstmt.setNString(2, "nval");
            pstmt.executeUpdate();
        }
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT TOP 1 vcol, nvcol FROM " + escapedTable + " ORDER BY id DESC")) {
            assertTrue(rs.next());
            assertEquals("vval", rs.getString(1));
            assertEquals("nval", rs.getString(2));
        }
    }

    // -------------------------------------------------------------------------
    // T34 – useBulkCopyForBatchInsert=true: hint applied correctly
    // -------------------------------------------------------------------------
    @Test
    public void testT34_BulkCopyBatchWithHint() throws SQLException {
        String[] values = {"t34bulk0", "t34bulk1", "t34bulk2", "t34bulk3", "t34bulk4"};
        int maxIdBefore = getMaxId();
        String connStr = connectionString + ";useBulkCopyForBatchInsert=true;";
        try (Connection con = PrepUtil.getConnection(connStr);
             PreparedStatement pstmt = con
                     .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {
            ISQLServerPreparedStatement sp = (ISQLServerPreparedStatement) pstmt;
            sp.defineParameterType(1, Types.VARCHAR, 50);
            for (String v : values) {
                pstmt.setString(1, v);
                pstmt.addBatch();
            }
            int[] counts = pstmt.executeBatch();
            assertEquals(values.length, counts.length);
        }
        // Verify every value was actually persisted
        List<String> persisted = readVarcharsSince(maxIdBefore);
        assertEquals(Arrays.asList(values), persisted,
                "Persisted rows do not match the inserted values");
    }

    // -------------------------------------------------------------------------
    // T35 – useBulkCopyForBatchInsert=true: no hint baseline
    // -------------------------------------------------------------------------
    @Test
    public void testT35_BulkCopyBatchNoHint() throws SQLException {
        String[] values = {"t35nohint0", "t35nohint1", "t35nohint2", "t35nohint3", "t35nohint4"};
        int maxIdBefore = getMaxId();
        String connStr = connectionString + ";useBulkCopyForBatchInsert=true;";
        try (Connection con = PrepUtil.getConnection(connStr);
             PreparedStatement pstmt = con
                     .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {
            for (String v : values) {
                pstmt.setString(1, v);
                pstmt.addBatch();
            }
            int[] counts = pstmt.executeBatch();
            assertEquals(values.length, counts.length);
        }
        // Verify every value was actually persisted
        List<String> persisted = readVarcharsSince(maxIdBefore);
        assertEquals(Arrays.asList(values), persisted,
                "Persisted rows do not match the inserted values");
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /**
     * Returns the TDS type definition string that was used for parameter {@code paramIndex} (1-based)
     * during the most recent execution of {@code pstmt}. Uses reflection to access the
     * package-private {@code inOutParam} array on {@link SQLServerStatement} and the private
     * {@code typeDefinition} field on {@code Parameter}.
     */
    private static String getTypeDefinition(PreparedStatement pstmt, int paramIndex) throws Exception {
        Field inOutParamField = SQLServerStatement.class.getDeclaredField("inOutParam");
        inOutParamField.setAccessible(true);
        Object[] inOutParam = (Object[]) inOutParamField.get(pstmt);
        Object param = inOutParam[paramIndex - 1];

        Field typeDefField = param.getClass().getDeclaredField("typeDefinition");
        typeDefField.setAccessible(true);
        return (String) typeDefField.get(param);
    }

    /** Returns the current maximum id in the test table (0 if the table is empty). */
    private int getMaxId() throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT ISNULL(MAX(id), 0) FROM " + escapedTable)) {
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    /** Returns the count of rows inserted after the given id snapshot. */
    private int countRowsSince(int minId) throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT COUNT(*) FROM " + escapedTable + " WHERE id > " + minId)) {
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    /** Returns vcol values for all rows inserted after the given id snapshot, ordered by id. */
    private List<String> readVarcharsSince(int minId) throws SQLException {
        List<String> result = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT vcol FROM " + escapedTable + " WHERE id > " + minId + " ORDER BY id")) {
            while (rs.next()) {
                result.add(rs.getString(1));
            }
        }
        return result;
    }

    /** Returns the vcol of the most recently inserted row. */
    private String readLastVarchar() throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT TOP 1 vcol FROM " + escapedTable + " ORDER BY id DESC")) {
            if (rs.next()) {
                return rs.getString(1);
            }
            fail("No rows found in table");
            return null;
        }
    }

    /** Returns the nvcol of the most recently inserted row. */
    private String readLastNvarchar() throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT TOP 1 nvcol FROM " + escapedTable + " ORDER BY id DESC")) {
            if (rs.next()) {
                return rs.getString(1);
            }
            fail("No rows found in table");
            return null;
        }
    }

    /** Returns the bincol of the most recently inserted row. */
    private byte[] readLastVarbinary() throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT TOP 1 bincol FROM " + escapedTable + " ORDER BY id DESC")) {
            if (rs.next()) {
                return rs.getBytes(1);
            }
            fail("No rows found in table");
            return null;
        }
    }
}
