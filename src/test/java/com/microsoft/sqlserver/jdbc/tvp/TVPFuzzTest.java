/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.tvp;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Random;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.jdbc.SQLServerDataColumn;
import com.microsoft.sqlserver.jdbc.SQLServerDataTable;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


/**
 * Tests TVP API fuzzing scenarios ported from FX test suite (fuzz.java / fxFuzzTVPAPI.java).
 * Validates driver robustness with malformed inputs, unicode names, random type IDs,
 * swapped data, null/empty TVP objects, and unicode parameter names.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDW)
public class TVPFuzzTest extends AbstractTest {

    private static String tvpName;
    private static String tableName;
    private static String procedureName;
    private static final int NUMBER_OF_ITERATIONS = 50;
    private static final int TABLE_COLUMNS = 24;
    private static final int MAX_IDENTIFIER = 128;

    private static final Random random = new Random(42);

    // SQL Types matching the FX fuzz test's 24-column TVP
    private static final int[] SQL_TYPES = {Types.INTEGER, Types.SMALLINT, Types.TINYINT, Types.BIT, Types.TIMESTAMP,
            Types.TIMESTAMP, Types.DECIMAL, Types.NUMERIC, Types.FLOAT, Types.REAL, Types.BIGINT, Types.DECIMAL,
            Types.VARCHAR, Types.CHAR, Types.BINARY, Types.VARBINARY, Types.NVARCHAR, Types.NCHAR, Types.CHAR,
            Types.BIGINT, Types.DATE, Types.TIMESTAMP, Types.TIME, microsoft.sql.Types.DATETIMEOFFSET};

    // Unicode character pool for fuzzing (from FX createunicodedata)
    private static final char[] UNICODE_POOL = {
            // Latin extended
            '\u00DF', '\u00FC', '\u00EE', '\u00DC', '\u00A9', '\u00F0', '\u00C3', '\u00E3', '\u00E4',
            '\u00E5', '\u00C4', '\u00D6', '\u00F6', '\u00D0', '\u00D1', '\u00FD', '\u00FE',
            // Symbols
            '\u20AC', '\u2030', '\u2122', '\u00AA', '\u00A2', '\u00A3',
            // Cyrillic
            '\u042F', '\u044F', '\u0451', '\u0414',
            // CJK
            '\u4E00', '\u4E01', '\u4E02', '\u4E03', '\u4E04', '\u9FA0', '\u9FA5',
            // Japanese Hiragana
            '\u3041', '\u3042', '\u3043', '\u3044', '\u3045', '\u3093',
            // Japanese Katakana
            '\u30A1', '\u30A2', '\u30A3', '\u30DF',
            // Thai
            '\u0E01', '\u0E02', '\u0E03', '\u0E04', '\u0E5B',
            // Turkish
            '\u0130', '\u0131', '\u015F',
            // Full-width Latin
            '\uFF41', '\uFF42', '\uFF43', '\uFF5A',
            // ASCII
            'a', 'b', 'c', 'A', 'B', 'C', '0', '1', '2', '*', '+', '.', '/', ':', '<', '>', '@', '_', '|', '~'};

    // Fuzz strings for callable statement parameter name testing
    private static final String[] FUZZ_STRINGS = {"\\n", "\\t", "\\v", "\u00DF", "@\u00C3~U", " rBo<'+\u00C3",
            " ''\u00AA\u00C2\u00A9*~\u00FD''", "[[/|]]"};

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @BeforeEach
    public void testSetup() throws SQLException {
        tvpName = RandomUtil.getIdentifier("TVP");
        tableName = RandomUtil.getIdentifier("TVPFuzzTable");
        procedureName = RandomUtil.getIdentifier("spTvpFuzz");
    }

    @AfterEach
    public void terminateVariation() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropProcedureIfExists(procedureName, stmt);
            TestUtils.dropTableIfExists(tableName, stmt);
            TestUtils.dropTypeIfExists(tvpName, stmt);
        }
    }

    @AfterAll
    public static void terminate() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropProcedureIfExists(procedureName, stmt);
            TestUtils.dropTableIfExists(tableName, stmt);
            TestUtils.dropTypeIfExists(tvpName, stmt);
        }
    }

    /**
     * Tests TVP type names with Unicode characters and special symbols.
     * Alternates between PreparedStatement and CallableStatement.
     * FX: fuzz.java::testFuzzTVPName()
     */
    @Test
    @DisplayName("Fuzz TVP Name with Unicode")
    public void testFuzzTVPName() throws Exception {
        createFuzzTable();
        createFuzzTVPS();
        createFuzzProcedure();

        for (int i = 0; i < NUMBER_OF_ITERATIONS; i++) {
            String fuzzedName = createUnicodeIdentifier("tvp", MAX_IDENTIFIER);
            SQLServerDataTable tvp = new SQLServerDataTable();
            addTVPColumnMetaData(tvp);
            Object[] rowData = createValidTVPRow();
            tvp.addRow(rowData);

            try {
                if (i % 2 == 0) {
                    // PreparedStatement path
                    try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                            "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                                    + " select * from ? ;")) {
                        pstmt.setStructured(1, fuzzedName, tvp);
                        pstmt.execute();
                    }
                } else {
                    // CallableStatement path
                    String sql = "{call " + AbstractSQLGenerator.escapeIdentifier(procedureName) + "(?)}";
                    try (SQLServerCallableStatement cstmt = (SQLServerCallableStatement) connection
                            .prepareCall(sql)) {
                        cstmt.setStructured(1, fuzzedName, tvp);
                        cstmt.execute();
                    }
                }
            } catch (SQLException e) {
                // Expected: invalid TVP names should cause SQL errors, not crashes
                assertNotNull(e.getMessage(), "SQLException should have a message");
            }
        }
    }

    /**
     * Tests SQLServerDataColumn with random Java type IDs and Unicode column names.
     * FX: fuzz.java::testFuzzDataColumn()
     */
    @Test
    @DisplayName("Fuzz DataColumn with Random Types")
    public void testFuzzDataColumn() throws Exception {
        for (int i = 0; i < NUMBER_OF_ITERATIONS; i++) {
            try {
                String colName = createUnicodeData(random, 50);
                int randomType = random.nextInt(5000) - 2500; // random type ID
                SQLServerDataColumn col = new SQLServerDataColumn(colName, randomType);
                assertNotNull(col, "SQLServerDataColumn should not be null even with random type");
            } catch (Exception e) {
                // Some random type IDs may cause exceptions - that's acceptable
                assertNotNull(e.getMessage(), "Exception should have a message");
            }
        }
    }

    /**
     * Tests addColumnMetadata with random type IDs and Unicode names.
     * Each iteration creates a fresh SQLServerDataTable.
     * FX: fuzz.java::testFuzzColumnMetadata()
     */
    @Test
    @DisplayName("Fuzz Column Metadata with Random Types")
    public void testFuzzColumnMetadata() throws Exception {
        for (int i = 0; i < NUMBER_OF_ITERATIONS; i++) {
            SQLServerDataTable tvp = new SQLServerDataTable();
            try {
                String colName = createUnicodeData(random, 50);
                int randomType = random.nextInt(5000) - 2500;
                tvp.addColumnMetadata(colName, randomType);
            } catch (Exception e) {
                // Invalid type IDs may cause exceptions
                assertNotNull(e.getMessage(), "Exception should have a message");
            } finally {
                tvp.clear();
            }
        }
    }

    /**
     * Tests adding column metadata repeatedly without clearing the TVP between iterations.
     * Validates memory accumulation doesn't cause crashes.
     * FX: fuzz.java::testFuzzColumnMetadata_WithoutClear()
     */
    @Test
    @DisplayName("Fuzz Column Metadata Without Clear")
    public void testFuzzColumnMetadataWithoutClear() throws Exception {
        SQLServerDataTable tvp = new SQLServerDataTable();
        for (int i = 0; i < NUMBER_OF_ITERATIONS; i++) {
            try {
                String colName = createUnicodeData(random, 50) + "_" + i;
                int randomType = random.nextInt(5000) - 2500;
                tvp.addColumnMetadata(colName, randomType);
            } catch (Exception e) {
                // Expected for invalid types or duplicate names
                assertNotNull(e.getMessage(), "Exception should have a message");
            }
        }
        // Final clear to release resources
        tvp.clear();
    }

    /**
     * Tests alternating valid/invalid data by swapping row data randomly between columns of
     * different types. Validates driver handles type mismatches gracefully.
     * FX: fuzz.java::testFuzzSwapValidData()
     */
    @Test
    @DisplayName("Fuzz Swap Valid Data Between Columns")
    public void testFuzzSwapValidData() throws Exception {
        createFuzzTable();
        createFuzzTVPS();

        for (int i = 0; i < NUMBER_OF_ITERATIONS; i++) {
            SQLServerDataTable tvp = new SQLServerDataTable();
            addTVPColumnMetaData(tvp);

            Object[] rowData = createValidTVPRow();
            // Swap two random columns' data
            int idx1 = random.nextInt(TABLE_COLUMNS);
            int idx2 = random.nextInt(TABLE_COLUMNS);
            Object temp = rowData[idx1];
            rowData[idx1] = rowData[idx2];
            rowData[idx2] = temp;

            try {
                tvp.addRow(rowData);
                try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                        "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                    pstmt.setStructured(1, tvpName, tvp);
                    pstmt.execute();
                }
            } catch (SQLException e) {
                // Type mismatch exceptions are expected
                assertNotNull(e.getMessage(), "SQLException should have a message");
            }
        }
    }

    /**
     * Tests alternating null/empty/valid SQLServerDataTable objects in setStructured().
     * Cycles through 6 patterns: empty TVP, null TVP, valid TVP × PreparedStatement/CallableStatement.
     * FX: fuzz.java::testFuzzVariationTVPObject()
     */
    @Test
    @DisplayName("Fuzz TVP Object Variations (null/empty/valid)")
    public void testFuzzVariationTVPObject() throws Exception {
        createFuzzTable();
        createFuzzTVPS();
        createFuzzProcedure();

        for (int i = 0; i < NUMBER_OF_ITERATIONS; i++) {
            SQLServerDataTable tvp;
            int variation = i % 6;

            switch (variation) {
                case 0: // Empty TVP via PreparedStatement
                    tvp = new SQLServerDataTable();
                    break;
                case 1: // Null TVP via PreparedStatement
                    tvp = null;
                    break;
                case 2: // Valid populated TVP via PreparedStatement
                    tvp = new SQLServerDataTable();
                    addTVPColumnMetaData(tvp);
                    tvp.addRow(createValidTVPRow());
                    break;
                case 3: // Empty TVP via CallableStatement
                    tvp = new SQLServerDataTable();
                    break;
                case 4: // Null TVP via CallableStatement
                    tvp = null;
                    break;
                default: // Valid populated TVP via CallableStatement
                    tvp = new SQLServerDataTable();
                    addTVPColumnMetaData(tvp);
                    tvp.addRow(createValidTVPRow());
                    break;
            }

            try {
                if (variation < 3) {
                    try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                            "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                                    + " select * from ? ;")) {
                        pstmt.setStructured(1, tvpName, tvp);
                        pstmt.execute();
                    }
                } else {
                    String sql = "{call " + AbstractSQLGenerator.escapeIdentifier(procedureName) + "(?)}";
                    try (SQLServerCallableStatement cstmt = (SQLServerCallableStatement) connection
                            .prepareCall(sql)) {
                        cstmt.setStructured(1, tvpName, tvp);
                        cstmt.execute();
                    }
                }
            } catch (SQLException e) {
                // Empty/null TVP or missing columns may cause errors
                assertNotNull(e.getMessage(), "SQLException should have a message");
            }
        }
    }

    /**
     * Tests CallableStatement with Unicode and special characters in parameter names.
     * FX: fuzz.java::testFuzzOverloadedCallableStmt()
     */
    @Test
    @DisplayName("Fuzz CallableStatement Parameter Names")
    public void testFuzzOverloadedCallableStmt() throws Exception {
        createFuzzTable();
        createFuzzTVPS();
        createFuzzProcedure();

        for (String fuzzStr : FUZZ_STRINGS) {
            SQLServerDataTable tvp = new SQLServerDataTable();
            addTVPColumnMetaData(tvp);
            tvp.addRow(createValidTVPRow());

            try {
                String sql = "{call " + AbstractSQLGenerator.escapeIdentifier(procedureName) + "(?)}";
                try (SQLServerCallableStatement cstmt = (SQLServerCallableStatement) connection.prepareCall(sql)) {
                    cstmt.setStructured(fuzzStr, tvpName, tvp);
                    cstmt.execute();
                }
            } catch (SQLException e) {
                // Invalid parameter names should cause SQL errors, not crashes
                assertNotNull(e.getMessage(), "SQLException should have a message");
            }
        }
    }

    /**
     * Tests alternating valid and invalid data objects in TVP rows.
     * FX: fxFuzzTVPAPI.java::testFuzzInvalidData()
     */
    @Test
    @DisplayName("Fuzz Invalid Data Objects in TVP Rows")
    public void testFuzzInvalidData() throws Exception {
        createFuzzTable();
        createFuzzTVPS();

        // Invalid objects that should not be accepted for any SQL type
        Object[] invalidObjects = {new Object(), new StringBuilder("notValid"), new int[] {1, 2, 3},
                new java.util.ArrayList<>(), Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY};

        for (int i = 0; i < NUMBER_OF_ITERATIONS; i++) {
            SQLServerDataTable tvp = new SQLServerDataTable();
            addTVPColumnMetaData(tvp);

            Object[] rowData = createValidTVPRow();
            // Replace a random column with an invalid object
            int targetCol = random.nextInt(TABLE_COLUMNS);
            rowData[targetCol] = invalidObjects[random.nextInt(invalidObjects.length)];

            try {
                tvp.addRow(rowData);
                try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                        "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                    pstmt.setStructured(1, tvpName, tvp);
                    pstmt.execute();
                }
            } catch (SQLException e) {
                // Expected: invalid data types should be rejected
                assertNotNull(e.getMessage(), "SQLException should have a message for invalid data");
            } catch (Exception e) {
                // Other exceptions (ClassCast, etc.) are also acceptable
                assertNotNull(e.getMessage(), "Exception should have a message");
            }
        }
    }

    /**
     * Tests systematic Unicode character fuzzing in TVP column values.
     * Exercises various Unicode character ranges through TVP string columns.
     */
    @Test
    @DisplayName("Fuzz Unicode Characters in TVP Values")
    public void testFuzzUnicodeInTVPValues() throws Exception {
        String colType = "nvarchar(max)";
        createSimpleTable(colType);
        createSimpleTVPS(colType);

        String[] unicodeTestStrings = {
                // Chinese characters
                "\u4E00\u4E01\u4E02\u4E03\u4E04",
                // Arabic
                "\u0627\u0628\u0629\u062A\u062B",
                // Cyrillic
                "\u042F\u044F\u0451\u0414\u0410",
                // Japanese Hiragana
                "\u3041\u3042\u3043\u3044\u3045",
                // Japanese Katakana
                "\u30A1\u30A2\u30A3\u30A4\u30A5",
                // Thai
                "\u0E01\u0E02\u0E03\u0E04\u0E05",
                // Turkish I variants
                "\u0130\u0131\u015F",
                // Mixed special symbols
                "\u20AC\u2030\u2122\u00A9\u00AE",
                // Latin Extended
                "\u00DF\u00FC\u00EE\u00DC\u00C3\u00E3",
                // Full-width Latin
                "\uFF41\uFF42\uFF43\uFF44\uFF45",
                // Empty string
                "",
                // Single character
                "\u00DF",
                // Surrogate pair (supplementary character)
                "\uD835\uDC00\uD835\uDC01"};

        for (String unicodeStr : unicodeTestStrings) {
            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c1", Types.NVARCHAR);
            tvp.addRow(unicodeStr);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.execute();
            }

            // Verify the data was written correctly
            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    java.sql.ResultSet rs = stmt.executeQuery(
                            "select c1 from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next(), "Should have at least one row");
                String actual = rs.getString(1);
                if (actual != null) {
                    // For non-null strings, verify the content matches
                    assertTrue(actual.equals(unicodeStr) || actual.trim().equals(unicodeStr.trim()),
                            "Unicode string mismatch for: " + unicodeStr);
                }
            }

            // Clean up for next iteration
            try (Statement stmt = connection.createStatement()) {
                stmt.executeUpdate(
                        "TRUNCATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName));
            }
        }
    }

    // ========= Helper Methods =========

    private void addTVPColumnMetaData(SQLServerDataTable tvp) throws SQLException {
        for (int i = 0; i < TABLE_COLUMNS; i++) {
            String colName = createUnicodeData(random, 10) + "_" + i;
            tvp.addColumnMetadata(colName, SQL_TYPES[i]);
        }
    }

    private Object[] createValidTVPRow() throws Exception {
        Object[] row = new Object[TABLE_COLUMNS];
        byte[] bytes = new byte[16];
        random.nextBytes(bytes);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        java.util.Date date = sdf.parse("2014-04-03T14:02:57");
        SimpleDateFormat timeSdf = new SimpleDateFormat("H:mm:ss.SSSSSSS");
        java.sql.Time time = new java.sql.Time(timeSdf.parse("23:59:59.5367894").getTime());

        // Match the 24 SQL_TYPES columns with valid data
        row[0] = random.nextInt();                          // INTEGER
        row[1] = (short) random.nextInt(Short.MAX_VALUE);   // SMALLINT
        row[2] = (short) random.nextInt(255);               // TINYINT
        row[3] = random.nextBoolean();                      // BIT
        row[4] = new java.sql.Timestamp(date.getTime());    // TIMESTAMP
        row[5] = new java.sql.Timestamp(date.getTime());    // TIMESTAMP
        row[6] = new BigDecimal("12345.67");                // DECIMAL
        row[7] = new BigDecimal("12345.67");                // NUMERIC
        row[8] = random.nextDouble();                       // FLOAT
        row[9] = random.nextFloat();                        // REAL
        row[10] = random.nextLong();                        // BIGINT
        row[11] = new BigDecimal("99.99");                  // DECIMAL
        row[12] = createUnicodeData(random, 20);            // VARCHAR
        row[13] = createUnicodeData(random, 10);            // CHAR
        row[14] = bytes;                                    // BINARY
        row[15] = bytes;                                    // VARBINARY
        row[16] = createUnicodeData(random, 20);            // NVARCHAR
        row[17] = createUnicodeData(random, 10);            // NCHAR
        row[18] = createUnicodeData(random, 10);            // CHAR
        row[19] = random.nextLong();                        // BIGINT
        row[20] = new java.sql.Date(date.getTime());        // DATE
        row[21] = new java.sql.Timestamp(date.getTime());   // TIMESTAMP
        row[22] = time;                                     // TIME
        row[23] = microsoft.sql.DateTimeOffset.valueOf(
                new java.sql.Timestamp(date.getTime()), 0); // DATETIMEOFFSET
        return row;
    }

    private static String createUnicodeIdentifier(String prefix, int maxLength) {
        StringBuilder sb = new StringBuilder(prefix + "_" + UUID.randomUUID().toString().substring(0, 8) + "_");
        while (sb.length() < maxLength) {
            sb.append(UNICODE_POOL[random.nextInt(UNICODE_POOL.length)]);
        }
        return sb.substring(0, Math.min(sb.length(), maxLength));
    }

    private static String createUnicodeData(Random rnd, int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(UNICODE_POOL[rnd.nextInt(UNICODE_POOL.length)]);
        }
        return sb.toString();
    }

    private void createFuzzTable() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            String cols = "c0 int null, c1 smallint null, c2 tinyint null, c3 bit null, "
                    + "c4 datetime null, c5 smalldatetime null, c6 decimal(10,2) null, c7 numeric(10,2) null, "
                    + "c8 float null, c9 real null, c10 bigint null, c11 decimal(5,2) null, "
                    + "c12 varchar(50) null, c13 char(20) null, c14 binary(20) null, c15 varbinary(50) null, "
                    + "c16 nvarchar(60) null, c17 nchar(30) null, c18 char(20) null, c19 bigint null, "
                    + "c20 date null, c21 datetime2 null, c22 time null, c23 datetimeoffset null";
            String sql = "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (" + cols + ")";
            stmt.execute(sql);
        }
    }

    private void createFuzzTVPS() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            String cols = "c0 int null, c1 smallint null, c2 tinyint null, c3 bit null, "
                    + "c4 datetime null, c5 smalldatetime null, c6 decimal(10,2) null, c7 numeric(10,2) null, "
                    + "c8 float null, c9 real null, c10 bigint null, c11 decimal(5,2) null, "
                    + "c12 varchar(50) null, c13 char(20) null, c14 binary(20) null, c15 varbinary(50) null, "
                    + "c16 nvarchar(60) null, c17 nchar(30) null, c18 char(20) null, c19 bigint null, "
                    + "c20 date null, c21 datetime2 null, c22 time null, c23 datetimeoffset null";
            String sql = "CREATE TYPE " + AbstractSQLGenerator.escapeIdentifier(tvpName) + " AS TABLE (" + cols + ")";
            stmt.executeUpdate(sql);
        }
    }

    private void createFuzzProcedure() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            String sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(procedureName) + " @InputData "
                    + AbstractSQLGenerator.escapeIdentifier(tvpName) + " READONLY AS BEGIN INSERT INTO "
                    + AbstractSQLGenerator.escapeIdentifier(tableName) + " SELECT * FROM @InputData END";
            stmt.execute(sql);
        }
    }

    private void createSimpleTable(String colType) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            String sql = "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (c1 " + colType + " null)";
            stmt.execute(sql);
        }
    }

    private void createSimpleTVPS(String colType) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTypeIfExists(tvpName, stmt);
            String sql = "CREATE TYPE " + AbstractSQLGenerator.escapeIdentifier(tvpName)
                    + " AS TABLE (c1 " + colType + " null)";
            stmt.executeUpdate(sql);
        }
    }
}
