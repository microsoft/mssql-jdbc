/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.tvp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ISQLServerDataRecord;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.jdbc.SQLServerDataTable;
import com.microsoft.sqlserver.jdbc.SQLServerMetaData;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


/**
 * Tests TVP with custom ISQLServerDataRecord implementations and ParameterMetaData validation.
 * Merged from FX test suites: TVPSQLServerDataRecord.java, TVPCS_DataRecord.java,
 * ParameterMetaData.java.
 * Uses nested classes to group related test categories:
 * - DataRecordTypes: ISQLServerDataRecord usage with PreparedStatement and CallableStatement
 * - ParameterMetaData: getParameterMetaData() verification across TVP sources
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDW)
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxTVP)
public class TVPDataRecordTest extends AbstractTest {

    private static String tvpName;
    private static String tableName;
    private static String destTableName;
    private static String procedureName;

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @BeforeEach
    public void testSetup() throws SQLException {
        tvpName = RandomUtil.getIdentifier("TVP");
        tableName = RandomUtil.getIdentifier("TVPDRTable");
        destTableName = RandomUtil.getIdentifier("TVPDRDest");
        procedureName = RandomUtil.getIdentifier("spTvpDR");
    }

    @AfterEach
    public void terminateVariation() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropProcedureIfExists(procedureName, stmt);
            TestUtils.dropTableIfExists(tableName, stmt);
            TestUtils.dropTableIfExists(destTableName, stmt);
            TestUtils.dropTypeIfExists(tvpName, stmt);
        }
    }

    @AfterAll
    public static void terminate() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropProcedureIfExists(procedureName, stmt);
            TestUtils.dropTableIfExists(tableName, stmt);
            TestUtils.dropTableIfExists(destTableName, stmt);
            TestUtils.dropTypeIfExists(tvpName, stmt);
        }
    }

    // ==============================
    // Shared Helper Methods
    // ==============================

    private void createTable(String tblName, String columnsDef) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(tblName, stmt);
            stmt.execute(
                    "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tblName) + " (" + columnsDef + ")");
        }
    }

    private void createTVPS(String columnsDef) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTypeIfExists(tvpName, stmt);
            stmt.executeUpdate("CREATE TYPE " + AbstractSQLGenerator.escapeIdentifier(tvpName)
                    + " AS TABLE (" + columnsDef + ")");
        }
    }

    private void createProcedure(String targetTable) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropProcedureIfExists(procedureName, stmt);
            String sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(procedureName)
                    + " @InputData " + AbstractSQLGenerator.escapeIdentifier(tvpName)
                    + " READONLY AS BEGIN INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(targetTable)
                    + " SELECT * FROM @InputData END";
            stmt.execute(sql);
        }
    }

    // ==============================
    // Shared ISQLServerDataRecord Implementation
    // ==============================

    /**
     * Simple ISQLServerDataRecord implementation for testing.
     */
    static class TestDataRecord implements ISQLServerDataRecord {
        private final SQLServerMetaData[] metadata;
        private final Iterator<Object[]> iterator;
        private Object[] currentRow;

        TestDataRecord(SQLServerMetaData[] metadata, List<Object[]> rows) {
            this.metadata = metadata;
            this.iterator = rows.iterator();
        }

        @Override
        public SQLServerMetaData getColumnMetaData(int column) {
            return metadata[column - 1];
        }

        @Override
        public int getColumnCount() {
            return metadata.length;
        }

        @Override
        public Object[] getRowData() {
            return currentRow;
        }

        @Override
        public boolean next() {
            if (iterator.hasNext()) {
                currentRow = iterator.next();
                return true;
            }
            return false;
        }
    }

    // ================================================================
    // Nested Class: DataRecord Type Tests (from TVPDataRecordTest.java)
    // ================================================================

    /**
     * Tests ISQLServerDataRecord with various data types via PreparedStatement and CallableStatement.
     * Ported from FX TVPSQLServerDataRecord.java, TVPCS_DataRecord.java.
     */
    @Nested
    @DisplayName("DataRecordTypes")
    class DataRecordTypes {

        /**
         * Test TVP with custom ISQLServerDataRecord
         * This test covers integer data types and validates insertion via PreparedStatement.
         * 
         * @throws SQLException
         */
        @Test
        @DisplayName("DataRecord: Integer Types via PreparedStatement")
        public void testDataRecordIntegerTypesPreparedStatement() throws SQLException {
            String tableDef = "c_int int null, c_smallint smallint null, c_tinyint tinyint null, c_bigint bigint null";
            createTable(tableName, tableDef);
            createTVPS(tableDef);

            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] {100, (short) 200, (short) 250, 9999999999L});
            rows.add(new Object[] {-100, (short) -200, (short) 0, -9999999999L});
            rows.add(new Object[] {Integer.MAX_VALUE, Short.MAX_VALUE, (short) 255, Long.MAX_VALUE});

            SQLServerMetaData[] metadata = new SQLServerMetaData[] {
                    new SQLServerMetaData("c_int", Types.INTEGER),
                    new SQLServerMetaData("c_smallint", Types.SMALLINT),
                    new SQLServerMetaData("c_tinyint", Types.TINYINT),
                    new SQLServerMetaData("c_bigint", Types.BIGINT)};

            ISQLServerDataRecord record = new TestDataRecord(metadata, rows);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, record);
                pstmt.execute();
            }

            // Validate inserted data
            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                int rowCount = 0;
                while (rs.next()) {
                    rowCount++;
                }
                assertEquals(3, rowCount, "Should have inserted 3 rows");
            }
        }

        /**
         * Test TVP with custom ISQLServerDataRecord via PreparedStatement
         * This test covers numeric data types and validates insertion via PreparedStatement.
         * 
         * @throws SQLException
         */
        @Test
        @DisplayName("DataRecord: Numeric Types via PreparedStatement")
        public void testDataRecordNumericTypesPreparedStatement() throws SQLException {
            String tableDef = "c_decimal decimal(10,2) null, c_numeric numeric(10,2) null, "
                    + "c_float float null, c_real real null, c_bit bit null";
            createTable(tableName, tableDef);
            createTVPS(tableDef);

            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] {new BigDecimal("12345.67"), new BigDecimal("99999.99"), 3.14159d, 2.71828f, true});
            rows.add(new Object[] {new BigDecimal("0.01"), new BigDecimal("-99999.99"), -1.5d, -1.5f, false});
            rows.add(new Object[] {null, null, null, null, null});

            SQLServerMetaData[] metadata = new SQLServerMetaData[] {
                    new SQLServerMetaData("c_decimal", Types.DECIMAL, 10, 2),
                    new SQLServerMetaData("c_numeric", Types.NUMERIC, 10, 2),
                    new SQLServerMetaData("c_float", Types.FLOAT),
                    new SQLServerMetaData("c_real", Types.REAL),
                    new SQLServerMetaData("c_bit", Types.BIT)};

            ISQLServerDataRecord record = new TestDataRecord(metadata, rows);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, record);
                pstmt.execute();
            }

            // Validate inserted data
            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next(), "Should have first row");
                assertEquals(new BigDecimal("12345.67"), rs.getBigDecimal(1));
                assertTrue(rs.next(), "Should have second row");
                assertTrue(rs.next(), "Should have third (null) row");
                assertTrue(rs.getObject(1) == null, "Decimal should be null");
            }
        }

        /**
         * Test TVP with custom ISQLServerDataRecord via PreparedStatement
         * This test covers string data types and validates insertion via PreparedStatement.
         * 
         * @throws SQLException
         */
        @Test
        @DisplayName("DataRecord: String Types via PreparedStatement")
        public void testDataRecordStringTypesPreparedStatement() throws SQLException {
            String tableDef = "c_char char(20) null, c_varchar varchar(50) null, c_nchar nchar(30) null, "
                    + "c_nvarchar nvarchar(60) null";
            createTable(tableName, tableDef);
            createTVPS(tableDef);

            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] {"hello", "world", "unicode\u4E2D\u6587", "nvarchar\u00FC\u00E4"});
            rows.add(new Object[] {"", "", "", ""});
            rows.add(new Object[] {null, null, null, null});

            SQLServerMetaData[] metadata = new SQLServerMetaData[] {
                    new SQLServerMetaData("c_char", Types.CHAR, 20),
                    new SQLServerMetaData("c_varchar", Types.VARCHAR, 50),
                    new SQLServerMetaData("c_nchar", Types.NCHAR, 30),
                    new SQLServerMetaData("c_nvarchar", Types.NVARCHAR, 60)};

            ISQLServerDataRecord record = new TestDataRecord(metadata, rows);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, record);
                pstmt.execute();
            }

            // Validate inserted data
            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next(), "Should have first row");
                assertEquals("hello", rs.getString(1).trim());
                assertEquals("world", rs.getString(2));
            }
        }

        /**
         * Test TVP with custom ISQLServerDataRecord via PreparedStatement
         * This test covers temporal data types and validates insertion via PreparedStatement.
         * 
         * @throws SQLException
         */
        @Test
        @DisplayName("DataRecord: Temporal Types via PreparedStatement")
        public void testDataRecordTemporalTypesPreparedStatement() throws SQLException {
            String tableDef = "c_date date null, c_time time null, c_datetime2 datetime2 null";
            createTable(tableName, tableDef);
            createTVPS(tableDef);

            java.sql.Date testDate = java.sql.Date.valueOf("2024-06-15");
            java.sql.Time testTime = java.sql.Time.valueOf("14:30:45");
            java.sql.Timestamp testTs = java.sql.Timestamp.valueOf("2024-06-15 14:30:45.123");

            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] {testDate, testTime, testTs});
            rows.add(new Object[] {null, null, null});

            SQLServerMetaData[] metadata = new SQLServerMetaData[] {
                    new SQLServerMetaData("c_date", Types.DATE),
                    new SQLServerMetaData("c_time", Types.TIME),
                    new SQLServerMetaData("c_datetime2", Types.TIMESTAMP)};

            ISQLServerDataRecord record = new TestDataRecord(metadata, rows);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, record);
                pstmt.execute();
            }

            // Validate inserted data
            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next(), "Should have first row");
                assertNotNull(rs.getDate(1), "Date should not be null");
            }
        }

        /**
         * Test TVP with custom ISQLServerDataRecord via PreparedStatement
         * This test covers binary data types and validates insertion via PreparedStatement.
         * 
         * @throws SQLException
         */
        @Test
        @DisplayName("DataRecord: Binary Types via PreparedStatement")
        public void testDataRecordBinaryTypesPreparedStatement() throws SQLException {
            String tableDef = "c_binary binary(10) null, c_varbinary varbinary(50) null";
            createTable(tableName, tableDef);
            createTVPS(tableDef);

            byte[] binData = new byte[] {0x01, 0x02, 0x03, 0x04, 0x05};
            byte[] varbinData = new byte[] {0x0A, 0x0B, 0x0C, 0x0D};

            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] {binData, varbinData});
            rows.add(new Object[] {null, null});

            SQLServerMetaData[] metadata = new SQLServerMetaData[] {
                    new SQLServerMetaData("c_binary", Types.BINARY, 10),
                    new SQLServerMetaData("c_varbinary", Types.VARBINARY, 50)};

            ISQLServerDataRecord record = new TestDataRecord(metadata, rows);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, record);
                pstmt.execute();
            }

            // Validate inserted data
            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next(), "Should have first row");
                byte[] resultBin = rs.getBytes(1);
                for (int i = 0; i < binData.length; i++) {
                    assertEquals(binData[i], resultBin[i], "Binary byte " + i + " should match");
                }
            }
        }

        /**
         * Test TVP with custom ISQLServerDataRecord via CallableStatement
         * This test covers all data types together and validates insertion via CallableStatement.
         * This is a comprehensive test to ensure DataRecord works end-to-end with a stored procedure.
         * Covers: int, smallint, tinyint, bigint, decimal, numeric, float, real, bit,
         * char, varchar, nchar, nvarchar, date, time, datetime2, binary, varbinary.
         * 
         * @throws SQLException
         */
        @Test
        @DisplayName("DataRecord: All Types via CallableStatement (Stored Procedure)")
        public void testDataRecordAllTypesCallableStatement() throws SQLException {
            String tableDef = "c_int int null, c_smallint smallint null, c_tinyint tinyint null, "
                    + "c_bigint bigint null, c_decimal decimal(10,2) null, c_numeric numeric(10,2) null, "
                    + "c_float float null, c_real real null, c_bit bit null, "
                    + "c_char char(20) null, c_varchar varchar(50) null, "
                    + "c_nchar nchar(30) null, c_nvarchar nvarchar(60) null, "
                    + "c_date date null, c_time time null, c_datetime2 datetime2 null, "
                    + "c_binary binary(10) null, c_varbinary varbinary(50) null";
            createTable(tableName, tableDef);
            createTVPS(tableDef);
            createProcedure(tableName);

            java.sql.Date testDate = java.sql.Date.valueOf("2024-01-15");
            java.sql.Time testTime = java.sql.Time.valueOf("14:30:45");
            java.sql.Timestamp testTs = java.sql.Timestamp.valueOf("2024-01-15 14:30:45.123");
            byte[] binData = new byte[] {0x01, 0x02, 0x03, 0x04, 0x05};
            byte[] varbinData = new byte[] {0x0A, 0x0B, 0x0C};

            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] {42, (short) 200, (short) 250, 9999999999L,
                    new BigDecimal("123.45"), new BigDecimal("99999.99"),
                    3.14159d, 2.71828f, true,
                    "charValue", "hello world", "nchar\u4E2D\u6587", "nvarchar\u00FC\u00E4",
                    testDate, testTime, testTs,
                    binData, varbinData});
            rows.add(new Object[] {-1, (short) -200, (short) 0, -9999999999L,
                    new BigDecimal("0.01"), new BigDecimal("-99999.99"),
                    -1.5d, -1.5f, false,
                    "second", "test", "nchar2", "nvarchar2",
                    null, null, null,
                    null, null});

            SQLServerMetaData[] metadata = new SQLServerMetaData[] {
                    new SQLServerMetaData("c_int", Types.INTEGER),
                    new SQLServerMetaData("c_smallint", Types.SMALLINT),
                    new SQLServerMetaData("c_tinyint", Types.TINYINT),
                    new SQLServerMetaData("c_bigint", Types.BIGINT),
                    new SQLServerMetaData("c_decimal", Types.DECIMAL, 10, 2),
                    new SQLServerMetaData("c_numeric", Types.NUMERIC, 10, 2),
                    new SQLServerMetaData("c_float", Types.FLOAT),
                    new SQLServerMetaData("c_real", Types.REAL),
                    new SQLServerMetaData("c_bit", Types.BIT),
                    new SQLServerMetaData("c_char", Types.CHAR, 20),
                    new SQLServerMetaData("c_varchar", Types.VARCHAR, 50),
                    new SQLServerMetaData("c_nchar", Types.NCHAR, 30),
                    new SQLServerMetaData("c_nvarchar", Types.NVARCHAR, 60),
                    new SQLServerMetaData("c_date", Types.DATE),
                    new SQLServerMetaData("c_time", Types.TIME),
                    new SQLServerMetaData("c_datetime2", Types.TIMESTAMP),
                    new SQLServerMetaData("c_binary", Types.BINARY, 10),
                    new SQLServerMetaData("c_varbinary", Types.VARBINARY, 50)};

            ISQLServerDataRecord record = new TestDataRecord(metadata, rows);

            final String sql = "{call " + AbstractSQLGenerator.escapeIdentifier(procedureName) + "(?)}";
            try (SQLServerCallableStatement cstmt = (SQLServerCallableStatement) connection.prepareCall(sql)) {
                cstmt.setStructured(1, tvpName, record);
                cstmt.execute();
            }

            // Validate inserted data — first row (all non-null)
            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName)
                                    + " order by c_int")) {
                assertTrue(rs.next(), "Should have first row");
                assertEquals(-1, rs.getInt(1), "int");
                assertEquals((short) -200, rs.getShort(2), "smallint");
                assertEquals((short) 0, rs.getShort(3), "tinyint");
                assertEquals(-9999999999L, rs.getLong(4), "bigint");
                assertEquals(new BigDecimal("0.01"), rs.getBigDecimal(5), "decimal");
                assertEquals(new BigDecimal("-99999.99"), rs.getBigDecimal(6), "numeric");
                assertEquals(false, rs.getBoolean(9), "bit");
                assertEquals("second", rs.getString(10).trim(), "char");
                assertEquals("test", rs.getString(11), "varchar");
                assertTrue(rs.getObject(14) == null, "date should be null in second row");
                assertTrue(rs.getObject(17) == null, "binary should be null in second row");

                assertTrue(rs.next(), "Should have second row");
                assertEquals(42, rs.getInt(1), "int");
                assertEquals((short) 200, rs.getShort(2), "smallint");
                assertEquals((short) 250, rs.getShort(3), "tinyint");
                assertEquals(9999999999L, rs.getLong(4), "bigint");
                assertEquals(new BigDecimal("123.45"), rs.getBigDecimal(5), "decimal");
                assertEquals(new BigDecimal("99999.99"), rs.getBigDecimal(6), "numeric");
                assertEquals(true, rs.getBoolean(9), "bit");
                assertEquals("charValue", rs.getString(10).trim(), "char");
                assertEquals("hello world", rs.getString(11), "varchar");
                assertEquals("nchar\u4E2D\u6587", rs.getString(12).trim(), "nchar");
                assertEquals("nvarchar\u00FC\u00E4", rs.getString(13), "nvarchar");
                assertNotNull(rs.getDate(14), "date should not be null");
                assertNotNull(rs.getTime(15), "time should not be null");
                assertNotNull(rs.getTimestamp(16), "datetime2 should not be null");
                byte[] resultBin = rs.getBytes(17);
                for (int i = 0; i < binData.length; i++) {
                    assertEquals(binData[i], resultBin[i], "binary byte " + i);
                }
                byte[] resultVarbin = rs.getBytes(18);
                for (int i = 0; i < varbinData.length; i++) {
                    assertEquals(varbinData[i], resultVarbin[i], "varbinary byte " + i);
                }
            }
        }

        /**
         * Test TVP with custom ISQLServerDataRecord via CallableStatement
         * This test validates that errors thrown during getRowData() are propagated properly.
         * Uses a DataRecord implementation that throws an exception on the second row.
         *
         * @throws SQLException
         */
        @Test
        @DisplayName("DataRecord: Error Handling During getRowData()")
        public void testDataRecordErrorHandling() throws SQLException {
            String tableDef = "c_int int null";
            createTable(tableName, tableDef);
            createTVPS(tableDef);

            SQLServerMetaData[] metadata = new SQLServerMetaData[] {
                    new SQLServerMetaData("c_int", Types.INTEGER)};

            ISQLServerDataRecord errorRecord = new ISQLServerDataRecord() {
                private int currentRow = -1;

                @Override
                public SQLServerMetaData getColumnMetaData(int column) {
                    return metadata[column - 1];
                }

                @Override
                public int getColumnCount() {
                    return 1;
                }

                @Override
                public Object[] getRowData() {
                    if (currentRow == 1) {
                        throw new RuntimeException("Simulated error in DataRecord");
                    }
                    return new Object[] {currentRow * 10};
                }

                @Override
                public boolean next() {
                    currentRow++;
                    return currentRow < 2;
                }
            };

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, errorRecord);
                pstmt.execute();
                fail("Should have thrown an exception from getRowData()");
            } catch (Exception e) {
                // We expect an exception to be thrown due to the simulated error in getRowData()
                assertNotNull(e.getMessage(), "Error should propagate with message");
            }
        }

        /**
         * Test TVP with custom ISQLServerDataRecord via CallableStatement
         * This test validates that an empty DataRecord (no rows) is handled correctly.
         *
         * @throws SQLException
         */
        @Test
        @DisplayName("DataRecord: Empty Record (No Rows)")
        public void testDataRecordEmptyRows() throws SQLException {
            String tableDef = "c_int int null";
            createTable(tableName, tableDef);
            createTVPS(tableDef);

            SQLServerMetaData[] metadata = new SQLServerMetaData[] {
                    new SQLServerMetaData("c_int", Types.INTEGER)};

            ISQLServerDataRecord emptyRecord = new TestDataRecord(metadata, new ArrayList<>());

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, emptyRecord);
                pstmt.execute();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select count(*) from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1), "Empty DataRecord should insert 0 rows");
            }
        }

        /**
         * Test TVP with custom ISQLServerDataRecord
         * This test validates that a large number of rows (1000) can be inserted successfully.
         *
         * @throws SQLException
         */
        @Test
        @DisplayName("DataRecord: Large Row Count (1000 rows)")
        public void testDataRecordLargeRowCount() throws SQLException {
            String tableDef = "c_int int null, c_varchar varchar(50) null";
            createTable(tableName, tableDef);
            createTVPS(tableDef);

            List<Object[]> rows = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                rows.add(new Object[] {i, "row_" + i});
            }

            SQLServerMetaData[] metadata = new SQLServerMetaData[] {
                    new SQLServerMetaData("c_int", Types.INTEGER),
                    new SQLServerMetaData("c_varchar", Types.VARCHAR, 50)};

            ISQLServerDataRecord record = new TestDataRecord(metadata, rows);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, record);
                pstmt.execute();
            }

            // Validate inserted data count
            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select count(*) from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next());
                assertEquals(1000, rs.getInt(1), "Should have inserted 1000 rows");
            }
        }

        /**
         * Test TVP with custom ISQLServerDataRecord via PreparedStatement
         * This test validates that a DataRecord with all null values is inserted correctly.
         *
         * @throws SQLException
         */
        @Test
        @DisplayName("DataRecord: All Null Row Data")
        public void testDataRecordAllNullData() throws SQLException {
            String tableDef = "c_int int null, c_varchar varchar(50) null, c_date date null";
            createTable(tableName, tableDef);
            createTVPS(tableDef);

            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] {null, null, null});
            rows.add(new Object[] {null, null, null});

            SQLServerMetaData[] metadata = new SQLServerMetaData[] {
                    new SQLServerMetaData("c_int", Types.INTEGER),
                    new SQLServerMetaData("c_varchar", Types.VARCHAR, 50),
                    new SQLServerMetaData("c_date", Types.DATE)};

            ISQLServerDataRecord record = new TestDataRecord(metadata, rows);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, record);
                pstmt.execute();
            }

            // Validate inserted data
            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                int rowCount = 0;
                while (rs.next()) {
                    assertTrue(rs.getObject(1) == null, "Int should be null");
                    assertTrue(rs.getObject(2) == null, "Varchar should be null");
                    assertTrue(rs.getObject(3) == null, "Date should be null");
                    rowCount++;
                }
                assertEquals(2, rowCount);
            }
        }

        /**
         * Test that a DataRecord instance cannot be reused after its iterator is consumed.
         * 
         * This is different from SQLServerDataTable, which stores rows in a Map and can be reused many times. 
         * An ISQLServerDataRecord backed by an Iterator is single-use by nature. 
         * The test validates the driver handles this gracefully (no crash, just 0 additional rows).
         * 
         * First pstmt.execute(): The driver calls record.next() repeatedly → gets row 1, row 2, then false. 
         * All rows sent. The iterator is now at the end — there's no way to go back.
         *
         * Second pstmt.execute() with the same record object: The driver calls record.next() → immediately 
         * returns false (iterator already exhausted). Driver sees 0 rows, inserts nothing.
         *
         * @throws SQLException
         */
        @Test
        @DisplayName("DataRecord: Reuse After Iterator Consumed")
        public void testDataRecordReuse() throws SQLException {
            String tableDef = "c_int int null, c_varchar varchar(50) null";
            createTable(tableName, tableDef);
            createTVPS(tableDef);

            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] {1, "first"});
            rows.add(new Object[] {2, "second"});

            SQLServerMetaData[] metadata = new SQLServerMetaData[] {
                    new SQLServerMetaData("c_int", Types.INTEGER),
                    new SQLServerMetaData("c_varchar", Types.VARCHAR, 50)};

            TestDataRecord record = new TestDataRecord(metadata, rows);

            // First execution — should insert 2 rows
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, record);
                pstmt.execute();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select count(*) from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1), "First execution should insert 2 rows");
            }

            // Second execution with same record — iterator is consumed, should insert 0 rows
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, record);
                pstmt.execute();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select count(*) from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1), "Second execution should still have only 2 rows (iterator consumed)");
            }
        }

        /**
         * Test TVP DataRecord via setObject(int, Object, int) with microsoft.sql.Types.STRUCTURED
         * instead of setStructured(). This validates the alternative API path for setting TVP parameters.
         * Note: setObject with DataRecord requires a CallableStatement with stored procedure so the
         * driver can derive the TVP name from the procedure's parameter metadata.
         *
         * @throws SQLException
         */
        @Test
        @DisplayName("DataRecord: Via setObject() with STRUCTURED Type")
        public void testDataRecordViaSetObject() throws SQLException {
            String tableDef = "c_int int null, c_varchar varchar(50) null";
            createTable(tableName, tableDef);
            createTVPS(tableDef);
            createProcedure(tableName);

            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] {10, "setObject_test"});
            rows.add(new Object[] {20, "structured_api"});

            SQLServerMetaData[] metadata = new SQLServerMetaData[] {
                    new SQLServerMetaData("c_int", Types.INTEGER),
                    new SQLServerMetaData("c_varchar", Types.VARCHAR, 50)};

            ISQLServerDataRecord record = new TestDataRecord(metadata, rows);

            final String sql = "{call " + AbstractSQLGenerator.escapeIdentifier(procedureName) + "(?)}";
            try (SQLServerCallableStatement cstmt = (SQLServerCallableStatement) connection.prepareCall(sql)) {
                cstmt.setObject(1, record, microsoft.sql.Types.STRUCTURED);
                cstmt.execute();
            }

            // Validate inserted data
            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName)
                                    + " order by c_int")) {
                assertTrue(rs.next(), "Should have first row");
                assertEquals(10, rs.getInt(1));
                assertEquals("setObject_test", rs.getString(2));
                assertTrue(rs.next(), "Should have second row");
                assertEquals(20, rs.getInt(1));
                assertEquals("structured_api", rs.getString(2));
            }
        }

        /**
         * Test that a DataRecord with more columns than the TVP type expects produces an error.
         * The TVP type has 1 column but the DataRecord provides 2 columns.
         *
         * @throws SQLException
         */
        @Test
        @DisplayName("DataRecord: Column Count Mismatch (More Columns)")
        public void testDataRecordColumnCountMismatch() throws SQLException {
            String tableDef = "c_int int null";
            createTable(tableName, tableDef);
            createTVPS(tableDef);

            // DataRecord declares 2 columns but TVP type only has 1
            SQLServerMetaData[] metadata = new SQLServerMetaData[] {
                    new SQLServerMetaData("c_int", Types.INTEGER),
                    new SQLServerMetaData("c_extra", Types.VARCHAR, 50)};

            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] {1, "extra_column"});

            ISQLServerDataRecord record = new TestDataRecord(metadata, rows);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, record);
                Exception thrown = assertThrows(SQLException.class, () -> pstmt.execute(),
                        "Should throw when DataRecord has more columns than TVP type");
                assertNotNull(thrown.getMessage(), "Exception should have a message");
            }
        }

        /**
         * Test that a DataRecord where getRowData() returns a type mismatched value
         * (e.g., String value for INTEGER metadata) is handled by the driver.
         * The driver may coerce or throw an error depending on the type pair.
         *
         * @throws SQLException
         */
        @Test
        @DisplayName("DataRecord: Type Mismatch (String for INTEGER)")
        public void testDataRecordTypeMismatch() throws SQLException {
            String tableDef = "c_int int null";
            createTable(tableName, tableDef);
            createTVPS(tableDef);

            SQLServerMetaData[] metadata = new SQLServerMetaData[] {
                    new SQLServerMetaData("c_int", Types.INTEGER)};

            // Provide a non-numeric String where INTEGER is expected
            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] {"not_a_number"});

            ISQLServerDataRecord record = new TestDataRecord(metadata, rows);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, record);
                assertThrows(Exception.class, () -> pstmt.execute(),
                        "Should throw when String 'not_a_number' is sent for INTEGER column");
            }
        }

        /**
         * Test that an exception thrown in next() is properly propagated.
         * Unlike the existing getRowData() error test, this validates the iteration path
         * where next() itself fails mid-stream.
         *
         * @throws SQLException
         */
        @Test
        @DisplayName("DataRecord: Exception in next() During Iteration")
        public void testDataRecordNextThrowsException() throws SQLException {
            String tableDef = "c_int int null";
            createTable(tableName, tableDef);
            createTVPS(tableDef);

            SQLServerMetaData[] metadata = new SQLServerMetaData[] {
                    new SQLServerMetaData("c_int", Types.INTEGER)};

            ISQLServerDataRecord failingRecord = new ISQLServerDataRecord() {
                private int callCount = 0;

                @Override
                public SQLServerMetaData getColumnMetaData(int column) {
                    return metadata[column - 1];
                }

                @Override
                public int getColumnCount() {
                    return 1;
                }

                @Override
                public Object[] getRowData() {
                    return new Object[] {callCount};
                }

                @Override
                public boolean next() {
                    callCount++;
                    if (callCount == 2) {
                        throw new RuntimeException("Simulated failure in next()");
                    }
                    return callCount <= 3;
                }
            };

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, failingRecord);
                pstmt.execute();
                fail("Should have thrown an exception from next()");
            } catch (Exception e) {
                assertNotNull(e.getMessage(), "Exception from next() should propagate with message");
            }
        }

        /**
         * Test TVP DataRecord with LOB types (LONGVARCHAR / LONGNVARCHAR) which map to
         * varchar(max) / nvarchar(max) on SQL Server. These types are tested via DataTable
         * in TVPTypesTest but not via ISQLServerDataRecord.
         *
         * @throws SQLException
         */
        @Test
        @DisplayName("DataRecord: LOB Types (LONGVARCHAR, LONGNVARCHAR)")
        public void testDataRecordLobTypes() throws SQLException {
            String tableDef = "c_longvarchar varchar(max) null, c_longnvarchar nvarchar(max) null";
            createTable(tableName, tableDef);
            createTVPS(tableDef);

            // Build a string > 4000 chars to exercise max-length path
            StringBuilder largeValue = new StringBuilder();
            for (int i = 0; i < 500; i++) {
                largeValue.append("longtext_");
            }
            String longStr = largeValue.toString();
            String unicodeStr = "Unicode\u4E2D\u6587\u00FC\u00E4_test_nvarchar_max";

            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] {longStr, unicodeStr});
            rows.add(new Object[] {null, null});

            SQLServerMetaData[] metadata = new SQLServerMetaData[] {
                    new SQLServerMetaData("c_longvarchar", Types.LONGVARCHAR, Integer.MAX_VALUE),
                    new SQLServerMetaData("c_longnvarchar", Types.LONGNVARCHAR, Integer.MAX_VALUE)};

            ISQLServerDataRecord record = new TestDataRecord(metadata, rows);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, record);
                pstmt.execute();
            }

            // Validate inserted data
            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName)
                                    + " order by c_longvarchar")) {
                // First row is null (nulls sort first)
                assertTrue(rs.next(), "Should have first (null) row");
                assertTrue(rs.getObject(1) == null, "longvarchar should be null");
                assertTrue(rs.getObject(2) == null, "longnvarchar should be null");

                // Second row has data
                assertTrue(rs.next(), "Should have second row");
                assertEquals(longStr, rs.getString(1), "LONGVARCHAR round-trip should match");
                assertEquals(unicodeStr, rs.getString(2), "LONGNVARCHAR round-trip should match");
            }
        }

        /**
         * Test TVP DataRecord with a schema-qualified TVP type name (e.g. dbo.tvpName).
         * TVPSchemaTest covers this for DataTable; this validates the same path for DataRecord.
         *
         * @throws SQLException
         */
        @Test
        @DisplayName("DataRecord: Schema-Qualified TVP Name")
        public void testDataRecordSchemaQualifiedTVP() throws SQLException {
            String columnsDef = "c_int int null, c_varchar varchar(50) null";
            createTable(tableName, columnsDef);

            // Create TVP type and use schema-qualified name
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTypeIfExists(tvpName, stmt);
                stmt.executeUpdate("CREATE TYPE " + AbstractSQLGenerator.escapeIdentifier(tvpName)
                        + " AS TABLE (" + columnsDef + ")");
            }

            // Use schema-qualified name: dbo.<tvpName>
            String schemaQualifiedTvpName = "dbo." + AbstractSQLGenerator.escapeIdentifier(tvpName);

            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] {1, "schema_test"});

            SQLServerMetaData[] metadata = new SQLServerMetaData[] {
                    new SQLServerMetaData("c_int", Types.INTEGER),
                    new SQLServerMetaData("c_varchar", Types.VARCHAR, 50)};

            ISQLServerDataRecord record = new TestDataRecord(metadata, rows);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, schemaQualifiedTvpName, record);
                pstmt.execute();
            }

            // Validate inserted data
            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next(), "Should have first row");
                assertEquals(1, rs.getInt(1));
                assertEquals("schema_test", rs.getString(2));
            }
        }
    }

    // ================================================================
    // Nested Class: ParameterMetaData Tests (from TVPParameterMetaDataTest.java)
    // ================================================================

    /**
     * Tests TVP ParameterMetaData validation ported from FX ParameterMetaData.java.
     * Validates getParameterMetaData() returns expected values for TVP parameters:
     * - parameterCount = 1, parameterType = -153 (TVP), parameterTypeName = TVP name,
     *   parameterClassName = "java.lang.Object", parameterMode = 1 (IN), precision = 2147483647.
     * Tests across 3 TVP sources: DataTable, ResultSet, DataRecord.
     */
    @Nested
    @DisplayName("ParameterMetaData")
    class ParameterMetaData {

        /**
         * Verifies ParameterMetaData values match FX expectations.
         * FX: ParameterMetaData.java verify() method
         */
        private void verifyParameterMetaData(java.sql.ParameterMetaData pmd) throws SQLException {
            assertEquals(1, pmd.getParameterCount(), "Parameter count should be 1");
            assertEquals(-153, pmd.getParameterType(1), "Parameter type should be -153 (TVP)");
            assertTrue(pmd.getParameterTypeName(1).equalsIgnoreCase(tvpName),
                    "Parameter type name should be TVP name. Expected: " + tvpName + ", Got: "
                            + pmd.getParameterTypeName(1));
            assertEquals("java.lang.Object", pmd.getParameterClassName(1),
                    "Parameter class name should be java.lang.Object");
            assertEquals(1, pmd.getParameterMode(1), "Parameter mode should be 1 (IN)");
            assertEquals(2147483647, pmd.getPrecision(1), "Parameter precision should be 2147483647");
        }

        /**
         * Test ParameterMetaData for a TVP parameter set with a SQLServerDataTable via CallableStatement.
         * This validates that getParameterMetaData() returns correct metadata for a 
         * TVP parameter when using a DataTable as the source.
         *
         * @throws SQLException
         */
        @Test
        @DisplayName("ParameterMetaData: DataTable via CallableStatement")
        public void testParameterMetaDataWithDataTable() throws SQLException {
            String columnsDef = "c_int int null";
            createTable(tableName, columnsDef);
            createTable(destTableName, columnsDef);
            createTVPS(columnsDef);
            createProcedure(destTableName);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c_int", java.sql.Types.INTEGER);
            tvp.addRow(1);

            final String sql = "{call " + AbstractSQLGenerator.escapeIdentifier(procedureName) + "(?)}";
            try (SQLServerCallableStatement cs = (SQLServerCallableStatement) connection.prepareCall(sql)) {
                cs.setStructured(1, tvpName, tvp);
                cs.executeUpdate();

                java.sql.ParameterMetaData pmd = cs.getParameterMetaData();
                // Verify ParameterMetaData values match expectations
                verifyParameterMetaData(pmd);
            }
        }

        /**
         * Test ParameterMetaData for a TVP parameter set with a ResultSet via CallableStatement.
         * This validates that getParameterMetaData() returns correct metadata for a 
         * TVP parameter when using a ResultSet as the source.
         *
         * @throws SQLException
         */
        @Test
        @DisplayName("ParameterMetaData: ResultSet via CallableStatement")
        public void testParameterMetaDataWithResultSet() throws SQLException {
            String columnsDef = "c_int int null";
            createTable(tableName, columnsDef);
            createTable(destTableName, columnsDef);
            createTVPS(columnsDef);
            createProcedure(destTableName);

            try (Statement stmt = connection.createStatement()) {
                stmt.executeUpdate(
                        "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " VALUES (1)");
            }

            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {

                final String sql = "{call " + AbstractSQLGenerator.escapeIdentifier(procedureName) + "(?)}";
                try (SQLServerCallableStatement cs = (SQLServerCallableStatement) connection.prepareCall(sql)) {
                    cs.setStructured(1, tvpName, rs);
                    cs.executeUpdate();

                    java.sql.ParameterMetaData pmd = cs.getParameterMetaData();
                    // Verify ParameterMetaData values match expectations
                    verifyParameterMetaData(pmd);
                }
            }
        }

        /**
         * Test ParameterMetaData for a TVP parameter set with a custom ISQLServerDataRecord via CallableStatement.
         * This validates that getParameterMetaData() returns correct metadata for a 
         * TVP parameter when using a custom DataRecord as the source.
         *
         * @throws SQLException
         */
        @Test
        @DisplayName("ParameterMetaData: DataRecord via CallableStatement")
        public void testParameterMetaDataWithDataRecord() throws SQLException {
            String columnsDef = "c_int int null";
            createTable(tableName, columnsDef);
            createTable(destTableName, columnsDef);
            createTVPS(columnsDef);
            createProcedure(destTableName);

            SQLServerMetaData[] metadata = new SQLServerMetaData[] {
                    new SQLServerMetaData("c_int", Types.INTEGER)};
            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] {1});

            ISQLServerDataRecord record = new TestDataRecord(metadata, rows);

            final String sql = "{call " + AbstractSQLGenerator.escapeIdentifier(procedureName) + "(?)}";
            try (SQLServerCallableStatement cs = (SQLServerCallableStatement) connection.prepareCall(sql)) {
                cs.setStructured(1, tvpName, record);
                cs.executeUpdate();

                java.sql.ParameterMetaData pmd = cs.getParameterMetaData();
                // Verify ParameterMetaData values match expectations
                verifyParameterMetaData(pmd);
            }
        }

        /**
         * Test ParameterMetaData for a TVP parameter set with multiple columns.
         * This validates that getParameterMetaData() returns correct metadata for a 
         * TVP parameter with multiple columns, ensuring it still reports as a single parameter of type TVP.
         *
         * @throws SQLException
         */
        @Test
        @DisplayName("ParameterMetaData: Multi-Column TVP")
        public void testParameterMetaDataMultiColumn() throws SQLException {
            String columnsDef = "c_int int null, c_varchar varchar(50) null, c_bit bit null";
            createTable(tableName, columnsDef);
            createTable(destTableName, columnsDef);
            createTVPS(columnsDef);
            createProcedure(destTableName);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c_int", java.sql.Types.INTEGER);
            tvp.addColumnMetadata("c_varchar", java.sql.Types.VARCHAR);
            tvp.addColumnMetadata("c_bit", java.sql.Types.BIT);
            tvp.addRow(1, "test", true);

            final String sql = "{call " + AbstractSQLGenerator.escapeIdentifier(procedureName) + "(?)}";
            try (SQLServerCallableStatement cs = (SQLServerCallableStatement) connection.prepareCall(sql)) {
                cs.setStructured(1, tvpName, tvp);
                cs.executeUpdate();

                java.sql.ParameterMetaData pmd = cs.getParameterMetaData();
                assertEquals(1, pmd.getParameterCount(), "TVP should be a single parameter");
                assertEquals(-153, pmd.getParameterType(1), "TVP parameter type should be -153");
            }
        }

        /**
         * Test ParameterMetaData for a stored procedure accepting two TVP parameters.
         * Verifies that parameterCount = 2 and both parameters report as TVP type (-153).
         *
         * @throws SQLException
         */
        @Test
        @DisplayName("ParameterMetaData: Multiple TVP Parameters")
        public void testParameterMetaDataMultipleTVPs() throws SQLException {
            String columnsDef = "c_int int null";
            String tvpName2 = RandomUtil.getIdentifier("TVP2");
            String destTableName2 = RandomUtil.getIdentifier("TVPDRDest2");

            createTable(tableName, columnsDef);
            createTable(destTableName, columnsDef);
            createTable(destTableName2, columnsDef);
            createTVPS(columnsDef);

            // Create second TVP type
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTypeIfExists(tvpName2, stmt);
                stmt.executeUpdate("CREATE TYPE " + AbstractSQLGenerator.escapeIdentifier(tvpName2)
                        + " AS TABLE (" + columnsDef + ")");
            }

            // Create stored procedure with 2 TVP parameters
            String multiTvpProc = RandomUtil.getIdentifier("spMultiTVP");
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropProcedureIfExists(multiTvpProc, stmt);
                String sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(multiTvpProc)
                        + " @Input1 " + AbstractSQLGenerator.escapeIdentifier(tvpName) + " READONLY,"
                        + " @Input2 " + AbstractSQLGenerator.escapeIdentifier(tvpName2) + " READONLY"
                        + " AS BEGIN"
                        + " INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(destTableName) + " SELECT * FROM @Input1"
                        + " INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(destTableName2) + " SELECT * FROM @Input2"
                        + " END";
                stmt.execute(sql);
            }

            try {
                SQLServerDataTable tvp1 = new SQLServerDataTable();
                tvp1.addColumnMetadata("c_int", java.sql.Types.INTEGER);
                tvp1.addRow(100);

                SQLServerDataTable tvp2 = new SQLServerDataTable();
                tvp2.addColumnMetadata("c_int", java.sql.Types.INTEGER);
                tvp2.addRow(200);

                final String sql = "{call " + AbstractSQLGenerator.escapeIdentifier(multiTvpProc) + "(?, ?)}";
                try (SQLServerCallableStatement cs = (SQLServerCallableStatement) connection.prepareCall(sql)) {
                    cs.setStructured(1, tvpName, tvp1);
                    cs.setStructured(2, tvpName2, tvp2);
                    cs.executeUpdate();

                    java.sql.ParameterMetaData pmd = cs.getParameterMetaData();
                    assertEquals(2, pmd.getParameterCount(), "Should have 2 TVP parameters");
                    assertEquals(-153, pmd.getParameterType(1), "First parameter should be TVP type");
                    assertEquals(-153, pmd.getParameterType(2), "Second parameter should be TVP type");
                }

                // Verify data was inserted into both destination tables
                try (Statement stmt = connection.createStatement();
                        ResultSet rs = stmt.executeQuery(
                                "select c_int from " + AbstractSQLGenerator.escapeIdentifier(destTableName))) {
                    assertTrue(rs.next(), "destTable should have a row");
                    assertEquals(100, rs.getInt(1));
                }
                try (Statement stmt = connection.createStatement();
                        ResultSet rs = stmt.executeQuery(
                                "select c_int from " + AbstractSQLGenerator.escapeIdentifier(destTableName2))) {
                    assertTrue(rs.next(), "destTable2 should have a row");
                    assertEquals(200, rs.getInt(1));
                }
            } finally {
                // Cleanup extra objects
                try (Statement stmt = connection.createStatement()) {
                    TestUtils.dropProcedureIfExists(multiTvpProc, stmt);
                    TestUtils.dropTableIfExists(destTableName2, stmt);
                    TestUtils.dropTypeIfExists(tvpName2, stmt);
                }
            }
        }
    }
}
