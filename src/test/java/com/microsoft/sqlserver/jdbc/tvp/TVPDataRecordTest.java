/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.tvp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
 * Merged from FX test suites: TVPSQLServerDataRecord.java, TVPCS_DataRecord.java, CSVRecord.java,
 * ParameterMetaData.java.
 * Uses nested classes to group related test categories:
 * - DataRecordTypes: ISQLServerDataRecord usage with PreparedStatement and CallableStatement
 * - ParameterMetaData: getParameterMetaData() verification across TVP sources
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDW)
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
     * Replaces the FX CSVRecord implementation (which reads from CSV files).
     */
    static class TestDataRecord implements ISQLServerDataRecord {
        private final SQLServerMetaData[] metadata;
        private final List<Object[]> rows;
        private final Iterator<Object[]> iterator;
        private Object[] currentRow;

        TestDataRecord(SQLServerMetaData[] metadata, List<Object[]> rows) {
            this.metadata = metadata;
            this.rows = rows;
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
     * Ported from FX TVPSQLServerDataRecord.java, TVPCS_DataRecord.java, CSVRecord.java.
     */
    @Nested
    @DisplayName("DataRecordTypes")
    class DataRecordTypes {

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

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next(), "Should have first row");
                assertEquals("hello", rs.getString(1).trim());
                assertEquals("world", rs.getString(2));
            }
        }

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

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next(), "Should have first row");
                assertNotNull(rs.getDate(1), "Date should not be null");
            }
        }

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

        @Test
        @DisplayName("DataRecord: All Types via CallableStatement (Stored Procedure)")
        public void testDataRecordAllTypesCallableStatement() throws SQLException {
            String tableDef = "c_int int null, c_varchar varchar(50) null, c_decimal decimal(10,2) null, "
                    + "c_bit bit null, c_date date null";
            createTable(tableName, tableDef);
            createTVPS(tableDef);
            createProcedure(tableName);

            java.sql.Date testDate = java.sql.Date.valueOf("2024-01-15");

            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[] {42, "hello world", new BigDecimal("123.45"), true, testDate});
            rows.add(new Object[] {-1, "test", new BigDecimal("0.01"), false, null});

            SQLServerMetaData[] metadata = new SQLServerMetaData[] {
                    new SQLServerMetaData("c_int", Types.INTEGER),
                    new SQLServerMetaData("c_varchar", Types.VARCHAR, 50),
                    new SQLServerMetaData("c_decimal", Types.DECIMAL, 10, 2),
                    new SQLServerMetaData("c_bit", Types.BIT),
                    new SQLServerMetaData("c_date", Types.DATE)};

            ISQLServerDataRecord record = new TestDataRecord(metadata, rows);

            final String sql = "{call " + AbstractSQLGenerator.escapeIdentifier(procedureName) + "(?)}";
            try (SQLServerCallableStatement cstmt = (SQLServerCallableStatement) connection.prepareCall(sql)) {
                cstmt.setStructured(1, tvpName, record);
                cstmt.execute();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next(), "Should have first row");
                assertEquals(42, rs.getInt(1));
                assertEquals("hello world", rs.getString(2));
                assertTrue(rs.next(), "Should have second row");
                assertEquals(-1, rs.getInt(1));
            }
        }

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
            } catch (Exception e) {
                assertNotNull(e.getMessage(), "Error should propagate with message");
            }
        }

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

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select count(*) from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next());
                assertEquals(1000, rs.getInt(1), "Should have inserted 1000 rows");
            }
        }

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
                verifyParameterMetaData(pmd);
            }
        }

        @Test
        @DisplayName("ParameterMetaData: DataTable via PreparedStatement")
        public void testParameterMetaDataWithDataTablePreparedStatement() throws SQLException {
            String columnsDef = "c_int int null";
            createTable(tableName, columnsDef);
            createTVPS(columnsDef);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c_int", java.sql.Types.INTEGER);
            tvp.addRow(1);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.executeUpdate();

                java.sql.ParameterMetaData pmd = pstmt.getParameterMetaData();
                verifyParameterMetaData(pmd);
            }
        }

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
                    verifyParameterMetaData(pmd);
                }
            }
        }

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
                verifyParameterMetaData(pmd);
            }
        }

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
    }
}
