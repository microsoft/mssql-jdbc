package com.microsoft.sqlserver.jdbc.datatypes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerResultSetMetaData;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
public class SparseTest extends AbstractTest {
    final static String tableName = RandomUtil.getIdentifier("SparseTestTable");
    final static String escapedTableName = AbstractSQLGenerator.escapeIdentifier(tableName);

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testSparse() throws Exception {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {

            // Create the test table
            TestUtils.dropTableIfExists(escapedTableName, stmt);

            StringBuilder bd = new StringBuilder();
            bd.append("create table " + escapedTableName + " (col1 int, col2 varbinary(max)");
            for (int i = 3; i <= 1024; i++) {
                bd.append(", col" + i + " varchar(20) SPARSE NULL");
            }
            bd.append(")");
            String query = bd.toString();

            stmt.executeUpdate(query);

            stmt.executeUpdate("insert into " + escapedTableName + " (col1, col2, col1023)values(1, 0x45, 'yo')");

            try (ResultSet rs = stmt.executeQuery("Select * from   " + escapedTableName)) {
                rs.next();
                assertEquals("yo", rs.getString("col1023"));
                assertEquals(1, rs.getInt("col1"));
                assertEquals(0x45, rs.getBytes("col2")[0]);
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(escapedTableName, stmt);
            }
        }
    }

    @Nested
    @Tag(Constants.legacyFx)
    @Tag(Constants.legacyFxDataTypes)
    @Tag(Constants.xAzureSQLDW)
    public class SparseCreationTests {

        private final String creationTable = RandomUtil.getIdentifier("SparseCreationTest");
        private final String creationTableName = AbstractSQLGenerator.escapeIdentifier(creationTable);

        @AfterEach
        public void cleanup() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(creationTableName, stmt);
            }
        }

        /** Creates a table with sparse and non-sparse columns (no column set) and validates metadata. */
        @Test
        public void testCreationNoColumnSet() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(creationTableName, stmt);
                stmt.executeUpdate("CREATE TABLE " + creationTableName
                        + " (identCol int IDENTITY(1,1) PRIMARY KEY, "
                        + "col1 int NULL, "
                        + "col2 varchar(50) SPARSE NULL, "
                        + "col3 smallint SPARSE NULL, "
                        + "col4 bigint NULL)");

                DatabaseMetaData dbmd = conn.getMetaData();
                try (ResultSet rs = dbmd.getColumns(null, null, creationTable, null)) {
                    int colCount = 0;
                    while (rs.next()) {
                        colCount++;
                    }
                    assertEquals(5, colCount, "Expected 5 columns in table");
                }
            }
        }

        /** Creates a table with sparse columns, non-sparse columns, and a column set. */
        @Test
        public void testCreationWithColumnSet() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(creationTableName, stmt);
                stmt.executeUpdate("CREATE TABLE " + creationTableName
                        + " (identCol int IDENTITY(1,1) PRIMARY KEY, "
                        + "col1 int NULL, "
                        + "col2 varchar(50) SPARSE NULL, "
                        + "col3 smallint SPARSE NULL, "
                        + "colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS, "
                        + "col4 bigint NULL)");

                DatabaseMetaData dbmd = conn.getMetaData();
                try (ResultSet rs = dbmd.getColumns(null, null, creationTable, null)) {
                    int colCount = 0;
                    while (rs.next()) {
                        colCount++;
                    }
                    assertEquals(6, colCount, "Expected 6 columns in table (including column set)");
                }
            }
        }

        /** Creates a table with only sparse columns (no non-sparse, no column set). */
        @Test
        public void testCreationSparseOnlyNoColumnSet() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(creationTableName, stmt);
                stmt.executeUpdate("CREATE TABLE " + creationTableName
                        + " (identCol int IDENTITY(1,1) PRIMARY KEY, "
                        + "col1 int SPARSE NULL, "
                        + "col2 varchar(50) SPARSE NULL, "
                        + "col3 smallint SPARSE NULL)");

                DatabaseMetaData dbmd = conn.getMetaData();
                try (ResultSet rs = dbmd.getColumns(null, null, creationTable, null)) {
                    int colCount = 0;
                    while (rs.next()) {
                        colCount++;
                    }
                    assertEquals(4, colCount, "Expected 4 columns in table");
                }
            }
        }

        /** Creates a table with only sparse columns and a column set (no non-sparse columns). */
        @Test
        public void testCreationSparseOnlyWithColumnSet() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(creationTableName, stmt);
                stmt.executeUpdate("CREATE TABLE " + creationTableName
                        + " (identCol int IDENTITY(1,1) PRIMARY KEY, "
                        + "col1 int SPARSE NULL, "
                        + "col2 varchar(50) SPARSE NULL, "
                        + "colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS)");

                DatabaseMetaData dbmd = conn.getMetaData();
                try (ResultSet rs = dbmd.getColumns(null, null, creationTable, null)) {
                    int colCount = 0;
                    while (rs.next()) {
                        colCount++;
                    }
                    assertEquals(4, colCount, "Expected 4 columns in table");
                }
            }
        }

        /** Creates a wide table (>1024 sparse columns) with column set and a non-sparse column. */
        @Test
        public void testCreationWideTable() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(creationTableName, stmt);

                StringBuilder sb = new StringBuilder();
                sb.append("CREATE TABLE ").append(creationTableName)
                        .append(" (identCol int IDENTITY(1,1) PRIMARY KEY, ")
                        .append("colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS, ")
                        .append("nonSparseCol varchar(50) NULL");

                for (int i = 0; i < 1100; i++) {
                    sb.append(", col").append(i).append(" int SPARSE NULL");
                }
                sb.append(")");
                stmt.executeUpdate(sb.toString());

                DatabaseMetaData dbmd = conn.getMetaData();
                try (ResultSet rs = dbmd.getColumns(null, null, creationTable, null)) {
                    int colCount = 0;
                    while (rs.next()) {
                        colCount++;
                    }
                    // identCol + colSetCol + nonSparseCol + 1100 sparse = 1103
                    assertEquals(1103, colCount, "Expected 1103 columns in wide table");
                }
            }
        }

        /** Creates a wide table (>1024 sparse columns) with column set but no non-sparse columns. */
        @Test
        public void testCreationWideTableSparseOnly() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(creationTableName, stmt);

                StringBuilder sb = new StringBuilder();
                sb.append("CREATE TABLE ").append(creationTableName)
                        .append(" (identCol int IDENTITY(1,1) PRIMARY KEY, ")
                        .append("colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS");

                for (int i = 0; i < 1100; i++) {
                    sb.append(", col").append(i).append(" int SPARSE NULL");
                }
                sb.append(")");
                stmt.executeUpdate(sb.toString());

                DatabaseMetaData dbmd = conn.getMetaData();
                try (ResultSet rs = dbmd.getColumns(null, null, creationTable, null)) {
                    int colCount = 0;
                    while (rs.next()) {
                        colCount++;
                    }
                    // identCol + colSetCol + 1100 sparse = 1102
                    assertEquals(1102, colCount, "Expected 1102 columns in wide sparse-only table");
                }
            }
        }

        /** Validates error code 1731 when invalid sparse type (ntext) is used. */
        @Test
        public void testIllegalSparseType() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(creationTableName, stmt);
                try {
                    stmt.executeUpdate("CREATE TABLE " + creationTableName
                            + " (identCol int IDENTITY(1,1) PRIMARY KEY, "
                            + "col1 ntext SPARSE NULL)");
                    fail("Expected SQLException for illegal sparse type (ntext)");
                } catch (SQLException e) {
                    assertEquals(1731, e.getErrorCode(),
                            "Expected error code 1731 for illegal sparse type, got: " + e.getErrorCode());
                }
            }
        }

        /** Validates error code 1702 when creating >1024 columns without a column set. */
        @Test
        public void testTooManyColumnsWithoutColumnSet() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(creationTableName, stmt);

                StringBuilder sb = new StringBuilder();
                sb.append("CREATE TABLE ").append(creationTableName)
                        .append(" (identCol int IDENTITY(1,1) PRIMARY KEY");

                for (int i = 0; i < 1024; i++) {
                    sb.append(", col").append(i).append(" int SPARSE NULL");
                }
                sb.append(")");

                try {
                    stmt.executeUpdate(sb.toString());
                    fail("Expected SQLException for too many columns without column set");
                } catch (SQLException e) {
                    assertEquals(1702, e.getErrorCode(),
                            "Expected error code 1702 for too many columns, got: " + e.getErrorCode());
                }
            }
        }

        /** Validates error code 1702 when creating >30000 columns. */
        @Test
        public void testMaxColumnsExceeded() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(creationTableName, stmt);

                StringBuilder sb = new StringBuilder();
                sb.append("CREATE TABLE ").append(creationTableName)
                        .append(" (identCol int IDENTITY(1,1) PRIMARY KEY, ")
                        .append("colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS");

                for (int i = 0; i < 30000; i++) {
                    sb.append(", col").append(i).append(" int SPARSE NULL");
                }
                sb.append(")");

                try {
                    stmt.executeUpdate(sb.toString());
                    fail("Expected SQLException for exceeding 30000 column maximum");
                } catch (SQLException e) {
                    assertEquals(1702, e.getErrorCode(),
                            "Expected error code 1702 for too many columns, got: " + e.getErrorCode());
                }
            }
        }
    }

    @Nested
    @Tag(Constants.legacyFx)
    @Tag(Constants.legacyFxDataTypes)
    @Tag(Constants.xAzureSQLDW)
    public class SparseInsertionTests {

        private final String insertionTableName = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("SparseInsertionTest"));

        @AfterEach
        public void cleanup() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(insertionTableName, stmt);
            }
        }

        /** Inserts by column name into a table with sparse and non-sparse columns, no column set. */
        @Test
        public void testNamedColumnInsertionNoColumnSet() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(insertionTableName, stmt);
                stmt.executeUpdate("CREATE TABLE " + insertionTableName
                        + " (identCol int IDENTITY(1,1) PRIMARY KEY, "
                        + "col1 int NULL, "
                        + "col2 varchar(50) SPARSE NULL, "
                        + "col3 smallint SPARSE NULL, "
                        + "col4 bigint NULL)");

                stmt.executeUpdate("INSERT INTO " + insertionTableName
                        + " (col1, col2, col3, col4) VALUES (100, 'hello', 10, 999)");

                try (ResultSet rs = stmt.executeQuery("SELECT col1, col2, col3, col4 FROM " + insertionTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    assertEquals(100, rs.getInt("col1"));
                    assertEquals("hello", rs.getString("col2"));
                    assertEquals(10, rs.getShort("col3"));
                    assertEquals(999, rs.getLong("col4"));
                }
            }
        }

        /** Inserts by column name into a table with sparse, non-sparse, and a column set. */
        @Test
        public void testNamedColumnInsertionWithColumnSet() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(insertionTableName, stmt);
                stmt.executeUpdate("CREATE TABLE " + insertionTableName
                        + " (identCol int IDENTITY(1,1) PRIMARY KEY, "
                        + "col1 int NULL, "
                        + "col2 varchar(50) SPARSE NULL, "
                        + "col3 smallint SPARSE NULL, "
                        + "colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS, "
                        + "col4 bigint NULL)");

                stmt.executeUpdate("INSERT INTO " + insertionTableName
                        + " (col1, col2, col3, col4) VALUES (200, 'world', 20, 888)");

                try (ResultSet rs = stmt.executeQuery(
                        "SELECT col1, col2, col3, col4 FROM " + insertionTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    assertEquals(200, rs.getInt("col1"));
                    assertEquals("world", rs.getString("col2"));
                    assertEquals(20, rs.getShort("col3"));
                    assertEquals(888, rs.getLong("col4"));
                }
            }
        }

        /** Inserts by column name into a sparse-only table without a column set. */
        @Test
        public void testNamedColumnInsertionSparseOnlyNoColumnSet() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(insertionTableName, stmt);
                stmt.executeUpdate("CREATE TABLE " + insertionTableName
                        + " (identCol int IDENTITY(1,1) PRIMARY KEY, "
                        + "col1 int SPARSE NULL, "
                        + "col2 varchar(50) SPARSE NULL, "
                        + "col3 smallint SPARSE NULL)");

                stmt.executeUpdate("INSERT INTO " + insertionTableName
                        + " (col1, col2, col3) VALUES (300, 'sparse', 30)");

                try (ResultSet rs = stmt.executeQuery("SELECT col1, col2, col3 FROM " + insertionTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    assertEquals(300, rs.getInt("col1"));
                    assertEquals("sparse", rs.getString("col2"));
                    assertEquals(30, rs.getShort("col3"));
                }
            }
        }

        /** Inserts by column name into a sparse-only table with a column set. */
        @Test
        public void testNamedColumnInsertionSparseOnlyWithColumnSet() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(insertionTableName, stmt);
                stmt.executeUpdate("CREATE TABLE " + insertionTableName
                        + " (identCol int IDENTITY(1,1) PRIMARY KEY, "
                        + "col1 int SPARSE NULL, "
                        + "col2 varchar(50) SPARSE NULL, "
                        + "colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS)");

                stmt.executeUpdate("INSERT INTO " + insertionTableName
                        + " (col1, col2) VALUES (400, 'sparseWithSet')");

                try (ResultSet rs = stmt.executeQuery("SELECT col1, col2 FROM " + insertionTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    assertEquals(400, rs.getInt("col1"));
                    assertEquals("sparseWithSet", rs.getString("col2"));
                }
            }
        }

        /** Inserts by column name into a wide table (>1024 sparse columns) with a non-sparse column. */
        @Test
        public void testNamedColumnInsertionWideTable() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(insertionTableName, stmt);

                StringBuilder sb = new StringBuilder();
                sb.append("CREATE TABLE ").append(insertionTableName)
                        .append(" (identCol int IDENTITY(1,1) PRIMARY KEY, ")
                        .append("colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS, ")
                        .append("nonSparseCol varchar(50) NULL");
                for (int i = 0; i < 1100; i++) {
                    sb.append(", col").append(i).append(" int SPARSE NULL");
                }
                sb.append(")");
                stmt.executeUpdate(sb.toString());

                stmt.executeUpdate("INSERT INTO " + insertionTableName
                        + " (nonSparseCol, col0, col500, col1099) VALUES ('test', 42, 84, 168)");

                try (ResultSet rs = stmt.executeQuery(
                        "SELECT nonSparseCol, col0, col500, col1099 FROM " + insertionTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    assertEquals("test", rs.getString("nonSparseCol"));
                    assertEquals(42, rs.getInt("col0"));
                    assertEquals(84, rs.getInt("col500"));
                    assertEquals(168, rs.getInt("col1099"));
                }
            }
        }

        /** Inserts by column name into a wide sparse-only table (>1024 sparse columns, no non-sparse). */
        @Test
        public void testNamedColumnInsertionWideTableSparseOnly() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(insertionTableName, stmt);

                StringBuilder sb = new StringBuilder();
                sb.append("CREATE TABLE ").append(insertionTableName)
                        .append(" (identCol int IDENTITY(1,1) PRIMARY KEY, ")
                        .append("colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS");
                for (int i = 0; i < 1100; i++) {
                    sb.append(", col").append(i).append(" int SPARSE NULL");
                }
                sb.append(")");
                stmt.executeUpdate(sb.toString());

                stmt.executeUpdate("INSERT INTO " + insertionTableName
                        + " (col0, col500, col1099) VALUES (42, 84, 168)");

                try (ResultSet rs = stmt.executeQuery(
                        "SELECT col0, col500, col1099 FROM " + insertionTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    assertEquals(42, rs.getInt("col0"));
                    assertEquals(84, rs.getInt("col500"));
                    assertEquals(168, rs.getInt("col1099"));
                }
            }
        }

        /** Inserts sparse column values via column set XML. */
        @Test
        public void testInsertWithColumnSetXml() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(insertionTableName, stmt);
                stmt.executeUpdate("CREATE TABLE " + insertionTableName
                        + " (identCol int IDENTITY(1,1) PRIMARY KEY, "
                        + "col1 int NULL, "
                        + "col2 int SPARSE NULL, "
                        + "col3 varchar(50) SPARSE NULL, "
                        + "colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS, "
                        + "col4 bigint NULL)");

                stmt.executeUpdate("INSERT INTO " + insertionTableName
                        + " (col1, colSetCol, col4) VALUES (100, '<col2>42</col2><col3>via_colset</col3>', 999)");

                try (ResultSet rs = stmt.executeQuery(
                        "SELECT col1, col2, col3, col4 FROM " + insertionTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    assertEquals(100, rs.getInt("col1"));
                    assertEquals(42, rs.getInt("col2"));
                    assertEquals("via_colset", rs.getString("col3"));
                    assertEquals(999, rs.getLong("col4"));
                }
            }
        }

        /** Inserts sparse values via column set XML into a sparse-only table. */
        @Test
        public void testInsertWithColumnSetXmlSparseOnly() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(insertionTableName, stmt);
                stmt.executeUpdate("CREATE TABLE " + insertionTableName
                        + " (identCol int IDENTITY(1,1) PRIMARY KEY, "
                        + "col1 int SPARSE NULL, "
                        + "col2 varchar(50) SPARSE NULL, "
                        + "colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS)");

                stmt.executeUpdate("INSERT INTO " + insertionTableName
                        + " (colSetCol) VALUES ('<col1>77</col1><col2>sparseXml</col2>')");

                try (ResultSet rs = stmt.executeQuery("SELECT col1, col2 FROM " + insertionTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    assertEquals(77, rs.getInt("col1"));
                    assertEquals("sparseXml", rs.getString("col2"));
                }
            }
        }

        /** Inserts sparse values via column set XML into a wide table with a non-sparse column. */
        @Test
        public void testInsertWithColumnSetXmlWideTable() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(insertionTableName, stmt);

                StringBuilder sb = new StringBuilder();
                sb.append("CREATE TABLE ").append(insertionTableName)
                        .append(" (identCol int IDENTITY(1,1) PRIMARY KEY, ")
                        .append("colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS, ")
                        .append("nonSparseCol varchar(50) NULL");
                for (int i = 0; i < 1100; i++) {
                    sb.append(", col").append(i).append(" int SPARSE NULL");
                }
                sb.append(")");
                stmt.executeUpdate(sb.toString());

                stmt.executeUpdate("INSERT INTO " + insertionTableName
                        + " (nonSparseCol, colSetCol) VALUES ('wide', '<col0>11</col0><col500>22</col500>')");

                try (ResultSet rs = stmt.executeQuery(
                        "SELECT nonSparseCol, col0, col500 FROM " + insertionTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    assertEquals("wide", rs.getString("nonSparseCol"));
                    assertEquals(11, rs.getInt("col0"));
                    assertEquals(22, rs.getInt("col500"));
                }
            }
        }

        /** Inserts sparse values via column set XML into a wide sparse-only table. */
        @Test
        public void testInsertWithColumnSetXmlWideTableSparseOnly() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(insertionTableName, stmt);

                StringBuilder sb = new StringBuilder();
                sb.append("CREATE TABLE ").append(insertionTableName)
                        .append(" (identCol int IDENTITY(1,1) PRIMARY KEY, ")
                        .append("colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS");
                for (int i = 0; i < 1100; i++) {
                    sb.append(", col").append(i).append(" int SPARSE NULL");
                }
                sb.append(")");
                stmt.executeUpdate(sb.toString());

                stmt.executeUpdate("INSERT INTO " + insertionTableName
                        + " (colSetCol) VALUES ('<col0>55</col0><col999>66</col999>')");

                try (ResultSet rs = stmt.executeQuery(
                        "SELECT col0, col999 FROM " + insertionTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    assertEquals(55, rs.getInt("col0"));
                    assertEquals(66, rs.getInt("col999"));
                }
            }
        }

        /** Inserts into a table with >1024 tinyint sparse columns via column set XML. */
        @Test
        public void testInsertMoreThan1024Columns() throws Exception {
            final int NUM_SPARSE_COLS = 1044;
            final int POPULATED_COLS = 50;

            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(insertionTableName, stmt);

                StringBuilder createSb = new StringBuilder();
                createSb.append("CREATE TABLE ").append(insertionTableName)
                        .append(" (identCol int IDENTITY(1,1) PRIMARY KEY, ")
                        .append("colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS");
                for (int i = 0; i < NUM_SPARSE_COLS; i++) {
                    createSb.append(", col").append(i).append(" tinyint SPARSE NULL");
                }
                createSb.append(")");
                stmt.executeUpdate(createSb.toString());

                StringBuilder xmlSb = new StringBuilder();
                for (int i = 0; i < POPULATED_COLS; i++) {
                    int colIdx = i * (NUM_SPARSE_COLS / POPULATED_COLS);
                    xmlSb.append("<col").append(colIdx).append(">").append((colIdx % 255) + 1)
                            .append("</col").append(colIdx).append(">");
                }
                stmt.executeUpdate("INSERT INTO " + insertionTableName
                        + " (colSetCol) VALUES ('" + xmlSb.toString() + "')");

                int step = NUM_SPARSE_COLS / POPULATED_COLS;
                try (ResultSet rs = stmt.executeQuery(
                        "SELECT col0, col" + step + ", col" + (2 * step) + " FROM " + insertionTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    assertEquals((0 % 255) + 1, rs.getInt("col0"));
                    assertEquals((step % 255) + 1, rs.getInt("col" + step));
                    assertEquals(((2 * step) % 255) + 1, rs.getInt("col" + (2 * step)));
                    assertFalse(rs.next(), "Expected only one row");
                }
            }
        }

        /** Validates error code 360 when inserting with both a named sparse column and column set. */
        @Test
        public void testNamedSparseAndColumnSetInsertError() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(insertionTableName, stmt);
                stmt.executeUpdate("CREATE TABLE " + insertionTableName
                        + " (identCol int IDENTITY(1,1) PRIMARY KEY, "
                        + "colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS, "
                        + "sparseCol int SPARSE NULL)");

                try {
                    stmt.executeUpdate("INSERT INTO " + insertionTableName
                            + " (sparseCol, colSetCol) VALUES (42, '<sparseCol>42</sparseCol>')");
                    fail("Expected SQLException when naming sparse column and column set in same INSERT");
                } catch (SQLException e) {
                    assertEquals(360, e.getErrorCode(),
                            "Expected error code 360, got: " + e.getErrorCode());
                }
            }
        }
    }

    @Nested
    @Tag(Constants.legacyFx)
    @Tag(Constants.legacyFxDataTypes)
    @Tag(Constants.xAzureSQLDW)
    public class SparseSelectTests {

        private final String selectTableName = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("SparseSelectTest"));

        @AfterEach
        public void cleanup() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(selectTableName, stmt);
            }
        }

        /** Selects from an empty table with sparse and non-sparse columns (no column set). */
        @Test
        public void testSelectAfterCreationSparseAndNonSparse() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(selectTableName, stmt);
                stmt.executeUpdate("CREATE TABLE " + selectTableName
                        + " (identCol int IDENTITY(1,1) PRIMARY KEY, "
                        + "col1 int NULL, "
                        + "col2 varchar(50) SPARSE NULL, "
                        + "col3 smallint SPARSE NULL, "
                        + "col4 bigint NULL)");

                try (ResultSet rs = stmt.executeQuery(
                        "SELECT identCol, col1, col2, col3, col4 FROM " + selectTableName)) {
                    assertFalse(rs.next(), "Expected no rows in empty table");
                    ResultSetMetaData rsmd = rs.getMetaData();
                    assertEquals(5, rsmd.getColumnCount(), "Expected 5 columns");
                }

                try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + selectTableName)) {
                    assertFalse(rs.next(), "Expected no rows");
                }
            }
        }

        /** SELECT * on a table with column set returns non-sparse + column set (not individual sparse cols). */
        @Test
        public void testSelectAfterCreationWithColumnSet() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(selectTableName, stmt);
                stmt.executeUpdate("CREATE TABLE " + selectTableName
                        + " (identCol int IDENTITY(1,1) PRIMARY KEY, "
                        + "col1 int NULL, "
                        + "col2 varchar(50) SPARSE NULL, "
                        + "col3 smallint SPARSE NULL, "
                        + "colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS, "
                        + "col4 bigint NULL)");

                try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + selectTableName)) {
                    assertFalse(rs.next(), "Expected no rows");
                    ResultSetMetaData rsmd = rs.getMetaData();
                    // identCol, col1, colSetCol, col4 = 4 columns
                    assertEquals(4, rsmd.getColumnCount(),
                            "SELECT * with column set should return non-sparse + colset columns");
                }

                try (ResultSet rs = stmt.executeQuery(
                        "SELECT identCol, col1, col2, col3, col4 FROM " + selectTableName)) {
                    assertFalse(rs.next(), "Expected no rows");
                    ResultSetMetaData rsmd = rs.getMetaData();
                    assertEquals(5, rsmd.getColumnCount(), "Expected 5 columns in named select");
                }
            }
        }

        /** Selects from an empty sparse-only table with a column set. */
        @Test
        public void testSelectAfterCreationSparseOnlyWithColumnSet() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(selectTableName, stmt);
                stmt.executeUpdate("CREATE TABLE " + selectTableName
                        + " (identCol int IDENTITY(1,1) PRIMARY KEY, "
                        + "col1 int SPARSE NULL, "
                        + "col2 varchar(50) SPARSE NULL, "
                        + "colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS)");

                try (ResultSet rs = stmt.executeQuery(
                        "SELECT identCol, col1, col2 FROM " + selectTableName)) {
                    assertFalse(rs.next(), "Expected no rows in empty table");
                    ResultSetMetaData rsmd = rs.getMetaData();
                    assertEquals(3, rsmd.getColumnCount(), "Expected 3 columns in named select");
                }

                try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + selectTableName)) {
                    assertFalse(rs.next(), "Expected no rows");
                    ResultSetMetaData rsmd = rs.getMetaData();
                    // identCol + colSetCol = 2 columns (no non-sparse)
                    assertEquals(2, rsmd.getColumnCount(),
                            "SELECT * on sparse-only with colset should return identCol + colset");
                }
            }
        }

        /** Selects from an empty wide table (>1024 sparse columns) with a non-sparse column. */
        @Test
        public void testSelectAfterCreationWideTable() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(selectTableName, stmt);

                StringBuilder sb = new StringBuilder();
                sb.append("CREATE TABLE ").append(selectTableName)
                        .append(" (identCol int IDENTITY(1,1) PRIMARY KEY, ")
                        .append("colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS, ")
                        .append("nonSparseCol varchar(50) NULL");
                for (int i = 0; i < 1100; i++) {
                    sb.append(", col").append(i).append(" int SPARSE NULL");
                }
                sb.append(")");
                stmt.executeUpdate(sb.toString());

                try (ResultSet rs = stmt.executeQuery(
                        "SELECT identCol, nonSparseCol, col0, col500 FROM " + selectTableName)) {
                    assertFalse(rs.next(), "Expected no rows in empty table");
                    ResultSetMetaData rsmd = rs.getMetaData();
                    assertEquals(4, rsmd.getColumnCount());
                }

                try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + selectTableName)) {
                    assertFalse(rs.next(), "Expected no rows");
                    ResultSetMetaData rsmd = rs.getMetaData();
                    // identCol, colSetCol, nonSparseCol = 3 columns
                    assertEquals(3, rsmd.getColumnCount(),
                            "SELECT * on wide table should return only non-sparse + colset");
                }
            }
        }

        /** Selects data after insertion into a table with sparse and non-sparse columns, no column set. */
        @Test
        public void testSelectAfterInsertionNoColumnSet() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(selectTableName, stmt);
                stmt.executeUpdate("CREATE TABLE " + selectTableName
                        + " (identCol int IDENTITY(1,1) PRIMARY KEY, "
                        + "col1 int NULL, "
                        + "col2 varchar(50) SPARSE NULL, "
                        + "col3 smallint SPARSE NULL, "
                        + "col4 bigint NULL)");

                stmt.executeUpdate("INSERT INTO " + selectTableName
                        + " (col1, col2, col3, col4) VALUES (10, 'test', 5, 1000)");

                try (ResultSet rs = stmt.executeQuery(
                        "SELECT col1, col2, col3, col4 FROM " + selectTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    assertEquals(10, rs.getInt("col1"));
                    assertEquals("test", rs.getString("col2"));
                    assertEquals(5, rs.getShort("col3"));
                    assertEquals(1000, rs.getLong("col4"));
                }

                try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + selectTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    ResultSetMetaData rsmd = rs.getMetaData();
                    assertEquals(5, rsmd.getColumnCount());
                }
            }
        }

        /** Selects data after insertion and validates column set XML content in SELECT *. */
        @Test
        public void testSelectAfterInsertionWithColumnSet() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(selectTableName, stmt);
                stmt.executeUpdate("CREATE TABLE " + selectTableName
                        + " (identCol int IDENTITY(1,1) PRIMARY KEY, "
                        + "col1 int NULL, "
                        + "col2 int SPARSE NULL, "
                        + "col3 varchar(50) SPARSE NULL, "
                        + "colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS, "
                        + "col4 bigint NULL)");

                stmt.executeUpdate("INSERT INTO " + selectTableName
                        + " (col1, col2, col3, col4) VALUES (20, 55, 'hello', 2000)");

                try (ResultSet rs = stmt.executeQuery(
                        "SELECT col1, col2, col3, col4 FROM " + selectTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    assertEquals(20, rs.getInt("col1"));
                    assertEquals(55, rs.getInt("col2"));
                    assertEquals("hello", rs.getString("col3"));
                    assertEquals(2000, rs.getLong("col4"));
                }

                try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + selectTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    ResultSetMetaData rsmd = rs.getMetaData();
                    // identCol, col1, colSetCol, col4 = 4 columns
                    assertEquals(4, rsmd.getColumnCount(),
                            "SELECT * with colset should show non-sparse + colset");

                    String colSetValue = rs.getString("colSetCol");
                    assertNotNull(colSetValue, "Column set value should not be null");
                    assertTrue(colSetValue.contains("col2"), "Column set XML should contain col2");
                    assertTrue(colSetValue.contains("col3"), "Column set XML should contain col3");
                }
            }
        }

        /** Selects data after insertion into a sparse-only table with a column set. */
        @Test
        public void testSelectAfterInsertionSparseOnlyWithColumnSet() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(selectTableName, stmt);
                stmt.executeUpdate("CREATE TABLE " + selectTableName
                        + " (identCol int IDENTITY(1,1) PRIMARY KEY, "
                        + "col1 int SPARSE NULL, "
                        + "col2 varchar(50) SPARSE NULL, "
                        + "colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS)");

                stmt.executeUpdate("INSERT INTO " + selectTableName
                        + " (col1, col2) VALUES (33, 'sparse_only')");

                try (ResultSet rs = stmt.executeQuery(
                        "SELECT col1, col2 FROM " + selectTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    assertEquals(33, rs.getInt("col1"));
                    assertEquals("sparse_only", rs.getString("col2"));
                }

                try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + selectTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    ResultSetMetaData rsmd = rs.getMetaData();
                    // identCol + colSetCol = 2 columns (no non-sparse)
                    assertEquals(2, rsmd.getColumnCount(),
                            "SELECT * on sparse-only with colset should return identCol + colset");

                    String colSetValue = rs.getString("colSetCol");
                    assertNotNull(colSetValue, "Column set value should not be null");
                    assertTrue(colSetValue.contains("col1"), "Column set XML should contain col1");
                    assertTrue(colSetValue.contains("col2"), "Column set XML should contain col2");
                }
            }
        }

        /** Selects data after insertion into a wide table (>1024 sparse columns). */
        @Test
        public void testSelectAfterInsertionWideTable() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(selectTableName, stmt);

                StringBuilder sb = new StringBuilder();
                sb.append("CREATE TABLE ").append(selectTableName)
                        .append(" (identCol int IDENTITY(1,1) PRIMARY KEY, ")
                        .append("colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS, ")
                        .append("nonSparseCol varchar(50) NULL");
                for (int i = 0; i < 1100; i++) {
                    sb.append(", col").append(i).append(" int SPARSE NULL");
                }
                sb.append(")");
                stmt.executeUpdate(sb.toString());

                stmt.executeUpdate("INSERT INTO " + selectTableName
                        + " (nonSparseCol, col0, col999) VALUES ('wideTest', 111, 222)");

                try (ResultSet rs = stmt.executeQuery(
                        "SELECT nonSparseCol, col0, col999 FROM " + selectTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    assertEquals("wideTest", rs.getString("nonSparseCol"));
                    assertEquals(111, rs.getInt("col0"));
                    assertEquals(222, rs.getInt("col999"));
                }

                try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + selectTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    ResultSetMetaData rsmd = rs.getMetaData();
                    // identCol, colSetCol, nonSparseCol = 3 columns
                    assertEquals(3, rsmd.getColumnCount());
                }
            }
        }

        /** Validates error code 1056 when selecting > 4096 columns. */
        @Test
        public void testTooManyColumnsInSelect() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(selectTableName, stmt);

                StringBuilder sb = new StringBuilder();
                sb.append("CREATE TABLE ").append(selectTableName)
                        .append(" (identCol int IDENTITY(1,1) PRIMARY KEY, ")
                        .append("colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS");
                int numCols = 200;
                for (int i = 0; i < numCols; i++) {
                    sb.append(", col").append(i).append(" int SPARSE NULL");
                }
                sb.append(")");
                stmt.executeUpdate(sb.toString());

                StringBuilder selectSb = new StringBuilder();
                selectSb.append("SELECT ");
                int colRefs = 0;
                while (colRefs < 4096) {
                    for (int i = 0; i < numCols && colRefs < 4100; i++) {
                        if (colRefs > 0) {
                            selectSb.append(", ");
                        }
                        selectSb.append("col").append(i);
                        colRefs++;
                    }
                }
                selectSb.append(" FROM ").append(selectTableName);

                try {
                    stmt.executeQuery(selectSb.toString());
                    fail("Expected SQLException for too many columns in select");
                } catch (SQLException e) {
                    assertEquals(1056, e.getErrorCode(),
                            "Expected error code 1056 for too many columns in select, got: " + e.getErrorCode());
                }
            }
        }

        /** Validates isSparseColumnSet throws for invalid index values (out of bounds, 0, negative). */
        @Test
        public void testIsSparseColumnSetBoundaryErrors() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(selectTableName, stmt);
                stmt.executeUpdate("CREATE TABLE " + selectTableName
                        + " (identCol int IDENTITY(1,1) PRIMARY KEY, "
                        + "col1 int SPARSE NULL, "
                        + "col2 varchar(50) SPARSE NULL, "
                        + "colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS)");

                stmt.executeUpdate("INSERT INTO " + selectTableName
                        + " (col1, col2) VALUES (1, 'test')");

                try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + selectTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    SQLServerResultSetMetaData ssrsmd = (SQLServerResultSetMetaData) rs.getMetaData();
                    int colCount = ssrsmd.getColumnCount();

                    assertThrows(ArrayIndexOutOfBoundsException.class,
                            () -> ssrsmd.isSparseColumnSet(colCount + 1),
                            "Expected exception for index > column count");

                    assertThrows(ArrayIndexOutOfBoundsException.class,
                            () -> ssrsmd.isSparseColumnSet(0),
                            "Expected exception for index 0");

                    assertThrows(ArrayIndexOutOfBoundsException.class,
                            () -> ssrsmd.isSparseColumnSet(-1),
                            "Expected exception for negative index");
                }
            }
        }

        /** Validates isSparseColumnSet behavior with selectMethod=cursor. */
        @Test
        public void testIsSparseColumnSetSelectMethodCursor() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(selectTableName, stmt);
                stmt.executeUpdate("CREATE TABLE " + selectTableName
                        + " (identCol int IDENTITY(1,1) PRIMARY KEY, "
                        + "col1 int NULL, "
                        + "col2 int SPARSE NULL, "
                        + "col3 varchar(50) SPARSE NULL, "
                        + "colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS)");

                stmt.executeUpdate("INSERT INTO " + selectTableName
                        + " (col1, col2, col3) VALUES (1, 2, 'cursor_test')");
            }

            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setURL(connectionString);
            ds.setSelectMethod("cursor");

            try (Connection cursorConn = ds.getConnection();
                    Statement cursorStmt = cursorConn.createStatement()) {

                try (ResultSet rs = cursorStmt.executeQuery(
                        "SELECT identCol, col1, col2, col3, colSetCol FROM " + selectTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    SQLServerResultSetMetaData ssrsmd = (SQLServerResultSetMetaData) rs.getMetaData();

                    for (int i = 1; i <= ssrsmd.getColumnCount(); i++) {
                        boolean isSparseColSet = ssrsmd.isSparseColumnSet(i);
                        if (i == 5) {
                            assertTrue(isSparseColSet,
                                    "isSparseColumnSet should return true for column set column");
                        } else {
                            assertFalse(isSparseColSet,
                                    "isSparseColumnSet should return false for non-column-set column " + i);
                        }
                    }
                }

                try (ResultSet rs = cursorStmt.executeQuery("SELECT * FROM " + selectTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    ResultSetMetaData rsmd = rs.getMetaData();
                    // identCol, col1, colSetCol = 3 columns
                    assertEquals(3, rsmd.getColumnCount(),
                            "SELECT * with cursor should show non-sparse + colset columns");
                }
            }
        }
    }

    @Nested
    @Tag(Constants.legacyFx)
    @Tag(Constants.legacyFxDataTypes)
    @Tag(Constants.xAzureSQLDW)
    public class SparseMetaDataTests {

        private final String metadataTableName = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("SparseMetaDataTest"));

        @AfterEach
        public void cleanup() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(metadataTableName, stmt);
            }
        }

        /**
         * Verifies ParameterMetaData for sparse columns without a column set.
         * Sparse columns must not report parameterNoNulls (modern servers may return
         * parameterNullableUnknown instead of parameterNullable).
         */
        @Test
        public void testParameterMetaDataWithoutColumnSet() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(metadataTableName, stmt);
                stmt.executeUpdate("CREATE TABLE " + metadataTableName
                        + " (identCol int IDENTITY(1,1) PRIMARY KEY, "
                        + "col1 int NULL, "
                        + "col2 varchar(50) SPARSE NULL, "
                        + "col3 smallint SPARSE NULL, "
                        + "col4 bigint NULL)");

                String insertSql = "INSERT INTO " + metadataTableName
                        + " (col1, col2, col3, col4) VALUES (?, ?, ?, ?)";
                try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                    ParameterMetaData pmd = pstmt.getParameterMetaData();
                    assertNotNull(pmd, "ParameterMetaData should not be null");
                    assertEquals(4, pmd.getParameterCount(), "Expected 4 parameters");

                    assertTrue(pmd.isNullable(2) != ParameterMetaData.parameterNoNulls,
                            "Sparse column col2 should not report parameterNoNulls");
                    assertTrue(pmd.isNullable(3) != ParameterMetaData.parameterNoNulls,
                            "Sparse column col3 should not report parameterNoNulls");
                }
            }
        }

        /** Verifies ParameterMetaData types for sparse columns with a column set. */
        @Test
        public void testParameterMetaDataWithColumnSet() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(metadataTableName, stmt);
                stmt.executeUpdate("CREATE TABLE " + metadataTableName
                        + " (identCol int IDENTITY(1,1) PRIMARY KEY, "
                        + "col1 int NULL, "
                        + "col2 varchar(50) SPARSE NULL, "
                        + "col3 smallint SPARSE NULL, "
                        + "colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS, "
                        + "col4 bigint NULL)");

                String insertSql = "INSERT INTO " + metadataTableName
                        + " (col1, col2, col3, col4) VALUES (?, ?, ?, ?)";
                try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                    ParameterMetaData pmd = pstmt.getParameterMetaData();
                    assertNotNull(pmd, "ParameterMetaData should not be null");
                    assertEquals(4, pmd.getParameterCount(), "Expected 4 parameters");

                    assertEquals(java.sql.Types.INTEGER, pmd.getParameterType(1),
                            "col1 should be INTEGER type");
                    assertEquals(java.sql.Types.BIGINT, pmd.getParameterType(4),
                            "col4 should be BIGINT type");
                }
            }
        }

        /**
         * Verifies ParameterMetaData on a wide table (>1024 sparse columns).
         * Sparse columns must not report parameterNoNulls.
         */
        @Test
        public void testParameterMetaDataWideTable() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(metadataTableName, stmt);

                StringBuilder sb = new StringBuilder();
                sb.append("CREATE TABLE ").append(metadataTableName)
                        .append(" (identCol int IDENTITY(1,1) PRIMARY KEY, ")
                        .append("colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS, ")
                        .append("nonSparseCol varchar(50) NULL");
                for (int i = 0; i < 1100; i++) {
                    sb.append(", col").append(i).append(" int SPARSE NULL");
                }
                sb.append(")");
                stmt.executeUpdate(sb.toString());

                String insertSql = "INSERT INTO " + metadataTableName
                        + " (nonSparseCol, col0, col500) VALUES (?, ?, ?)";
                try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                    ParameterMetaData pmd = pstmt.getParameterMetaData();
                    assertNotNull(pmd, "ParameterMetaData should not be null");
                    assertEquals(3, pmd.getParameterCount(), "Expected 3 parameters");

                    assertTrue(pmd.isNullable(2) != ParameterMetaData.parameterNoNulls,
                            "Sparse column col0 should not report parameterNoNulls");
                    assertTrue(pmd.isNullable(3) != ParameterMetaData.parameterNoNulls,
                            "Sparse column col500 should not report parameterNoNulls");
                }
            }
        }
    }

    @Nested
    @Tag(Constants.legacyFx)
    @Tag(Constants.legacyFxDataTypes)
    @Tag(Constants.xAzureSQLDW)
    public class SparseUpdateTests {

        private final String updateTableName = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("SparseUpdateTest"));

        @AfterEach
        public void cleanup() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(updateTableName, stmt);
            }
        }

        /** Updates sparse and non-sparse columns by name in a table without a column set. */
        @Test
        public void testNamedUpdateNoColumnSet() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(updateTableName, stmt);
                stmt.executeUpdate("CREATE TABLE " + updateTableName
                        + " (identCol int IDENTITY(1,1) PRIMARY KEY, "
                        + "col1 int NULL, "
                        + "col2 varchar(50) SPARSE NULL, "
                        + "col3 smallint SPARSE NULL, "
                        + "col4 bigint NULL)");

                stmt.executeUpdate("INSERT INTO " + updateTableName
                        + " (col1, col2, col3, col4) VALUES (10, 'old', 5, 100)");

                stmt.executeUpdate("UPDATE " + updateTableName
                        + " SET col1 = 20, col2 = 'new', col3 = 15, col4 = 200 WHERE identCol = 1");

                try (ResultSet rs = stmt.executeQuery(
                        "SELECT col1, col2, col3, col4 FROM " + updateTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    assertEquals(20, rs.getInt("col1"));
                    assertEquals("new", rs.getString("col2"));
                    assertEquals(15, rs.getShort("col3"));
                    assertEquals(200, rs.getLong("col4"));
                }
            }
        }

        /** Updates sparse and non-sparse columns by name in a table with a column set. */
        @Test
        public void testNamedUpdateWithColumnSet() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(updateTableName, stmt);
                stmt.executeUpdate("CREATE TABLE " + updateTableName
                        + " (identCol int IDENTITY(1,1) PRIMARY KEY, "
                        + "col1 int NULL, "
                        + "col2 int SPARSE NULL, "
                        + "col3 varchar(50) SPARSE NULL, "
                        + "colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS, "
                        + "col4 bigint NULL)");

                stmt.executeUpdate("INSERT INTO " + updateTableName
                        + " (col1, col2, col3, col4) VALUES (10, 5, 'old', 100)");

                stmt.executeUpdate("UPDATE " + updateTableName
                        + " SET col1 = 20, col2 = 15, col3 = 'updated', col4 = 200 WHERE identCol = 1");

                try (ResultSet rs = stmt.executeQuery(
                        "SELECT col1, col2, col3, col4 FROM " + updateTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    assertEquals(20, rs.getInt("col1"));
                    assertEquals(15, rs.getInt("col2"));
                    assertEquals("updated", rs.getString("col3"));
                    assertEquals(200, rs.getLong("col4"));
                }
            }
        }

        /** Updates columns by name in a sparse-only table without a column set. */
        @Test
        public void testNamedUpdateSparseOnlyNoColumnSet() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(updateTableName, stmt);
                stmt.executeUpdate("CREATE TABLE " + updateTableName
                        + " (identCol int IDENTITY(1,1) PRIMARY KEY, "
                        + "col1 int SPARSE NULL, "
                        + "col2 varchar(50) SPARSE NULL)");

                stmt.executeUpdate("INSERT INTO " + updateTableName
                        + " (col1, col2) VALUES (10, 'old')");

                stmt.executeUpdate("UPDATE " + updateTableName
                        + " SET col1 = 30, col2 = 'new_sparse' WHERE identCol = 1");

                try (ResultSet rs = stmt.executeQuery("SELECT col1, col2 FROM " + updateTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    assertEquals(30, rs.getInt("col1"));
                    assertEquals("new_sparse", rs.getString("col2"));
                }
            }
        }

        /** Updates columns by name in a sparse-only table with a column set. */
        @Test
        public void testNamedUpdateSparseOnlyWithColumnSet() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(updateTableName, stmt);
                stmt.executeUpdate("CREATE TABLE " + updateTableName
                        + " (identCol int IDENTITY(1,1) PRIMARY KEY, "
                        + "col1 int SPARSE NULL, "
                        + "col2 varchar(50) SPARSE NULL, "
                        + "colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS)");

                stmt.executeUpdate("INSERT INTO " + updateTableName
                        + " (col1, col2) VALUES (10, 'old')");

                stmt.executeUpdate("UPDATE " + updateTableName
                        + " SET col1 = 40, col2 = 'updated_sparse' WHERE identCol = 1");

                try (ResultSet rs = stmt.executeQuery("SELECT col1, col2 FROM " + updateTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    assertEquals(40, rs.getInt("col1"));
                    assertEquals("updated_sparse", rs.getString("col2"));
                }
            }
        }

        /** Updates columns by name in a wide table (>1024 sparse columns) with a non-sparse column. */
        @Test
        public void testNamedUpdateWideTable() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(updateTableName, stmt);

                StringBuilder sb = new StringBuilder();
                sb.append("CREATE TABLE ").append(updateTableName)
                        .append(" (identCol int IDENTITY(1,1) PRIMARY KEY, ")
                        .append("colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS, ")
                        .append("nonSparseCol varchar(50) NULL");
                for (int i = 0; i < 1100; i++) {
                    sb.append(", col").append(i).append(" int SPARSE NULL");
                }
                sb.append(")");
                stmt.executeUpdate(sb.toString());

                stmt.executeUpdate("INSERT INTO " + updateTableName
                        + " (nonSparseCol, col0, col500) VALUES ('original', 1, 2)");

                stmt.executeUpdate("UPDATE " + updateTableName
                        + " SET nonSparseCol = 'updated', col0 = 10, col500 = 20 WHERE identCol = 1");

                try (ResultSet rs = stmt.executeQuery(
                        "SELECT nonSparseCol, col0, col500 FROM " + updateTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    assertEquals("updated", rs.getString("nonSparseCol"));
                    assertEquals(10, rs.getInt("col0"));
                    assertEquals(20, rs.getInt("col500"));
                }
            }
        }

        /** Updates columns by name in a wide sparse-only table (>1024 sparse columns, no non-sparse). */
        @Test
        public void testNamedUpdateWideTableSparseOnly() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(updateTableName, stmt);

                StringBuilder sb = new StringBuilder();
                sb.append("CREATE TABLE ").append(updateTableName)
                        .append(" (identCol int IDENTITY(1,1) PRIMARY KEY, ")
                        .append("colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS");
                for (int i = 0; i < 1100; i++) {
                    sb.append(", col").append(i).append(" int SPARSE NULL");
                }
                sb.append(")");
                stmt.executeUpdate(sb.toString());

                stmt.executeUpdate("INSERT INTO " + updateTableName
                        + " (col0, col500) VALUES (1, 2)");

                stmt.executeUpdate("UPDATE " + updateTableName
                        + " SET col0 = 10, col500 = 20 WHERE identCol = 1");

                try (ResultSet rs = stmt.executeQuery(
                        "SELECT col0, col500 FROM " + updateTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    assertEquals(10, rs.getInt("col0"));
                    assertEquals(20, rs.getInt("col500"));
                }
            }
        }

        /** Updates sparse columns via column set XML. */
        @Test
        public void testUpdateWithColumnSetXml() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(updateTableName, stmt);
                stmt.executeUpdate("CREATE TABLE " + updateTableName
                        + " (identCol int IDENTITY(1,1) PRIMARY KEY, "
                        + "col1 int NULL, "
                        + "col2 int SPARSE NULL, "
                        + "col3 varchar(50) SPARSE NULL, "
                        + "colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS, "
                        + "col4 bigint NULL)");

                stmt.executeUpdate("INSERT INTO " + updateTableName
                        + " (col1, col2, col3, col4) VALUES (10, 5, 'old', 100)");

                stmt.executeUpdate("UPDATE " + updateTableName
                        + " SET col1 = 20, colSetCol = '<col2>55</col2><col3>via_colset_update</col3>', col4 = 200"
                        + " WHERE identCol = 1");

                try (ResultSet rs = stmt.executeQuery(
                        "SELECT col1, col2, col3, col4 FROM " + updateTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    assertEquals(20, rs.getInt("col1"));
                    assertEquals(55, rs.getInt("col2"));
                    assertEquals("via_colset_update", rs.getString("col3"));
                    assertEquals(200, rs.getLong("col4"));
                }
            }
        }

        /** Updates sparse columns via column set XML in a sparse-only table. */
        @Test
        public void testUpdateWithColumnSetXmlSparseOnly() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(updateTableName, stmt);
                stmt.executeUpdate("CREATE TABLE " + updateTableName
                        + " (identCol int IDENTITY(1,1) PRIMARY KEY, "
                        + "col1 int SPARSE NULL, "
                        + "col2 varchar(50) SPARSE NULL, "
                        + "colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS)");

                stmt.executeUpdate("INSERT INTO " + updateTableName
                        + " (col1, col2) VALUES (10, 'old')");

                stmt.executeUpdate("UPDATE " + updateTableName
                        + " SET colSetCol = '<col1>99</col1><col2>updated_xml</col2>' WHERE identCol = 1");

                try (ResultSet rs = stmt.executeQuery("SELECT col1, col2 FROM " + updateTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    assertEquals(99, rs.getInt("col1"));
                    assertEquals("updated_xml", rs.getString("col2"));
                }
            }
        }

        /** Updates sparse columns via column set XML in a wide table with a non-sparse column. */
        @Test
        public void testUpdateWithColumnSetXmlWideTable() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(updateTableName, stmt);

                StringBuilder sb = new StringBuilder();
                sb.append("CREATE TABLE ").append(updateTableName)
                        .append(" (identCol int IDENTITY(1,1) PRIMARY KEY, ")
                        .append("colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS, ")
                        .append("nonSparseCol varchar(50) NULL");
                for (int i = 0; i < 1100; i++) {
                    sb.append(", col").append(i).append(" int SPARSE NULL");
                }
                sb.append(")");
                stmt.executeUpdate(sb.toString());

                stmt.executeUpdate("INSERT INTO " + updateTableName
                        + " (nonSparseCol, colSetCol) VALUES ('original', '<col0>1</col0><col500>2</col500>')");

                stmt.executeUpdate("UPDATE " + updateTableName
                        + " SET nonSparseCol = 'updated', colSetCol = '<col0>111</col0><col500>222</col500>'"
                        + " WHERE identCol = 1");

                try (ResultSet rs = stmt.executeQuery(
                        "SELECT nonSparseCol, col0, col500 FROM " + updateTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    assertEquals("updated", rs.getString("nonSparseCol"));
                    assertEquals(111, rs.getInt("col0"));
                    assertEquals(222, rs.getInt("col500"));
                }
            }
        }

        /** Updates sparse columns via column set XML in a wide sparse-only table. */
        @Test
        public void testUpdateWithColumnSetXmlWideTableSparseOnly() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(updateTableName, stmt);

                StringBuilder sb = new StringBuilder();
                sb.append("CREATE TABLE ").append(updateTableName)
                        .append(" (identCol int IDENTITY(1,1) PRIMARY KEY, ")
                        .append("colSetCol XML COLUMN_SET FOR ALL_SPARSE_COLUMNS");
                for (int i = 0; i < 1100; i++) {
                    sb.append(", col").append(i).append(" int SPARSE NULL");
                }
                sb.append(")");
                stmt.executeUpdate(sb.toString());

                stmt.executeUpdate("INSERT INTO " + updateTableName
                        + " (colSetCol) VALUES ('<col0>1</col0><col999>2</col999>')");

                stmt.executeUpdate("UPDATE " + updateTableName
                        + " SET colSetCol = '<col0>333</col0><col999>444</col999>' WHERE identCol = 1");

                try (ResultSet rs = stmt.executeQuery(
                        "SELECT col0, col999 FROM " + updateTableName)) {
                    assertTrue(rs.next(), "Expected one row");
                    assertEquals(333, rs.getInt("col0"));
                    assertEquals(444, rs.getInt("col999"));
                }
            }
        }
    }
}
