/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.databasemetadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;

/**
 * ResultSet metadata tests: column metadata accuracy, data type mapping,
 * nullable/autoincrement detection, column name/label resolution.
 * Ported from FX cts/metaDataClient.java and resultset metadata tests.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxMetadata)
public class ResultSetMetadataTest extends AbstractTest {

    private static final String tableName = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("RSMeta_Tab"));

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            stmt.executeUpdate("CREATE TABLE " + tableName
                    + " (ID INT IDENTITY PRIMARY KEY NOT NULL, "
                    + "INT_COL INT NULL, BIGINT_COL BIGINT NULL, "
                    + "VARCHAR_COL VARCHAR(200) NOT NULL, NVARCHAR_COL NVARCHAR(200) NULL, "
                    + "DECIMAL_COL DECIMAL(18,6) NULL, FLOAT_COL FLOAT NULL, "
                    + "DATE_COL DATE NULL, DATETIME2_COL DATETIME2(7) NULL, "
                    + "BIT_COL BIT NULL, VARBINARY_COL VARBINARY(500) NULL, "
                    + "VARCHAR_MAX_COL VARCHAR(MAX) NULL)");
            stmt.executeUpdate("INSERT INTO " + tableName
                    + " (VARCHAR_COL) VALUES ('metadata_test')");
        }
    }

    @AfterAll
    public static void cleanupTests() throws Exception {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
        }
    }

    @Test
    public void testColumnCount() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            ResultSetMetaData rsmd = rs.getMetaData();
            assertEquals(12, rsmd.getColumnCount());
        }
    }

    @Test
    public void testColumnNames() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            ResultSetMetaData rsmd = rs.getMetaData();
            assertEquals("ID", rsmd.getColumnName(1));
            assertEquals("INT_COL", rsmd.getColumnName(2));
            assertEquals("BIGINT_COL", rsmd.getColumnName(3));
            assertEquals("VARCHAR_COL", rsmd.getColumnName(4));
        }
    }

    @Test
    public void testColumnLabels() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT ID as pk_id, VARCHAR_COL as name FROM " + tableName)) {
            ResultSetMetaData rsmd = rs.getMetaData();
            assertEquals("pk_id", rsmd.getColumnLabel(1));
            assertEquals("name", rsmd.getColumnLabel(2));
        }
    }

    @Test
    public void testColumnTypes() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            ResultSetMetaData rsmd = rs.getMetaData();
            // ID is INT
            assertEquals(Types.INTEGER, rsmd.getColumnType(1));
            // INT_COL is INT
            assertEquals(Types.INTEGER, rsmd.getColumnType(2));
            // BIGINT_COL is BIGINT
            assertEquals(Types.BIGINT, rsmd.getColumnType(3));
            // VARCHAR_COL is VARCHAR
            assertEquals(Types.VARCHAR, rsmd.getColumnType(4));
            // NVARCHAR_COL is NVARCHAR
            assertEquals(Types.NVARCHAR, rsmd.getColumnType(5));
            // BIT_COL is BIT
            assertEquals(Types.BIT, rsmd.getColumnType(10));
        }
    }

    @Test
    public void testColumnTypeNames() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            ResultSetMetaData rsmd = rs.getMetaData();
            assertNotNull(rsmd.getColumnTypeName(1));
            assertTrue(rsmd.getColumnTypeName(1).equalsIgnoreCase("int"));
            assertTrue(rsmd.getColumnTypeName(4).equalsIgnoreCase("varchar"));
        }
    }

    @Test
    public void testNullable() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            ResultSetMetaData rsmd = rs.getMetaData();
            // ID is NOT NULL (identity)
            assertEquals(ResultSetMetaData.columnNoNulls, rsmd.isNullable(1));
            // INT_COL is NULL
            assertEquals(ResultSetMetaData.columnNullable, rsmd.isNullable(2));
            // VARCHAR_COL is NOT NULL
            assertEquals(ResultSetMetaData.columnNoNulls, rsmd.isNullable(4));
        }
    }

    @Test
    public void testAutoIncrement() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            ResultSetMetaData rsmd = rs.getMetaData();
            // ID is IDENTITY
            assertTrue(rsmd.isAutoIncrement(1));
            // INT_COL is not IDENTITY
            assertTrue(!rsmd.isAutoIncrement(2));
        }
    }

    @Test
    public void testPrecisionAndScale() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            ResultSetMetaData rsmd = rs.getMetaData();
            // Find DECIMAL_COL (column 6)
            assertEquals(18, rsmd.getPrecision(6));
            assertEquals(6, rsmd.getScale(6));
        }
    }

    @Test
    public void testDisplaySize() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            ResultSetMetaData rsmd = rs.getMetaData();
            // VARCHAR(200) display size
            assertTrue(rsmd.getColumnDisplaySize(4) >= 200);
        }
    }

    @Test
    public void testIsReadOnlyWritable() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            ResultSetMetaData rsmd = rs.getMetaData();
            // Standard columns should be writable
            for (int i = 2; i <= rsmd.getColumnCount(); i++) {
                // isReadOnly or isWritable: at least one should be callable without error
                rsmd.isReadOnly(i);
                rsmd.isWritable(i);
            }
        }
    }

    @Test
    public void testSchemaAndTableName() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            ResultSetMetaData rsmd = rs.getMetaData();
            // Schema should be "dbo" or similar
            String schema = rsmd.getSchemaName(1);
            assertNotNull(schema);
            // Table name should be present
            String table = rsmd.getTableName(1);
            assertNotNull(table);
        }
    }

    @Test
    public void testCatalogName() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            ResultSetMetaData rsmd = rs.getMetaData();
            String catalog = rsmd.getCatalogName(1);
            assertNotNull(catalog);
            assertTrue(catalog.length() > 0);
        }
    }

    @Test
    public void testColumnClassName() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            ResultSetMetaData rsmd = rs.getMetaData();
            String className = rsmd.getColumnClassName(1); // INT
            assertNotNull(className);
            assertTrue(className.contains("Integer") || className.contains("int"));
        }
    }

    @Test
    public void testIsSigned() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            ResultSetMetaData rsmd = rs.getMetaData();
            // INT is signed
            assertTrue(rsmd.isSigned(1));
            // VARCHAR is not signed
            assertTrue(!rsmd.isSigned(4));
        }
    }

    @Test
    public void testIsSearchable() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            ResultSetMetaData rsmd = rs.getMetaData();
            // Standard types should be searchable
            assertTrue(rsmd.isSearchable(1)); // INT
            assertTrue(rsmd.isSearchable(4)); // VARCHAR
        }
    }

    @Test
    public void testIsCaseSensitive() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            ResultSetMetaData rsmd = rs.getMetaData();
            // INT is not case sensitive
            assertTrue(!rsmd.isCaseSensitive(1));
        }
    }

    @Test
    public void testMetadataForComputedColumn() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT ID, ID * 2 AS DOUBLED FROM " + tableName)) {
            ResultSetMetaData rsmd = rs.getMetaData();
            assertEquals(2, rsmd.getColumnCount());
            assertEquals("DOUBLED", rsmd.getColumnLabel(2));
        }
    }

    @Test
    public void testMetadataForVarcharMax() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT VARCHAR_MAX_COL FROM " + tableName)) {
            ResultSetMetaData rsmd = rs.getMetaData();
            assertEquals(Types.VARCHAR, rsmd.getColumnType(1));
            // Max length for varchar(max)
            assertTrue(rsmd.getPrecision(1) >= 2147483647 || rsmd.getPrecision(1) == 0);
        }
    }

    // DatabaseMetaData tests
    @Test
    public void testGetIndexInfo() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            try (ResultSet rs = dbmd.getIndexInfo(conn.getCatalog(), null, "%", false, true)) {
                assertNotNull(rs);
            }
        }
    }

    @Test
    public void testGetImportedKeys() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            try (ResultSet rs = dbmd.getImportedKeys(conn.getCatalog(), null, "%")) {
                assertNotNull(rs);
            }
        }
    }

    @Test
    public void testGetExportedKeys() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            try (ResultSet rs = dbmd.getExportedKeys(conn.getCatalog(), null, "%")) {
                assertNotNull(rs);
            }
        }
    }

    @Test
    public void testGetBestRowIdentifier() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            try (ResultSet rs = dbmd.getBestRowIdentifier(conn.getCatalog(), null, "%",
                    DatabaseMetaData.bestRowTransaction, true)) {
                assertNotNull(rs);
            }
        }
    }

    @Test
    public void testGetVersionColumns() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            try (ResultSet rs = dbmd.getVersionColumns(conn.getCatalog(), null, "%")) {
                assertNotNull(rs);
            }
        }
    }
}
