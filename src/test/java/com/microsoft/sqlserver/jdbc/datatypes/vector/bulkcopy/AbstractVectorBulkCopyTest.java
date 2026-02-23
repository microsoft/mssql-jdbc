/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.datatypes.vector.bulkcopy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCSVFileRecord;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.vectorJsonTest;

import microsoft.sql.Vector;
import microsoft.sql.Vector.VectorDimensionType;

/**
 * Abstract base class for Vector bulk copy tests. Contains all bulk copy test methods
 * that can be run for different vector types (FLOAT32, FLOAT16, etc.).
 *
 * <p>Concrete implementations should extend this class and provide the vector type-specific
 * configurations via abstract methods.</p>
 *
 * <p>This class includes tests for:</p>
 * <ul>
 *   <li>Direct bulk copy using {@link ISQLServerBulkData}</li>
 *   <li>Bulk copy via PreparedStatement with {@code useBulkCopyForBatchInsert=true}</li>
 *   <li>Table-to-table bulk copy between different column types</li>
 *   <li>Error handling for incompatible types and mismatched dimensions</li>
 *   <li>Performance testing with large numbers of records</li>
 * </ul>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractVectorBulkCopyTest extends AbstractTest {

    protected final String uuid = UUID.randomUUID().toString().replaceAll("-", "");
    protected String csvFilePath;

    // CSV file name constants
    protected static final String VECTOR_INPUT_CSV_FILE = "BulkCopyCSVTestInputWithVector.csv";
    protected static final String VECTOR_INPUT_CSV_FILE_MULTI_COLUMN = "BulkCopyCSVTestInputWithMultipleVectorColumn.csv";
    protected static final String VECTOR_INPUT_CSV_FILE_MULTI_COLUMN_PIPE = "BulkCopyCSVTestWithMultipleVectorColumnWithPipeDelimiter.csv";

    // ============================================================================
    // Abstract methods to be implemented by concrete test classes
    // ============================================================================

    /**
     * Returns the VectorDimensionType for this test class.
     * @return VectorDimensionType.FLOAT32 or VectorDimensionType.FLOAT16
     */
    protected abstract VectorDimensionType getVectorDimensionType();

    /**
     * Returns the SQL column definition for a vector with the given dimension count.
     * For FLOAT32: "VECTOR(3)"
     * For FLOAT16: "VECTOR(3, float16)"
     * @param dimensionCount the number of dimensions
     * @return the SQL column definition string
     */
    protected abstract String getColumnDefinition(int dimensionCount);

    /**
     * Returns the scale value for this vector type (bytes per dimension).
     * FLOAT32 = 4, FLOAT16 = 2
     * @return scale value
     */
    protected abstract int getScale();

    /**
     * Returns the type name for display purposes.
     * @return "FLOAT32" or "FLOAT16"
     */
    protected abstract String getTypeName();

    /**
     * Returns the maximum dimension count for this vector type.
     * @return maximum dimension count (e.g., 1998 for FLOAT32, 3996 for FLOAT16)
     */
    protected abstract int getMaxDimensionCount();

    /**
     * Returns the required vectorTypeSupport value for this test class.
     * FLOAT32 tests use "v1", FLOAT16 tests use "v2".
     * @return "v1" or "v2"
     */
    protected abstract String getRequiredVectorTypeSupport();

    // ============================================================================
    // Setup
    // ============================================================================

    @BeforeAll
    public void setupTest() throws Exception {
        String connStr = connectionString + ";vectorTypeSupport=" + getRequiredVectorTypeSupport();
        connection = (SQLServerConnection) DriverManager.getConnection(connStr);
        csvFilePath = TestUtils.getCurrentClassPath();
    }

    // ============================================================================
    // Helper Methods
    // ============================================================================

    /**
     * Returns the connection string with vectorTypeSupport set.
     */
    protected String getVectorConnectionString() {
        return connectionString + ";vectorTypeSupport=" + getRequiredVectorTypeSupport();
    }

    /**
     * Creates test data array for this vector type.
     * @param values the float values
     * @return Float[] array for the data
     */
    protected Float[] createTestData(float... values) {
        Float[] data = new Float[values.length];
        for (int i = 0; i < values.length; i++) {
            data[i] = values[i];
        }
        return data;
    }

    /**
     * Compares vector data arrays with tolerance for FLOAT16 precision loss.
     * FLOAT16 has a 10-bit mantissa (~3 decimal digits of precision).
     * @param expected the expected data array
     * @param actual the actual data array
     * @param message the assertion message
     */
    protected void assertVectorDataEquals(Object[] expected, Object[] actual, String message) {
        if (expected == null) {
            assertNull(actual, message + " - expected null but actual is not null");
            return;
        }
        assertNotNull(actual, message + " - actual is null");
        assertEquals(expected.length, actual.length, message + " - length mismatch");

        boolean isFloat16 = (getVectorDimensionType() == VectorDimensionType.FLOAT16);

        for (int i = 0; i < expected.length; i++) {
            float expectedVal = (Float) expected[i];
            float actualVal = (Float) actual[i];
            float tolerance = isFloat16 ? Math.max(0.001f * Math.abs(expectedVal), 1e-7f) : 0.0f;
            assertEquals(expectedVal, actualVal, tolerance,
                    message + " at index " + i + ": expected " + expectedVal + " but was " + actualVal);
        }
    }

    /**
     * Validates a vector-to-varchar string representation by parsing values and comparing
     * with tolerance. This avoids dependence on exact server-side formatting differences
     * between FLOAT32 and FLOAT16 scientific notation.
     */
    protected void assertVectorVarcharEquals(Float[] expected, String actual, String message) {
        assertNotNull(actual, message + " - result is null");
        assertTrue(actual.startsWith("[") && actual.endsWith("]"),
                message + " - expected bracket-enclosed format but got: " + actual);
        String inner = actual.substring(1, actual.length() - 1);
        String[] parts = inner.split(",");
        assertEquals(expected.length, parts.length, message + " - element count mismatch");
        for (int i = 0; i < parts.length; i++) {
            float actualVal = Float.parseFloat(parts[i].trim());
            float expectedVal = expected[i];
            boolean isFloat16 = (getVectorDimensionType() == VectorDimensionType.FLOAT16);
            float tolerance = isFloat16 ? Math.max(0.001f * Math.abs(expectedVal), 1e-7f) : 1e-6f;
            assertEquals(expectedVal, actualVal, tolerance,
                    message + " - value mismatch at index " + i);
        }
    }

    // ============================================================================
    // Bulk Copy Tests (bulkcopy)
    // ============================================================================

    /**
     * Test bulk copy with a single Vector row using ISQLServerBulkData.
     */
    @Test
    public void testBulkCopyVector() throws SQLException {
        String dstTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTableBulkCopyVector")));

        try (Connection conn = DriverManager.getConnection(getVectorConnectionString())) {
            try (Statement dstStmt = conn.createStatement();
                    SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn)) {

                dstStmt.executeUpdate(
                        "CREATE TABLE " + dstTable + " (vectorCol " + getColumnDefinition(3) + ");");

                bulkCopy.setDestinationTableName(dstTable);
                Float[] vectorData = createTestData(1.0f, 2.0f, 3.0f);
                Vector vector = new Vector(vectorData.length, getVectorDimensionType(), vectorData);
                VectorBulkData vectorBulkData = new VectorBulkData(vector, vectorData.length,
                        vector.getVectorDimensionType());
                bulkCopy.writeToServer(vectorBulkData);

                String select = "SELECT * FROM " + dstTable;
                try (ResultSet rs = dstStmt.executeQuery(select)) {
                    assertTrue(rs.next());
                    Vector resultVector = rs.getObject("vectorCol", Vector.class);
                    assertNotNull(resultVector, "Retrieved vector is null.");
                    assertEquals(3, resultVector.getDimensionCount(), "Dimension count mismatch.");
                    assertVectorDataEquals(vectorData, resultVector.getData(), "Vector data mismatch");
                } finally {
                    try (Statement stmt = conn.createStatement()) {
                        TestUtils.dropTableIfExists(dstTable, stmt);
                    }
                }
            }
        }
    }

    /**
     * Test bulk copy with null Vector data using ISQLServerBulkData.
     */
    @Test
    public void testBulkCopyVectorNull() throws SQLException {
        String dstTable = TestUtils
                .escapeSingleQuotes(
                        AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTableBulkCopyVectorNull")));

        try (Connection conn = DriverManager.getConnection(getVectorConnectionString())) {
            try (Statement dstStmt = conn.createStatement();
                    SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn)) {

                dstStmt.executeUpdate(
                        "CREATE TABLE " + dstTable + " (vectorCol " + getColumnDefinition(3) + ");");

                bulkCopy.setDestinationTableName(dstTable);
                Vector vector = new Vector(3, getVectorDimensionType(), null);
                VectorBulkData vectorBulkData = new VectorBulkData(vector, 3, vector.getVectorDimensionType());
                bulkCopy.writeToServer(vectorBulkData);

                String select = "SELECT * FROM " + dstTable;
                try (ResultSet rs = dstStmt.executeQuery(select)) {
                    int rowCount = 0;
                    while (rs.next()) {
                        Vector vectorObject = rs.getObject("vectorCol", Vector.class);
                        assertEquals(null, vectorObject.getData());
                        assertEquals(3, vectorObject.getDimensionCount(), "Dimension count mismatch.");
                        assertEquals(getVectorDimensionType(), vectorObject.getVectorDimensionType(),
                                "Vector dimension type mismatch.");
                        rowCount++;
                    }
                    assertEquals(1, rowCount, "Row count mismatch after inserting null vector data.");
                }

            } finally {
                try (Statement stmt = conn.createStatement()) {
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }
    }

    /**
     * Test bulk copy with a large number of records using ISQLServerBulkData to check performance.
     */
    @Test
    public void testBulkCopyPerformance() throws SQLException {
        String tableName = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("BulkCopyPerfTest_" + getTypeName()));
        int recordCount = 100;
        int dimensionCount = getMaxDimensionCount();
        Object[] vectorData = new Float[dimensionCount];

        for (int i = 0; i < dimensionCount; i++) {
            vectorData[i] = i + 0.5f;
        }

        try (Connection conn = DriverManager.getConnection(getVectorConnectionString());
                Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
        }

        try (Connection conn = DriverManager.getConnection(getVectorConnectionString());
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(
                    "CREATE TABLE " + tableName + " (v " + getColumnDefinition(dimensionCount) + ")");
        }

        List<Object[]> bulkData = new ArrayList<>();
        for (int i = 1; i <= recordCount; i++) {
            Vector vector = new Vector(dimensionCount, getVectorDimensionType(), vectorData);
            bulkData.add(new Object[] {vector});
        }

        long startTime = System.nanoTime();
        try (Connection conn = DriverManager.getConnection(getVectorConnectionString());
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn)) {

            SQLServerBulkCopyOptions bulkCopyOptions = new SQLServerBulkCopyOptions();
            bulkCopyOptions.setBulkCopyTimeout(60000);
            bulkCopyOptions.setBatchSize(1000001);
            bulkCopy.setBulkCopyOptions(bulkCopyOptions);

            bulkCopy.setDestinationTableName(tableName);

            ISQLServerBulkData vectorBulkData = new VectorBulkDataPerformance(bulkData, dimensionCount,
                    getVectorDimensionType());
            bulkCopy.writeToServer(vectorBulkData);
        }
        long endTime = System.nanoTime();

        long durationMs = (endTime - startTime) / 1_000_000;
        System.out.println(getTypeName() + " bulk copy (ISQLServerBulkData) for " + recordCount
                + " records with " + dimensionCount + " dimensions in " + durationMs + " ms.");

        // Cleanup
        try (Connection conn = DriverManager.getConnection(getVectorConnectionString());
                Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
        }
    }

    // ============================================================================
    // Bulk Copy for Batch Insert Tests (bulkcopyforbatchinsert)
    // ============================================================================

    /**
     * Test inserting vector data using prepared statement with bulk copy enabled.
     *
     * @throws Exception
     */
    @Test
    public void testInsertVectorWithBulkCopy() throws Exception {
        String bulkCopyTableName = RandomUtil.getIdentifier("BulkCopyVectorTest");
        String connStr = connectionString + ";vectorTypeSupport=" + getRequiredVectorTypeSupport()
                + ";useBulkCopyForBatchInsert=true;";
        String sqlString = "insert into " + AbstractSQLGenerator.escapeIdentifier(bulkCopyTableName)
                + " (vectorCol) values (?)";

        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connStr);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn.prepareStatement(sqlString);
                Statement stmt = conn.createStatement()) {

            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(bulkCopyTableName), stmt);
            String createTable = "create table " + AbstractSQLGenerator.escapeIdentifier(bulkCopyTableName)
                    + " (vectorCol " + getColumnDefinition(3) + ")";
            stmt.execute(createTable);

            Float[] vectorData = createTestData(4.0f, 5.0f, 6.0f);
            Vector vector = new Vector(vectorData.length, getVectorDimensionType(), vectorData);

            pstmt.setObject(1, vector, microsoft.sql.Types.VECTOR);
            pstmt.addBatch();
            pstmt.executeBatch();

            try (ResultSet rs = stmt.executeQuery(
                    "select vectorCol from " + AbstractSQLGenerator.escapeIdentifier(bulkCopyTableName))) {
                assertTrue(rs.next());
                Vector resultVector = rs.getObject("vectorCol", Vector.class);
                assertNotNull(resultVector, "Retrieved vector is null.");
                assertEquals(3, resultVector.getDimensionCount());
                assertVectorDataEquals(vectorData, resultVector.getData(), "Vector data mismatch.");
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(bulkCopyTableName), stmt);
            }
        }
    }

    /**
     * Test inserting null vector data using prepared statement with bulk copy enabled.
     *
     * @throws Exception
     */
    @Test
    public void testInsertNullVectorWithBulkCopy() throws Exception {
        String bulkCopyTableName = RandomUtil.getIdentifier("BulkCopyVectorTest");
        String connStr = connectionString + ";vectorTypeSupport=" + getRequiredVectorTypeSupport()
                + ";useBulkCopyForBatchInsert=true;";
        String sqlString = "insert into " + AbstractSQLGenerator.escapeIdentifier(bulkCopyTableName)
                + " (vectorCol) values (?)";

        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connStr);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn.prepareStatement(sqlString);
                Statement stmt = conn.createStatement()) {

            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(bulkCopyTableName), stmt);
            String createTable = "create table " + AbstractSQLGenerator.escapeIdentifier(bulkCopyTableName)
                    + " (vectorCol " + getColumnDefinition(3) + ")";
            stmt.execute(createTable);

            Vector vector = new Vector(3, getVectorDimensionType(), null);

            pstmt.setObject(1, vector, microsoft.sql.Types.VECTOR);
            pstmt.addBatch();
            pstmt.executeBatch();

            try (ResultSet rs = stmt.executeQuery(
                    "select vectorCol from " + AbstractSQLGenerator.escapeIdentifier(bulkCopyTableName))) {
                int rowCount = 0;
                while (rs.next()) {
                    Vector vectorObject = rs.getObject("vectorCol", Vector.class);
                    assertNull(vectorObject.getData(), "Expected null data for null vector insert.");
                    rowCount++;
                }
                assertEquals(1, rowCount);
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(bulkCopyTableName), stmt);
            }
        }
    }

    /**
     * Test inserting vector data using prepared statement with bulk copy enabled for performance.
     *
     * @throws SQLException
     */
    @Test
    public void testInsertWithBulkCopyPerformance() throws SQLException {
        String bulkCopyTableName = AbstractSQLGenerator
                .escapeIdentifier("BulkCopyVectorPerfTest_" + getTypeName());
        String connStr = connectionString + ";vectorTypeSupport=" + getRequiredVectorTypeSupport()
                + ";useBulkCopyForBatchInsert=true;bulkCopyForBatchInsertBatchSize=1000001;";

        int recordCount = 100;
        int dimensionCount = getMaxDimensionCount();
        Object[] vectorData = new Float[dimensionCount];

        for (int i = 0; i < dimensionCount; i++) {
            vectorData[i] = i + 0.5f;
        }

        // Drop the table if it already exists
        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connStr);
                Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(bulkCopyTableName, stmt);
        }

        // Create the destination table with a single VECTOR column
        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connStr);
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(
                    "CREATE TABLE " + bulkCopyTableName + " (vectorCol " + getColumnDefinition(dimensionCount) + ")");
        }

        long startTime = System.nanoTime();
        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connStr);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn.prepareStatement(
                        "INSERT INTO " + bulkCopyTableName + " (vectorCol) VALUES (?)")) {

            for (int i = 1; i <= recordCount; i++) {
                Vector vector = new Vector(dimensionCount, getVectorDimensionType(), vectorData);
                pstmt.setObject(1, vector, microsoft.sql.Types.VECTOR);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
        long endTime = System.nanoTime();
        long durationMs = (endTime - startTime) / 1_000_000;
        System.out.println(getTypeName() + " bulk copy insert for " + recordCount + " records with "
                + dimensionCount + " dimensions in " + durationMs + " ms.");

        // Cleanup
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(bulkCopyTableName, stmt);
        }
    }

    // ============================================================================
    // Bulk Copy Type Conversion Tests (bulkcopy)
    // ============================================================================

    /**
     * Test bulk copy from a varbinary source column to VECTOR as destination column.
     * The operation should fail with an error: "Operand type clash: varbinary(max) is
     * incompatible with vector".
     */
    @Test
    public void testBulkCopyVectorUsingBulkCopySourceAsVarBinary() {
        String varbinaryTable = TestUtils
                .escapeSingleQuotes(
                        AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("testVarbinaryTable")));
        String vectorTable = TestUtils
                .escapeSingleQuotes(
                        AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("testVectorTable")));

        try (Connection connection = DriverManager.getConnection(getVectorConnectionString());
                Statement statement = connection.createStatement()) {

            // Create the source table with a varbinary column
            statement.executeUpdate("CREATE TABLE " + varbinaryTable + " (varbinaryCol VARBINARY(MAX))");

            // Insert sample data into the source table
            statement.executeUpdate("INSERT INTO " + varbinaryTable + " (varbinaryCol) VALUES (0xDEADBEEF)");

            // Create the destination table with a VECTOR column
            statement.executeUpdate(
                    "CREATE TABLE " + vectorTable + " (vectorCol " + getColumnDefinition(3) + ")");

            // Retrieve the data from the source table
            String selectSql = "SELECT * FROM " + varbinaryTable;
            try (ResultSet resultSet = statement.executeQuery(selectSql)) {

                // Set up the bulk copy options
                SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
                options.setKeepIdentity(false);
                options.setBulkCopyTimeout(60);

                // Perform the bulk copy operation
                try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(connection)) {
                    bulkCopy.setDestinationTableName(vectorTable);
                    bulkCopy.addColumnMapping("varbinaryCol", "vectorCol");
                    bulkCopy.writeToServer(resultSet);
                    fail("Expected SQLServerException was not thrown.");
                }

            } catch (SQLServerException e) {
                // Verify the exception message
                assertTrue(
                        e.getMessage().contains("Operand type clash: varbinary(max) is incompatible with vector"),
                        "Unexpected exception message: " + e.getMessage());
            }

        } catch (Exception e) {
            fail("Test failed with unexpected exception: " + e.getMessage());
        } finally {
            // Cleanup: Drop the tables
            try (Connection conn = DriverManager.getConnection(getVectorConnectionString());
                    Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(varbinaryTable, stmt);
                TestUtils.dropTableIfExists(vectorTable, stmt);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Test bulk copy from a VECTOR source column to a VARBINARY(MAX) destination column.
     * The operation should fail with an error: "Operand type clash: vector is
     * incompatible with varbinary(max)".
     */
    @Test
    public void testBulkCopyVectorUsingBulkCopyDestinationAsVarBinary() {
        String vectorTable = TestUtils
                .escapeSingleQuotes(
                        AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("testVectorTable")));
        String varbinaryTable = TestUtils
                .escapeSingleQuotes(
                        AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("testVarbinaryTable")));

        try (Connection connection = DriverManager.getConnection(getVectorConnectionString());
                Statement statement = connection.createStatement()) {

            // Create the source table with a VECTOR column
            statement.executeUpdate(
                    "CREATE TABLE " + vectorTable + " (vectorCol " + getColumnDefinition(3) + ")");

            // Insert sample data into the source table
            Object[] data = createTestData(1.0f, 2.0f, 3.0f);
            Vector vectorData = new Vector(3, getVectorDimensionType(), data);

            try (PreparedStatement pstmt = connection.prepareStatement(
                    "INSERT INTO " + vectorTable + " (vectorCol) VALUES (?)")) {
                pstmt.setObject(1, vectorData, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            // Create the destination table with a VARBINARY(MAX) column
            statement.executeUpdate("CREATE TABLE " + varbinaryTable + " (varbinaryCol VARBINARY(MAX))");

            // Retrieve the data from the source table
            String selectSql = "SELECT * FROM " + vectorTable;
            try (ResultSet resultSet = statement.executeQuery(selectSql)) {

                // Set up the bulk copy options
                SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
                options.setKeepIdentity(false);
                options.setBulkCopyTimeout(60);

                // Perform the bulk copy operation
                try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(connection)) {
                    bulkCopy.setDestinationTableName(varbinaryTable);
                    bulkCopy.addColumnMapping("vectorCol", "varbinaryCol");
                    bulkCopy.writeToServer(resultSet);
                    fail("Expected SQLServerException was not thrown.");
                }

            } catch (SQLServerException e) {
                // Verify the exception message
                assertTrue(
                        e.getMessage().contains("Operand type clash: vector is incompatible with varbinary(max)"),
                        "Unexpected exception message: " + e.getMessage());
            }

        } catch (Exception e) {
            fail("Test failed with unexpected exception: " + e.getMessage());
        } finally {
            // Cleanup: Drop the tables
            try (Connection conn = DriverManager.getConnection(getVectorConnectionString());
                    Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(varbinaryTable, stmt);
                TestUtils.dropTableIfExists(vectorTable, stmt);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Test bulk copy from a VARCHAR source column to a VECTOR destination column.
     * The operation should succeed, and the data should be validated.
     */
    @Test
    public void testBulkCopyVectorUsingBulkCopySourceAsVarchar() {
        String varcharTable = TestUtils
                .escapeSingleQuotes(
                        AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("testVarcharTable")));
        String vectorTable = TestUtils
                .escapeSingleQuotes(
                        AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("testVectorTable")));

        try (Connection connection = DriverManager.getConnection(getVectorConnectionString());
                Statement statement = connection.createStatement()) {

            // Create the source table with a VARCHAR column
            statement.executeUpdate("CREATE TABLE " + varcharTable + " (varcharCol VARCHAR(MAX))");

            // Insert sample data into the source table
            String vectorString = "[1.0, 2.0, 3.0]";
            statement.executeUpdate(
                    "INSERT INTO " + varcharTable + " (varcharCol) VALUES ('" + vectorString + "')");

            // Create the destination table with a VECTOR column
            statement.executeUpdate(
                    "CREATE TABLE " + vectorTable + " (vectorCol " + getColumnDefinition(3) + ")");

            // Retrieve the data from the source table
            String selectSql = "SELECT * FROM " + varcharTable;
            try (ResultSet resultSet = statement.executeQuery(selectSql)) {

                // Set up the bulk copy options
                SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
                options.setKeepIdentity(false);
                options.setBulkCopyTimeout(60);

                // Perform the bulk copy operation
                try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(connection)) {
                    bulkCopy.setDestinationTableName(vectorTable);
                    bulkCopy.addColumnMapping("varcharCol", "vectorCol");
                    bulkCopy.writeToServer(resultSet);
                }

                // Validate the data in the destination table
                String validateSql = "SELECT * FROM " + vectorTable;
                try (ResultSet rs = statement.executeQuery(validateSql)) {
                    assertTrue(rs.next(), "No data found in the destination table.");
                    Vector resultVector = rs.getObject("vectorCol", Vector.class);
                    assertNotNull(resultVector, "Retrieved vector is null.");
                    assertEquals(3, resultVector.getDimensionCount(), "Dimension count mismatch.");
                    assertEquals(getVectorDimensionType(), resultVector.getVectorDimensionType(),
                            "Vector dimension type mismatch.");
                    assertVectorDataEquals(new Float[] {1.0f, 2.0f, 3.0f}, resultVector.getData(),
                            "Vector data mismatch");
                }

            }

        } catch (Exception e) {
            fail("Test failed with unexpected exception: " + e.getMessage());
        } finally {
            // Cleanup: Drop the tables
            try (Connection conn = DriverManager.getConnection(getVectorConnectionString());
                    Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(varcharTable, stmt);
                TestUtils.dropTableIfExists(vectorTable, stmt);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Test bulk copy from a VECTOR source column to a VARCHAR destination column.
     * The operation should succeed, and the data should be validated.
     */
    @Test
    public void testBulkCopyVectorUsingBulkCopyDestinationAsVarchar() {
        String vectorTable = TestUtils
                .escapeSingleQuotes(
                        AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("testVectorTable")));
        String varcharTable = TestUtils
                .escapeSingleQuotes(
                        AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("testVarcharTable")));

        try (Connection connection = DriverManager.getConnection(getVectorConnectionString());
                Statement statement = connection.createStatement()) {

            // Create the source table with a VECTOR column
            statement.executeUpdate(
                    "CREATE TABLE " + vectorTable + " (vectorCol " + getColumnDefinition(3) + ")");

            // Insert sample data into the source table
            Float[] data = createTestData(1.0f, 2.0f, 3.0f);
            Vector vector = new Vector(data.length, getVectorDimensionType(), data);
            try (PreparedStatement pstmt = connection.prepareStatement(
                    "INSERT INTO " + vectorTable + " (vectorCol) VALUES (?)")) {
                pstmt.setObject(1, vector, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            // Create the destination table with a VARCHAR column
            statement.executeUpdate("CREATE TABLE " + varcharTable + " (varcharCol VARCHAR(MAX))");

            // Retrieve the data from the source table
            String selectSql = "SELECT * FROM " + vectorTable;
            try (ResultSet resultSet = statement.executeQuery(selectSql)) {

                // Set up the bulk copy options
                SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
                options.setKeepIdentity(false);
                options.setBulkCopyTimeout(60);

                // Perform the bulk copy operation
                try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(connection)) {
                    bulkCopy.setDestinationTableName(varcharTable);
                    bulkCopy.addColumnMapping("vectorCol", "varcharCol");
                    bulkCopy.writeToServer(resultSet);
                }

                // Validate the data in the destination table
                String validateSql = "SELECT * FROM " + varcharTable;
                try (ResultSet rs = statement.executeQuery(validateSql)) {
                    assertTrue(rs.next(), "No data found in the destination table.");
                    String resultString = rs.getString("varcharCol");
                    assertVectorVarcharEquals(data, resultString, "Vector varchar data mismatch");
                }

            }

        } catch (Exception e) {
            fail("Test failed with unexpected exception: " + e.getMessage());
        } finally {
            // Cleanup: Drop the tables
            try (Connection conn = DriverManager.getConnection(getVectorConnectionString());
                    Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(varcharTable, stmt);
                TestUtils.dropTableIfExists(vectorTable, stmt);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Test bulk copy from a VECTOR source column to a VECTOR destination column with different dimensions.
     * The operation should fail with an error: "The vector dimensions 3 and 4 do not match."
     */
    @Test
    public void testBulkCopyVectorWithMismatchedDimensions() {
        String srcTable = TestUtils
                .escapeSingleQuotes(
                        AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("testSrcTable")));
        String desTable = TestUtils
                .escapeSingleQuotes(
                        AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("testDesTable")));

        try (Connection connection = DriverManager.getConnection(getVectorConnectionString());
                Statement statement = connection.createStatement()) {

            // Create the source table with a VECTOR column
            statement.executeUpdate(
                    "CREATE TABLE " + srcTable + " (vectorCol1 " + getColumnDefinition(3) + ")");

            // Insert sample data into the source table
            Object[] data = createTestData(1.0f, 2.0f, 3.0f);
            Vector vector = new Vector(data.length, getVectorDimensionType(), data);
            try (PreparedStatement pstmt = connection.prepareStatement(
                    "INSERT INTO " + srcTable + " (vectorCol1) VALUES (?)")) {
                pstmt.setObject(1, vector, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            // Create the destination table with a different dimension count
            statement.executeUpdate(
                    "CREATE TABLE " + desTable + " (vectorCol2 " + getColumnDefinition(4) + ")");

            // Retrieve the data from the source table
            String selectSql = "SELECT * FROM " + srcTable;
            try (ResultSet resultSet = statement.executeQuery(selectSql);
                    SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(connection)) {

                // Set up the bulk copy options
                SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
                options.setKeepIdentity(false);
                options.setBulkCopyTimeout(60);

                bulkCopy.setDestinationTableName(desTable);
                bulkCopy.addColumnMapping("vectorCol1", "vectorCol2");
                bulkCopy.writeToServer(resultSet);
            }

        } catch (Exception e) {
            assertTrue(e.getMessage().contains("The vector dimensions 3 and 4 do not match."),
                    "Unexpected error message: " + e.getMessage());
        } finally {
            // Cleanup: Drop the tables
            try (Connection conn = DriverManager.getConnection(getVectorConnectionString());
                    Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(srcTable, stmt);
                TestUtils.dropTableIfExists(desTable, stmt);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    // ============================================================================
    // Bulk Copy CSV Tests (bulkcopycsv)
    // ============================================================================

    /**
     * Test bulk copy with different format of vector data from CSV file.
     */
    @Test
    @vectorJsonTest
    public void testBulkCopyVectorFromCSV() throws SQLException {
        String dstTable = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("dstTableBulkCopyVectorCsv"));
        String fileName = csvFilePath + VECTOR_INPUT_CSV_FILE;

        try (Connection con = DriverManager.getConnection(getVectorConnectionString());
                Statement stmt = con.createStatement();
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
                SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(fileName, null, ",", true)) {

            // Create the destination table
            stmt.executeUpdate(
                    "CREATE TABLE " + dstTable + " (id INT, vectorCol " + getColumnDefinition(3) + ");");

            // Add column metadata for the CSV file
            fileRecord.addColumnMetadata(1, "id", java.sql.Types.INTEGER, 0, 0);
            fileRecord.addColumnMetadata(2, "vectorCol", microsoft.sql.Types.VECTOR, 3, getScale());
            fileRecord.setEscapeColumnDelimitersCSV(true);

            // Set the destination table name
            bulkCopy.setDestinationTableName(dstTable);

            // Configure bulk copy options
            SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
            options.setKeepIdentity(false);
            options.setTableLock(true);
            options.setBulkCopyTimeout(60);
            bulkCopy.setBulkCopyOptions(options);

            // Perform the bulk copy
            bulkCopy.writeToServer(fileRecord);

            // Validate the data
            String selectQuery = "SELECT id, vectorCol FROM " + dstTable;
            try (ResultSet rs = stmt.executeQuery(selectQuery)) {
                int rowCount = 0;

                // Expected data from the CSV file
                List<Object[]> expectedData = Arrays.asList(
                        new Object[] {1, createTestData(1.0f, 2.0f, 3.0f)},
                        new Object[] {2, createTestData(4.0f, 5.0f, 6.0f)},
                        new Object[] {3, null});

                while (rs.next()) {
                    int id = rs.getInt("id");
                    Vector vectorObject = rs.getObject("vectorCol", Vector.class);

                    // Validate ID
                    assertEquals(expectedData.get(rowCount)[0], id, "Mismatch in ID at row " + (rowCount + 1));

                    // Validate vector data
                    if (expectedData.get(rowCount)[1] == null) {
                        assertEquals(null, vectorObject.getData(),
                                "Expected null vector at row " + (rowCount + 1));
                    } else {
                        assertNotNull(vectorObject, "Expected non-null vector at row " + (rowCount + 1));
                        assertVectorDataEquals((Float[]) expectedData.get(rowCount)[1], vectorObject.getData(),
                                "Mismatch in vector data at row " + (rowCount + 1));
                    }

                    rowCount++;
                }

                // Validate row count
                assertEquals(expectedData.size(), rowCount, "Row count mismatch.");
            }
        } finally {
            try (Connection con = DriverManager.getConnection(getVectorConnectionString());
                    Statement stmt = con.createStatement()) {
                TestUtils.dropTableIfExists(dstTable, stmt);
            }
        }
    }

    /**
     * Test bulk copy with multiple columns of vector data from CSV file.
     */
    @Test
    @vectorJsonTest
    public void testBulkCopyVectorFromCSVWithMultipleColumns() throws SQLException {
        String dstTable = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("dstTableVectorCsvMulti"));
        String fileName = csvFilePath + VECTOR_INPUT_CSV_FILE_MULTI_COLUMN;

        try (Connection con = DriverManager.getConnection(getVectorConnectionString());
                Statement stmt = con.createStatement();
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
                SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(fileName, null, ",", true)) {

            stmt.executeUpdate(
                    "CREATE TABLE " + dstTable + " (vectorCol1 " + getColumnDefinition(3)
                            + ", vectorCol2 " + getColumnDefinition(3) + ");");

            fileRecord.addColumnMetadata(1, "vectorCol1", microsoft.sql.Types.VECTOR, 3, getScale());
            fileRecord.addColumnMetadata(2, "vectorCol2", microsoft.sql.Types.VECTOR, 3, getScale());
            fileRecord.setEscapeColumnDelimitersCSV(true);

            // Set the destination table name
            bulkCopy.setDestinationTableName(dstTable);

            SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
            options.setKeepIdentity(false);
            options.setTableLock(true);
            options.setBulkCopyTimeout(60);
            bulkCopy.setBulkCopyOptions(options);

            bulkCopy.writeToServer(fileRecord);

            // Validate the data
            String selectQuery = "SELECT * FROM " + dstTable;
            try (ResultSet rs = stmt.executeQuery(selectQuery)) {
                int rowCount = 0;

                // Expected data: each row is {vectorCol1Data, vectorCol2Data}
                List<Float[][]> expectedData = Arrays.asList(
                        new Float[][] {null, null},
                        new Float[][] {createTestData(1.0f, 2.0f, 3.0f), createTestData(1.0f, 2.0f, 3.0f)},
                        new Float[][] {createTestData(3.0f, 4.0f, 5.0f), createTestData(6.0f, 7.0f, 8.0f)});

                while (rs.next()) {
                    Vector vectorCol1 = rs.getObject("vectorCol1", Vector.class);
                    Vector vectorCol2 = rs.getObject("vectorCol2", Vector.class);

                    Float[][] expected = expectedData.get(rowCount);
                    Object[] actualData1 = vectorCol1 != null ? vectorCol1.getData() : null;
                    Object[] actualData2 = vectorCol2 != null ? vectorCol2.getData() : null;

                    assertVectorDataEquals(expected[0], actualData1,
                            "Mismatch in vectorCol1 data at row " + (rowCount + 1));
                    assertVectorDataEquals(expected[1], actualData2,
                            "Mismatch in vectorCol2 data at row " + (rowCount + 1));
                    rowCount++;
                }

                // Validate row count
                assertEquals(expectedData.size(), rowCount, "Row count mismatch.");
            }
        } finally {
            try (Connection con = DriverManager.getConnection(getVectorConnectionString());
                    Statement stmt = con.createStatement()) {
                TestUtils.dropTableIfExists(dstTable, stmt);
            }
        }
    }

    /**
     * Test bulk copy with multiple columns of vector data with pipe delimiter from CSV file.
     */
    @Test
    @vectorJsonTest
    public void testBulkCopyVectorFromCSVWithMultipleColumnsWithPipeDelimiter() throws SQLException {
        String dstTable = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("dstTableBulkCopyVectorCsvMultiPipe"));
        String fileName = csvFilePath + VECTOR_INPUT_CSV_FILE_MULTI_COLUMN_PIPE;

        try (Connection con = DriverManager.getConnection(getVectorConnectionString());
                Statement stmt = con.createStatement();
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
                SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(fileName, null, "|", true)) {

            stmt.executeUpdate(
                    "CREATE TABLE " + dstTable + " (vectorCol1 " + getColumnDefinition(3)
                            + ", vectorCol2 " + getColumnDefinition(3) + ");");

            fileRecord.addColumnMetadata(1, "vectorCol1", microsoft.sql.Types.VECTOR, 3, getScale());
            fileRecord.addColumnMetadata(2, "vectorCol2", microsoft.sql.Types.VECTOR, 3, getScale());
            fileRecord.setEscapeColumnDelimitersCSV(true);

            // Set the destination table name
            bulkCopy.setDestinationTableName(dstTable);

            SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
            options.setKeepIdentity(false);
            options.setTableLock(true);
            options.setBulkCopyTimeout(60);
            bulkCopy.setBulkCopyOptions(options);

            bulkCopy.writeToServer(fileRecord);

            // Validate the data
            String selectQuery = "SELECT * FROM " + dstTable;
            try (ResultSet rs = stmt.executeQuery(selectQuery)) {
                int rowCount = 0;

                // Expected data: each row is {vectorCol1Data, vectorCol2Data}
                List<Float[][]> expectedData = Arrays.asList(
                        new Float[][] {null, null},
                        new Float[][] {createTestData(1.0f, 2.0f, 3.0f), createTestData(1.0f, 2.0f, 3.0f)},
                        new Float[][] {createTestData(3.0f, 4.0f, 5.0f), createTestData(6.0f, 7.0f, 8.0f)});

                while (rs.next()) {
                    Vector vectorCol1 = rs.getObject("vectorCol1", Vector.class);
                    Vector vectorCol2 = rs.getObject("vectorCol2", Vector.class);

                    Float[][] expected = expectedData.get(rowCount);
                    Object[] actualData1 = vectorCol1 != null ? vectorCol1.getData() : null;
                    Object[] actualData2 = vectorCol2 != null ? vectorCol2.getData() : null;

                    assertVectorDataEquals(expected[0], actualData1,
                            "Mismatch in vectorCol1 data at row " + (rowCount + 1));
                    assertVectorDataEquals(expected[1], actualData2,
                            "Mismatch in vectorCol2 data at row " + (rowCount + 1));
                    rowCount++;
                }

                // Validate row count
                assertEquals(expectedData.size(), rowCount, "Row count mismatch.");
            }
        } finally {
            try (Connection con = DriverManager.getConnection(getVectorConnectionString());
                    Statement stmt = con.createStatement()) {
                TestUtils.dropTableIfExists(dstTable, stmt);
            }
        }
    }

    /**
     * Test bulk copy with mismatched vector dimensions in CSV file and provided column metadata.
     */
    @Test
    @vectorJsonTest
    public void testBulkCopyVectorFromCSVWithIncorrectDimension() throws SQLException {
        String dstTable = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("dstTableBulkCopyVectorCsvBadDim"));
        String fileName = csvFilePath + VECTOR_INPUT_CSV_FILE;

        try (Connection con = DriverManager.getConnection(getVectorConnectionString());
                Statement stmt = con.createStatement();
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
                SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(fileName, null, ",", true)) {

            // Create the destination table
            stmt.executeUpdate(
                    "CREATE TABLE " + dstTable + " (id INT, vectorCol " + getColumnDefinition(3) + ");");

            // Add column metadata with intentionally wrong dimension count (4 instead of 3)
            fileRecord.addColumnMetadata(1, "id", java.sql.Types.INTEGER, 0, 0);
            fileRecord.addColumnMetadata(2, "vectorCol", microsoft.sql.Types.VECTOR, 4, getScale());
            fileRecord.setEscapeColumnDelimitersCSV(true);

            // Set the destination table name
            bulkCopy.setDestinationTableName(dstTable);

            // Configure bulk copy options
            SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
            options.setKeepIdentity(false);
            options.setTableLock(true);
            options.setBulkCopyTimeout(60);
            bulkCopy.setBulkCopyOptions(options);

            // Perform the bulk copy
            bulkCopy.writeToServer(fileRecord);

            fail("Expected an exception due to vector data type mismatch, but none was thrown.");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("The vector dimensions 4 and 3 do not match."),
                    "Unexpected error message: " + e.getMessage());
        } finally {
            try (Connection con = DriverManager.getConnection(getVectorConnectionString());
                    Statement stmt = con.createStatement()) {
                TestUtils.dropTableIfExists(dstTable, stmt);
            }
        }
    }

    // ============================================================================
    // Inner Helper Classes
    // ============================================================================

    /**
     * ISQLServerBulkData implementation for a single Vector row.
     */
    public class VectorBulkData implements ISQLServerBulkData {
        boolean anyMoreData = true;
        Object[] data;
        int precision;
        VectorDimensionType dimensionType;

        VectorBulkData(Object data, int precision, VectorDimensionType dimensionType) {
            this.data = new Object[1];
            this.data[0] = data;
            this.dimensionType = dimensionType;
            this.precision = precision;
        }

        @Override
        public Set<Integer> getColumnOrdinals() {
            Set<Integer> ords = new HashSet<>();
            ords.add(1);
            return ords;
        }

        @Override
        public String getColumnName(int column) {
            return "vectorCol";
        }

        @Override
        public int getColumnType(int column) {
            return microsoft.sql.Types.VECTOR;
        }

        @Override
        public int getPrecision(int column) {
            return precision;
        }

        @Override
        public int getScale(int column) {
            if (dimensionType == VectorDimensionType.FLOAT32) {
                return 4;
            } else if (dimensionType == VectorDimensionType.FLOAT16) {
                return 2;
            } else {
                return 0;
            }
        }

        @Override
        public Object[] getRowData() {
            return data;
        }

        @Override
        public boolean next() {
            if (!anyMoreData)
                return false;
            anyMoreData = false;
            return true;
        }
    }

    /**
     * ISQLServerBulkData implementation for multiple Vector rows (performance testing).
     */
    public class VectorBulkDataPerformance implements ISQLServerBulkData {
        List<Object[]> data;
        int precision;
        VectorDimensionType dimensionType;
        int counter = 0;

        VectorBulkDataPerformance(List<Object[]> data, int precision, VectorDimensionType dimensionType) {
            this.data = data;
            this.dimensionType = dimensionType;
            this.precision = precision;
        }

        @Override
        public Set<Integer> getColumnOrdinals() {
            Set<Integer> ords = new HashSet<>();
            ords.add(1);
            return ords;
        }

        @Override
        public String getColumnName(int column) {
            return "v";
        }

        @Override
        public int getColumnType(int column) {
            return microsoft.sql.Types.VECTOR;
        }

        @Override
        public int getPrecision(int column) {
            return precision;
        }

        @Override
        public int getScale(int column) {
            if (dimensionType == VectorDimensionType.FLOAT32) {
                return 4;
            } else if (dimensionType == VectorDimensionType.FLOAT16) {
                return 2;
            } else {
                return 0;
            }
        }

        @Override
        public Object[] getRowData() {
            return data.get(counter++);
        }

        @Override
        public boolean next() {
            return counter < data.size();
        }
    }
}
