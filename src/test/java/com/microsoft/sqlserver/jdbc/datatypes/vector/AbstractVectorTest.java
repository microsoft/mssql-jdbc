/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.datatypes.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Field;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerDataTable;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;

import microsoft.sql.Vector;
import microsoft.sql.Vector.VectorDimensionType;

/**
 * Abstract base class for Vector data type tests. This class contains all parameterized test methods
 * that can be run for different vector types (FLOAT32, FLOAT16, etc.).
 * 
 * <p>Concrete implementations should extend this class and provide the vector type-specific
 * configurations via abstract methods.</p>
 * 
 * <p>Example concrete implementations:</p>
 * <ul>
 *   <li>{@link VectorFloat32Test} - Tests for FLOAT32 vector type</li>
 *   <li>{@link VectorFloat16Test} - Tests for FLOAT16 vector type</li>
 * </ul>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractVectorTest extends AbstractTest {

    // Table and procedure names - will be unique per test instance
    protected String tableName;
    protected String tableNameWithMultipleVectorColumn;
    protected String maxVectorDataTableName;
    protected String procedureName;
    protected String functionName;
    protected String functionTvpName;
    protected String vectorUdf;
    protected String TABLE_NAME;
    protected String TVP_NAME;
    protected String TVP;
    protected final String uuid = UUID.randomUUID().toString().replaceAll("-", "");

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
     * Returns the scale value for this vector type.
     * FLOAT32 = 4, FLOAT16 = 2
     * @return scale value
     */
    protected abstract int getScale();

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
        
        // FLOAT16 has a 10-bit mantissa, so its rounding step grows with magnitude
        // (e.g. step = 1.0 for values in [1024, 2048]). A fixed absolute tolerance
        // like 0.01 is therefore insufficient for large values. Using a relative
        // tolerance of 0.1% (≈ 2^-10) accounts for precision loss at any magnitude.
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
     * Returns the type name for display purposes.
     * @return "FLOAT32" or "FLOAT16"
     */
    protected abstract String getTypeName();

    /**
     * Returns the maximum dimension count for this vector type.
     * @return maximum dimension count (e.g., 1998 for FLOAT32)
     */
    protected abstract int getMaxDimensionCount();

    /**
     * Returns the required vectorTypeSupport value for this test class.
     * FLOAT32 tests use "v1", FLOAT16 tests use "v2".
     * @return "v1" or "v2"
     */
    protected abstract String getRequiredVectorTypeSupport();

    // ============================================================================
    // Setup and Teardown
    // ============================================================================

    @BeforeAll
    public void setupTest() throws Exception {
        // Use vectorTypeSupport appropriate for this vector type
        String vectorSupport = getRequiredVectorTypeSupport();
        String connStr = connectionString + ";vectorTypeSupport=" + vectorSupport;
        connection = (SQLServerConnection) java.sql.DriverManager.getConnection(connStr);
        
        // Verify the server negotiated the required version
        byte negotiatedVersion = connection.getNegotiatedVectorVersion();
        byte requiredVersion = "v2".equals(vectorSupport) ? (byte) 2 : (byte) 1;
        if (negotiatedVersion < requiredVersion) {
            System.out.println("WARNING: Server negotiated vector version " + negotiatedVersion + 
                ", but " + getTypeName() + " tests require version " + requiredVersion + 
                ". Some tests may be skipped.");
        }
        
        initializeTableNames();
        createTestTables();
    }

    private void initializeTableNames() {
        String suffix = getTypeName() + "_" + uuid.substring(0, 8);
        tableName = RandomUtil.getIdentifier("VECTOR_Test_" + suffix);
        tableNameWithMultipleVectorColumn = RandomUtil.getIdentifier("VECTOR_Test_MultiCol_" + suffix);
        maxVectorDataTableName = RandomUtil.getIdentifier("Max_Vector_Test_" + suffix);
        procedureName = RandomUtil.getIdentifier("VECTOR_Test_Proc_" + suffix);
        functionName = RandomUtil.getIdentifier("VECTOR_Test_Func_" + suffix);
        functionTvpName = RandomUtil.getIdentifier("VECTOR_Test_TVP_Func_" + suffix);
        vectorUdf = RandomUtil.getIdentifier("VectorUdf_" + suffix);
        TABLE_NAME = RandomUtil.getIdentifier("VECTOR_TVP_Test_" + suffix);
        TVP_NAME = RandomUtil.getIdentifier("VECTOR_TVP_Type_" + suffix);
        TVP = RandomUtil.getIdentifier("VECTOR_TVP_UDF_Type_" + suffix);
    }

    private void createTestTables() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (id INT PRIMARY KEY, v " + getColumnDefinition(3) + ")");
            stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableNameWithMultipleVectorColumn)
                    + " (id INT PRIMARY KEY, v1 " + getColumnDefinition(3) + ", v2 " + getColumnDefinition(4) + ")");
            stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(maxVectorDataTableName)
                    + " (id INT PRIMARY KEY, v " + getColumnDefinition(getMaxDimensionCount()) + ")");
            stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(TABLE_NAME) 
                    + " (rowId INT IDENTITY, c1 " + getColumnDefinition(3) + " NULL)");
            stmt.executeUpdate("CREATE TYPE " + AbstractSQLGenerator.escapeIdentifier(TVP_NAME) 
                    + " AS TABLE (c1 " + getColumnDefinition(3) + " NULL)");
            stmt.executeUpdate("CREATE TYPE " + AbstractSQLGenerator.escapeIdentifier(TVP) 
                    + " AS TABLE (c1 " + getColumnDefinition(4) + " NULL)");
        }
    }

    @AfterAll
    public void cleanupTest() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            // Drop functions first (they depend on types)
            // Use direct DROP statements because inline table-valued functions have type 'IF',
            // not 'FN', and dropFunctionIfExists only checks for 'FN'
            TestUtils.dropFunctionIfExists(functionName, stmt);
            stmt.executeUpdate("DROP FUNCTION IF EXISTS " + AbstractSQLGenerator.escapeIdentifier(functionTvpName));
            stmt.executeUpdate("DROP FUNCTION IF EXISTS " + AbstractSQLGenerator.escapeIdentifier(vectorUdf));
            
            // Drop procedures
            TestUtils.dropProcedureIfExists(procedureName, stmt);
            
            // Drop tables
            TestUtils.dropTableIfExists(tableName, stmt);
            TestUtils.dropTableIfExists(tableNameWithMultipleVectorColumn, stmt);
            TestUtils.dropTableIfExists(maxVectorDataTableName, stmt);
            TestUtils.dropTableIfExists(TABLE_NAME, stmt);
            
            // Drop types last (they may be referenced by functions/procedures)
            TestUtils.dropTypeIfExists(TVP_NAME, stmt);
            TestUtils.dropTypeIfExists(TVP, stmt);
        }
    }

    @AfterEach
    public void cleanupAfterEachTest() throws SQLException {
        // Clean up data from shared tables after each test to avoid conflicts
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("DELETE FROM " + AbstractSQLGenerator.escapeIdentifier(tableName));
            stmt.executeUpdate("DELETE FROM " + AbstractSQLGenerator.escapeIdentifier(tableNameWithMultipleVectorColumn));
            stmt.executeUpdate("DELETE FROM " + AbstractSQLGenerator.escapeIdentifier(maxVectorDataTableName));
            stmt.executeUpdate("DELETE FROM " + AbstractSQLGenerator.escapeIdentifier(TABLE_NAME));
        }
    }

    // ============================================================================
    // Vector Support Tests
    // ============================================================================

    @Test
    public void testVectorSupport() {
        boolean isVectorSupportEnabled;
        try {
            java.lang.reflect.Method method = SQLServerConnection.class.getDeclaredMethod("getServerSupportsVector");
            method.setAccessible(true);
            isVectorSupportEnabled = (boolean) method.invoke(connection);
        } catch (Exception e) {
            throw new RuntimeException("Failed to access getServerSupportsVector method", e);
        }
        assertTrue(isVectorSupportEnabled, "Vector support is not enabled");
    }

    // ============================================================================
    // Basic Insert and Retrieve Tests
    // ============================================================================

    @Test
    public void validateVectorDataWithPrecisionScale() throws SQLException {
        String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (id, v) VALUES (?, ?)";
        Float[] data = createTestData(0.4f, 0.5f, 0.6f);
        Vector vector = new Vector(3, getScale(), data);

        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 1);
            pstmt.setObject(2, vector, microsoft.sql.Types.VECTOR);
            pstmt.executeUpdate();
        }

        String query = "SELECT id, v FROM " + AbstractSQLGenerator.escapeIdentifier(tableName) + " WHERE id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, 1);
            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue(rs.next(), "No result found for inserted vector.");

                Vector resultVector = rs.getObject("v", Vector.class);
                assertNotNull(resultVector, "Retrieved vector is null.");
                assertEquals(3, resultVector.getDimensionCount(), "Dimension count mismatch.");
                assertVectorDataEquals(data, resultVector.getData(), "Vector data mismatch.");
            }
        }
    }

    @Test
    public void testInsertNullVector() throws SQLException {
        String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (id, v) VALUES (?, ?)";
        Vector nullVector = new Vector(3, getVectorDimensionType(), null);

        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 1);
            pstmt.setObject(2, nullVector, microsoft.sql.Types.VECTOR);
            int rowsInserted = pstmt.executeUpdate();
            assertEquals(1, rowsInserted, "Expected one row to be inserted.");
        }

        String query = "SELECT id, v FROM " + AbstractSQLGenerator.escapeIdentifier(tableName) + " WHERE id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, 1);
            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue(rs.next(), "No result found for inserted vector.");

                Vector resultVector = rs.getObject("v", Vector.class);
                assertEquals(3, resultVector.getDimensionCount(), "Dimension count mismatch.");
                assertNull(resultVector.getData(), "Expected null vector data.");
            }
        }
    }

    @Test
    public void testInsertFromTableWithNullVectorData() throws SQLException {
        String sourceTable = AbstractSQLGenerator.escapeIdentifier("SourceTable_" + getTypeName() + "_" + uuid.substring(0, 8));
        String destinationTable = AbstractSQLGenerator.escapeIdentifier("DestTable_" + getTypeName() + "_" + uuid.substring(0, 8));

        try (Statement stmt = connection.createStatement()) {
            String createSourceTableSQL = "CREATE TABLE " + sourceTable + " (id INT PRIMARY KEY, v " + getColumnDefinition(3) + ")";
            stmt.executeUpdate(createSourceTableSQL);

            String insertSourceSQL = "INSERT INTO " + sourceTable + " (id, v) VALUES (?, ?)";
            Vector nullVector = new Vector(3, getVectorDimensionType(), null);
            try (PreparedStatement pstmt = connection.prepareStatement(insertSourceSQL)) {
                for (int i = 1; i <= 3; i++) {
                    pstmt.setInt(1, i);
                    pstmt.setObject(2, nullVector, microsoft.sql.Types.VECTOR);
                    pstmt.executeUpdate();
                }
            }

            String createDestinationTableSQL = "CREATE TABLE " + destinationTable + " (id INT PRIMARY KEY, v " + getColumnDefinition(3) + ")";
            stmt.executeUpdate(createDestinationTableSQL);

            String selectSourceSQL = "SELECT id, v FROM " + sourceTable;
            String insertDestinationSQL = "INSERT INTO " + destinationTable + " (id, v) VALUES (?, ?)";
            try (PreparedStatement selectStmt = connection.prepareStatement(selectSourceSQL);
                    PreparedStatement insertStmt = connection.prepareStatement(insertDestinationSQL);
                    ResultSet rs = selectStmt.executeQuery()) {

                while (rs.next()) {
                    int id = rs.getInt("id");
                    insertStmt.setInt(1, id);
                    insertStmt.setObject(2, rs.getObject("v", Vector.class), microsoft.sql.Types.VECTOR);
                    insertStmt.executeUpdate();
                }
            }

            String validateSQL = "SELECT id, v FROM " + destinationTable + " ORDER BY id";
            try (ResultSet rs = stmt.executeQuery(validateSQL)) {
                int rowCount = 0;
                while (rs.next()) {
                    int id = rs.getInt("id");
                    Vector resultVector = rs.getObject("v", Vector.class);

                    assertNotNull(id, "ID should not be null.");
                    assertEquals(3, resultVector.getDimensionCount(), "Dimension count mismatch.");
                    assertNull(resultVector.getData(), "Expected null vector data.");
                    rowCount++;
                }
                assertEquals(3, rowCount, "Row count mismatch in destination table.");
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(sourceTable, stmt);
                TestUtils.dropTableIfExists(destinationTable, stmt);
            }
        }
    }

    @Test
    public void testInsertVectorWithMismatchedDimensions() throws SQLException {
        String sourceTable = AbstractSQLGenerator.escapeIdentifier("SourceTable_Mismatch_" + uuid.substring(0, 8));
        String destinationTable = AbstractSQLGenerator.escapeIdentifier("DestTable_Mismatch_" + uuid.substring(0, 8));

        try (Statement stmt = connection.createStatement()) {
            String createSourceTableSQL = "CREATE TABLE " + sourceTable + " (id INT PRIMARY KEY, v " + getColumnDefinition(4) + ")";
            stmt.executeUpdate(createSourceTableSQL);

            String insertSourceSQL = "INSERT INTO " + sourceTable + " (id, v) VALUES (?, ?)";
            try (PreparedStatement pstmt = connection.prepareStatement(insertSourceSQL)) {
                for (int i = 1; i <= 4; i++) {
                    Float[] vectorData = createTestData(i * 1.0f, i * 2.0f, i * 3.0f, i * 4.0f);
                    Vector vector = new Vector(4, getVectorDimensionType(), vectorData);
                    pstmt.setInt(1, i);
                    pstmt.setObject(2, vector, microsoft.sql.Types.VECTOR);
                    pstmt.executeUpdate();
                }
            }

            String createDestinationTableSQL = "CREATE TABLE " + destinationTable + " (id INT PRIMARY KEY, v " + getColumnDefinition(3) + ")";
            stmt.executeUpdate(createDestinationTableSQL);

            String selectSourceSQL = "SELECT id, v FROM " + sourceTable;
            String insertDestinationSQL = "INSERT INTO " + destinationTable + " (id, v) VALUES (?, ?)";
            try (PreparedStatement selectStmt = connection.prepareStatement(selectSourceSQL);
                    PreparedStatement insertStmt = connection.prepareStatement(insertDestinationSQL);
                    ResultSet rs = selectStmt.executeQuery()) {

                while (rs.next()) {
                    int id = rs.getInt("id");
                    Vector vector = rs.getObject("v", Vector.class);

                    insertStmt.setInt(1, id);
                    insertStmt.setObject(2, vector, microsoft.sql.Types.VECTOR);
                    insertStmt.executeUpdate();
                }
            } catch (SQLException e) {
                assertTrue(e.getMessage().contains("The vector dimensions 4 and 3 do not match."),
                        "Unexpected error message: " + e.getMessage());
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(sourceTable, stmt);
                TestUtils.dropTableIfExists(destinationTable, stmt);
            }
        }
    }

    @Test
    public void validateVectorDataWithMultipleVectorColumn() throws SQLException {
        String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableNameWithMultipleVectorColumn)
                + " (id, v1, v2) VALUES (?, ?, ?)";
        Float[] originalData1 = createTestData(0.45f, 7.9f, 63.0f);
        Vector vector1 = new Vector(3, getVectorDimensionType(), originalData1);
        Float[] originalData2 = createTestData(0.45f, 7.9f, 63.0f, 1.2f);
        Vector vector2 = new Vector(4, getVectorDimensionType(), originalData2);

        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 1);
            pstmt.setObject(2, vector1, microsoft.sql.Types.VECTOR);
            pstmt.setObject(3, vector2, microsoft.sql.Types.VECTOR);
            pstmt.executeUpdate();
        }

        String query = "SELECT id, v1, v2 FROM " + AbstractSQLGenerator.escapeIdentifier(tableNameWithMultipleVectorColumn) + " WHERE id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, 1);
            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue(rs.next(), "No result found for inserted vector.");

                Vector resultVector1 = rs.getObject("v1", Vector.class);
                assertNotNull(resultVector1, "Retrieved vector v1 is null.");
                assertEquals(3, resultVector1.getDimensionCount(), "Dimension count mismatch for v1.");
                assertVectorDataEquals(originalData1, resultVector1.getData(), "Vector data mismatch for v1.");

                Vector resultVector2 = rs.getObject("v2", Vector.class);
                assertNotNull(resultVector2, "Retrieved vector v2 is null.");
                assertEquals(4, resultVector2.getDimensionCount(), "Dimension count mismatch for v2.");
                assertVectorDataEquals(originalData2, resultVector2.getData(), "Vector data mismatch for v2.");
            }
        }
    }

    /**
     * Test for inserting a vector with Double[] data type. 
     * This validates that Double[] throws an error when FLOAT32/FLOAT16 expects Float[].
     */
    @Test
    public void testInsertDoubleVectorDataThrowsError() throws SQLException {
        Object[] originalData = new Double[] { 0.45, 7.9, 63.0 };
        try {
            new microsoft.sql.Vector(3, VectorDimensionType.FLOAT32, originalData);
            fail("Expected an exception due to type mismatch, but none was thrown.");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Invalid vector data type."),
                    "Unexpected error message: " + e.getMessage());
        }
    }

    // ============================================================================
    // Backward Compatibility Tests
    // ============================================================================

    @Test
    public void validateVectorDataUsingGetString() throws SQLException {
        String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (id, v) VALUES (?, ?)";
        Float[] originalData = createTestData(0.45f, 7.9f, 63.0f);
        Vector initialVector = new Vector(3, getVectorDimensionType(), originalData);

        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 1);
            pstmt.setObject(2, initialVector, microsoft.sql.Types.VECTOR);
            pstmt.executeUpdate();
        }

        String query = "SELECT id, v FROM " + AbstractSQLGenerator.escapeIdentifier(tableName) + " WHERE id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, 1);
            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue(rs.next(), "No result found for inserted vector.");

                try {
                    String vectorString = rs.getString("v");
                    fail("Expected an exception when calling getString() on VECTOR type, but none was thrown.");
                } catch (SQLException e) {
                    assertEquals("The conversion from vector to CHAR is unsupported.", e.getMessage(),
                            "Error message does not match the expected message.");
                }
            }
        }
    }

    @Test
    public void validateNullVectorDataUsingGetString() throws SQLException {
        String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (id, v) VALUES (?, ?)";
        Vector initialVector = new Vector(3, getVectorDimensionType(), null);

        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 1);
            pstmt.setObject(2, initialVector, microsoft.sql.Types.VECTOR);
            pstmt.executeUpdate();
        }

        String query = "SELECT id, v FROM " + AbstractSQLGenerator.escapeIdentifier(tableName) + " WHERE id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, 1);
            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue(rs.next(), "No result found for inserted vector.");

                String vectorString = rs.getString("v");
                assertNull(vectorString, "Retrieved vector string should be null.");
            }
        }
    }

    // ============================================================================
    // Validation Tests
    // ============================================================================

    @Test
    public void testVectorDataWithNegativeDimensionCount() throws SQLException {
        Float[] originalData = createTestData(0.45f, 7.9f, 63.0f);
        try {
            new Vector(-5, getVectorDimensionType(), originalData);
            fail("Expected an exception due to negative dimension count, but none was thrown.");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Invalid vector dimension count."),
                    "Unexpected error message: " + e.getMessage());
        }
    }

    @Test
    public void testVectorDataWithIncorrectDimensionCount() throws SQLException {
        Float[] originalData = createTestData(0.45f, 7.9f, 63.0f);
        try {
            new Vector(7, getVectorDimensionType(), originalData);
            fail("Expected an exception due to dimension count mismatch, but none was thrown.");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Mismatch between vector dimension count and provided data."),
                    "Unexpected error message: " + e.getMessage());
        }
    }

    // ============================================================================
    // Max Vector Data Tests
    // ============================================================================

    @Test
    public void validateMaxVectorData() throws SQLException {
        String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(maxVectorDataTableName) + " (id, v) VALUES (?, ?)";

        int dimensionCount = getMaxDimensionCount();
        Float[] originalData = new Float[dimensionCount];

        for (int i = 0; i < dimensionCount; i++) {
            originalData[i] = i + 0.5f;
        }

        Vector initialVector = new Vector(dimensionCount, getVectorDimensionType(), originalData);

        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 1);
            pstmt.setObject(2, initialVector, microsoft.sql.Types.VECTOR);
            pstmt.executeUpdate();
        }

        String query = "SELECT id, v FROM " + AbstractSQLGenerator.escapeIdentifier(maxVectorDataTableName) + " WHERE id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, 1);
            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue(rs.next(), "No result found for inserted max vector.");

                Vector resultVector = rs.getObject("v", Vector.class);
                assertNotNull(resultVector, "Retrieved vector is null.");
                assertEquals(dimensionCount, resultVector.getDimensionCount(), "Dimension count mismatch.");
                assertVectorDataEquals(originalData, resultVector.getData(), "Vector data mismatch.");
            }
        }
    }

    @Test
    public void validateMaxVectorDataAtScale() throws SQLException {
        String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(maxVectorDataTableName) + " (id, v) VALUES (?, ?)";
        String selectSql = "SELECT id, v FROM " + AbstractSQLGenerator.escapeIdentifier(maxVectorDataTableName);

        int dimensionCount = getMaxDimensionCount();
        Float[] vectorData = new Float[dimensionCount];

        for (int i = 0; i < dimensionCount; i++) {
            vectorData[i] = i + 0.5f;
        }

        Vector vector = new Vector(dimensionCount, getVectorDimensionType(), vectorData);

        int recordCount = 100;

        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("TRUNCATE TABLE " + AbstractSQLGenerator.escapeIdentifier(maxVectorDataTableName));
        }

        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            for (int i = 1; i <= recordCount; i++) {
                pstmt.setInt(1, i);
                pstmt.setObject(2, vector, microsoft.sql.Types.VECTOR);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }

        int rowsRead = 0;
        try (PreparedStatement stmt = connection.prepareStatement(selectSql);
                ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                int id = rs.getInt("id");
                Vector resultVector = rs.getObject("v", Vector.class);

                assertNotNull(resultVector, "Vector is null for ID " + id);
                assertEquals(dimensionCount, resultVector.getDimensionCount(), "Mismatch at ID " + id);
                rowsRead++;
            }
        }

        assertEquals(recordCount, rowsRead, "Mismatch in number of rows read vs inserted.");
    }

    @Test
    public void validateVectorExceedingMaxDimension() throws SQLException {
        String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(maxVectorDataTableName) + " (id, v) VALUES (?, ?)";

        int dimensionCount = getMaxDimensionCount() + 1;
        Float[] originalData = new Float[dimensionCount];

        for (int i = 0; i < dimensionCount; i++) {
            originalData[i] = i + 0.5f;
        }

        Vector initialVector = new Vector(dimensionCount, getVectorDimensionType(), originalData);

        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 1);
            pstmt.setObject(2, initialVector, microsoft.sql.Types.VECTOR);

            try {
                pstmt.executeUpdate();
                fail("Expected SQLServerException due to exceeding maximum allowed vector size, but none was thrown.");
            } catch (SQLServerException e) {
                assertTrue(e.getMessage().contains("The size (" + dimensionCount + ") given to the type 'vector' exceeds the maximum allowed"),
                        "Unexpected error message: " + e.getMessage());
            }
        }
    }

    // ============================================================================
    // Stored Procedure Tests
    // ============================================================================

    private void createProcedure() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            String sql = "CREATE OR ALTER PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(procedureName) + "\n" +
                    "    @inVector " + getColumnDefinition(3) + ",\n" +
                    "    @outVector " + getColumnDefinition(3) + " OUTPUT\n" +
                    "AS\n" +
                    "BEGIN\n" +
                    "    SET @outVector = @inVector\n" +
                    "END";
            stmt.execute(sql);
        }
    }

    @Test
    public void testVectorStoredProcedureInputOutput() throws SQLException {
        createProcedure();

        String call = "{call " + AbstractSQLGenerator.escapeIdentifier(procedureName) + "(?, ?)}";
        try (SQLServerCallableStatement cstmt = (SQLServerCallableStatement) connection.prepareCall(call)) {
            Vector inputVector = new Vector(3, getVectorDimensionType(), createTestData(0.5f, 1.0f, 1.5f));

            cstmt.setObject(1, inputVector, microsoft.sql.Types.VECTOR);
            cstmt.registerOutParameter(2, microsoft.sql.Types.VECTOR, 3, getScale());
            cstmt.execute();

            Vector result = cstmt.getObject(2, Vector.class);
            assertNotNull(result, "Returned vector should not be null");
            assertVectorDataEquals(inputVector.getData(), result.getData(), "Vector data mismatch.");
        }
    }

    @Test
    public void testNullVectorStoredProcedureInputOutput() throws SQLException {
        createProcedure();

        String call = "{call " + AbstractSQLGenerator.escapeIdentifier(procedureName) + "(?, ?)}";
        try (SQLServerCallableStatement cstmt = (SQLServerCallableStatement) connection.prepareCall(call)) {
            Vector inputVector = new Vector(3, getVectorDimensionType(), null);

            cstmt.setObject(1, inputVector, microsoft.sql.Types.VECTOR);
            cstmt.registerOutParameter(2, microsoft.sql.Types.VECTOR, 3, getScale());
            cstmt.execute();

            Vector result = cstmt.getObject(2, Vector.class);
            assertNotNull(result, "Returned vector should not be null");
            assertVectorDataEquals(inputVector.getData(), result.getData(), "Vector data mismatch.");
        }
    }

    // ============================================================================
    // TVP Tests
    // ============================================================================

    @Test
    public void testVectorTVP() throws SQLException {
        Vector expectedVector = new Vector(3, getVectorDimensionType(), createTestData(0.1f, 0.2f, 0.3f));

        SQLServerDataTable tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", microsoft.sql.Types.VECTOR);
        tvp.addRow(expectedVector);

        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(TABLE_NAME) + " SELECT * FROM ?;")) {
            pstmt.setStructured(1, TVP_NAME, tvp);
            pstmt.execute();

            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "SELECT c1 FROM " + AbstractSQLGenerator.escapeIdentifier(TABLE_NAME) + " ORDER BY rowId")) {
                while (rs.next()) {
                    Vector actual = rs.getObject("c1", Vector.class);
                    assertNotNull(actual, "Returned vector should not be null");
                    assertVectorDataEquals(expectedVector.getData(), actual.getData(), "Vector data mismatch.");
                }
            }
        }
    }

    @Test
    public void testNullVectorInsertTVP() throws SQLException {
        String testTableName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("NullVectorTVPTest_" + uuid.substring(0, 8)));
        String testTvpName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("NullVectorTVPType_" + uuid.substring(0, 8)));

        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + testTableName + " (rowId INT IDENTITY, c1 " + getColumnDefinition(3) + " NULL)");
            stmt.executeUpdate("CREATE TYPE " + testTvpName + " AS TABLE (c1 " + getColumnDefinition(3) + " NULL)");
        }

        Vector expectedVector = new Vector(3, getVectorDimensionType(), null);

        SQLServerDataTable tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", microsoft.sql.Types.VECTOR);
        tvp.addRow(expectedVector);

        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                "INSERT INTO " + testTableName + " SELECT * FROM ?;")) {
            pstmt.setStructured(1, testTvpName, tvp);
            pstmt.execute();

            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT c1 FROM " + testTableName + " ORDER BY rowId")) {
                while (rs.next()) {
                    Vector actual = rs.getObject("c1", Vector.class);
                    assertEquals(3, actual.getDimensionCount(), "Dimension count mismatch.");
                    assertNull(actual.getData(), "Expected null vector data.");
                }
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(testTableName, stmt);
                TestUtils.dropTypeIfExists(testTvpName, stmt);
            }
        }
    }

    private void createTVPReturnProcedure(String procName, String tvpTypeName) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            String sql = "CREATE OR ALTER PROCEDURE " + procName + "\n" +
                    "    @tvpInput " + tvpTypeName + " READONLY\n" +
                    "AS\n" +
                    "BEGIN\n" +
                    "    SELECT * FROM @tvpInput;\n" +
                    "END";
            stmt.execute(sql);
        }
    }

    @Test
    public void testStoredProcedureReturnsTVPWithVector() throws SQLException {
        String tvpTypeName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("VectorTVPType_" + uuid.substring(0, 8)));
        String procName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("sp_ReturnVectorTVP_" + uuid.substring(0, 8)));

        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("CREATE TYPE " + tvpTypeName + " AS TABLE (c1 " + getColumnDefinition(3) + " NULL)");
        }

        createTVPReturnProcedure(procName, tvpTypeName);

        SQLServerDataTable tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", microsoft.sql.Types.VECTOR);
        Vector v1 = new Vector(3, getVectorDimensionType(), createTestData(1.0f, 2.0f, 3.0f));
        Vector v2 = new Vector(3, getVectorDimensionType(), createTestData(4.0f, 5.0f, 6.0f));
        tvp.addRow(v1);
        tvp.addRow(v2);

        String call = "{call " + procName + "(?)}";
        try (SQLServerCallableStatement cstmt = (SQLServerCallableStatement) connection.prepareCall(call)) {
            cstmt.setStructured(1, tvpTypeName, tvp);
            boolean hasResultSet = cstmt.execute();
            assertTrue(hasResultSet, "Stored procedure should return a result set");

            try (ResultSet rs = cstmt.getResultSet()) {
                int rowCount = 0;
                while (rs.next()) {
                    Vector v = rs.getObject("c1", Vector.class);
                    assertNotNull(v, "Returned vector should not be null");
                    if (rowCount == 0) {
                        assertVectorDataEquals(v1.getData(), v.getData(), "Vector data mismatch in row 1.");
                    } else if (rowCount == 1) {
                        assertVectorDataEquals(v2.getData(), v.getData(), "Vector data mismatch in row 2.");
                    }
                    rowCount++;
                }
                assertEquals(2, rowCount, "Row count mismatch.");
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropProcedureIfExists(procName, stmt);
                TestUtils.dropTypeIfExists(tvpTypeName, stmt);
            }
        }
    }

    // ============================================================================
    // UDF Tests
    // ============================================================================

    private void createVectorUdf() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            String sql =
                    "CREATE OR ALTER FUNCTION " + AbstractSQLGenerator.escapeIdentifier(functionName) + "\n" +
                            "(@v " + getColumnDefinition(3) + ")\n" +
                            "RETURNS " + getColumnDefinition(3) + "\n" +
                            "AS\n" +
                            "BEGIN\n" +
                            "    RETURN @v;\n" +
                            "END";
            stmt.execute(sql);
        }
    }

    @Test
    public void testVectorUdf() throws SQLException {
        createVectorUdf();

        String tempTable = "#vec_input_" + uuid.substring(0, 8);
        Vector inputVector = new Vector(3, getVectorDimensionType(), createTestData(1.1f, 2.2f, 3.3f));

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE " + tempTable + " (v " + getColumnDefinition(3) + ");");
        }
        try (PreparedStatement pstmt = connection.prepareStatement("INSERT INTO " + tempTable + " VALUES (?);")) {
            pstmt.setObject(1, inputVector, microsoft.sql.Types.VECTOR);
            pstmt.executeUpdate();
        }

        String query = "SELECT dbo." + AbstractSQLGenerator.escapeIdentifier(functionName) + "((SELECT TOP 1 v FROM " + tempTable + "));";
        try (PreparedStatement selectStmt = connection.prepareStatement(query);
                ResultSet rs = selectStmt.executeQuery()) {
            assertTrue(rs.next(), "Result set is empty");

            Vector result = rs.getObject(1, Vector.class);
            assertNotNull(result, "Returned vector should not be null");
            assertVectorDataEquals(inputVector.getData(), result.getData(), "Vector data mismatch.");
        } finally {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + tempTable + ";");
            }
        }
    }

    private void createTVPReturnUdf() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            String sql = "CREATE OR ALTER FUNCTION " + AbstractSQLGenerator.escapeIdentifier(vectorUdf) +
                    "(@tvpInput " + AbstractSQLGenerator.escapeIdentifier(TVP) + " READONLY)\n" +
                    "RETURNS TABLE\n" +
                    "AS\n" +
                    "RETURN SELECT c1 FROM @tvpInput;";
            stmt.execute(sql);
        }
    }

    @Test
    public void testVectorUdfReturnsTVP() throws SQLException {
        createTVPReturnUdf();

        SQLServerDataTable tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", microsoft.sql.Types.VECTOR);
        Vector v1 = new Vector(4, getVectorDimensionType(), createTestData(1.0f, 2.0f, 3.0f, 4.0f));
        Vector v2 = new Vector(4, getVectorDimensionType(), createTestData(4.0f, 5.0f, 6.0f, 7.0f));
        tvp.addRow(v1);
        tvp.addRow(v2);

        String query = "SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(vectorUdf) + "(?)";
        try (SQLServerPreparedStatement selectStmt = (SQLServerPreparedStatement) connection.prepareStatement(query)) {
            selectStmt.setStructured(1, TVP, tvp);
            try (ResultSet rs = selectStmt.executeQuery()) {
                int rowCount = 0;
                while (rs.next()) {
                    Vector v = rs.getObject("c1", Vector.class);
                    assertNotNull(v, "Returned vector should not be null");
                    if (rowCount == 0) {
                        assertVectorDataEquals(v1.getData(), v.getData(), "Vector data mismatch in row 1.");
                    } else if (rowCount == 1) {
                        assertVectorDataEquals(v2.getData(), v.getData(), "Vector data mismatch in row 2.");
                    }
                    rowCount++;
                }
                assertEquals(2, rowCount, "Row count mismatch.");
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                stmt.executeUpdate("DROP FUNCTION IF EXISTS " + AbstractSQLGenerator.escapeIdentifier(vectorUdf));
            }
        }
    }

    // ============================================================================
    // SELECT INTO Tests
    // ============================================================================

    @Test
    public void testSelectIntoForVector() throws SQLException {
        String sourceTable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("srcTableVector_" + uuid.substring(0, 8)));
        String destinationTable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("destTable_" + uuid.substring(0, 8)));

        try (Statement stmt = connection.createStatement()) {
            String createSourceTableSql = "CREATE TABLE " + sourceTable + " (id INT, v " + getColumnDefinition(3) + ")";
            stmt.executeUpdate(createSourceTableSql);

            Float[] vectorData = createTestData(1.1f, 2.2f, 3.3f);
            Vector vector = new Vector(3, getVectorDimensionType(), vectorData);

            String insertSql = "INSERT INTO " + sourceTable + " (id, v) VALUES (?, ?)";
            try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
                pstmt.setInt(1, 1);
                pstmt.setObject(2, vector, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            Float[] vectorData2 = null;
            Vector vector2 = new Vector(3, getVectorDimensionType(), vectorData2);

            try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
                pstmt.setInt(1, 2);
                pstmt.setObject(2, vector2, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            TestUtils.dropTableIfExists(destinationTable, stmt);

            String selectIntoSql = "SELECT * INTO " + destinationTable + " FROM " + sourceTable;
            stmt.executeUpdate(selectIntoSql);

            String validateSql = "SELECT id, v FROM " + destinationTable + " ORDER BY id";
            try (ResultSet rs = stmt.executeQuery(validateSql)) {
                int rowCount = 0;
                while (rs.next()) {
                    int id = rs.getInt("id");
                    Vector resultVector = rs.getObject("v", Vector.class);

                    assertNotNull(resultVector, "Vector is null in destination table for ID " + id);

                    if (id == 1) {
                        assertVectorDataEquals(vector.getData(), resultVector.getData(), "Vector data mismatch for ID 1.");
                    } else if (id == 2) {
                        assertVectorDataEquals(vector2.getData(), resultVector.getData(), "Vector data mismatch for ID 2.");
                    } else {
                        fail("Unexpected ID found in destination table: " + id);
                    }
                    rowCount++;
                }
                assertEquals(2, rowCount, "Row count mismatch in destination table.");
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(sourceTable, stmt);
                TestUtils.dropTableIfExists(destinationTable, stmt);
            }
        }
    }

    // ============================================================================
    // Temporary Table Tests
    // ============================================================================

    @Test
    public void testVectorInsertionInGlobalTempTable() throws SQLException {
        String dstTable = TestUtils.escapeSingleQuotes(
                AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("##TempVector_" + uuid.substring(0, 8))));

        String createTableSQL = "CREATE TABLE " + dstTable + " (id INT PRIMARY KEY, data " + getColumnDefinition(3) + ")";
        String insertSQL = "INSERT INTO " + dstTable + " VALUES (?, ?)";
        String selectSQL = "SELECT data FROM " + dstTable + " WHERE id = ?";

        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(dstTable, stmt);
            stmt.execute(createTableSQL);
        }

        try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) {
            Float[] vectorData = createTestData(1.0f, 2.0f, 3.0f);
            Vector vector = new Vector(3, getVectorDimensionType(), vectorData);

            pstmt.setInt(1, 1);
            pstmt.setObject(2, vector, microsoft.sql.Types.VECTOR);
            pstmt.executeUpdate();
        }

        try (PreparedStatement pstmt = connection.prepareStatement(selectSQL)) {
            pstmt.setInt(1, 1);
            try (ResultSet rs = pstmt.executeQuery()) {
                assertTrue(rs.next());
                Vector resultVector = rs.getObject(1, Vector.class);
                assertNotNull(resultVector, "Retrieved vector is null.");
                assertVectorDataEquals(createTestData(1.0f, 2.0f, 3.0f), resultVector.getData(), "Vector data mismatch.");
            }
        }

        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(dstTable, stmt);
        }
    }

    @Test
    public void testVectorInsertionInLocalTempTable() throws SQLException {
        String connStr = connectionString + ";vectorTypeSupport=" + getRequiredVectorTypeSupport();
        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connStr)) {
            String dstTable = TestUtils.escapeSingleQuotes(
                    AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("#TempVector_" + uuid.substring(0, 8))));
            String createTableSQL = "CREATE TABLE " + dstTable + " (id INT PRIMARY KEY, data " + getColumnDefinition(3) + ")";
            String insertSQL = "INSERT INTO " + dstTable + " VALUES (?, ?)";
            String selectSQL = "SELECT data FROM " + dstTable + " WHERE id = ?";

            try (Statement stmt = conn.createStatement()) {
                stmt.execute(createTableSQL);
            }

            try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                Float[] vectorData = createTestData(4.0f, 5.0f, 6.0f);
                Vector vector = new Vector(3, getVectorDimensionType(), vectorData);

                pstmt.setInt(1, 1);
                pstmt.setObject(2, vector, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            try (PreparedStatement pstmt = conn.prepareStatement(selectSQL)) {
                pstmt.setInt(1, 1);
                try (ResultSet rs = pstmt.executeQuery()) {
                    assertTrue(rs.next());
                    Vector resultVector = rs.getObject(1, Vector.class);
                    assertNotNull(resultVector, "Retrieved vector is null.");
                    assertVectorDataEquals(createTestData(4.0f, 5.0f, 6.0f), resultVector.getData(), "Vector data mismatch.");
                }
            }
        }
    }

    // ============================================================================
    // Transaction Tests
    // ============================================================================

    @Test
    public void testTransactionRollbackForVector() throws SQLException {
        String transactionTable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("transactionTable_" + uuid.substring(0, 8)));

        try (Statement outerStmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(transactionTable, outerStmt);
            String createTableSQL = "CREATE TABLE " + transactionTable + " (id INT PRIMARY KEY, v " + getColumnDefinition(3) + ")";
            outerStmt.executeUpdate(createTableSQL);

            Float[] initialData = createTestData(1.0f, 2.0f, 3.0f);
            Vector initialVector = new Vector(3, getVectorDimensionType(), initialData);

            try (PreparedStatement pstmt = connection.prepareStatement(
                    "INSERT INTO " + transactionTable + " (id, v) VALUES (?, ?)")) {
                pstmt.setInt(1, 1);
                pstmt.setObject(2, initialVector, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            connection.setAutoCommit(false);
            try {
                Float[] newData = createTestData(4.0f, 5.0f, 6.0f);
                Vector newVector = new Vector(3, getVectorDimensionType(), newData);

                try (PreparedStatement pstmt = connection.prepareStatement(
                        "INSERT INTO " + transactionTable + " (id, v) VALUES (?, ?)")) {
                    pstmt.setInt(1, 2);
                    pstmt.setObject(2, newVector, microsoft.sql.Types.VECTOR);
                    pstmt.executeUpdate();
                }

                Float[] updatedData = createTestData(7.0f, 8.0f, 9.0f);
                Vector updatedVector = new Vector(3, getVectorDimensionType(), updatedData);

                try (PreparedStatement pstmt = connection.prepareStatement(
                        "UPDATE " + transactionTable + " SET v = ? WHERE id = ?")) {
                    pstmt.setObject(1, updatedVector, microsoft.sql.Types.VECTOR);
                    pstmt.setInt(2, 1);
                    pstmt.executeUpdate();
                }

                try (PreparedStatement pstmt = connection.prepareStatement(
                        "DELETE FROM " + transactionTable + " WHERE id = ?")) {
                    pstmt.setInt(1, 2);
                    pstmt.executeUpdate();
                }

                throw new RuntimeException("Simulated failure to trigger rollback");

            } catch (RuntimeException e) {
                connection.rollback();
            } finally {
                connection.setAutoCommit(true);
            }

            String validateSql = "SELECT id, v FROM " + transactionTable + " ORDER BY id";
            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery(validateSql)) {

                assertTrue(rs.next(), "No data found in the table after rollback.");
                int id = rs.getInt("id");
                Vector resultVector = rs.getObject("v", Vector.class);

                assertEquals(1, id, "ID mismatch after rollback.");
                assertNotNull(resultVector, "Vector is null after rollback.");
                assertVectorDataEquals(initialData, resultVector.getData(), "Vector data mismatch after rollback.");

                assertFalse(rs.next(), "Unexpected additional rows found after rollback.");
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(transactionTable, stmt);
            }
        }
    }

    // ============================================================================
    // View Tests
    // ============================================================================

    @Test
    public void testViewWithVectorDataType() throws SQLException {
        String viewTableName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("VectorTable_" + uuid.substring(0, 8)));
        String viewName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("VectorView_" + uuid.substring(0, 8)));

        try (Statement stmt = connection.createStatement()) {
            String createTableSQL = "CREATE TABLE " + viewTableName + " (id INT PRIMARY KEY, v " + getColumnDefinition(3) + ")";
            stmt.executeUpdate(createTableSQL);

            Float[] vectorData1 = createTestData(1.1f, 2.2f, 3.3f);
            Vector vector1 = new Vector(3, getVectorDimensionType(), vectorData1);

            Float[] vectorData2 = createTestData(4.4f, 5.5f, 6.6f);
            Vector vector2 = new Vector(3, getVectorDimensionType(), vectorData2);

            String insertSQL = "INSERT INTO " + viewTableName + " (id, v) VALUES (?, ?)";
            try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) {
                pstmt.setInt(1, 1);
                pstmt.setObject(2, vector1, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();

                pstmt.setInt(1, 2);
                pstmt.setObject(2, vector2, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            String createViewSQL = "CREATE VIEW " + viewName + " AS SELECT id, v FROM " + viewTableName;
            stmt.executeUpdate(createViewSQL);

            String queryViewSQL = "SELECT id, v FROM " + viewName + " ORDER BY id";
            try (ResultSet rs = stmt.executeQuery(queryViewSQL)) {
                int rowCount = 0;
                while (rs.next()) {
                    int id = rs.getInt("id");
                    Vector resultVector = rs.getObject("v", Vector.class);

                    assertNotNull(resultVector, "Vector is null in view for ID " + id);

                    if (id == 1) {
                        assertVectorDataEquals(vectorData1, resultVector.getData(), "Vector data mismatch in view for ID 1.");
                    } else if (id == 2) {
                        assertVectorDataEquals(vectorData2, resultVector.getData(), "Vector data mismatch in view for ID 2.");
                    } else {
                        fail("Unexpected ID found in view: " + id);
                    }
                    rowCount++;
                }
                assertEquals(2, rowCount, "Row count mismatch in view.");
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropViewIfExists(viewName, stmt);
                TestUtils.dropTableIfExists(viewTableName, stmt);
            }
        }
    }

    // ============================================================================
    // Metadata Tests
    // ============================================================================

    /**
     * Test to verify that the vector column is present in DatabaseMetaData.getColumns().
     * 
     * Note: The DATA_TYPE value returned by the JDBC driver may differ from the SQL Server
     * native type code. To retrieve the actual SQL Server data type, access the
     * {@code SQL_DATA_TYPE} column from the result set.
     */
    @Test
    @Tag(Constants.xAzureSQLDB)
    @Tag(Constants.xAzureSQLDW)
    public void testConnectionGetMetaData() throws Exception {
        DatabaseMetaData metaData = connection.getMetaData();
        assertNotNull(metaData, "DatabaseMetaData should not be null");

        try (ResultSet rs = metaData.getColumns(null, null, tableName, "%")) {
            boolean vectorColumnFound = false;
            while (rs.next()) {
                if ("v".equalsIgnoreCase(rs.getString("COLUMN_NAME"))) {
                    vectorColumnFound = true;
                }
            }
            assertTrue(vectorColumnFound, "Vector column 'v' found in metadata");
        }
    }

    /**
     * Test to verify that the vector column is present in DatabaseMetaData.getColumns() for Azure DW.
     * Added this to increase code coverage for Azure DW code path in getColumns method.
     */
    @Test
    @Tag(Constants.xAzureSQLDB)
    @Tag(Constants.xAzureSQLDW)
    public void testConnectionGetMetaDataAzureDW() throws Exception {
        try (SQLServerConnection conn = getConnection()) {

            // Use reflection to simulate Azure DW connection
            Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
            f1.setAccessible(true);
            f1.set(conn, true);

            Field f2 = SQLServerConnection.class.getDeclaredField("isAzure");
            f2.setAccessible(true);
            f2.set(conn, true);

            DatabaseMetaData metaData = conn.getMetaData();
            assertNotNull(metaData, "DatabaseMetaData should not be null");

            try (ResultSet rs = metaData.getColumns(null, null, tableName, "%")) {
                boolean vectorColumnFound = false;
                while (rs.next()) {
                    if ("v".equalsIgnoreCase(rs.getString("COLUMN_NAME"))) {
                        vectorColumnFound = true;
                    }
                }
                assertTrue(vectorColumnFound, "Vector column 'v' found in metadata");
            }
        }
    }


    @Test
    public void validatePreparedStatementMetaData() throws SQLException {
        String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (id, v) VALUES (?, ?)";
        Float[] originalData = createTestData(0.45f, 7.9f, 63.0f);
        Vector initialVector = new Vector(3, getVectorDimensionType(), originalData);

        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            ParameterMetaData pMetaData = pstmt.getParameterMetaData();

            pstmt.setInt(1, 1);
            pstmt.setObject(2, initialVector, microsoft.sql.Types.VECTOR);
            pstmt.executeUpdate();

            assertNotNull(pMetaData, "ParameterMetaData should not be null");
            assertEquals(2, pMetaData.getParameterCount(), "Parameter count mismatch.");

            assertEquals(java.sql.Types.INTEGER, pMetaData.getParameterType(1),
                    "Parameter type mismatch for integer column.");
            assertEquals("int", pMetaData.getParameterTypeName(1), "Parameter type name mismatch for integer column.");

            assertEquals(microsoft.sql.Types.VECTOR, pMetaData.getParameterType(2),
                    "Parameter type mismatch for vector column.");
            assertEquals("vector", pMetaData.getParameterTypeName(2), "Parameter type name mismatch for vector column.");
        }
    }

    // ============================================================================
    // Helper Methods
    // ============================================================================
}
