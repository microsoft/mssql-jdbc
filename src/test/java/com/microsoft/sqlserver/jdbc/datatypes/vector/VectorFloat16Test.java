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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerDataTable;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.vectorJsonTest;

import microsoft.sql.Vector;
import microsoft.sql.Vector.VectorDimensionType;

/**
 * Test class for FLOAT16 vector data type.
 *
 * This class extends {@link VectorTest} and provides FLOAT16-specific
 * configuration. All single-type FLOAT16 tests are inherited from the abstract
 * base class. In addition, the {@link MixedVectorTypeTest} nested class validates
 * scenarios involving tables with both FLOAT32 and FLOAT16 vector columns, including
 * DML, stored procedures, UDFs, transactions, savepoints, metadata, temp tables,
 * views, and table-to-table copy operations.
 *
 * - SQL Syntax: VECTOR(n, float16) where n is the dimension count
 * - Scale: 2 (FLOAT16)
 * - Max Dimensions: 3996
 *
 * Note: This test class is tagged with {@link Constants#vectorFloat16Test}
 * for pipeline exclusion since the server does not yet support FLOAT16 vectors.
 */
@RunWith(JUnitPlatform.class)
@DisplayName("Test Vector Data Type - FLOAT16")
@vectorJsonTest
@Tag(Constants.vectorTest)
@Tag(Constants.vectorFloat16Test)
public class VectorFloat16Test extends VectorTest {

    @Override
    protected VectorDimensionType getVectorDimensionType() {
        return VectorDimensionType.FLOAT16;
    }

    @Override
    protected String getColumnDefinition(int dimensionCount) {
        // FLOAT16 requires explicit type specification
        return "VECTOR(" + dimensionCount + ", float16)";
    }

    @Override
    protected int getScale() {
        return 2; // FLOAT16 scale
    }

    @Override
    protected String getTypeName() {
        return "FLOAT16";
    }

    /**
     * The server implementation restricts vectors to a total of 8000 bytes.  
     * Subtracting the 8 byte header leaves 7992 bytes for data.  
     * For float16, each dimension requires 2 bytes, so the maximum number of dimensions is 3996.
     * Calculation:  (3996 * 2) + 8 == 8000 
     */
    @Override
    protected int getMaxDimensionCount() { 
        return 3996;
    }

    @Override
    protected String getRequiredVectorTypeSupport() {
        return "v2"; // FLOAT16 requires v2
    }

    // ============================================================================
    // FLOAT16-Specific Tests
    // ============================================================================

    /**
     * Tests that inserting a FLOAT32 vector is rejected on a v0 (off) connection.
     * When vectorTypeSupport is "off", the server does not negotiate vector support
     * and any vector insert should throw an IllegalArgumentException.
     */
    @Test
    public void testFloat32VectorRejectedOnV0Connection() throws SQLException {
        String connStr = connectionString + ";vectorTypeSupport=off";
        try (SQLServerConnection v0Connection = (SQLServerConnection) DriverManager.getConnection(connStr)) {
            Assumptions.assumeTrue(v0Connection.getNegotiatedVectorVersion() == 0,
                    "Expected negotiated vector version 0 for 'off' connection. Skipping test.");

            String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (id, v) VALUES (?, ?)";
            Float[] data = createTestData(1.0f, 2.0f, 3.0f);
            Vector float32Vector = new Vector(3, VectorDimensionType.FLOAT32, data);

            try (PreparedStatement pstmt = v0Connection.prepareStatement(insertSql)) {
                pstmt.setInt(1, 1);
                pstmt.setObject(2, float32Vector, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
                fail("Expected IllegalArgumentException when inserting FLOAT32 vector on v0 connection.");
            } catch (IllegalArgumentException e) {
                assertTrue(e.getMessage().contains("Vector type is not supported by the server"),
                        "Expected vectorNotSupported error, but got: " + e.getMessage());
            }
        }
    }

    /**
     * Tests that inserting a FLOAT16 vector is rejected on a v0 (off) connection.
     * When vectorTypeSupport is "off", the server does not negotiate vector support
     * and any vector insert should throw an IllegalArgumentException.
     */
    @Test
    public void testFloat16VectorRejectedOnV0Connection() throws SQLException {
        String connStr = connectionString + ";vectorTypeSupport=off";
        try (SQLServerConnection v0Connection = (SQLServerConnection) DriverManager.getConnection(connStr)) {
            Assumptions.assumeTrue(v0Connection.getNegotiatedVectorVersion() == 0,
                    "Expected negotiated vector version 0 for 'off' connection. Skipping test.");

            String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (id, v) VALUES (?, ?)";
            Float[] data = createTestData(1.0f, 2.0f, 3.0f);
            Vector float16Vector = new Vector(3, VectorDimensionType.FLOAT16, data);

            try (PreparedStatement pstmt = v0Connection.prepareStatement(insertSql)) {
                pstmt.setInt(1, 1);
                pstmt.setObject(2, float16Vector, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
                fail("Expected IllegalArgumentException when inserting FLOAT16 vector on v0 connection.");
            } catch (IllegalArgumentException e) {
                assertTrue(e.getMessage().contains("Vector type is not supported by the server"),
                        "Expected vectorNotSupported error, but got: " + e.getMessage());
            }
        }
    }

    /**
     * Tests that inserting a FLOAT16 vector is rejected when using a v1 connection.
     * Even though this test class normally uses a v2 connection, this test explicitly
     * creates a v1 connection to verify the FLOAT16 validation guard in getTypeDefinition.
     */
    @Test
    public void testFloat16VectorRejectedOnV1Connection() throws SQLException {
        String connStr = connectionString + ";vectorTypeSupport=v1";
        try (SQLServerConnection v1Connection = (SQLServerConnection) DriverManager.getConnection(connStr)) {
            Assumptions.assumeTrue(v1Connection.getNegotiatedVectorVersion() >= 1,
                    "Server did not negotiate vector version 1. Skipping test.");

            String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (id, v) VALUES (?, ?)";
            Float[] data = createTestData(1.0f, 2.0f, 3.0f);
            Vector float16Vector = new Vector(3, VectorDimensionType.FLOAT16, data);

            try (PreparedStatement pstmt = v1Connection.prepareStatement(insertSql)) {
                pstmt.setInt(1, 1);
                pstmt.setObject(2, float16Vector, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
                fail("Expected IllegalArgumentException when inserting FLOAT16 vector on v1 connection.");
            } catch (IllegalArgumentException e) {
                assertTrue(e.getMessage().contains("FLOAT16 vector type is not supported"),
                        "Expected FLOAT16 not supported error, but got: " + e.getMessage());
            }
        }
    }

    /**
     * Validates basic vector data insert and retrieve for FLOAT16.
     * FLOAT16 has reduced precision (10-bit mantissa) so values may differ slightly after round-trip.
     */
    @Test
    public void validateVectorData() throws java.sql.SQLException {
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

                Vector resultVector = rs.getObject("v", Vector.class);
                assertNotNull(resultVector, "Retrieved vector is null.");
                assertEquals(3, resultVector.getDimensionCount(), "Dimension count mismatch.");
                assertVectorDataEquals(originalData, resultVector.getData(), "Vector data mismatch.");

                // FLOAT16 has precision loss, verify toString format
                String expectedToString = "VECTOR(FLOAT16, 3) : [0.44995117, 7.8984375, 63.0]";
                assertEquals(expectedToString, resultVector.toString(), "Vector toString output mismatch.");
            }
        }
    }

    /**
     * Verifies that inserting Float.MAX_VALUE into a FLOAT16 vector is rejected by the server.
     * Float.MAX_VALUE (~3.4e38) exceeds FLOAT16 max (~65504), overflows to infinity, and the
     * server returns an error indicating the dimensions are not valid half precision floats.
     */
    @Test
    public void testVectorWithMaxAndMinFloatValues() throws SQLException {
        String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                + " (id, v) VALUES (?, ?)";
        Float[] extremeData = createTestData(Float.MAX_VALUE, Float.MIN_VALUE, 0.0f);
        Vector vector = new Vector(3, getVectorDimensionType(), extremeData);

        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 1);
            pstmt.setObject(2, vector, microsoft.sql.Types.VECTOR);
            pstmt.executeUpdate();
            fail("Expected SQLServerException for out-of-range FLOAT16 values, but insert succeeded.");
        } catch (SQLServerException e) {
            // Note: The server error message contains a typo ("precison" instead of "precision")
            assertTrue(e.getMessage().contains("not a valid half precison floating point number"),
                    "Expected error about invalid half precision float, but got: " + e.getMessage());
        }
    }

    /**
     * Validates that FLOAT16 max range boundary values can be inserted and retrieved.
     * FLOAT16 max: 65504, min positive (subnormal): ~5.96e-8.
     */
    @Test
    public void testVectorWithFloat16RangeBoundary() throws SQLException {
        // 65504 is the max representable finite FLOAT16 value
        // 5.96046448e-8 is the smallest positive subnormal FLOAT16 value
        String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                + " (id, v) VALUES (?, ?)";
        Float[] boundaryData = createTestData(65504.0f, -65504.0f, 5.96046448e-8f);
        Vector vector = new Vector(3, getVectorDimensionType(), boundaryData);

        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 1);
            pstmt.setObject(2, vector, microsoft.sql.Types.VECTOR);
            pstmt.executeUpdate();
        }

        String query = "SELECT id, v FROM " + AbstractSQLGenerator.escapeIdentifier(tableName) + " WHERE id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, 1);
            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue(rs.next(), "No result found for inserted vector with FLOAT16 boundary values.");

                Vector resultVector = rs.getObject("v", Vector.class);
                assertNotNull(resultVector, "Retrieved vector is null.");
                assertEquals(3, resultVector.getDimensionCount(), "Dimension count mismatch.");
                assertVectorDataEquals(boundaryData, resultVector.getData(),
                        "Vector data mismatch for FLOAT16 range boundary values.");
            }
        }
    }

    // ============================================================================
    // Cross-Type Conversion Failure Tests (FLOAT32 <-> FLOAT16)
    // ============================================================================

    /**
     * Tests that INSERT INTO a FLOAT32 table by SELECTing from a FLOAT16 table
     * is rejected by the server with a conversion error.
     *
     * SQL Server does not allow implicit conversion between vector subtypes:
     * "Conversion of vector from data type float16 to float32 is not allowed."
     */
    @Test
    public void testInsertFloat32FromFloat16TableFails() throws SQLException {
        String f16Table = AbstractSQLGenerator.escapeIdentifier(
                RandomUtil.getIdentifier("F16_CrossType_" + uuid.substring(0, 8)));
        String f32Table = AbstractSQLGenerator.escapeIdentifier(
                RandomUtil.getIdentifier("F32_CrossType_" + uuid.substring(0, 8)));

        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + f16Table + " (f16_col VECTOR(3, float16))");
            stmt.executeUpdate("CREATE TABLE " + f32Table + " (f32_col VECTOR(3))");

            // Seed the FLOAT16 table
            stmt.executeUpdate("INSERT INTO " + f16Table + " (f16_col) VALUES ('[0.3, 0.2, 0.1]')");

            // Attempt cross-type insert: FLOAT16 -> FLOAT32 should fail
            try {
                stmt.executeUpdate("INSERT INTO " + f32Table + " (f32_col) SELECT f16_col FROM " + f16Table);
                fail("Expected SQLException for cross-type vector conversion (float16 -> float32).");
            } catch (SQLException e) {
                assertTrue(e.getMessage().contains("Conversion of vector from data type float16 to float32 is not allowed."),
                        "Expected vector conversion error, but got: " + e.getMessage());
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(f32Table, stmt);
                TestUtils.dropTableIfExists(f16Table, stmt);
            }
        }
    }

    /**
     * Tests that INSERT INTO a FLOAT16 table by SELECTing from a FLOAT32 table
     * is rejected by the server with a conversion error.
     *
     * SQL Server does not allow implicit conversion between vector subtypes:
     * "Conversion of vector from data type float32 to float16 is not allowed."
     */
    @Test
    public void testInsertFloat16FromFloat32TableFails() throws SQLException {
        String f16Table = AbstractSQLGenerator.escapeIdentifier(
                RandomUtil.getIdentifier("F16_CrossType2_" + uuid.substring(0, 8)));
        String f32Table = AbstractSQLGenerator.escapeIdentifier(
                RandomUtil.getIdentifier("F32_CrossType2_" + uuid.substring(0, 8)));

        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + f16Table + " (f16_col VECTOR(3, float16))");
            stmt.executeUpdate("CREATE TABLE " + f32Table + " (f32_col VECTOR(3))");

            // Seed the FLOAT32 table
            stmt.executeUpdate("INSERT INTO " + f32Table + " (f32_col) VALUES ('[0.3, 0.2, 0.1]')");

            // Attempt cross-type insert: FLOAT32 -> FLOAT16 should fail
            try {
                stmt.executeUpdate("INSERT INTO " + f16Table + " (f16_col) SELECT f32_col FROM " + f32Table);
                fail("Expected SQLException for cross-type vector conversion (float32 -> float16).");
            } catch (SQLException e) {
                assertTrue(e.getMessage().contains("Conversion of vector from data type float32 to float16 is not allowed."),
                        "Expected vector conversion error, but got: " + e.getMessage());
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(f32Table, stmt);
                TestUtils.dropTableIfExists(f16Table, stmt);
            }
        }
    }

    /**
     * Tests that reading a Vector object from a FLOAT32 column and using that
     * object as-is via setObject to insert into a FLOAT16 column is rejected
     * by the server with a conversion error.
     *
     * The driver faithfully sends the FLOAT32 Vector (scale=4) to the server,
     * which rejects the type mismatch:
     * "Conversion of vector from data type float32 to float16 is not allowed."
     */
    @Test
    public void testSetObjectFloat32VectorIntoFloat16ColumnFails() throws SQLException {
        String f32Table = AbstractSQLGenerator.escapeIdentifier(
                RandomUtil.getIdentifier("F32_SetObj_" + uuid.substring(0, 8)));
        String f16Table = AbstractSQLGenerator.escapeIdentifier(
                RandomUtil.getIdentifier("F16_SetObj_" + uuid.substring(0, 8)));

        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + f32Table + " (id INT PRIMARY KEY, v VECTOR(3))");
            stmt.executeUpdate("CREATE TABLE " + f16Table + " (id INT PRIMARY KEY, v VECTOR(3, float16))");

            // Seed a FLOAT32 row
            stmt.executeUpdate("INSERT INTO " + f32Table + " (id, v) VALUES (1, '[1.0, 2.0, 3.0]')");

            // Read the Vector object from the FLOAT32 column
            Vector float32Vector;
            try (ResultSet rs = stmt.executeQuery("SELECT v FROM " + f32Table + " WHERE id = 1")) {
                assertTrue(rs.next(), "No result found in FLOAT32 table.");
                float32Vector = rs.getObject("v", Vector.class);
                assertNotNull(float32Vector, "Retrieved FLOAT32 vector is null.");
                assertEquals(VectorDimensionType.FLOAT32, float32Vector.getVectorDimensionType(),
                        "Expected FLOAT32 vector type.");
            }

            // Use the FLOAT32 Vector object as-is to insert into the FLOAT16 column
            String insertSql = "INSERT INTO " + f16Table + " (id, v) VALUES (?, ?)";
            try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
                pstmt.setInt(1, 1);
                pstmt.setObject(2, float32Vector, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
                fail("Expected SQLException when inserting FLOAT32 Vector object into FLOAT16 column.");
            } catch (SQLException e) {
                assertTrue(e.getMessage().contains("Conversion of vector from data type float32 to float16 is not allowed."),
                        "Expected vector conversion error, but got: " + e.getMessage());
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(f16Table, stmt);
                TestUtils.dropTableIfExists(f32Table, stmt);
            }
        }
    }

    /**
     * Tests that reading a Vector object from a FLOAT16 column and using that
     * object as-is via setObject to insert into a FLOAT32 column is rejected
     * by the server with a conversion error.
     *
     * The driver faithfully sends the FLOAT16 Vector (scale=2) to the server,
     * which rejects the type mismatch:
     * "Conversion of vector from data type float16 to float32 is not allowed."
     */
    @Test
    public void testSetObjectFloat16VectorIntoFloat32ColumnFails() throws SQLException {
        String f16Table = AbstractSQLGenerator.escapeIdentifier(
                RandomUtil.getIdentifier("F16_SetObj2_" + uuid.substring(0, 8)));
        String f32Table = AbstractSQLGenerator.escapeIdentifier(
                RandomUtil.getIdentifier("F32_SetObj2_" + uuid.substring(0, 8)));

        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + f16Table + " (id INT PRIMARY KEY, v VECTOR(3, float16))");
            stmt.executeUpdate("CREATE TABLE " + f32Table + " (id INT PRIMARY KEY, v VECTOR(3))");

            // Seed a FLOAT16 row
            stmt.executeUpdate("INSERT INTO " + f16Table + " (id, v) VALUES (1, '[1.0, 2.0, 3.0]')");

            // Read the Vector object from the FLOAT16 column
            Vector float16Vector;
            try (ResultSet rs = stmt.executeQuery("SELECT v FROM " + f16Table + " WHERE id = 1")) {
                assertTrue(rs.next(), "No result found in FLOAT16 table.");
                float16Vector = rs.getObject("v", Vector.class);
                assertNotNull(float16Vector, "Retrieved FLOAT16 vector is null.");
                assertEquals(VectorDimensionType.FLOAT16, float16Vector.getVectorDimensionType(),
                        "Expected FLOAT16 vector type.");
            }

            // Use the FLOAT16 Vector object as-is to insert into the FLOAT32 column
            String insertSql = "INSERT INTO " + f32Table + " (id, v) VALUES (?, ?)";
            try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
                pstmt.setInt(1, 1);
                pstmt.setObject(2, float16Vector, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
                fail("Expected SQLException when inserting FLOAT16 Vector object into FLOAT32 column.");
            } catch (SQLException e) {
                assertTrue(e.getMessage().contains("Conversion of vector from data type float16 to float32 is not allowed."),
                        "Expected vector conversion error, but got: " + e.getMessage());
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(f32Table, stmt);
                TestUtils.dropTableIfExists(f16Table, stmt);
            }
        }
    }

    // ============================================================================
    // Mixed Vector Type Tests (FLOAT32 + FLOAT16 in same table)
    // ============================================================================

    /**
     * Nested test class for testing tables that contain both FLOAT32 and FLOAT16
     * vector columns. These tests require a v2 connection since FLOAT16 needs v2.
     *
     * This validates the driver correctly handles mixed vector types when:
     * - Inserting and retrieving rows with both column types
     * - Handling NULLs in one or both columns
     * - Using stored procedures with mixed-type parameters
     * - Using TVPs with mixed-type columns
     * - Updating individual columns without affecting the other type
     */
    @Nested
    @DisplayName("Mixed FLOAT32 + FLOAT16 Vector Tests")
    @Tag(Constants.vectorTest)
    @Tag(Constants.vectorFloat16Test)
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class MixedVectorTypeTest {

        private SQLServerConnection mixedConnection;
        private String mixedTableName;
        private String mixedProcedureName;
        private String mixedTvpTableName;
        private String mixedTvpTypeName;
        private final String mixedUuid = UUID.randomUUID().toString().replaceAll("-", "");

        // Setup before all tests in this nested class run.
        // Creates a v2 connection and necessary database objects for mixed-type testing.
        @BeforeAll
        public void setupMixedTests() throws Exception {
            String connStr = connectionString + ";vectorTypeSupport=v2";
            mixedConnection = (SQLServerConnection) DriverManager.getConnection(connStr);

            byte negotiatedVersion = mixedConnection.getNegotiatedVectorVersion();
            Assumptions.assumeTrue(negotiatedVersion >= 2,
                    "Server negotiated vector version " + negotiatedVersion
                            + ", but mixed-type tests require version 2. Skipping tests.");

            String suffix = "Mixed_" + mixedUuid.substring(0, 8);
            mixedTableName = RandomUtil.getIdentifier("VECTOR_Mixed_" + suffix);
            mixedProcedureName = RandomUtil.getIdentifier("VECTOR_Mixed_Proc_" + suffix);
            mixedTvpTableName = RandomUtil.getIdentifier("VECTOR_Mixed_TVP_" + suffix);
            mixedTvpTypeName = RandomUtil.getIdentifier("VECTOR_Mixed_TVP_Type_" + suffix);

            try (Statement stmt = mixedConnection.createStatement()) {
                // Table with both FLOAT32 (VECTOR(3)) and FLOAT16 (VECTOR(3, float16)) columns
                stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(mixedTableName)
                        + " (id INT PRIMARY KEY,"
                        + " v_float32 VECTOR(3),"
                        + " v_float16 VECTOR(3, float16))");

                stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(mixedTvpTableName)
                        + " (rowId INT IDENTITY,"
                        + " c_float32 VECTOR(3) NULL,"
                        + " c_float16 VECTOR(3, float16) NULL)");

                stmt.executeUpdate("CREATE TYPE " + AbstractSQLGenerator.escapeIdentifier(mixedTvpTypeName)
                        + " AS TABLE ("
                        + " c_float32 VECTOR(3) NULL,"
                        + " c_float16 VECTOR(3, float16) NULL)");
            }
        }

        // Cleanup after all tests in this class have run
        @AfterAll
        public void cleanupMixedTests() throws SQLException {
            if (mixedConnection != null && !mixedConnection.isClosed()) {
                try (Statement stmt = mixedConnection.createStatement()) {
                    TestUtils.dropProcedureIfExists(mixedProcedureName, stmt);
                    TestUtils.dropTableIfExists(mixedTableName, stmt);
                    TestUtils.dropTableIfExists(mixedTvpTableName, stmt);
                    TestUtils.dropTypeIfExists(mixedTvpTypeName, stmt);
                }
                mixedConnection.close();
            }
        }

        // Cleanup after each test to ensure isolation between tests
        @AfterEach
        public void cleanupAfterEachMixedTest() throws SQLException {
            try (Statement stmt = mixedConnection.createStatement()) {
                stmt.executeUpdate("DELETE FROM " + AbstractSQLGenerator.escapeIdentifier(mixedTableName));
                stmt.executeUpdate("DELETE FROM " + AbstractSQLGenerator.escapeIdentifier(mixedTvpTableName));
            }
        }

        // Helper to create Float[] test data
        private Float[] data(float... values) {
            Float[] arr = new Float[values.length];
            for (int i = 0; i < values.length; i++) {
                arr[i] = values[i];
            }
            return arr;
        }

        /**
         * Compares vector data with tolerance appropriate for the vector's type.
         * FLOAT16 gets relative tolerance; FLOAT32 gets exact comparison.
         */
        private void assertMixedVectorEquals(Object[] expected, Object[] actual,
                VectorDimensionType type, String message) {
            if (expected == null) {
                assertNull(actual, message + " - expected null but actual is not null");
                return;
            }
            assertNotNull(actual, message + " - actual is null");
            assertEquals(expected.length, actual.length, message + " - length mismatch");

            boolean isFloat16 = (type == VectorDimensionType.FLOAT16);
            for (int i = 0; i < expected.length; i++) {
                float expectedVal = (Float) expected[i];
                float actualVal = (Float) actual[i];
                float tolerance = isFloat16 ? Math.max(0.0005f * Math.abs(expectedVal), 1e-7f) : 0.0f;
                assertEquals(expectedVal, actualVal, tolerance,
                        message + " at index " + i + ": expected " + expectedVal + " but was " + actualVal);
            }
        }

        // ====================================================================
        // Insert and Retrieve Tests
        // ====================================================================

        /**
         * Tests inserting and retrieving a row with both FLOAT32 and FLOAT16 vector columns.
         * Validates that each column preserves its respective type and data.
         */
        @Test
        public void testMixedFloat32AndFloat16Insert() throws SQLException {
            String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(mixedTableName)
                    + " (id, v_float32, v_float16) VALUES (?, ?, ?)";
            Float[] float32Data = data(1.0f, 2.0f, 3.0f);
            Float[] float16Data = data(0.5f, 1.5f, 2.5f);
            Vector f32Vector = new Vector(3, VectorDimensionType.FLOAT32, float32Data);
            Vector f16Vector = new Vector(3, VectorDimensionType.FLOAT16, float16Data);

            try (PreparedStatement pstmt = mixedConnection.prepareStatement(insertSql)) {
                pstmt.setInt(1, 1);
                pstmt.setObject(2, f32Vector, microsoft.sql.Types.VECTOR);
                pstmt.setObject(3, f16Vector, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            String query = "SELECT id, v_float32, v_float16 FROM "
                    + AbstractSQLGenerator.escapeIdentifier(mixedTableName) + " WHERE id = ?";
            try (PreparedStatement stmt = mixedConnection.prepareStatement(query)) {
                stmt.setInt(1, 1);
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next(), "No result found for mixed-type insert.");

                    Vector resultF32 = rs.getObject("v_float32", Vector.class);
                    assertNotNull(resultF32, "FLOAT32 vector is null.");
                    assertEquals(3, resultF32.getDimensionCount(), "FLOAT32 dimension count mismatch.");
                    assertEquals(VectorDimensionType.FLOAT32, resultF32.getVectorDimensionType(),
                            "Expected FLOAT32 type.");
                    assertMixedVectorEquals(float32Data, resultF32.getData(),
                            VectorDimensionType.FLOAT32, "FLOAT32 data mismatch");

                    Vector resultF16 = rs.getObject("v_float16", Vector.class);
                    assertNotNull(resultF16, "FLOAT16 vector is null.");
                    assertEquals(3, resultF16.getDimensionCount(), "FLOAT16 dimension count mismatch.");
                    assertEquals(VectorDimensionType.FLOAT16, resultF16.getVectorDimensionType(),
                            "Expected FLOAT16 type.");
                    assertMixedVectorEquals(float16Data, resultF16.getData(),
                            VectorDimensionType.FLOAT16, "FLOAT16 data mismatch");
                }
            }
        }

        /**
         * Tests multiple rows with both FLOAT32 and FLOAT16 columns to validate
         * consistent handling across rows.
         */
        @Test
        public void testMixedFloat32AndFloat16MultipleRows() throws SQLException {
            String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(mixedTableName)
                    + " (id, v_float32, v_float16) VALUES (?, ?, ?)";

            Float[][] f32Rows = {data(1.0f, 2.0f, 3.0f), data(4.0f, 5.0f, 6.0f), data(7.0f, 8.0f, 9.0f)};
            Float[][] f16Rows = {data(0.1f, 0.2f, 0.3f), data(10.0f, 20.0f, 30.0f), data(100.0f, 200.0f, 300.0f)};

            try (PreparedStatement pstmt = mixedConnection.prepareStatement(insertSql)) {
                for (int i = 0; i < 3; i++) {
                    pstmt.setInt(1, i + 1);
                    pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT32, f32Rows[i]),
                            microsoft.sql.Types.VECTOR);
                    pstmt.setObject(3, new Vector(3, VectorDimensionType.FLOAT16, f16Rows[i]),
                            microsoft.sql.Types.VECTOR);
                    pstmt.executeUpdate();
                }
            }

            String query = "SELECT id, v_float32, v_float16 FROM "
                    + AbstractSQLGenerator.escapeIdentifier(mixedTableName) + " ORDER BY id";
            try (Statement stmt = mixedConnection.createStatement();
                    ResultSet rs = stmt.executeQuery(query)) {
                for (int i = 0; i < 3; i++) {
                    assertTrue(rs.next(), "Expected row " + (i + 1));
                    Vector f32 = rs.getObject("v_float32", Vector.class);
                    Vector f16 = rs.getObject("v_float16", Vector.class);
                    assertMixedVectorEquals(f32Rows[i], f32.getData(),
                            VectorDimensionType.FLOAT32, "FLOAT32 row " + (i + 1));
                    assertMixedVectorEquals(f16Rows[i], f16.getData(),
                            VectorDimensionType.FLOAT16, "FLOAT16 row " + (i + 1));
                }
            }
        }

        // ====================================================================
        // Null Handling Tests
        // ====================================================================

        /**
         * Tests inserting NULL in the FLOAT16 column while FLOAT32 has data.
         */
        @Test
        public void testMixedFloat32DataFloat16Null() throws SQLException {
            String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(mixedTableName)
                    + " (id, v_float32, v_float16) VALUES (?, ?, ?)";
            Float[] float32Data = data(1.0f, 2.0f, 3.0f);

            try (PreparedStatement pstmt = mixedConnection.prepareStatement(insertSql)) {
                pstmt.setInt(1, 1);
                pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT32, float32Data),
                        microsoft.sql.Types.VECTOR);
                pstmt.setObject(3, new Vector(3, VectorDimensionType.FLOAT16, null),
                        microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            String query = "SELECT v_float32, v_float16 FROM "
                    + AbstractSQLGenerator.escapeIdentifier(mixedTableName) + " WHERE id = 1";
            try (Statement stmt = mixedConnection.createStatement();
                    ResultSet rs = stmt.executeQuery(query)) {
                assertTrue(rs.next(), "No result found.");
                Vector f32 = rs.getObject("v_float32", Vector.class);
                Vector f16 = rs.getObject("v_float16", Vector.class);
                assertMixedVectorEquals(float32Data, f32.getData(),
                        VectorDimensionType.FLOAT32, "FLOAT32 data mismatch");
                assertNull(f16.getData(), "FLOAT16 data should be null.");
            }
        }

        /**
         * Tests inserting NULL in the FLOAT32 column while FLOAT16 has data.
         */
        @Test
        public void testMixedFloat32NullFloat16Data() throws SQLException {
            String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(mixedTableName)
                    + " (id, v_float32, v_float16) VALUES (?, ?, ?)";
            Float[] float16Data = data(0.5f, 1.5f, 2.5f);

            try (PreparedStatement pstmt = mixedConnection.prepareStatement(insertSql)) {
                pstmt.setInt(1, 1);
                pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT32, null),
                        microsoft.sql.Types.VECTOR);
                pstmt.setObject(3, new Vector(3, VectorDimensionType.FLOAT16, float16Data),
                        microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            String query = "SELECT v_float32, v_float16 FROM "
                    + AbstractSQLGenerator.escapeIdentifier(mixedTableName) + " WHERE id = 1";
            try (Statement stmt = mixedConnection.createStatement();
                    ResultSet rs = stmt.executeQuery(query)) {
                assertTrue(rs.next(), "No result found.");
                Vector f32 = rs.getObject("v_float32", Vector.class);
                Vector f16 = rs.getObject("v_float16", Vector.class);
                assertNull(f32.getData(), "FLOAT32 data should be null.");
                assertMixedVectorEquals(float16Data, f16.getData(),
                        VectorDimensionType.FLOAT16, "FLOAT16 data mismatch");
            }
        }

        /**
         * Tests inserting NULL in both FLOAT32 and FLOAT16 columns simultaneously.
         */
        @Test
        public void testMixedBothColumnsNull() throws SQLException {
            String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(mixedTableName)
                    + " (id, v_float32, v_float16) VALUES (?, ?, ?)";

            try (PreparedStatement pstmt = mixedConnection.prepareStatement(insertSql)) {
                pstmt.setInt(1, 1);
                pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT32, null),
                        microsoft.sql.Types.VECTOR);
                pstmt.setObject(3, new Vector(3, VectorDimensionType.FLOAT16, null),
                        microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            String query = "SELECT v_float32, v_float16 FROM "
                    + AbstractSQLGenerator.escapeIdentifier(mixedTableName) + " WHERE id = 1";
            try (Statement stmt = mixedConnection.createStatement();
                    ResultSet rs = stmt.executeQuery(query)) {
                assertTrue(rs.next(), "No result found.");
                Vector f32 = rs.getObject("v_float32", Vector.class);
                Vector f16 = rs.getObject("v_float16", Vector.class);
                assertNull(f32.getData(), "FLOAT32 data should be null.");
                assertNull(f16.getData(), "FLOAT16 data should be null.");
            }
        }

        // ====================================================================
        // Update Tests
        // ====================================================================

        /**
         * Tests updating only the FLOAT16 column without affecting the FLOAT32 column.
         */
        @Test
        public void testMixedUpdateFloat16Only() throws SQLException {
            // Insert initial row
            String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(mixedTableName)
                    + " (id, v_float32, v_float16) VALUES (?, ?, ?)";
            Float[] float32Data = data(1.0f, 2.0f, 3.0f);
            Float[] float16Original = data(0.5f, 1.5f, 2.5f);

            try (PreparedStatement pstmt = mixedConnection.prepareStatement(insertSql)) {
                pstmt.setInt(1, 1);
                pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT32, float32Data),
                        microsoft.sql.Types.VECTOR);
                pstmt.setObject(3, new Vector(3, VectorDimensionType.FLOAT16, float16Original),
                        microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            // Update only the FLOAT16 column
            Float[] float16Updated = data(10.0f, 20.0f, 30.0f);
            String updateSql = "UPDATE " + AbstractSQLGenerator.escapeIdentifier(mixedTableName)
                    + " SET v_float16 = ? WHERE id = ?";
            try (PreparedStatement pstmt = mixedConnection.prepareStatement(updateSql)) {
                pstmt.setObject(1, new Vector(3, VectorDimensionType.FLOAT16, float16Updated),
                        microsoft.sql.Types.VECTOR);
                pstmt.setInt(2, 1);
                pstmt.executeUpdate();
            }

            // Verify FLOAT32 is unchanged and FLOAT16 is updated
            String query = "SELECT v_float32, v_float16 FROM "
                    + AbstractSQLGenerator.escapeIdentifier(mixedTableName) + " WHERE id = 1";
            try (Statement stmt = mixedConnection.createStatement();
                    ResultSet rs = stmt.executeQuery(query)) {
                assertTrue(rs.next(), "No result found.");
                Vector f32 = rs.getObject("v_float32", Vector.class);
                Vector f16 = rs.getObject("v_float16", Vector.class);
                assertMixedVectorEquals(float32Data, f32.getData(),
                        VectorDimensionType.FLOAT32, "FLOAT32 should be unchanged after FLOAT16 update");
                assertMixedVectorEquals(float16Updated, f16.getData(),
                        VectorDimensionType.FLOAT16, "FLOAT16 should be updated");
            }
        }

        /**
         * Tests updating only the FLOAT32 column without affecting the FLOAT16 column.
         */
        @Test
        public void testMixedUpdateFloat32Only() throws SQLException {
            // Insert initial row
            String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(mixedTableName)
                    + " (id, v_float32, v_float16) VALUES (?, ?, ?)";
            Float[] float32Original = data(1.0f, 2.0f, 3.0f);
            Float[] float16Data = data(0.5f, 1.5f, 2.5f);

            try (PreparedStatement pstmt = mixedConnection.prepareStatement(insertSql)) {
                pstmt.setInt(1, 1);
                pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT32, float32Original),
                        microsoft.sql.Types.VECTOR);
                pstmt.setObject(3, new Vector(3, VectorDimensionType.FLOAT16, float16Data),
                        microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            // Update only the FLOAT32 column
            Float[] float32Updated = data(10.0f, 20.0f, 30.0f);
            String updateSql = "UPDATE " + AbstractSQLGenerator.escapeIdentifier(mixedTableName)
                    + " SET v_float32 = ? WHERE id = ?";
            try (PreparedStatement pstmt = mixedConnection.prepareStatement(updateSql)) {
                pstmt.setObject(1, new Vector(3, VectorDimensionType.FLOAT32, float32Updated),
                        microsoft.sql.Types.VECTOR);
                pstmt.setInt(2, 1);
                pstmt.executeUpdate();
            }

            // Verify FLOAT16 is unchanged and FLOAT32 is updated
            String query = "SELECT v_float32, v_float16 FROM "
                    + AbstractSQLGenerator.escapeIdentifier(mixedTableName) + " WHERE id = 1";
            try (Statement stmt = mixedConnection.createStatement();
                    ResultSet rs = stmt.executeQuery(query)) {
                assertTrue(rs.next(), "No result found.");
                Vector f32 = rs.getObject("v_float32", Vector.class);
                Vector f16 = rs.getObject("v_float16", Vector.class);
                assertMixedVectorEquals(float32Updated, f32.getData(),
                        VectorDimensionType.FLOAT32, "FLOAT32 should be updated");
                assertMixedVectorEquals(float16Data, f16.getData(),
                        VectorDimensionType.FLOAT16, "FLOAT16 should be unchanged after FLOAT32 update");
            }
        }

        // ====================================================================
        // Stored Procedure Tests
        // ====================================================================

        /**
         * Tests a stored procedure with both FLOAT32 and FLOAT16 input parameters
         * and output parameters. Verifies that each type is correctly round-tripped.
         */
        @Test
        public void testMixedStoredProcedureInputOutput() throws SQLException {
            try (Statement stmt = mixedConnection.createStatement()) {
                String sql = "CREATE OR ALTER PROCEDURE "
                        + AbstractSQLGenerator.escapeIdentifier(mixedProcedureName) + "\n"
                        + "    @inFloat32 VECTOR(3),\n"
                        + "    @inFloat16 VECTOR(3, float16),\n"
                        + "    @outFloat32 VECTOR(3) OUTPUT,\n"
                        + "    @outFloat16 VECTOR(3, float16) OUTPUT\n"
                        + "AS\n"
                        + "BEGIN\n"
                        + "    SET @outFloat32 = @inFloat32;\n"
                        + "    SET @outFloat16 = @inFloat16;\n"
                        + "END";
                stmt.execute(sql);
            }

            Float[] float32Data = data(1.0f, 2.0f, 3.0f);
            Float[] float16Data = data(0.5f, 1.5f, 2.5f);

            String call = "{call " + AbstractSQLGenerator.escapeIdentifier(mixedProcedureName)
                    + "(?, ?, ?, ?)}";
            try (SQLServerCallableStatement cstmt = (SQLServerCallableStatement) mixedConnection
                    .prepareCall(call)) {
                cstmt.setObject(1, new Vector(3, VectorDimensionType.FLOAT32, float32Data),
                        microsoft.sql.Types.VECTOR);
                cstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT16, float16Data),
                        microsoft.sql.Types.VECTOR);
                cstmt.registerOutParameter(3, microsoft.sql.Types.VECTOR, 3, 4); // scale 4 = FLOAT32
                cstmt.registerOutParameter(4, microsoft.sql.Types.VECTOR, 3, 2); // scale 2 = FLOAT16
                cstmt.execute();

                Vector resultF32 = cstmt.getObject(3, Vector.class);
                assertNotNull(resultF32, "FLOAT32 output should not be null.");
                assertMixedVectorEquals(float32Data, resultF32.getData(),
                        VectorDimensionType.FLOAT32, "FLOAT32 SP output mismatch");

                Vector resultF16 = cstmt.getObject(4, Vector.class);
                assertNotNull(resultF16, "FLOAT16 output should not be null.");
                assertMixedVectorEquals(float16Data, resultF16.getData(),
                        VectorDimensionType.FLOAT16, "FLOAT16 SP output mismatch");
            }
        }

        /**
         * Tests a stored procedure with null data in both FLOAT32 and FLOAT16
         * output parameters.
         */
        @Test
        public void testMixedStoredProcedureNullInputOutput() throws SQLException {
            try (Statement stmt = mixedConnection.createStatement()) {
                String sql = "CREATE OR ALTER PROCEDURE "
                        + AbstractSQLGenerator.escapeIdentifier(mixedProcedureName) + "\n"
                        + "    @inFloat32 VECTOR(3),\n"
                        + "    @inFloat16 VECTOR(3, float16),\n"
                        + "    @outFloat32 VECTOR(3) OUTPUT,\n"
                        + "    @outFloat16 VECTOR(3, float16) OUTPUT\n"
                        + "AS\n"
                        + "BEGIN\n"
                        + "    SET @outFloat32 = @inFloat32;\n"
                        + "    SET @outFloat16 = @inFloat16;\n"
                        + "END";
                stmt.execute(sql);
            }

            String call = "{call " + AbstractSQLGenerator.escapeIdentifier(mixedProcedureName)
                    + "(?, ?, ?, ?)}";
            try (SQLServerCallableStatement cstmt = (SQLServerCallableStatement) mixedConnection
                    .prepareCall(call)) {
                cstmt.setObject(1, new Vector(3, VectorDimensionType.FLOAT32, null),
                        microsoft.sql.Types.VECTOR);
                cstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT16, null),
                        microsoft.sql.Types.VECTOR);
                cstmt.registerOutParameter(3, microsoft.sql.Types.VECTOR, 3, 4);
                cstmt.registerOutParameter(4, microsoft.sql.Types.VECTOR, 3, 2);
                cstmt.execute();

                Vector resultF32 = cstmt.getObject(3, Vector.class);
                assertNotNull(resultF32, "FLOAT32 output object should not be null.");
                assertNull(resultF32.getData(), "FLOAT32 output data should be null.");

                Vector resultF16 = cstmt.getObject(4, Vector.class);
                assertNotNull(resultF16, "FLOAT16 output object should not be null.");
                assertNull(resultF16.getData(), "FLOAT16 output data should be null.");
            }
        }

        // ====================================================================
        // TVP Tests
        // ====================================================================

        /**
         * Tests inserting rows via TVP where the TVP type has both FLOAT32 and
         * FLOAT16 vector columns.
         */
        @Test
        public void testMixedTVPInsert() throws SQLException {
            Float[] float32Data = data(1.0f, 2.0f, 3.0f);
            Float[] float16Data = data(0.5f, 1.5f, 2.5f);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c_float32", microsoft.sql.Types.VECTOR);
            tvp.addColumnMetadata("c_float16", microsoft.sql.Types.VECTOR);
            tvp.addRow(new Vector(3, VectorDimensionType.FLOAT32, float32Data),
                    new Vector(3, VectorDimensionType.FLOAT16, float16Data));

            String escapedTable = AbstractSQLGenerator.escapeIdentifier(mixedTvpTableName);
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) mixedConnection
                    .prepareStatement("INSERT INTO " + escapedTable + " SELECT * FROM ?;")) {
                pstmt.setStructured(1, mixedTvpTypeName, tvp);
                pstmt.execute();

                try (Statement stmt = mixedConnection.createStatement();
                        ResultSet rs = stmt.executeQuery(
                                "SELECT c_float32, c_float16 FROM " + escapedTable)) {
                    assertTrue(rs.next(), "Expected a row from mixed TVP insert.");
                    Vector f32 = rs.getObject("c_float32", Vector.class);
                    Vector f16 = rs.getObject("c_float16", Vector.class);
                    assertNotNull(f32, "FLOAT32 TVP column should not be null.");
                    assertNotNull(f16, "FLOAT16 TVP column should not be null.");
                    assertMixedVectorEquals(float32Data, f32.getData(),
                            VectorDimensionType.FLOAT32, "FLOAT32 TVP data mismatch");
                    assertMixedVectorEquals(float16Data, f16.getData(),
                            VectorDimensionType.FLOAT16, "FLOAT16 TVP data mismatch");
                }
            }
        }

        /**
         * Tests TVP with multiple rows including null values in mixed-type columns.
         */
        @Test
        public void testMixedTVPWithNulls() throws SQLException {
            Float[] float32Data = data(1.0f, 2.0f, 3.0f);
            Float[] float16Data = data(0.5f, 1.5f, 2.5f);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c_float32", microsoft.sql.Types.VECTOR);
            tvp.addColumnMetadata("c_float16", microsoft.sql.Types.VECTOR);

            // Row 1: both non-null
            tvp.addRow(new Vector(3, VectorDimensionType.FLOAT32, float32Data),
                    new Vector(3, VectorDimensionType.FLOAT16, float16Data));
            // Row 2: FLOAT32 null, FLOAT16 non-null
            tvp.addRow(new Vector(3, VectorDimensionType.FLOAT32, null),
                    new Vector(3, VectorDimensionType.FLOAT16, float16Data));
            // Row 3: FLOAT32 non-null, FLOAT16 null
            tvp.addRow(new Vector(3, VectorDimensionType.FLOAT32, float32Data),
                    new Vector(3, VectorDimensionType.FLOAT16, null));
            // Row 4: both null
            tvp.addRow(new Vector(3, VectorDimensionType.FLOAT32, null),
                    new Vector(3, VectorDimensionType.FLOAT16, null));

            String escapedTable = AbstractSQLGenerator.escapeIdentifier(mixedTvpTableName);
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) mixedConnection
                    .prepareStatement("INSERT INTO " + escapedTable + " SELECT * FROM ?;")) {
                pstmt.setStructured(1, mixedTvpTypeName, tvp);
                pstmt.execute();

                try (Statement stmt = mixedConnection.createStatement();
                        ResultSet rs = stmt.executeQuery(
                                "SELECT c_float32, c_float16 FROM " + escapedTable + " ORDER BY rowId")) {
                    // Row 1: both non-null
                    assertTrue(rs.next(), "Expected row 1.");
                    assertMixedVectorEquals(float32Data,
                            rs.getObject("c_float32", Vector.class).getData(),
                            VectorDimensionType.FLOAT32, "Row 1 FLOAT32");
                    assertMixedVectorEquals(float16Data,
                            rs.getObject("c_float16", Vector.class).getData(),
                            VectorDimensionType.FLOAT16, "Row 1 FLOAT16");

                    // Row 2: FLOAT32 null, FLOAT16 non-null
                    assertTrue(rs.next(), "Expected row 2.");
                    assertNull(rs.getObject("c_float32", Vector.class).getData(),
                            "Row 2 FLOAT32 should be null.");
                    assertMixedVectorEquals(float16Data,
                            rs.getObject("c_float16", Vector.class).getData(),
                            VectorDimensionType.FLOAT16, "Row 2 FLOAT16");

                    // Row 3: FLOAT32 non-null, FLOAT16 null
                    assertTrue(rs.next(), "Expected row 3.");
                    assertMixedVectorEquals(float32Data,
                            rs.getObject("c_float32", Vector.class).getData(),
                            VectorDimensionType.FLOAT32, "Row 3 FLOAT32");
                    assertNull(rs.getObject("c_float16", Vector.class).getData(),
                            "Row 3 FLOAT16 should be null.");

                    // Row 4: both null
                    assertTrue(rs.next(), "Expected row 4.");
                    assertNull(rs.getObject("c_float32", Vector.class).getData(),
                            "Row 4 FLOAT32 should be null.");
                    assertNull(rs.getObject("c_float16", Vector.class).getData(),
                            "Row 4 FLOAT16 should be null.");
                }
            }
        }

        /**
         * Tests a stored procedure that receives a mixed-type TVP and returns
         * its contents as a result set.
         */
        @Test
        public void testMixedStoredProcedureReturnsTVP() throws SQLException {
            String spName = AbstractSQLGenerator.escapeIdentifier(
                    RandomUtil.getIdentifier("sp_MixedTVP_" + mixedUuid.substring(0, 8)));

            try (Statement stmt = mixedConnection.createStatement()) {
                String sql = "CREATE OR ALTER PROCEDURE " + spName + "\n"
                        + "    @tvpInput " + AbstractSQLGenerator.escapeIdentifier(mixedTvpTypeName)
                        + " READONLY\n"
                        + "AS\n"
                        + "BEGIN\n"
                        + "    SELECT * FROM @tvpInput;\n"
                        + "END";
                stmt.execute(sql);
            }

            Float[] float32Data = data(1.0f, 2.0f, 3.0f);
            Float[] float16Data = data(0.5f, 1.5f, 2.5f);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c_float32", microsoft.sql.Types.VECTOR);
            tvp.addColumnMetadata("c_float16", microsoft.sql.Types.VECTOR);
            tvp.addRow(new Vector(3, VectorDimensionType.FLOAT32, float32Data),
                    new Vector(3, VectorDimensionType.FLOAT16, float16Data));

            String call = "{call " + spName + "(?)}";
            try (SQLServerCallableStatement cstmt = (SQLServerCallableStatement) mixedConnection
                    .prepareCall(call)) {
                cstmt.setStructured(1, mixedTvpTypeName, tvp);
                boolean hasResultSet = cstmt.execute();
                assertTrue(hasResultSet, "Stored procedure should return a result set.");

                try (ResultSet rs = cstmt.getResultSet()) {
                    assertTrue(rs.next(), "Expected a row from mixed TVP SP.");
                    Vector f32 = rs.getObject("c_float32", Vector.class);
                    Vector f16 = rs.getObject("c_float16", Vector.class);
                    assertMixedVectorEquals(float32Data, f32.getData(),
                            VectorDimensionType.FLOAT32, "SP FLOAT32 data mismatch");
                    assertMixedVectorEquals(float16Data, f16.getData(),
                            VectorDimensionType.FLOAT16, "SP FLOAT16 data mismatch");
                }
            } finally {
                try (Statement stmt = mixedConnection.createStatement()) {
                    TestUtils.dropProcedureIfExists(spName, stmt);
                }
            }
        }

        // ====================================================================
        // Corner Cases
        // ====================================================================

        // ====================================================================
        // Explicit FLOAT32 Syntax Tests
        // ====================================================================

        /**
         * Tests using explicit VECTOR(3, float32) syntax alongside VECTOR(3, float16)
         * to confirm both explicit type declarations work correctly in the same table.
         */
        @Test
        public void testMixedExplicitFloat32WithFloat16() throws SQLException {
            String explicitTable = AbstractSQLGenerator.escapeIdentifier(
                    RandomUtil.getIdentifier("MixedExplicit_" + mixedUuid.substring(0, 8)));

            try (Statement stmt = mixedConnection.createStatement()) {
                // Use VECTOR(3, float32) explicitly instead of just VECTOR(3)
                stmt.executeUpdate("CREATE TABLE " + explicitTable
                        + " (id INT PRIMARY KEY,"
                        + " v_float32 VECTOR(3, float32),"
                        + " v_float16 VECTOR(3, float16))");

                Float[] float32Data = data(1.0f, 2.0f, 3.0f);
                Float[] float16Data = data(0.5f, 1.5f, 2.5f);

                try (PreparedStatement pstmt = mixedConnection.prepareStatement(
                        "INSERT INTO " + explicitTable + " (id, v_float32, v_float16) VALUES (?, ?, ?)")) {
                    pstmt.setInt(1, 1);
                    pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT32, float32Data),
                            microsoft.sql.Types.VECTOR);
                    pstmt.setObject(3, new Vector(3, VectorDimensionType.FLOAT16, float16Data),
                            microsoft.sql.Types.VECTOR);
                    pstmt.executeUpdate();
                }

                try (ResultSet rs = stmt.executeQuery(
                        "SELECT v_float32, v_float16 FROM " + explicitTable + " WHERE id = 1")) {
                    assertTrue(rs.next(), "No result found.");
                    Vector f32 = rs.getObject("v_float32", Vector.class);
                    Vector f16 = rs.getObject("v_float16", Vector.class);
                    assertEquals(VectorDimensionType.FLOAT32, f32.getVectorDimensionType(),
                            "Expected FLOAT32 type for explicit float32 column.");
                    assertEquals(VectorDimensionType.FLOAT16, f16.getVectorDimensionType(),
                            "Expected FLOAT16 type for explicit float16 column.");
                    assertMixedVectorEquals(float32Data, f32.getData(),
                            VectorDimensionType.FLOAT32, "Explicit FLOAT32 data mismatch");
                    assertMixedVectorEquals(float16Data, f16.getData(),
                            VectorDimensionType.FLOAT16, "Explicit FLOAT16 data mismatch");
                }
            } finally {
                try (Statement stmt = mixedConnection.createStatement()) {
                    TestUtils.dropTableIfExists(explicitTable, stmt);
                }
            }
        }

        // ====================================================================
        // Different Dimension Count Tests
        // ====================================================================

        /**
         * Tests a table with different dimension counts per column:
         * VECTOR(3) for FLOAT32 and VECTOR(5, float16) for FLOAT16.
         * Validates that per-column dimension metadata doesn't bleed between columns.
         */
        @Test
        public void testMixedDifferentDimensionCounts() throws SQLException {
            String diffDimTable = AbstractSQLGenerator.escapeIdentifier(
                    RandomUtil.getIdentifier("MixedDiffDim_" + mixedUuid.substring(0, 8)));

            try (Statement stmt = mixedConnection.createStatement()) {
                stmt.executeUpdate("CREATE TABLE " + diffDimTable
                        + " (id INT PRIMARY KEY,"
                        + " v_float32 VECTOR(3),"
                        + " v_float16 VECTOR(5, float16))");

                Float[] f32Data = data(1.0f, 2.0f, 3.0f);
                Float[] f16Data = data(0.1f, 0.2f, 0.3f, 0.4f, 0.5f);

                try (PreparedStatement pstmt = mixedConnection.prepareStatement(
                        "INSERT INTO " + diffDimTable + " (id, v_float32, v_float16) VALUES (?, ?, ?)")) {
                    pstmt.setInt(1, 1);
                    pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT32, f32Data),
                            microsoft.sql.Types.VECTOR);
                    pstmt.setObject(3, new Vector(5, VectorDimensionType.FLOAT16, f16Data),
                            microsoft.sql.Types.VECTOR);
                    pstmt.executeUpdate();
                }

                try (ResultSet rs = stmt.executeQuery(
                        "SELECT v_float32, v_float16 FROM " + diffDimTable + " WHERE id = 1")) {
                    assertTrue(rs.next(), "No result found.");
                    Vector f32 = rs.getObject("v_float32", Vector.class);
                    Vector f16 = rs.getObject("v_float16", Vector.class);

                    assertEquals(3, f32.getDimensionCount(), "FLOAT32 dimension count mismatch.");
                    assertEquals(5, f16.getDimensionCount(), "FLOAT16 dimension count mismatch.");
                    assertMixedVectorEquals(f32Data, f32.getData(),
                            VectorDimensionType.FLOAT32, "FLOAT32 data with diff dims");
                    assertMixedVectorEquals(f16Data, f16.getData(),
                            VectorDimensionType.FLOAT16, "FLOAT16 data with diff dims");
                }
            } finally {
                try (Statement stmt = mixedConnection.createStatement()) {
                    TestUtils.dropTableIfExists(diffDimTable, stmt);
                }
            }
        }

        // ====================================================================
        // Metadata Tests
        // ====================================================================

        /**
         * Verifies ResultSetMetaData correctly reports scale=4 for FLOAT32 and
         * scale=2 for FLOAT16 columns in the same result set.
         */
        @Test
        public void testMixedResultSetMetaData() throws SQLException {
            String query = "SELECT v_float32, v_float16 FROM "
                    + AbstractSQLGenerator.escapeIdentifier(mixedTableName);
            try (Statement stmt = mixedConnection.createStatement();
                    ResultSet rs = stmt.executeQuery(query)) {
                ResultSetMetaData rsmd = rs.getMetaData();
                assertEquals(2, rsmd.getColumnCount(), "Expected 2 columns.");

                // Column 1: v_float32
                assertEquals(microsoft.sql.Types.VECTOR, rsmd.getColumnType(1),
                        "FLOAT32 column type mismatch.");
                assertEquals("vector", rsmd.getColumnTypeName(1).toLowerCase(),
                        "FLOAT32 column type name mismatch.");
                assertEquals(4, rsmd.getScale(1), "FLOAT32 scale should be 4.");

                // Column 2: v_float16
                assertEquals(microsoft.sql.Types.VECTOR, rsmd.getColumnType(2),
                        "FLOAT16 column type mismatch.");
                assertEquals("vector", rsmd.getColumnTypeName(2).toLowerCase(),
                        "FLOAT16 column type name mismatch.");
                assertEquals(2, rsmd.getScale(2), "FLOAT16 scale should be 2.");
            }
        }

        /**
         * Verifies DatabaseMetaData.getColumns() correctly reports both FLOAT32
         * and FLOAT16 columns in a mixed-type table.
         */
        @Test
        @Tag(Constants.xAzureSQLDB)
        @Tag(Constants.xAzureSQLDW)
        public void testMixedConnectionGetMetaData() throws Exception {
            DatabaseMetaData metaData = mixedConnection.getMetaData();
            assertNotNull(metaData, "DatabaseMetaData should not be null");

            try (ResultSet rs = metaData.getColumns(null, null, mixedTableName, "%")) {
                boolean f32Found = false;
                boolean f16Found = false;
                while (rs.next()) {
                    String colName = rs.getString("COLUMN_NAME");
                    if ("v_float32".equalsIgnoreCase(colName)) {
                        f32Found = true;
                    } else if ("v_float16".equalsIgnoreCase(colName)) {
                        f16Found = true;
                    }
                }
                assertTrue(f32Found, "FLOAT32 vector column 'v_float32' not found in metadata.");
                assertTrue(f16Found, "FLOAT16 vector column 'v_float16' not found in metadata.");
            }
        }

        /**
         * Validates that ParameterMetaData correctly reports vector type for both
         * FLOAT32 and FLOAT16 parameters in a mixed-type INSERT statement.
         */
        @Test
        public void testMixedPreparedStatementMetaData() throws SQLException {
            String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(mixedTableName)
                    + " (id, v_float32, v_float16) VALUES (?, ?, ?)";
            Float[] f32Data = data(1.0f, 2.0f, 3.0f);
            Float[] f16Data = data(0.5f, 1.5f, 2.5f);

            try (PreparedStatement pstmt = mixedConnection.prepareStatement(insertSql)) {
                pstmt.setInt(1, 1);
                pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT32, f32Data),
                        microsoft.sql.Types.VECTOR);
                pstmt.setObject(3, new Vector(3, VectorDimensionType.FLOAT16, f16Data),
                        microsoft.sql.Types.VECTOR);

                ParameterMetaData pmd = pstmt.getParameterMetaData();
                assertNotNull(pmd, "ParameterMetaData should not be null.");
                assertEquals(3, pmd.getParameterCount(), "Parameter count mismatch.");

                // Param 1: INT
                assertEquals(java.sql.Types.INTEGER, pmd.getParameterType(1),
                        "First param should be INTEGER.");

                // Param 2: VECTOR (FLOAT32)
                assertEquals(microsoft.sql.Types.VECTOR, pmd.getParameterType(2),
                        "Second param should be VECTOR.");
                assertEquals("vector", pmd.getParameterTypeName(2).toLowerCase(),
                        "Second param type name should be 'vector'.");

                // Param 3: VECTOR (FLOAT16)
                assertEquals(microsoft.sql.Types.VECTOR, pmd.getParameterType(3),
                        "Third param should be VECTOR.");
                assertEquals("vector", pmd.getParameterTypeName(3).toLowerCase(),
                        "Third param type name should be 'vector'.");

                pstmt.executeUpdate();
            }
        }

        // ====================================================================
        // Transaction Tests
        // ====================================================================

        /**
         * Tests that a full transaction rollback correctly undoes insert, update,
         * and delete operations across both FLOAT32 and FLOAT16 vector columns.
         */
        @Test
        public void testMixedTransactionRollback() throws SQLException {
            String txnTable = AbstractSQLGenerator.escapeIdentifier(
                    RandomUtil.getIdentifier("MixedTxn_" + mixedUuid.substring(0, 8)));

            try (Statement outerStmt = mixedConnection.createStatement()) {
                outerStmt.executeUpdate("CREATE TABLE " + txnTable
                        + " (id INT PRIMARY KEY, v_float32 VECTOR(3), v_float16 VECTOR(3, float16))");

                // Insert initial committed row
                Float[] f32Initial = data(1.0f, 2.0f, 3.0f);
                Float[] f16Initial = data(0.5f, 1.5f, 2.5f);
                try (PreparedStatement pstmt = mixedConnection.prepareStatement(
                        "INSERT INTO " + txnTable + " (id, v_float32, v_float16) VALUES (?, ?, ?)")) {
                    pstmt.setInt(1, 1);
                    pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT32, f32Initial),
                            microsoft.sql.Types.VECTOR);
                    pstmt.setObject(3, new Vector(3, VectorDimensionType.FLOAT16, f16Initial),
                            microsoft.sql.Types.VECTOR);
                    pstmt.executeUpdate();
                }

                // Begin transaction, modify data, then rollback
                mixedConnection.setAutoCommit(false);
                try {
                    // Insert a second row
                    try (PreparedStatement pstmt = mixedConnection.prepareStatement(
                            "INSERT INTO " + txnTable + " (id, v_float32, v_float16) VALUES (?, ?, ?)")) {
                        pstmt.setInt(1, 2);
                        pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT32, data(4.0f, 5.0f, 6.0f)),
                                microsoft.sql.Types.VECTOR);
                        pstmt.setObject(3, new Vector(3, VectorDimensionType.FLOAT16, data(10.0f, 20.0f, 30.0f)),
                                microsoft.sql.Types.VECTOR);
                        pstmt.executeUpdate();
                    }

                    // Update FLOAT16 column on row 1
                    try (PreparedStatement pstmt = mixedConnection.prepareStatement(
                            "UPDATE " + txnTable + " SET v_float16 = ? WHERE id = ?")) {
                        pstmt.setObject(1, new Vector(3, VectorDimensionType.FLOAT16, data(99.0f, 99.0f, 99.0f)),
                                microsoft.sql.Types.VECTOR);
                        pstmt.setInt(2, 1);
                        pstmt.executeUpdate();
                    }

                    // Delete the original row
                    try (PreparedStatement pstmt = mixedConnection.prepareStatement(
                            "DELETE FROM " + txnTable + " WHERE id = ?")) {
                        pstmt.setInt(1, 1);
                        pstmt.executeUpdate();
                    }

                    throw new RuntimeException("Simulated failure to trigger rollback");
                } catch (RuntimeException e) {
                    mixedConnection.rollback();
                } finally {
                    mixedConnection.setAutoCommit(true);
                }

                // After rollback: only the original row 1 with original data should exist
                try (ResultSet rs = outerStmt.executeQuery(
                        "SELECT id, v_float32, v_float16 FROM " + txnTable + " ORDER BY id")) {
                    assertTrue(rs.next(), "Row 1 should exist after rollback.");
                    assertEquals(1, rs.getInt("id"), "ID mismatch after rollback.");
                    assertMixedVectorEquals(f32Initial,
                            rs.getObject("v_float32", Vector.class).getData(),
                            VectorDimensionType.FLOAT32, "FLOAT32 should be unchanged after rollback");
                    assertMixedVectorEquals(f16Initial,
                            rs.getObject("v_float16", Vector.class).getData(),
                            VectorDimensionType.FLOAT16, "FLOAT16 should be unchanged after rollback");
                    assertFalse(rs.next(), "Row 2 should not exist after rollback.");
                }
            } finally {
                try (Statement stmt = mixedConnection.createStatement()) {
                    TestUtils.dropTableIfExists(txnTable, stmt);
                }
            }
        }

        /**
         * Tests that rolling back to a savepoint mid-transaction correctly undoes only
         * the work after the savepoint, while preserving earlier mixed-type vector inserts.
         */
        @Test
        public void testMixedSavepointRollback() throws SQLException {
            String spTable = AbstractSQLGenerator.escapeIdentifier(
                    RandomUtil.getIdentifier("MixedSavepoint_" + mixedUuid.substring(0, 8)));

            try (Statement outerStmt = mixedConnection.createStatement()) {
                outerStmt.executeUpdate("CREATE TABLE " + spTable
                        + " (id INT PRIMARY KEY, v_float32 VECTOR(3), v_float16 VECTOR(3, float16))");

                mixedConnection.setAutoCommit(false);
                try {
                    // Insert first row — this should survive
                    Float[] f32Data1 = data(1.0f, 2.0f, 3.0f);
                    Float[] f16Data1 = data(0.5f, 1.5f, 2.5f);
                    try (PreparedStatement pstmt = mixedConnection.prepareStatement(
                            "INSERT INTO " + spTable + " (id, v_float32, v_float16) VALUES (?, ?, ?)")) {
                        pstmt.setInt(1, 1);
                        pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT32, f32Data1),
                                microsoft.sql.Types.VECTOR);
                        pstmt.setObject(3, new Vector(3, VectorDimensionType.FLOAT16, f16Data1),
                                microsoft.sql.Types.VECTOR);
                        pstmt.executeUpdate();
                    }

                    Savepoint sp = mixedConnection.setSavepoint("afterFirstInsert");

                    // Insert second row — this should be rolled back
                    try (PreparedStatement pstmt = mixedConnection.prepareStatement(
                            "INSERT INTO " + spTable + " (id, v_float32, v_float16) VALUES (?, ?, ?)")) {
                        pstmt.setInt(1, 2);
                        pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT32, data(4.0f, 5.0f, 6.0f)),
                                microsoft.sql.Types.VECTOR);
                        pstmt.setObject(3, new Vector(3, VectorDimensionType.FLOAT16, data(10.0f, 20.0f, 30.0f)),
                                microsoft.sql.Types.VECTOR);
                        pstmt.executeUpdate();
                    }

                    mixedConnection.rollback(sp);
                    mixedConnection.commit();

                } catch (Exception e) {
                    mixedConnection.rollback();
                    throw e;
                } finally {
                    mixedConnection.setAutoCommit(true);
                }

                // Only row 1 should exist
                try (ResultSet rs = outerStmt.executeQuery(
                        "SELECT id, v_float32, v_float16 FROM " + spTable + " ORDER BY id")) {
                    assertTrue(rs.next(), "Row 1 should exist after partial rollback.");
                    assertEquals(1, rs.getInt("id"));
                    assertMixedVectorEquals(data(1.0f, 2.0f, 3.0f),
                            rs.getObject("v_float32", Vector.class).getData(),
                            VectorDimensionType.FLOAT32, "FLOAT32 after savepoint rollback");
                    assertMixedVectorEquals(data(0.5f, 1.5f, 2.5f),
                            rs.getObject("v_float16", Vector.class).getData(),
                            VectorDimensionType.FLOAT16, "FLOAT16 after savepoint rollback");
                    assertFalse(rs.next(), "Row 2 should not exist after savepoint rollback.");
                }
            } finally {
                try (Statement stmt = mixedConnection.createStatement()) {
                    TestUtils.dropTableIfExists(spTable, stmt);
                }
            }
        }

        // ====================================================================
        // Stored Procedure INOUT Overwrite Tests
        // ====================================================================

        /**
         * Tests a stored procedure with INOUT parameters where the SP overwrites
         * the input with different values. Verifies the driver reads the server's
         * output (not cached input) for each type, catching bugs where FLOAT16
         * output might be accidentally decoded as FLOAT32 or vice versa.
         */
        @Test
        public void testMixedStoredProcedureInOutOverwrite() throws SQLException {
            String spName = AbstractSQLGenerator.escapeIdentifier(
                    RandomUtil.getIdentifier("sp_MixedInOut_" + mixedUuid.substring(0, 8)));

            try (Statement stmt = mixedConnection.createStatement()) {
                String sql = "CREATE OR ALTER PROCEDURE " + spName + "\n"
                        + "    @ioFloat32 VECTOR(3) OUTPUT,\n"
                        + "    @ioFloat16 VECTOR(3, float16) OUTPUT\n"
                        + "AS\n"
                        + "BEGIN\n"
                        + "    SET @ioFloat32 = CAST('[4.0, 5.0, 6.0]' AS VECTOR(3));\n"
                        + "    SET @ioFloat16 = CAST('[10.0, 20.0, 30.0]' AS VECTOR(3, float16));\n"
                        + "END";
                stmt.execute(sql);
            }

            Float[] expectedF32 = data(4.0f, 5.0f, 6.0f);
            Float[] expectedF16 = data(10.0f, 20.0f, 30.0f);

            String call = "{call " + spName + "(?, ?)}";
            try (SQLServerCallableStatement cstmt = (SQLServerCallableStatement) mixedConnection
                    .prepareCall(call)) {
                // Provide input values that differ from what the SP will assign
                cstmt.setObject(1, new Vector(3, VectorDimensionType.FLOAT32, data(1.0f, 2.0f, 3.0f)),
                        microsoft.sql.Types.VECTOR);
                cstmt.registerOutParameter(1, microsoft.sql.Types.VECTOR, 3, 4);

                cstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT16, data(0.5f, 1.5f, 2.5f)),
                        microsoft.sql.Types.VECTOR);
                cstmt.registerOutParameter(2, microsoft.sql.Types.VECTOR, 3, 2);

                cstmt.execute();

                Vector resultF32 = cstmt.getObject(1, Vector.class);
                assertNotNull(resultF32, "FLOAT32 INOUT output should not be null.");
                assertMixedVectorEquals(expectedF32, resultF32.getData(),
                        VectorDimensionType.FLOAT32, "FLOAT32 INOUT should contain SP-assigned value");

                Vector resultF16 = cstmt.getObject(2, Vector.class);
                assertNotNull(resultF16, "FLOAT16 INOUT output should not be null.");
                assertMixedVectorEquals(expectedF16, resultF16.getData(),
                        VectorDimensionType.FLOAT16, "FLOAT16 INOUT should contain SP-assigned value");
            } finally {
                try (Statement stmt = mixedConnection.createStatement()) {
                    TestUtils.dropProcedureIfExists(spName, stmt);
                }
            }
        }

        // ====================================================================
        // Temporary Table Tests
        // ====================================================================

        /**
         * Tests inserting mixed-type vectors into a global temporary table and
         * retrieving them. Global temp tables use a different metadata resolution
         * path in TDS.
         */
        @Test
        public void testMixedGlobalTempTable() throws SQLException {
            String globalTemp = TestUtils.escapeSingleQuotes(
                    AbstractSQLGenerator.escapeIdentifier(
                            RandomUtil.getIdentifier("##MixedGlobal_" + mixedUuid.substring(0, 8))));

            try (Statement stmt = mixedConnection.createStatement()) {
                TestUtils.dropTableIfExists(globalTemp, stmt);
                stmt.executeUpdate("CREATE TABLE " + globalTemp
                        + " (id INT PRIMARY KEY, v_float32 VECTOR(3), v_float16 VECTOR(3, float16))");

                Float[] f32Data = data(1.0f, 2.0f, 3.0f);
                Float[] f16Data = data(0.5f, 1.5f, 2.5f);

                try (PreparedStatement pstmt = mixedConnection.prepareStatement(
                        "INSERT INTO " + globalTemp + " (id, v_float32, v_float16) VALUES (?, ?, ?)")) {
                    pstmt.setInt(1, 1);
                    pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT32, f32Data),
                            microsoft.sql.Types.VECTOR);
                    pstmt.setObject(3, new Vector(3, VectorDimensionType.FLOAT16, f16Data),
                            microsoft.sql.Types.VECTOR);
                    pstmt.executeUpdate();
                }

                try (ResultSet rs = stmt.executeQuery(
                        "SELECT v_float32, v_float16 FROM " + globalTemp + " WHERE id = 1")) {
                    assertTrue(rs.next(), "No result found in global temp table.");
                    assertMixedVectorEquals(f32Data,
                            rs.getObject("v_float32", Vector.class).getData(),
                            VectorDimensionType.FLOAT32, "Global temp FLOAT32 mismatch");
                    assertMixedVectorEquals(f16Data,
                            rs.getObject("v_float16", Vector.class).getData(),
                            VectorDimensionType.FLOAT16, "Global temp FLOAT16 mismatch");
                }
            } finally {
                try (Statement stmt = mixedConnection.createStatement()) {
                    TestUtils.dropTableIfExists(globalTemp, stmt);
                }
            }
        }

        /**
         * Tests inserting mixed-type vectors into a local temporary table.
         * Uses a separate connection to ensure the local temp table is session-scoped.
         */
        @Test
        public void testMixedLocalTempTable() throws SQLException {
            String connStr = connectionString + ";vectorTypeSupport=v2";
            try (Connection conn = DriverManager.getConnection(connStr)) {
                String localTemp = TestUtils.escapeSingleQuotes(
                        AbstractSQLGenerator.escapeIdentifier(
                                RandomUtil.getIdentifier("#MixedLocal_" + mixedUuid.substring(0, 8))));

                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate("CREATE TABLE " + localTemp
                            + " (id INT PRIMARY KEY, v_float32 VECTOR(3), v_float16 VECTOR(3, float16))");

                    Float[] f32Data = data(4.0f, 5.0f, 6.0f);
                    Float[] f16Data = data(10.0f, 20.0f, 30.0f);

                    try (PreparedStatement pstmt = conn.prepareStatement(
                            "INSERT INTO " + localTemp
                                    + " (id, v_float32, v_float16) VALUES (?, ?, ?)")) {
                        pstmt.setInt(1, 1);
                        pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT32, f32Data),
                                microsoft.sql.Types.VECTOR);
                        pstmt.setObject(3, new Vector(3, VectorDimensionType.FLOAT16, f16Data),
                                microsoft.sql.Types.VECTOR);
                        pstmt.executeUpdate();
                    }

                    try (ResultSet rs = stmt.executeQuery(
                            "SELECT v_float32, v_float16 FROM " + localTemp + " WHERE id = 1")) {
                        assertTrue(rs.next(), "No result found in local temp table.");
                        assertMixedVectorEquals(f32Data,
                                rs.getObject("v_float32", Vector.class).getData(),
                                VectorDimensionType.FLOAT32, "Local temp FLOAT32 mismatch");
                        assertMixedVectorEquals(f16Data,
                                rs.getObject("v_float16", Vector.class).getData(),
                                VectorDimensionType.FLOAT16, "Local temp FLOAT16 mismatch");
                    }
                }
            }
        }

        // ====================================================================
        // View Tests
        // ====================================================================

        /**
         * Tests creating a view over a mixed-type vector table and querying through
         * it. Verifies that column metadata is preserved through the view.
         */
        @Test
        public void testMixedViewWithVectorDataType() throws SQLException {
            String viewTable = AbstractSQLGenerator.escapeIdentifier(
                    RandomUtil.getIdentifier("MixedViewTbl_" + mixedUuid.substring(0, 8)));
            String viewName = AbstractSQLGenerator.escapeIdentifier(
                    RandomUtil.getIdentifier("MixedView_" + mixedUuid.substring(0, 8)));

            try (Statement stmt = mixedConnection.createStatement()) {
                stmt.executeUpdate("CREATE TABLE " + viewTable
                        + " (id INT PRIMARY KEY, v_float32 VECTOR(3), v_float16 VECTOR(3, float16))");

                Float[] f32Data1 = data(1.1f, 2.2f, 3.3f);
                Float[] f16Data1 = data(0.5f, 1.5f, 2.5f);
                Float[] f32Data2 = data(4.4f, 5.5f, 6.6f);
                Float[] f16Data2 = data(10.0f, 20.0f, 30.0f);

                try (PreparedStatement pstmt = mixedConnection.prepareStatement(
                        "INSERT INTO " + viewTable + " (id, v_float32, v_float16) VALUES (?, ?, ?)")) {
                    pstmt.setInt(1, 1);
                    pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT32, f32Data1),
                            microsoft.sql.Types.VECTOR);
                    pstmt.setObject(3, new Vector(3, VectorDimensionType.FLOAT16, f16Data1),
                            microsoft.sql.Types.VECTOR);
                    pstmt.executeUpdate();

                    pstmt.setInt(1, 2);
                    pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT32, f32Data2),
                            microsoft.sql.Types.VECTOR);
                    pstmt.setObject(3, new Vector(3, VectorDimensionType.FLOAT16, f16Data2),
                            microsoft.sql.Types.VECTOR);
                    pstmt.executeUpdate();
                }

                stmt.executeUpdate("CREATE VIEW " + viewName
                        + " AS SELECT id, v_float32, v_float16 FROM " + viewTable);

                try (ResultSet rs = stmt.executeQuery(
                        "SELECT id, v_float32, v_float16 FROM " + viewName + " ORDER BY id")) {
                    // Row 1
                    assertTrue(rs.next(), "Expected row 1 from view.");
                    assertEquals(1, rs.getInt("id"));
                    assertMixedVectorEquals(f32Data1,
                            rs.getObject("v_float32", Vector.class).getData(),
                            VectorDimensionType.FLOAT32, "View row 1 FLOAT32");
                    assertMixedVectorEquals(f16Data1,
                            rs.getObject("v_float16", Vector.class).getData(),
                            VectorDimensionType.FLOAT16, "View row 1 FLOAT16");

                    // Row 2
                    assertTrue(rs.next(), "Expected row 2 from view.");
                    assertEquals(2, rs.getInt("id"));
                    assertMixedVectorEquals(f32Data2,
                            rs.getObject("v_float32", Vector.class).getData(),
                            VectorDimensionType.FLOAT32, "View row 2 FLOAT32");
                    assertMixedVectorEquals(f16Data2,
                            rs.getObject("v_float16", Vector.class).getData(),
                            VectorDimensionType.FLOAT16, "View row 2 FLOAT16");

                    assertFalse(rs.next(), "Expected only 2 rows from view.");
                }
            } finally {
                try (Statement stmt = mixedConnection.createStatement()) {
                    TestUtils.dropViewIfExists(viewName, stmt);
                    TestUtils.dropTableIfExists(viewTable, stmt);
                }
            }
        }

        // ====================================================================
        // UDF Tests
        // ====================================================================

        /**
         * Tests a scalar UDF that accepts both FLOAT32 and FLOAT16 vector parameters.
         * The UDF echoes the FLOAT16 input back, verifying UDF parameter binding
         * works correctly for both types.
         */
        @Test
        public void testMixedVectorUdf() throws SQLException {
            String udfName = AbstractSQLGenerator.escapeIdentifier(
                    RandomUtil.getIdentifier("fn_MixedUdf_" + mixedUuid.substring(0, 8)));

            try (Statement stmt = mixedConnection.createStatement()) {
                String sql = "CREATE OR ALTER FUNCTION " + udfName + "\n"
                        + "(@v_f32 VECTOR(3), @v_f16 VECTOR(3, float16))\n"
                        + "RETURNS VECTOR(3, float16)\n"
                        + "AS\n"
                        + "BEGIN\n"
                        + "    RETURN @v_f16;\n"
                        + "END";
                stmt.execute(sql);
            }

            // Store input in a temp table for the scalar subquery
            String tempTable = "#mixed_udf_" + mixedUuid.substring(0, 8);
            Float[] f32Input = data(1.0f, 2.0f, 3.0f);
            Float[] f16Input = data(0.5f, 1.5f, 2.5f);

            try (Statement stmt = mixedConnection.createStatement()) {
                stmt.execute("CREATE TABLE " + tempTable
                        + " (v_f32 VECTOR(3), v_f16 VECTOR(3, float16))");
            }
            try (PreparedStatement pstmt = mixedConnection.prepareStatement(
                    "INSERT INTO " + tempTable + " VALUES (?, ?)")) {
                pstmt.setObject(1, new Vector(3, VectorDimensionType.FLOAT32, f32Input),
                        microsoft.sql.Types.VECTOR);
                pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT16, f16Input),
                        microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            String query = "SELECT dbo." + udfName
                    + "((SELECT TOP 1 v_f32 FROM " + tempTable + "),"
                    + " (SELECT TOP 1 v_f16 FROM " + tempTable + "))";
            try (Statement stmt = mixedConnection.createStatement();
                    ResultSet rs = stmt.executeQuery(query)) {
                assertTrue(rs.next(), "UDF result set is empty.");
                Vector result = rs.getObject(1, Vector.class);
                assertNotNull(result, "UDF result should not be null.");
                assertMixedVectorEquals(f16Input, result.getData(),
                        VectorDimensionType.FLOAT16, "UDF should return FLOAT16 input");
            } finally {
                try (Statement stmt = mixedConnection.createStatement()) {
                    TestUtils.dropTableIfExists(tempTable, stmt);
                    TestUtils.dropFunctionIfExists(udfName, stmt);
                }
            }
        }

        // ====================================================================
        // Insert from Table Copy (non-bulk-copy path)
        // ====================================================================

        /**
         * Tests copying mixed-type vector data between tables via SELECT+INSERT
         * (the regular statement path, not bulk copy). Includes null rows.
         */
        @Test
        public void testMixedInsertFromTableCopy() throws SQLException {
            String srcTable = AbstractSQLGenerator.escapeIdentifier(
                    RandomUtil.getIdentifier("MixedCopySrc_" + mixedUuid.substring(0, 8)));
            String dstTable = AbstractSQLGenerator.escapeIdentifier(
                    RandomUtil.getIdentifier("MixedCopyDst_" + mixedUuid.substring(0, 8)));

            try (Statement stmt = mixedConnection.createStatement()) {
                stmt.executeUpdate("CREATE TABLE " + srcTable
                        + " (id INT PRIMARY KEY, v_float32 VECTOR(3), v_float16 VECTOR(3, float16))");
                stmt.executeUpdate("CREATE TABLE " + dstTable
                        + " (id INT PRIMARY KEY, v_float32 VECTOR(3), v_float16 VECTOR(3, float16))");

                Float[] f32Data = data(1.0f, 2.0f, 3.0f);
                Float[] f16Data = data(0.5f, 1.5f, 2.5f);

                try (PreparedStatement pstmt = mixedConnection.prepareStatement(
                        "INSERT INTO " + srcTable + " (id, v_float32, v_float16) VALUES (?, ?, ?)")) {
                    // Row 1: both non-null
                    pstmt.setInt(1, 1);
                    pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT32, f32Data),
                            microsoft.sql.Types.VECTOR);
                    pstmt.setObject(3, new Vector(3, VectorDimensionType.FLOAT16, f16Data),
                            microsoft.sql.Types.VECTOR);
                    pstmt.executeUpdate();

                    // Row 2: both null
                    pstmt.setInt(1, 2);
                    pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT32, null),
                            microsoft.sql.Types.VECTOR);
                    pstmt.setObject(3, new Vector(3, VectorDimensionType.FLOAT16, null),
                            microsoft.sql.Types.VECTOR);
                    pstmt.executeUpdate();

                    // Row 3: FLOAT32 data, FLOAT16 null
                    pstmt.setInt(1, 3);
                    pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT32, f32Data),
                            microsoft.sql.Types.VECTOR);
                    pstmt.setObject(3, new Vector(3, VectorDimensionType.FLOAT16, null),
                            microsoft.sql.Types.VECTOR);
                    pstmt.executeUpdate();
                }

                // Copy via SELECT+INSERT
                try (ResultSet rs = stmt.executeQuery("SELECT id, v_float32, v_float16 FROM " + srcTable);
                        PreparedStatement insertStmt = mixedConnection.prepareStatement(
                                "INSERT INTO " + dstTable + " (id, v_float32, v_float16) VALUES (?, ?, ?)")) {
                    while (rs.next()) {
                        insertStmt.setInt(1, rs.getInt("id"));
                        insertStmt.setObject(2, rs.getObject("v_float32", Vector.class),
                                microsoft.sql.Types.VECTOR);
                        insertStmt.setObject(3, rs.getObject("v_float16", Vector.class),
                                microsoft.sql.Types.VECTOR);
                        insertStmt.executeUpdate();
                    }
                }

                // Validate destination
                try (ResultSet rs = stmt.executeQuery(
                        "SELECT id, v_float32, v_float16 FROM " + dstTable + " ORDER BY id")) {
                    // Row 1
                    assertTrue(rs.next(), "Expected row 1.");
                    assertMixedVectorEquals(f32Data,
                            rs.getObject("v_float32", Vector.class).getData(),
                            VectorDimensionType.FLOAT32, "Copy row 1 FLOAT32");
                    assertMixedVectorEquals(f16Data,
                            rs.getObject("v_float16", Vector.class).getData(),
                            VectorDimensionType.FLOAT16, "Copy row 1 FLOAT16");

                    // Row 2: nulls
                    assertTrue(rs.next(), "Expected row 2.");
                    assertNull(rs.getObject("v_float32", Vector.class).getData(),
                            "Copy row 2 FLOAT32 should be null.");
                    assertNull(rs.getObject("v_float16", Vector.class).getData(),
                            "Copy row 2 FLOAT16 should be null.");

                    // Row 3
                    assertTrue(rs.next(), "Expected row 3.");
                    assertMixedVectorEquals(f32Data,
                            rs.getObject("v_float32", Vector.class).getData(),
                            VectorDimensionType.FLOAT32, "Copy row 3 FLOAT32");
                    assertNull(rs.getObject("v_float16", Vector.class).getData(),
                            "Copy row 3 FLOAT16 should be null.");

                    assertFalse(rs.next(), "Expected only 3 rows.");
                }
            } finally {
                try (Statement stmt = mixedConnection.createStatement()) {
                    TestUtils.dropTableIfExists(dstTable, stmt);
                    TestUtils.dropTableIfExists(srcTable, stmt);
                }
            }
        }

        // ====================================================================
        // Corner Cases
        // ====================================================================

        /**
         * Tests inserting a FLOAT16 vector into a FLOAT32 column to confirm
         * the server rejects the type mismatch.
         */
        @Test
        public void testMixedWrongTypeInsertion() throws SQLException {
            String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(mixedTableName)
                    + " (id, v_float32, v_float16) VALUES (?, ?, ?)";

            // Intentionally swap: put FLOAT16 data into the FLOAT32 column
            Float[] float16Data = data(0.5f, 1.5f, 2.5f);
            Float[] float32Data = data(1.0f, 2.0f, 3.0f);

            try (PreparedStatement pstmt = mixedConnection.prepareStatement(insertSql)) {
                pstmt.setInt(1, 1);
                // FLOAT16 vector into FLOAT32 column
                pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT16, float16Data),
                        microsoft.sql.Types.VECTOR);
                // FLOAT32 vector into FLOAT16 column
                pstmt.setObject(3, new Vector(3, VectorDimensionType.FLOAT32, float32Data),
                        microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
                fail("Expected an error when inserting wrong vector types into mixed columns.");
            } catch (SQLException e) {
                // Server should reject the type mismatch
                assertTrue(e.getMessage() != null && e.getMessage()
                                .contains("Conversion of vector from data type float16 to float32 is not allowed."));
            }
        }

        /**
         * Tests SELECT INTO with a mixed-type source table to verify schema
         * preservation across both column types.
         */
        @Test
        public void testMixedSelectInto() throws SQLException {
            // Insert source data
            String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(mixedTableName)
                    + " (id, v_float32, v_float16) VALUES (?, ?, ?)";
            Float[] float32Data = data(1.0f, 2.0f, 3.0f);
            Float[] float16Data = data(0.5f, 1.5f, 2.5f);

            try (PreparedStatement pstmt = mixedConnection.prepareStatement(insertSql)) {
                pstmt.setInt(1, 1);
                pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT32, float32Data),
                        microsoft.sql.Types.VECTOR);
                pstmt.setObject(3, new Vector(3, VectorDimensionType.FLOAT16, float16Data),
                        microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            String destTable = AbstractSQLGenerator.escapeIdentifier(
                    RandomUtil.getIdentifier("MixedSelectInto_" + mixedUuid.substring(0, 8)));

            try (Statement stmt = mixedConnection.createStatement()) {
                stmt.executeUpdate("SELECT * INTO " + destTable + " FROM "
                        + AbstractSQLGenerator.escapeIdentifier(mixedTableName));

                try (ResultSet rs = stmt.executeQuery(
                        "SELECT v_float32, v_float16 FROM " + destTable + " WHERE id = 1")) {
                    assertTrue(rs.next(), "No result in destination table.");
                    Vector f32 = rs.getObject("v_float32", Vector.class);
                    Vector f16 = rs.getObject("v_float16", Vector.class);
                    assertMixedVectorEquals(float32Data, f32.getData(),
                            VectorDimensionType.FLOAT32, "SELECT INTO FLOAT32 mismatch");
                    assertMixedVectorEquals(float16Data, f16.getData(),
                            VectorDimensionType.FLOAT16, "SELECT INTO FLOAT16 mismatch");
                }
            } finally {
                try (Statement stmt = mixedConnection.createStatement()) {
                    TestUtils.dropTableIfExists(destTable, stmt);
                }
            }
        }

        /**
         * Tests inserting null vectors from a source table to a destination table
         * via SELECT+INSERT with mixed FLOAT32 and FLOAT16 columns.
         */
        @Test
        public void testMixedInsertFromTableWithNullVectorData() throws SQLException {
            String srcTable = AbstractSQLGenerator.escapeIdentifier(
                    RandomUtil.getIdentifier("MixedNullCopySrc_" + mixedUuid.substring(0, 8)));
            String dstTable = AbstractSQLGenerator.escapeIdentifier(
                    RandomUtil.getIdentifier("MixedNullCopyDst_" + mixedUuid.substring(0, 8)));

            try (Statement stmt = mixedConnection.createStatement()) {
                stmt.executeUpdate("CREATE TABLE " + srcTable
                        + " (id INT PRIMARY KEY, v_float32 VECTOR(3), v_float16 VECTOR(3, float16))");
                stmt.executeUpdate("CREATE TABLE " + dstTable
                        + " (id INT PRIMARY KEY, v_float32 VECTOR(3), v_float16 VECTOR(3, float16))");

                // Insert rows with null vector data
                try (PreparedStatement pstmt = mixedConnection.prepareStatement(
                        "INSERT INTO " + srcTable + " (id, v_float32, v_float16) VALUES (?, ?, ?)")) {
                    for (int i = 1; i <= 3; i++) {
                        pstmt.setInt(1, i);
                        pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT32, null),
                                microsoft.sql.Types.VECTOR);
                        pstmt.setObject(3, new Vector(3, VectorDimensionType.FLOAT16, null),
                                microsoft.sql.Types.VECTOR);
                        pstmt.executeUpdate();
                    }
                }

                // Copy via SELECT+INSERT
                try (ResultSet rs = stmt.executeQuery(
                        "SELECT id, v_float32, v_float16 FROM " + srcTable);
                        PreparedStatement insertStmt = mixedConnection.prepareStatement(
                                "INSERT INTO " + dstTable
                                        + " (id, v_float32, v_float16) VALUES (?, ?, ?)")) {
                    while (rs.next()) {
                        insertStmt.setInt(1, rs.getInt("id"));
                        insertStmt.setObject(2, rs.getObject("v_float32", Vector.class),
                                microsoft.sql.Types.VECTOR);
                        insertStmt.setObject(3, rs.getObject("v_float16", Vector.class),
                                microsoft.sql.Types.VECTOR);
                        insertStmt.executeUpdate();
                    }
                }

                // Validate all rows have null data
                try (ResultSet rs = stmt.executeQuery(
                        "SELECT id, v_float32, v_float16 FROM " + dstTable + " ORDER BY id")) {
                    int rowCount = 0;
                    while (rs.next()) {
                        Vector f32 = rs.getObject("v_float32", Vector.class);
                        Vector f16 = rs.getObject("v_float16", Vector.class);
                        assertEquals(3, f32.getDimensionCount(),
                                "FLOAT32 dimension count mismatch in row " + rs.getInt("id"));
                        assertNull(f32.getData(),
                                "FLOAT32 data should be null in row " + rs.getInt("id"));
                        assertEquals(3, f16.getDimensionCount(),
                                "FLOAT16 dimension count mismatch in row " + rs.getInt("id"));
                        assertNull(f16.getData(),
                                "FLOAT16 data should be null in row " + rs.getInt("id"));
                        rowCount++;
                    }
                    assertEquals(3, rowCount, "Row count mismatch in destination table.");
                }
            } finally {
                try (Statement stmt = mixedConnection.createStatement()) {
                    TestUtils.dropTableIfExists(dstTable, stmt);
                    TestUtils.dropTableIfExists(srcTable, stmt);
                }
            }
        }
    }

}
