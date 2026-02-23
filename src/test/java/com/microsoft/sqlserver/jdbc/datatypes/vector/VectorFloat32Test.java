/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.datatypes.vector;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.vectorJsonTest;

import microsoft.sql.Vector;
import microsoft.sql.Vector.VectorDimensionType;

/**
 * Test class for FLOAT32 vector data type.
 *
 * This class extends {@link AbstractVectorTest} and provides FLOAT32-specific
 * configuration. All test methods are inherited from the abstract base class.
 *
 * - SQL Syntax: VECTOR(n) where n is the dimension count
 * - Scale: 4 (FLOAT32)
 * - Max Dimensions: 1998
 */
@RunWith(JUnitPlatform.class)
@DisplayName("Test Vector Data Type - FLOAT32")
@vectorJsonTest
@Tag(Constants.vectorTest)
public class VectorFloat32Test extends AbstractVectorTest {

    @Override
    protected VectorDimensionType getVectorDimensionType() {
        return VectorDimensionType.FLOAT32;
    }

    @Override
    protected String getColumnDefinition(int dimensionCount) {
        return "VECTOR(" + dimensionCount + ")";
    }

    @Override
    protected int getScale() {
        return 4; // FLOAT32 scale
    }

    @Override
    protected String getTypeName() {
        return "FLOAT32";
    }

    /**
     * The server implementation restricts vectors to a total of 8000 bytes.  
     * Subtracting the 8 byte header leaves 7992 bytes for data.  
     * For float32, each dimension requires 4 bytes, so the maximum number of dimensions is 1998.
     * Calculation:  (1998 * 4) + 8 == 8000 
     */
    @Override
    protected int getMaxDimensionCount() {
        return 1998;
    }

    @Override
    protected String getRequiredVectorTypeSupport() {
        return "v1"; // FLOAT32 works with v1
    }

    // ============================================================================
    // FLOAT32-Specific Tests
    // ============================================================================

    /**
     * Tests inserting Float.MAX_VALUE and Float.MIN_VALUE into a FLOAT32 vector.
     * FLOAT32 supports the full 32-bit IEEE 754 range, so these values round-trip successfully.
     */
    @Test
    public void testVectorWithMaxAndMinFloatValues() throws SQLException {
        String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (id, v) VALUES (?, ?)";
        Float[] extremeData = createTestData(Float.MAX_VALUE, Float.MIN_VALUE, 0.0f);
        Vector vector = new Vector(3, getVectorDimensionType(), extremeData);

        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 1);
            pstmt.setObject(2, vector, microsoft.sql.Types.VECTOR);
            pstmt.executeUpdate();
        }

        String query = "SELECT id, v FROM " + AbstractSQLGenerator.escapeIdentifier(tableName) + " WHERE id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, 1);
            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue(rs.next(), "No result found for inserted vector with extreme float values.");

                Vector resultVector = rs.getObject("v", Vector.class);
                assertNotNull(resultVector, "Retrieved vector is null.");
                assertEquals(3, resultVector.getDimensionCount(), "Dimension count mismatch.");
                assertVectorDataEquals(extremeData, resultVector.getData(), "Vector data mismatch for extreme float values.");
            }
        }
    }

    /**
     * Validates basic vector data insert and retrieve for FLOAT32.
     * FLOAT32 has full precision so exact value comparison is expected.
     */
    @Test
    public void validateVectorData() throws SQLException {
        String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (id, v) VALUES (?, ?)";
        Float[] originalData = createTestData(0.45f, 7.9f, 63.0f);
        microsoft.sql.Vector initialVector = new microsoft.sql.Vector(3, getVectorDimensionType(), originalData);

        try (java.sql.PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 1);
            pstmt.setObject(2, initialVector, microsoft.sql.Types.VECTOR);
            pstmt.executeUpdate();
        }

        String query = "SELECT id, v FROM " + AbstractSQLGenerator.escapeIdentifier(tableName) + " WHERE id = ?";
        try (java.sql.PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, 1);
            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue(rs.next(), "No result found for inserted vector.");

                microsoft.sql.Vector resultVector = rs.getObject("v", microsoft.sql.Vector.class);
                assertNotNull(resultVector, "Retrieved vector is null.");
                assertEquals(3, resultVector.getDimensionCount(), "Dimension count mismatch.");
                assertVectorDataEquals(originalData, resultVector.getData(), "Vector data mismatch.");

                // FLOAT32 preserves exact values
                String expectedToString = "VECTOR(FLOAT32, 3) : [0.45, 7.9, 63.0]";
                assertEquals(expectedToString, resultVector.toString(), "Vector toString output mismatch.");
            }
        }
    }

    // ============================================================================
    // UDF Normalization Tests (FLOAT32 only)
    // ============================================================================

    /**
     * Tests the vector_normalize UDF with FLOAT32 vectors.
     * This test verifies that the UDF correctly normalizes a FLOAT32 vector and returns expected results.
     */
    @Test
    public void testVectorNormalizeUdf() throws SQLException {
        String vectorsTable = TestUtils.escapeSingleQuotes(
                AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("Vectors_" + uuid.substring(0, 8))));
        String udfName = TestUtils.escapeSingleQuotes(
                AbstractSQLGenerator.escapeIdentifier(functionTvpName));

        try (Statement stmt = connection.createStatement()) {
            String createUdfSQL = "CREATE OR ALTER FUNCTION " + udfName + " (@p " + getColumnDefinition(3) + ") " +
                    "RETURNS TABLE AS " +
                    "RETURN (SELECT vector_normalize(@p, 'norm2') AS d)";
            stmt.execute(createUdfSQL);

            String createTableSQL = "CREATE TABLE " + vectorsTable + " (id INT PRIMARY KEY, data " + getColumnDefinition(3) + ")";
            stmt.execute(createTableSQL);

            String insertSQL = "INSERT INTO " + vectorsTable + " (id, data) VALUES (?, ?)";
            try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) {
                Float[] vectorData = createTestData(1.0f, 2.0f, 3.0f);
                Vector vector = new Vector(3, getVectorDimensionType(), vectorData);

                pstmt.setInt(1, 1);
                pstmt.setObject(2, vector, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            String udfTestSQL = "DECLARE @v " + getColumnDefinition(3) + " = (SELECT data FROM " + vectorsTable + " WHERE id = 1); " +
                    "SELECT * FROM " + udfName + "(@v)";

            try (ResultSet rs = stmt.executeQuery(udfTestSQL)) {
                assertTrue(rs.next(), "No result returned from UDF.");
                Vector normalizedVector = rs.getObject("d", Vector.class);
                assertNotNull(normalizedVector, "Normalized vector is null.");

                Float[] expectedNormalizedData = createTestData(0.2673f, 0.5345f, 0.8018f);
                Object[] actualNormalizedData = normalizedVector.getData();

                for (int i = 0; i < expectedNormalizedData.length; i++) {
                    assertEquals(expectedNormalizedData[i], (float) actualNormalizedData[i], 0.0001f,
                            "Normalized vector mismatch at index " + i);
                }
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropFunctionIfExists(udfName, stmt);
                TestUtils.dropTableIfExists(vectorsTable, stmt);
            }
        }
    }

    private void setupSVF(String schemaName, String funcName, String tableName) throws SQLException {
        String escapedSchema = AbstractSQLGenerator.escapeIdentifier(schemaName);
        String escapedFunc = AbstractSQLGenerator.escapeIdentifier(funcName);

        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(
                    "IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '" + schemaName + "') " +
                            "EXEC('CREATE SCHEMA " + escapedSchema + "')");

            String createSvfSQL = "CREATE FUNCTION " + escapedSchema + "." + escapedFunc + " (@p " + getColumnDefinition(3) + ") " +
                    "RETURNS " + getColumnDefinition(3) + " AS " +
                    "BEGIN " +
                    "    DECLARE @v " + getColumnDefinition(3) + "; " +
                    "    SET @v = vector_normalize(@p, 'norm2'); " +
                    "    RETURN @v; " +
                    "END";
            stmt.executeUpdate(createSvfSQL);

            String createTableSQL = "CREATE TABLE " + tableName + " (id INT PRIMARY KEY, vec " + getColumnDefinition(3) + ")";
            stmt.execute(createTableSQL);
        }
    }

    /**
     * Tests the vector_identity UDF with FLOAT32 vectors.
     * This test verifies that the UDF correctly returns the input vector without modification.
     */
    @Test
    public void testVectorIdentityScalarFunction() throws SQLException {
        String schemaName = "testschemaVector" + uuid.substring(0, 8);
        String funcName = "svf" + uuid.substring(0, 8);
        String escapedSchema = AbstractSQLGenerator.escapeIdentifier(schemaName);
        String escapedFunc = AbstractSQLGenerator.escapeIdentifier(funcName);
        String tableName = escapedSchema + "." + "Vectors" + uuid.substring(0, 8);

        try {
            setupSVF(schemaName, funcName, tableName);

            try (PreparedStatement pstmt = connection.prepareStatement(
                    "INSERT INTO " + tableName + " (id, vec) VALUES (?, ?)")) {
                Vector v = new Vector(3, getVectorDimensionType(), createTestData(1.0f, 2.0f, 3.0f));
                pstmt.setInt(1, 1);
                pstmt.setObject(2, v, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            String svfTestSQL = "DECLARE @v " + getColumnDefinition(3) + " = (SELECT vec FROM " + tableName + " WHERE id = 1); " +
                    "SELECT " + escapedSchema + "." + escapedFunc + "(@v) AS normalizedVector";

            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery(svfTestSQL)) {

                assertTrue(rs.next(), "No result from SVF.");
                Vector normalizedVector = rs.getObject(1, Vector.class);
                assertNotNull(normalizedVector, "Returned vector is null.");

                Float[] expectedNormalizedData = createTestData(0.26726124f, 0.5345225f, 0.8017837f);
                assertVectorDataEquals(expectedNormalizedData, normalizedVector.getData(), "Vector roundtrip mismatch.");
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropFunctionWithSchemaIfExists(schemaName + "." + funcName, stmt);
                TestUtils.dropTableWithSchemaIfExists(tableName, stmt);
                TestUtils.dropSchemaIfExists(schemaName, stmt);
            }
        }
    }

}