/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.datatypes.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.vectorJsonTest;

import microsoft.sql.Vector;
import microsoft.sql.Vector.VectorDimensionType;

/**
 * Test class for FLOAT16 vector data type.
 *
 * This class extends {@link AbstractVectorTest} and provides FLOAT16-specific
 * configuration. All test methods are inherited from the abstract base class.
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
public class VectorFloat16Test extends AbstractVectorTest {

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

}
