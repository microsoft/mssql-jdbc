/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.datatypes.vector.bulkcopy;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCSVFileRecord;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.vectorJsonTest;

import microsoft.sql.Vector;
import microsoft.sql.Vector.VectorDimensionType;

/**
 * Test class for FLOAT32 vector bulk copy operations.
 *
 * This class extends {@link VectorBulkCopyTest} and provides FLOAT32-specific
 * configuration. All test methods are inherited from the abstract base class.
 *
 * SQL Syntax: VECTOR(n) where n is the dimension count
 * Scale: 4 (FLOAT32)
 * Max Dimensions: 1998
 */
@RunWith(JUnitPlatform.class)
@DisplayName("Test Vector Bulk Copy - FLOAT32")
@vectorJsonTest
@Tag(Constants.vectorTest)
public class VectorFloat32BulkCopyTest extends VectorBulkCopyTest {

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
     * Calculation: (1998 * 4) + 8 == 8000
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
    // FLOAT16 on V1 Connection Tests (version mismatch)
    // ============================================================================

    /**
     * Test that bulk copying FLOAT16 vector data via ISQLServerBulkData on a v1 connection
     * throws an error. V1 only supports FLOAT32; FLOAT16 requires v2.
     */
    @Test
    public void testBulkCopyFloat16VectorOnV1Connection() throws SQLException {
        String dstTable = TestUtils
                .escapeSingleQuotes(
                        AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstBCF16onV1")));

        try (Connection conn = DriverManager.getConnection(getVectorConnectionString());
                Statement dstStmt = conn.createStatement();
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn)) {

            // Create a FLOAT32 table (supported on v1)
            dstStmt.executeUpdate(
                    "CREATE TABLE " + dstTable + " (vectorCol VECTOR(3));");

            bulkCopy.setDestinationTableName(dstTable);
            Float[] vectorData = createTestData(1.0f, 2.0f, 3.0f);
            Vector vector = new Vector(vectorData.length, VectorDimensionType.FLOAT16, vectorData);
            VectorBulkData vectorBulkData = new VectorBulkData(vector, vectorData.length,
                    VectorDimensionType.FLOAT16);

            bulkCopy.writeToServer(vectorBulkData);
            fail("Expected SQLException for FLOAT16 on v1 connection.");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("FLOAT16 vector type is not supported"),
                    "Expected FLOAT16 not supported error, got: " + e.getMessage());
        } finally {
            try (Connection conn = DriverManager.getConnection(getVectorConnectionString());
                    Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(dstTable, stmt);
            }
        }
    }

    /**
     * Test that bulk copying FLOAT16 vector data from CSV on a v1 connection
     * throws an error. V1 only supports FLOAT32; FLOAT16 requires v2.
     */
    @Test
    @vectorJsonTest
    public void testBulkCopyFloat16VectorFromCSVOnV1Connection() throws SQLException {
        String dstTable = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("dstBCCsvF16onV1"));
        String fileName = csvFilePath + VECTOR_INPUT_CSV_FILE;

        try (Connection con = DriverManager.getConnection(getVectorConnectionString());
                Statement stmt = con.createStatement();
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
                SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(fileName, null, ",", true)) {

            // Create a FLOAT32 table (supported on v1)
            stmt.executeUpdate(
                    "CREATE TABLE " + dstTable + " (id INT, vectorCol VECTOR(3));");

            // Add column metadata with FLOAT16 scale (2) — mismatch with v1 connection
            fileRecord.addColumnMetadata(1, "id", java.sql.Types.INTEGER, 0, 0);
            fileRecord.addColumnMetadata(2, "vectorCol", microsoft.sql.Types.VECTOR, 3, 2);
            fileRecord.setEscapeColumnDelimitersCSV(true);

            bulkCopy.setDestinationTableName(dstTable);

            SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
            options.setKeepIdentity(false);
            options.setTableLock(true);
            options.setBulkCopyTimeout(60);
            bulkCopy.setBulkCopyOptions(options);

            bulkCopy.writeToServer(fileRecord);
            fail("Expected SQLException for FLOAT16 CSV on v1 connection.");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("FLOAT16 vector type is not supported"),
                    "Expected FLOAT16 not supported error, got: " + e.getMessage());
        } finally {
            try (Connection con = DriverManager.getConnection(getVectorConnectionString());
                    Statement stmt = con.createStatement()) {
                TestUtils.dropTableIfExists(dstTable, stmt);
            }
        }
    }
}
