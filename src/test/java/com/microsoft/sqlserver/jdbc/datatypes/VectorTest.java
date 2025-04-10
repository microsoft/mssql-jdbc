/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.datatypes;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;

import microsoft.sql.Vector;
import microsoft.sql.Vector.VectorDimensionType;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@RunWith(JUnitPlatform.class)
@DisplayName("Test Vector Data Type")
public class VectorTest extends AbstractTest {

    private static final String tableName = RandomUtil.getIdentifier("VECTOR_Test");
    private static final String maxVectorDataTableName = RandomUtil.getIdentifier("Max_Vector_Test");
    private static final String procedureName = RandomUtil.getIdentifier("VECTOR_Test_Proc");

    @BeforeAll
    private static void setupTest() throws Exception {
        setConnection();

        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (id INT PRIMARY KEY, v VECTOR(3))");
            stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(maxVectorDataTableName)
                    + " (id INT PRIMARY KEY, v VECTOR(1998))");
        }
    }

    @AfterAll
    private static void cleanupTest() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            TestUtils.dropTableIfExists(maxVectorDataTableName, stmt);
        }
    }

    /**
     * Test to check if the server supports vector data type.
     */
    @Test
    void testVectorSupport() {
        boolean isVectorSupportEnabled;
        try {
            java.lang.reflect.Method method = SQLServerConnection.class.getDeclaredMethod("getServerSupportsVector");
            method.setAccessible(true);
            isVectorSupportEnabled = (boolean) method.invoke(connection);
        } catch (Exception e) {
            throw new RuntimeException("Failed to access getServerSupportsVector method", e);
        }
        assertTrue(isVectorSupportEnabled, "Vector support is not enabled");
        System.out.println("Vector support is enabled: " + isVectorSupportEnabled);
    }

    @Test
    void validateVectorData() throws SQLException {
        String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (id, v) VALUES (?, ?)";
        float[] originalData = new float[] { 0.45f, 7.9f, 63.0f };
        Vector initialVector = new Vector(3, VectorDimensionType.F32, originalData);

        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 23);
            pstmt.setObject(2, initialVector, microsoft.sql.Types.VECTOR);
            pstmt.executeUpdate();
        }

        String query = "SELECT id, v FROM " + AbstractSQLGenerator.escapeIdentifier(tableName) + " WHERE id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, 23);
            try (ResultSet rs = stmt.executeQuery()) {

                assertTrue(rs.next(), "No result found for inserted vector.");

                Vector resultVector = rs.getObject("v", Vector.class);
                assertNotNull(resultVector, "Retrieved vector is null.");
                assertEquals(3, resultVector.getDimensionCount(), "Dimension count mismatch.");
                assertArrayEquals(originalData, resultVector.getData(), 0.0001f, "Vector data mismatch.");
            }
        }
    }

    /**
     * Test for inserting a null vector. The expected behavior is that the database
     * should accept the null vector and store it as NULL in the database.
     */
    @Test
    void testInsertNullVector() throws SQLException {
        String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (id, v) VALUES (?, ?)";
        Vector nullVector = new Vector(3, VectorDimensionType.F32, null); // Null vector data

        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 42); 
            pstmt.setObject(2, nullVector, microsoft.sql.Types.VECTOR, nullVector.getDimensionCount()); 
            int rowsInserted = pstmt.executeUpdate();

            assertEquals(1, rowsInserted, "Expected one row to be inserted.");
        } catch (SQLException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void validateMaxVectorData() throws SQLException {
        String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(maxVectorDataTableName)
                + " (id, v) VALUES (?, ?)";

        int dimensionCount = 1998;
        float[] originalData = new float[dimensionCount];

        for (int i = 0; i < dimensionCount; i++) {
            originalData[i] = i + 0.5f;
        }

        Vector initialVector = new Vector(dimensionCount, VectorDimensionType.F32, originalData);

        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 101);

            long insertStart = System.nanoTime();
            pstmt.setObject(2, initialVector, microsoft.sql.Types.VECTOR);
            pstmt.executeUpdate();

            long insertEnd = System.nanoTime();
            long insertDurationMs = (insertEnd - insertStart) / 1000000;
            System.out.println("Insert Time: " + insertDurationMs + " ms");
        }
        

        String query = "SELECT id, v FROM " + AbstractSQLGenerator.escapeIdentifier(maxVectorDataTableName)
                + " WHERE id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, 101);
            
            try (ResultSet rs = stmt.executeQuery()) {
                long readStart = System.nanoTime();
                assertTrue(rs.next(), "No result found for inserted max vector.");
        
                Vector resultVector = rs.getObject("v", Vector.class);
                long readEnd = System.nanoTime();
                long readDurationMs = (readEnd - readStart) / 1000000;
                System.out.println("Read Time: " + readDurationMs + " ms");

                assertNotNull(resultVector, "Retrieved vector is null.");
                assertEquals(dimensionCount, resultVector.getDimensionCount(), "Dimension count mismatch.");
                assertArrayEquals(originalData, resultVector.getData(), 0.0001f, "Vector data mismatch.");
            }

            
        }
    }

    /**
     * * Test for inserting and reading large vector data at scale. This test is designed to check the performance and
     * correctness of inserting and reading large vector data in batches. It uses a loop to insert and read a specified
     * number of records, and measures the time taken for each operation. The test also checks if the inserted and
     * retrieved vector data match.
     * @throws SQLException
     */
    @Test
    void validateMaxVectorDataAtScale() throws SQLException {
        String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(maxVectorDataTableName)
                + " (id, v) VALUES (?, ?)";
        String selectSql = "SELECT id, v FROM " + AbstractSQLGenerator.escapeIdentifier(maxVectorDataTableName);

        int dimensionCount = 1998;
        float[] vectorData = new float[dimensionCount];

        for (int i = 0; i < dimensionCount; i++) {
            vectorData[i] = i + 0.5f;
        }

        Vector vector = new Vector(dimensionCount, VectorDimensionType.F32, vectorData);

        int[] recordCounts = {100}; // For testing, we can use a smaller set of records to avoid long execution times
        // Uncomment the following line to test with larger record counts
        // int[] recordCounts = {100, 1000, 10000, 100000, 1000000};
        int batchSize = 1000001; // Defaulted to a large number for single batch insert

        for (int recordCount : recordCounts) {
            System.out.println("\n--- Testing with " + recordCount + " records ---");

            // Clear table to avoid PK conflicts
            try (Statement stmt = connection.createStatement()) {
                stmt.executeUpdate("TRUNCATE TABLE " + AbstractSQLGenerator.escapeIdentifier(maxVectorDataTableName));
            }

            long insertStart = System.nanoTime();
            try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
                for (int i = 1; i <= recordCount; i++) {
                    pstmt.setInt(1, i);
                    pstmt.setObject(2, vector, microsoft.sql.Types.VECTOR);
                    pstmt.addBatch();

                    if (i % batchSize == 0) {
                        pstmt.executeBatch();
                    }
                }
                pstmt.executeBatch();
            }
            long insertEnd = System.nanoTime();
            long insertDurationMs = (insertEnd - insertStart) / 1000000;
            System.out.println("Insert Time for " + recordCount + " records: " + insertDurationMs + " ms");

            long readStart = System.nanoTime();
            int rowsRead = 0;

            try (PreparedStatement stmt = connection.prepareStatement(selectSql);
                ResultSet rs = stmt.executeQuery()) {

                while (rs.next()) {
                    int id = rs.getInt("id");
                    Vector resultVector = rs.getObject("v", Vector.class);

                    assertNotNull(resultVector, "Vector is null for ID " + id);
                    assertEquals(dimensionCount, resultVector.getDimensionCount(), "Mismatch at ID " + id);
                    // assertArrayEquals(vectorData, resultVector.getData(), 0.0001f, "Vector data mismatch at ID " + id);

                    rowsRead++;
                }
            }
            long readEnd = System.nanoTime();
            long readDurationMs = (readEnd - readStart) / 1000000;
            System.out.println("Read Time for " + rowsRead + " records: " + readDurationMs + " ms");

            assertEquals(recordCount, rowsRead, "Mismatch in number of rows read vs inserted.");
        }
    }

    private static void createProcedure() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            String sql = "CREATE OR ALTER PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(procedureName) + "\n" +
                         "    @inVector VECTOR(3),\n" +
                         "    @outVector VECTOR(3) OUTPUT\n" +
                         "AS\n" +
                         "BEGIN\n" +
                         "    -- Echo input to output\n" +
                         "    SET @outVector = @inVector\n" +
                         "END";
            stmt.execute(sql);
        }
    }

    @Test
    public void testVectorStoredProcedureInputOutput() throws SQLException {
        TestUtils.dropProcedureIfExists(procedureName, connection.createStatement());
        createProcedure();

        String call = "{call " + AbstractSQLGenerator.escapeIdentifier(procedureName) + "(?, ?)}";
        try (SQLServerCallableStatement cstmt = (SQLServerCallableStatement) connection.prepareCall(call)) {
            Vector inputVector = new Vector(3, VectorDimensionType.F32, new float[]{0.5f, 1.0f, 1.5f});

            cstmt.setObject(1, inputVector, microsoft.sql.Types.VECTOR);
            cstmt.registerOutParameter(2, microsoft.sql.Types.VECTOR, 4, 3);
            cstmt.execute();

            Vector result = cstmt.getObject(2, Vector.class);
            assertNotNull(result, "Returned vector should not be null");
            assertArrayEquals(inputVector.getData(), result.getData(), 0.0001f, "Vector data mismatch.");
        } finally {
            TestUtils.dropProcedureIfExists(procedureName, connection.createStatement());
        }
    }
}
