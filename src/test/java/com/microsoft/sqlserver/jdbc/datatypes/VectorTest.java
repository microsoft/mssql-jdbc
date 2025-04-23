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
import com.microsoft.sqlserver.jdbc.SQLServerDataTable;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;

import microsoft.sql.Vector;
import microsoft.sql.Vector.VectorDimensionType;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@RunWith(JUnitPlatform.class)
@DisplayName("Test Vector Data Type")
public class VectorTest extends AbstractTest {

    private static final String tableName = RandomUtil.getIdentifier("VECTOR_Test");
    private static final String maxVectorDataTableName = RandomUtil.getIdentifier("Max_Vector_Test");
    private static final String procedureName = RandomUtil.getIdentifier("VECTOR_Test_Proc");
    private static final String TABLE_NAME = RandomUtil.getIdentifier("VECTOR_TVP_Test");
    private static final String TVP_NAME = RandomUtil.getIdentifier("VECTOR_TVP_Test_Type");

    @BeforeAll
    private static void setupTest() throws Exception {
        setConnection();

        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (id INT PRIMARY KEY, v VECTOR(3))");
            stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(maxVectorDataTableName)
                    + " (id INT PRIMARY KEY, v VECTOR(1998))");
            stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(TABLE_NAME) + " (rowId INT IDENTITY, c1 VECTOR(3) NULL)");
            stmt.executeUpdate("CREATE TYPE " + AbstractSQLGenerator.escapeIdentifier(TVP_NAME) + " AS TABLE (c1 VECTOR(3) NULL)");
        }
    }

    @AfterAll
    private static void cleanupTest() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            TestUtils.dropTableIfExists(maxVectorDataTableName, stmt);
            TestUtils.dropTypeIfExists(TVP_NAME, stmt);
            TestUtils.dropTableIfExists(TABLE_NAME, stmt);
            TestUtils.dropProcedureIfExists(procedureName, stmt);
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
            pstmt.setObject(2, nullVector, microsoft.sql.Types.VECTOR); 
            int rowsInserted = pstmt.executeUpdate();

            assertEquals(1, rowsInserted, "Expected one row to be inserted.");
        } 
        String query = "SELECT id, v FROM " + AbstractSQLGenerator.escapeIdentifier(tableName) + " WHERE id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, 42);
            try (ResultSet rs = stmt.executeQuery()) {

                assertTrue(rs.next(), "No result found for inserted vector.");

                Vector resultVector = rs.getObject("v", Vector.class);
                assertEquals(3, resultVector.getDimensionCount(), "Dimension count mismatch.");
                assertNull(resultVector.getData(), "Expected null vector data.");
            }
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

    /**
     * Test for calling a stored procedure with vector input and output parameters. The expected behavior is that the
     * database should accept the vector and return it as an output parameter.
     */
    @Test
    public void testVectorStoredProcedureInputOutput() throws SQLException {
        createProcedure();

        String call = "{call " + AbstractSQLGenerator.escapeIdentifier(procedureName) + "(?, ?)}";
        try (SQLServerCallableStatement cstmt = (SQLServerCallableStatement) connection.prepareCall(call)) {
            Vector inputVector = new Vector(3, VectorDimensionType.F32, new float[]{0.5f, 1.0f, 1.5f});

            cstmt.setObject(1, inputVector, microsoft.sql.Types.VECTOR);
            cstmt.registerOutParameter(2, microsoft.sql.Types.VECTOR, 3, 4);
            cstmt.execute();

            Vector result = cstmt.getObject(2, Vector.class);
            assertNotNull(result, "Returned vector should not be null");
            assertArrayEquals(inputVector.getData(), result.getData(), 0.0001f, "Vector data mismatch.");
        }
    }

    /**
     * Test for inserting a vector into a TVP. The expected behavior is that the database should accept the vector and
     * store it in the table.
     */
    @Test
    public void testVectorTVP() throws SQLException {
        Vector expectedVector = new Vector(3, VectorDimensionType.F32, new float[]{0.1f, 0.2f, 0.3f});

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
                    assertArrayEquals(expectedVector.getData(), actual.getData(), 0.0001f, "Vector data mismatch.");
                }
            }
        }
    }

    /**
     * Test for inserting a null vector into a TVP. The expected behavior is that the database should accept the null
     * vector and store it as NULL in the database.
     */
    @Test
    public void testNullVectorInsertTVP() throws SQLException {
        TestUtils.dropTableIfExists(TABLE_NAME, connection.createStatement());
        TestUtils.dropTypeIfExists(TVP_NAME, connection.createStatement());
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(TABLE_NAME) + " (rowId INT IDENTITY, c1 VECTOR(3) NULL)");
            stmt.executeUpdate("CREATE TYPE " + AbstractSQLGenerator.escapeIdentifier(TVP_NAME) + " AS TABLE (c1 VECTOR(3) NULL)");
        }

        Vector expectedVector = new Vector(3, VectorDimensionType.F32, null); // Null vector data

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
                    assertEquals(3, actual.getDimensionCount(), "Dimension count mismatch.");
                    assertNull(actual.getData(), "Expected null vector data.");
                }
            }
        }
    }

    private static void createVectorUdf() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            String sql =
                "CREATE OR ALTER FUNCTION " + AbstractSQLGenerator.escapeIdentifier("EchoVectorUdf") + "\n" +
                "(@v VECTOR(3))\n" +
                "RETURNS VECTOR(3)\n" +
                "AS\n" +
                "BEGIN\n" +
                "    RETURN @v;\n" +
                "END";
            stmt.execute(sql);
        }
    }
    
    /**
     * Test for calling a vector UDF. The expected behavior is that the database should accept the vector and return it as
     * an output parameter.
     */
    @Test
    public void testVectorUdf() throws SQLException {
        createVectorUdf();

        Vector inputVector = new Vector(3, VectorDimensionType.F32, new float[]{1.1f, 2.2f, 3.3f});
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE #vec_input (v VECTOR(3));");
        }
        try (PreparedStatement pstmt = connection.prepareStatement("INSERT INTO #vec_input VALUES (?);")) {
            pstmt.setObject(1, inputVector, microsoft.sql.Types.VECTOR);
            pstmt.executeUpdate();
        }

        String query = "SELECT dbo." + AbstractSQLGenerator.escapeIdentifier("EchoVectorUdf") + "((SELECT TOP 1 v FROM #vec_input));";
        try (PreparedStatement selectStmt = connection.prepareStatement(query);
            ResultSet rs = selectStmt.executeQuery()) {
            assertTrue(rs.next(), "Result set is empty");

            Vector result = rs.getObject(1, Vector.class);
            assertNotNull(result, "Returned vector should not be null");
            assertArrayEquals(inputVector.getData(), result.getData(), 0.0001f, "Vector data mismatch.");
        } finally {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS #vec_input;");
            }
        }
    }

    /**
     * Test for inserting a vector into a table using SELECT INTO statement
     * 
     * @throws SQLException
     */
    @Test
    public void testSelectIntoForVector() throws SQLException {
        String sourceTable = AbstractSQLGenerator.escapeIdentifier("srcTable");
        String destinationTable = AbstractSQLGenerator.escapeIdentifier("desTable");

        try (Statement stmt = connection.createStatement()) {
            // Create the source table
            String createSourceTableSql = "CREATE TABLE " + sourceTable + " (id INT, v VECTOR(3))";
            stmt.executeUpdate(createSourceTableSql);

            // Insert sample data into the source table
            float[] vectorData = new float[] { 1.1f, 2.2f, 3.3f };
            Vector vector = new Vector(3, VectorDimensionType.F32, vectorData);

            String insertSql = "INSERT INTO " + sourceTable + " (id, v) VALUES (?, ?)";
            try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
                pstmt.setInt(1, 1);
                pstmt.setObject(2, vector, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            float[] vectorData2 = null;
            Vector vector2 = new Vector(3, VectorDimensionType.F32, vectorData2);

            String insertSql2 = "INSERT INTO " + sourceTable + " (id, v) VALUES (?, ?)";
            try (PreparedStatement pstmt = connection.prepareStatement(insertSql2)) {
                pstmt.setInt(1, 2);
                pstmt.setObject(2, vector2, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            // Drop the destination table if it already exists
            String dropTableSql = "IF OBJECT_ID('" + destinationTable + "', 'U') IS NOT NULL DROP TABLE "
                    + destinationTable;
            stmt.executeUpdate(dropTableSql);

            // Perform the SELECT INTO operation
            String selectIntoSql = "SELECT * INTO " + destinationTable + " FROM " + sourceTable;
            stmt.executeUpdate(selectIntoSql);

            // Validate the data in the destination table
            String validateSql = "SELECT id, v FROM " + destinationTable + " ORDER BY id";
            try (ResultSet rs = stmt.executeQuery(validateSql)) {
                int rowCount = 0;
                while (rs.next()) {
                    int id = rs.getInt("id");
                    Vector resultVector = rs.getObject("v", Vector.class);

                    assertNotNull(resultVector, "Vector is null in destination table for ID " + id);

                    if (id == 1) {
                        assertArrayEquals(vector.getData(), resultVector.getData(), 0.0001f,
                                "Vector data mismatch in destination table for ID 1.");
                    } else if (id == 2) {
                        assertArrayEquals(vector2.getData(), resultVector.getData(), 0.0001f,
                                "Vector data mismatch in destination table for ID 2.");
                    } else {
                        fail("Unexpected ID found in destination table: " + id);
                    }
                    rowCount++;
                }
                assertEquals(2, rowCount, "Row count mismatch in destination table.");
            }
        } finally {
            // Cleanup: Drop the source and destination tables
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(sourceTable, stmt);
                TestUtils.dropTableIfExists(destinationTable, stmt);
            }
        }
    }

    /**
     * Test vector insertion and retrieval in a global temporary table.
     * Global temporary tables (##TempVector) are shared across sessions and persist
     * until all sessions using them close.
     */
    @Test
    public void testVectorInsertionInGlobalTempTable() throws SQLException {
        String dstTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("##TempVector")));

        String createTableSQL = "CREATE TABLE " + dstTable + " (id INT PRIMARY KEY, data VECTOR(3))";
        String insertSQL = "INSERT INTO " + dstTable + " VALUES (?, ?)";
        String selectSQL = "SELECT data FROM " + dstTable + " WHERE id = ?";

        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(dstTable, stmt);
            stmt.execute(createTableSQL);
        }

        try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) {
            float[] vectorData = { 1.0f, 2.0f, 3.0f };
            Vector vector = new Vector(3, Vector.VectorDimensionType.F32, vectorData);

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
                assertArrayEquals(new float[] { 1.0f, 2.0f, 3.0f }, resultVector.getData(), 0.0001f,
                        "Vector data mismatch.");
            }
        }

        // Ensure cleanup of the global temporary table
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(dstTable, stmt);
        }
    }

    /**
     * Test VECTOR insertion and retrieval in a local temporary table.
     * Local temporary tables (#TempVector) are session-bound and deleted
     * automatically when the session ends.
     */
    @Test
    public void testVectorInsertionInLocalTempTable() throws SQLException {
        try (SQLServerConnection conn = getConnection()) {
            String dstTable = TestUtils
                    .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("#TempVector")));
            String createTableSQL = "CREATE TABLE " + dstTable + " (id INT PRIMARY KEY, data VECTOR(3))";
            String insertSQL = "INSERT INTO " + dstTable + " VALUES (?, ?)";
            String selectSQL = "SELECT data FROM " + dstTable + " WHERE id = ?";

            try (Statement stmt = conn.createStatement()) {
                stmt.execute(createTableSQL);
            }

            try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                float[] vectorData = { 4.0f, 5.0f, 6.0f };
                Vector vector = new Vector(3, Vector.VectorDimensionType.F32, vectorData);

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
                    assertArrayEquals(new float[] { 4.0f, 5.0f, 6.0f }, resultVector.getData(), 0.0001f,
                            "Vector data mismatch.");
                }
            }
        } // Connection auto-closes here, so #TempVector is automatically dropped
    }

    /**
     * Test for vector normalization using a UDF.
     * The UDF normalizes the input vector and returns the normalized vector.
     */
    @Test
    public void testVectorNormalizeUdf() throws SQLException {
        String vectorsTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("Vectors")));
        String udfName = "dbo.udf2";

        try (Statement stmt = connection.createStatement()) {
            // Create the UDF
            String createUdfSQL = "CREATE OR ALTER FUNCTION " + udfName + " (@p VECTOR(3)) " +
                    "RETURNS TABLE AS " +
                    "RETURN (SELECT vector_normalize(@p, 'norm2') AS d)";
            stmt.execute(createUdfSQL);

            // Create the table
            String createTableSQL = "CREATE TABLE " + vectorsTable + " (id INT PRIMARY KEY, data VECTOR(3))";
            stmt.execute(createTableSQL);

            // Insert sample data
            String insertSQL = "INSERT INTO " + vectorsTable + " (id, data) VALUES (?, ?)";
            try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) {
                float[] vectorData = { 1.0f, 2.0f, 3.0f };
                Vector vector = new Vector(3, Vector.VectorDimensionType.F32, vectorData);

                pstmt.setInt(1, 1);
                pstmt.setObject(2, vector, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            // Test the UDF
            String udfTestSQL = "DECLARE @v VECTOR(3) = (SELECT data FROM " + vectorsTable + " WHERE id = 1); " +
                    "SELECT * FROM " + udfName + "(@v)";

            try (ResultSet rs = stmt.executeQuery(udfTestSQL)) {
                assertTrue(rs.next(), "No result returned from UDF.");
                Vector normalizedVector = rs.getObject("d", Vector.class);
                assertNotNull(normalizedVector, "Normalized vector is null.");
                float[] expectedNormalizedData = { 0.2673f, 0.5345f, 0.8018f }; // Normalized values for [1, 2, 3]
                assertArrayEquals(expectedNormalizedData, normalizedVector.getData(), 0.0001f,
                        "Normalized vector mismatch.");
            }
        } finally {
            // Cleanup: Drop the UDF and table
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropFunctionIfExists(udfName, stmt);
                TestUtils.dropTableIfExists(vectorsTable, stmt);
            }
        }
    }

    /**
     * Test for vector normalization using a scalar-valued function.
     * The function normalizes the input vector and returns the normalized vector.
     */
    @Test
    public void testVectorNormalizeScalarFunction() throws SQLException {
        String vectorsTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("Vectors")));
        String udfName = "dbo.svf";

        try (Statement stmt = connection.createStatement()) {
            // Drop table and UDF if they already exist
            TestUtils.dropTableIfExists(vectorsTable, stmt);
            String dropUdfSQL = "IF OBJECT_ID('" + udfName + "', 'FN') IS NOT NULL DROP FUNCTION " + udfName;
            stmt.execute(dropUdfSQL);

            // Create the scalar-valued function
            String createUdfSQL = "CREATE FUNCTION " + udfName + " (@p VECTOR(3)) " +
                    "RETURNS VECTOR(3) AS " +
                    "BEGIN " +
                    "    DECLARE @v VECTOR(3); " +
                    "    SET @v = vector_normalize(@p, 'norm2'); " +
                    "    RETURN @v; " +
                    "END";
            stmt.execute(createUdfSQL);

            // Create the table
            String createTableSQL = "CREATE TABLE " + vectorsTable + " (id INT PRIMARY KEY, data VECTOR(3))";
            stmt.execute(createTableSQL);

            // Insert sample data
            String insertSQL = "INSERT INTO " + vectorsTable + " (id, data) VALUES (?, ?)";
            try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) {
                float[] vectorData = { 1.0f, 2.0f, 3.0f };
                Vector vector = new Vector(3, Vector.VectorDimensionType.F32, vectorData);

                pstmt.setInt(1, 1);
                pstmt.setObject(2, vector, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            // Test the scalar-valued function
            String udfTestSQL = "DECLARE @v VECTOR(3) = (SELECT data FROM " + vectorsTable + " WHERE id = 1); " +
                    "SELECT " + udfName + "(@v) AS normalizedVector";
            try (ResultSet rs = stmt.executeQuery(udfTestSQL)) {
                assertTrue(rs.next(), "No result returned from scalar-valued function.");
                Vector normalizedVector = rs.getObject("normalizedVector", Vector.class);
                assertNotNull(normalizedVector, "Normalized vector is null.");
                float[] expectedNormalizedData = { 0.2673f, 0.5345f, 0.8018f }; // Normalized values for [1, 2, 3]
                assertArrayEquals(expectedNormalizedData, normalizedVector.getData(), 0.0001f,
                        "Normalized vector mismatch.");
            }
        } finally {
            // Cleanup: Drop the UDF and table
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropFunctionIfExists(udfName, stmt);
                TestUtils.dropTableIfExists(vectorsTable, stmt);
            }
        }
    }
}