/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.datatypes;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
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
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.vectorJsonTest;

import microsoft.sql.Vector;
import microsoft.sql.Vector.VectorDimensionType;

@RunWith(JUnitPlatform.class)
@DisplayName("Test Vector Data Type")
@vectorJsonTest
@Tag(Constants.vectorTest)
// Vector datatype is supported starting with SQL Server 2025. 
// Earlier versions expose vector values as JSON strings.
public class VectorTest extends AbstractTest {

    private static final String tableName = RandomUtil.getIdentifier("VECTOR_Test");
    private static final String tableNameWithMultipleVectorColumn = RandomUtil.getIdentifier("VECTOR_Test_MultipleColumn");
    private static final String maxVectorDataTableName = RandomUtil.getIdentifier("Max_Vector_Test");
    private static final String procedureName = RandomUtil.getIdentifier("VECTOR_Test_Proc");
    private static final String functionName = RandomUtil.getIdentifier("VECTOR_Test_Func");
    private static final String functionTvpName = RandomUtil.getIdentifier("VECTOR_Test_TVP_Func");
    private static final String vectorUdf = RandomUtil.getIdentifier("VectorUdf");
    private static final String TABLE_NAME = RandomUtil.getIdentifier("VECTOR_TVP_Test");
    private static final String TVP_NAME = RandomUtil.getIdentifier("VECTOR_TVP_Test_Type");
    private static final String TVP = RandomUtil.getIdentifier("VECTOR_TVP_UDF_Test_Type");
    private static final String uuid = UUID.randomUUID().toString().replaceAll("-", "");

    @BeforeAll
    private static void setupTest() throws Exception {
        setConnection();

        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (id INT PRIMARY KEY, v VECTOR(3))");
            stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableNameWithMultipleVectorColumn)
                    + " (id INT PRIMARY KEY, v1 VECTOR(3), v2 VECTOR(4))");
            stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(maxVectorDataTableName)
                    + " (id INT PRIMARY KEY, v VECTOR(1998))");
            stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(TABLE_NAME) + " (rowId INT IDENTITY, c1 VECTOR(3) NULL)");
            stmt.executeUpdate("CREATE TYPE " + AbstractSQLGenerator.escapeIdentifier(TVP_NAME) + " AS TABLE (c1 VECTOR(3) NULL)");
            stmt.executeUpdate("CREATE TYPE " + AbstractSQLGenerator.escapeIdentifier(TVP) + " AS TABLE (c1 VECTOR(4) NULL)");
        }
    }

    @AfterAll
    private static void cleanupTest() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            TestUtils.dropTableIfExists(tableNameWithMultipleVectorColumn, stmt);
            TestUtils.dropTableIfExists(maxVectorDataTableName, stmt);
            TestUtils.dropTypeIfExists(TVP_NAME, stmt);
            TestUtils.dropTableIfExists(TABLE_NAME, stmt);
            TestUtils.dropProcedureIfExists(procedureName, stmt);
            TestUtils.dropFunctionIfExists(functionName, stmt);
            TestUtils.dropFunctionIfExists(functionTvpName, stmt);
        }
    }

    /**
     * Test to verify that the vector data type is correctly reported in DatabaseMetaData.
     */
    @Test
    public void testConnectionGetMetaData() throws Exception {
        DatabaseMetaData metaData = connection.getMetaData();
        assertNotNull(metaData, "DatabaseMetaData should not be null");

        try (ResultSet rs = metaData.getColumns(null, null, tableName, "%")) {

            while (rs.next()) {
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    String columnName = rs.getMetaData().getColumnName(i);
                    String columnValue = rs.getString(i);
                    // Uncomment the line below to see all column names and values
                    // System.out.println(columnName + " : " + columnValue);
                }
                // Below will show -156 as vector data type value but right now sp_columns_170 is not introduced in SQL DB instances 
                // which will return value as -3. For now below assertion test is commented out.

                // if ("v".equalsIgnoreCase(rs.getString("COLUMN_NAME"))) {
                //     assertEquals(microsoft.sql.Types.VECTOR, rs.getInt("DATA_TYPE"));
                // }
            }
        }
    }

    /**
     * Test to verify that the vector data type is correctly reported in DatabaseMetaData for Azure DW.
     */
    @Test
    public void testConnectionGetMetaDataAzureDW() throws Exception {

        // Use reflection to simulate Azure DW connection
        Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
        f1.setAccessible(true);
        f1.set(connection, true);

        // Set isAzure to true as well since some code paths check both
        Field f2 = SQLServerConnection.class.getDeclaredField("isAzure");
        f2.setAccessible(true);
        f2.set(connection, true);

        DatabaseMetaData metaData = connection.getMetaData();
        assertNotNull(metaData, "DatabaseMetaData should not be null");

        try (ResultSet rs = metaData.getColumns(null, null, tableName, "%")) {

            while (rs.next()) {
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    String columnName = rs.getMetaData().getColumnName(i);
                    String columnValue = rs.getString(i);
                    // Uncomment the line below to see all column names and values
                    // System.out.println(columnName + " : " + columnValue);
                }
                // Below will show -156 as vector data type value but right now sp_columns_170 is not introduced in SQL DB instances 
                // which will return value as -3. For now below assertion test is commented out.
                
                // if ("v".equalsIgnoreCase(rs.getString("COLUMN_NAME"))) {
                //     assertEquals(microsoft.sql.Types.VECTOR, rs.getInt("DATA_TYPE"));
                // }
            }
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
        Object[] originalData = new Float[] { 0.45f, 7.9f, 63.0f };
        Vector initialVector = new Vector(3, VectorDimensionType.FLOAT32, originalData);

        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 19);
            pstmt.setObject(2, initialVector, microsoft.sql.Types.VECTOR);
            pstmt.executeUpdate();
        }

        String query = "SELECT id, v FROM " + AbstractSQLGenerator.escapeIdentifier(tableName) + " WHERE id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, 19);
            try (ResultSet rs = stmt.executeQuery()) {

                assertTrue(rs.next(), "No result found for inserted vector.");

                Vector resultVector = rs.getObject("v", Vector.class);
                assertNotNull(resultVector, "Retrieved vector is null.");
                assertEquals(3, resultVector.getDimensionCount(), "Dimension count mismatch.");
                assertArrayEquals(originalData, resultVector.getData(), "Vector data mismatch.");

                // Validate the toString output
                String expectedToString = "VECTOR(FLOAT32, 3) : [0.45, 7.9, 63.0]";
                assertEquals(expectedToString, resultVector.toString(), "Vector toString output mismatch.");
            }
        }
    }

    /**
     * Test for inserting a vector with specifying precision and scale validating the data.
     */
    @Test
    void validateVectorDataWithPrecisionScale() throws SQLException {
        String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (id, v) VALUES (?, ?)";
        Object[] data = new Float[] { 0.4f, 0.5f, 0.6f };
        // Create a vector object with precision and scale for vector data type
        Vector vector = new Vector(3, 4, data); 

        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 24);
            pstmt.setObject(2, vector, microsoft.sql.Types.VECTOR);
            pstmt.executeUpdate();
        }

        String query = "SELECT id, v FROM " + AbstractSQLGenerator.escapeIdentifier(tableName) + " WHERE id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, 24);
            try (ResultSet rs = stmt.executeQuery()) {

                assertTrue(rs.next(), "No result found for inserted vector.");

                Vector resultVector = rs.getObject("v", Vector.class);
                assertNotNull(resultVector, "Retrieved vector is null.");
                assertEquals(3, resultVector.getDimensionCount(), "Dimension count mismatch.");
                assertArrayEquals(data, resultVector.getData(), "Vector data mismatch.");
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
        Vector nullVector = new Vector(3, VectorDimensionType.FLOAT32, null); // Null vector data

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

    /**
     * Test for inserting a vector from one table to another using setObject(getObject()).
     * The expected behavior is that the database should accept the vector and store it in the destination table.
     */
    @Test
    public void testInsertFromTableWithNullVectorData() throws SQLException {
        String sourceTable = AbstractSQLGenerator.escapeIdentifier("SourceTable");
        String destinationTable = AbstractSQLGenerator.escapeIdentifier("DestinationTable");

        try (Statement stmt = connection.createStatement()) {
            // Create the source table
            String createSourceTableSQL = "CREATE TABLE " + sourceTable + " (id INT PRIMARY KEY, v VECTOR(3))";
            stmt.executeUpdate(createSourceTableSQL);

            // Insert rows with null vector data into the source table
            String insertSourceSQL = "INSERT INTO " + sourceTable + " (id, v) VALUES (?, ?)";
            Vector nullVector = new Vector(3, VectorDimensionType.FLOAT32, null); // Null vector data
            try (PreparedStatement pstmt = connection.prepareStatement(insertSourceSQL)) {
                for (int i = 1; i <= 3; i++) {
                    pstmt.setInt(1, i);
                    pstmt.setObject(2, nullVector, microsoft.sql.Types.VECTOR);
                    pstmt.executeUpdate();
                }
            }

            // Create the destination table
            String createDestinationTableSQL = "CREATE TABLE " + destinationTable
                    + " (id INT PRIMARY KEY, v VECTOR(3))";
            stmt.executeUpdate(createDestinationTableSQL);

            // Insert data from the source table to the destination table using setObject(getObject())
            String selectSourceSQL = "SELECT id, v FROM " + sourceTable;
            String insertDestinationSQL = "INSERT INTO " + destinationTable + " (id, v) VALUES (?, ?)";
            try (PreparedStatement selectStmt = connection.prepareStatement(selectSourceSQL);
                    PreparedStatement insertStmt = connection.prepareStatement(insertDestinationSQL);
                    ResultSet rs = selectStmt.executeQuery()) {

                while (rs.next()) {
                    int id = rs.getInt("id");
                    insertStmt.setInt(1, id);
                    insertStmt.setObject(2, rs.getObject("v", Vector.class), microsoft.sql.Types.VECTOR); // Use
                                                                                                          // setObject(getObject())
                    insertStmt.executeUpdate();
                }
            }

            // Validate the data in the destination table
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
            // Cleanup: Drop the source and destination tables
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(sourceTable, stmt);
                TestUtils.dropTableIfExists(destinationTable, stmt);
            }
        }
    }

    /**
     * Test for inserting a vector with mismatched dimensions from one table to another.
     * The expected behavior is that the database should throw an error due to dimension mismatch.
     */
    @Test
    public void testInsertVectorWithMismatchedDimensions() throws SQLException {
        String sourceTable = AbstractSQLGenerator.escapeIdentifier("SourceTable");
        String destinationTable = AbstractSQLGenerator.escapeIdentifier("DestinationTable");

        try (Statement stmt = connection.createStatement()) {
            // Create the source table with 4 dimensions
            String createSourceTableSQL = "CREATE TABLE " + sourceTable + " (id INT PRIMARY KEY, v VECTOR(4))";
            stmt.executeUpdate(createSourceTableSQL);

            // Insert rows into the source table
            String insertSourceSQL = "INSERT INTO " + sourceTable + " (id, v) VALUES (?, ?)";
            try (PreparedStatement pstmt = connection.prepareStatement(insertSourceSQL)) {
                for (int i = 1; i <= 4; i++) {
                    Object[] vectorData = new Float[] { i * 1.0f, i * 2.0f, i * 3.0f, i * 4.0f }; // 4 dimensions
                    Vector vector = new Vector(4, VectorDimensionType.FLOAT32, vectorData);
                    pstmt.setInt(1, i);
                    pstmt.setObject(2, vector, microsoft.sql.Types.VECTOR);
                    pstmt.executeUpdate();
                }
            }

            // Create the destination table with 3 dimensions
            String createDestinationTableSQL = "CREATE TABLE " + destinationTable
                    + " (id INT PRIMARY KEY, v VECTOR(3))";
            stmt.executeUpdate(createDestinationTableSQL);

            // Attempt to insert data from the source table into the destination table
            String selectSourceSQL = "SELECT id, v FROM " + sourceTable;
            String insertDestinationSQL = "INSERT INTO " + destinationTable + " (id, v) VALUES (?, ?)";
            try (PreparedStatement selectStmt = connection.prepareStatement(selectSourceSQL);
                    PreparedStatement insertStmt = connection.prepareStatement(insertDestinationSQL);
                    ResultSet rs = selectStmt.executeQuery()) {

                while (rs.next()) {
                    int id = rs.getInt("id");
                    Vector vector = rs.getObject("v", Vector.class); // Retrieve vector from source table

                    insertStmt.setInt(1, id);
                    insertStmt.setObject(2, vector, microsoft.sql.Types.VECTOR); // Attempt to insert into destination
                                                                                 // table
                    insertStmt.executeUpdate();
                }
            } catch (SQLException e) {
                // Validate the error message
                assertTrue(e.getMessage().contains("The vector dimensions 4 and 3 do not match."),
                        "Unexpected error message: " + e.getMessage());
            }
        } finally {
            // Cleanup: Drop the source and destination tables
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(sourceTable, stmt);
                TestUtils.dropTableIfExists(destinationTable, stmt);
            }
        }
    }

    @Test
    void validateVectorDataWithMultipleVectorColumn() throws SQLException {
        String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableNameWithMultipleVectorColumn)
                + " (id, v1, v2) VALUES (?, ?, ?)";
        Object[] originalData1 = new Float[] { 0.45f, 7.9f, 63.0f };
        Vector vector1 = new Vector(3, VectorDimensionType.FLOAT32, originalData1);
        Object[] originalData2 = new Float[] { 0.45f, 7.9f, 63.0f, 1.2f };
        Vector vector2 = new Vector(4, VectorDimensionType.FLOAT32, originalData2);

        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 10);
            pstmt.setObject(2, vector1, microsoft.sql.Types.VECTOR);
            pstmt.setObject(3, vector2, microsoft.sql.Types.VECTOR);
            pstmt.executeUpdate();
        }

        String query = "SELECT id, v1, v2 FROM "
                + AbstractSQLGenerator.escapeIdentifier(tableNameWithMultipleVectorColumn) + " WHERE id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, 10);
            try (ResultSet rs = stmt.executeQuery()) {

                assertTrue(rs.next(), "No result found for inserted vector.");

                Vector resultVector1 = rs.getObject("v1", Vector.class);
                assertNotNull(resultVector1, "Retrieved vector v1 is null.");
                assertEquals(3, resultVector1.getDimensionCount(), "Dimension count mismatch for v1.");
                assertArrayEquals(originalData1, resultVector1.getData(), "Vector data mismatch for v1.");
                String expectedToString1 = "VECTOR(FLOAT32, 3) : [0.45, 7.9, 63.0]";
                assertEquals(expectedToString1, resultVector1.toString(), "Vector toString output mismatch.");

                Vector resultVector2 = rs.getObject("v2", Vector.class);
                assertNotNull(resultVector2, "Retrieved vector v2 is null.");
                assertEquals(4, resultVector2.getDimensionCount(), "Dimension count mismatch for v2.");
                assertArrayEquals(originalData2, resultVector2.getData(), "Vector data mismatch for v2.");
                String expectedToString2 = "VECTOR(FLOAT32, 4) : [0.45, 7.9, 63.0, 1.2]";
                assertEquals(expectedToString2, resultVector2.toString(), "Vector toString output mismatch for v2.");
            }
        }
    }

    /**
     * Test inserting and retrieving a vector containing Float.MAX_VALUE and Float.MIN_VALUE.
     */
    @Test
    void testVectorWithMaxAndMinFloatValues() throws SQLException {
        String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (id, v) VALUES (?, ?)";
        Object[] extremeData = new Float[] { Float.MAX_VALUE, Float.MIN_VALUE, 0.0f };
        Vector vector = new Vector(3, VectorDimensionType.FLOAT32, extremeData);

        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 99);
            pstmt.setObject(2, vector, microsoft.sql.Types.VECTOR);
            pstmt.executeUpdate();
        }

        String query = "SELECT id, v FROM " + AbstractSQLGenerator.escapeIdentifier(tableName) + " WHERE id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, 99);
            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue(rs.next(), "No result found for inserted vector with extreme float values.");

                Vector resultVector = rs.getObject("v", Vector.class);
                assertNotNull(resultVector, "Retrieved vector is null.");
                assertEquals(3, resultVector.getDimensionCount(), "Dimension count mismatch.");
                assertArrayEquals(extremeData, resultVector.getData(),
                        "Vector data mismatch for extreme float values.");

                String expectedToString = "VECTOR(FLOAT32, 3) : [" + Float.MAX_VALUE + ", " + Float.MIN_VALUE
                        + ", 0.0]";
                assertEquals(expectedToString, resultVector.toString(),
                        "Vector toString output mismatch for extreme float values.");
            }
        }
    }

    /*
     * Test to check backward compatibility of vector data type. 
     * This test throws an exception when trying to retrieve the vector data using getString() method.
     */
    @Test
    void validateVectorDataUsingGetString() throws SQLException {
        String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (id, v) VALUES (?, ?)";
        Object[] originalData = new Float[] { 0.45f, 7.9f, 63.0f };
        Vector initialVector = new Vector(3, VectorDimensionType.FLOAT32, originalData);

        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 25);
            pstmt.setObject(2, initialVector, microsoft.sql.Types.VECTOR);
            pstmt.executeUpdate();
        }

        String query = "SELECT id, v FROM " + AbstractSQLGenerator.escapeIdentifier(tableName) + " WHERE id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, 25);
            try (ResultSet rs = stmt.executeQuery()) {

                assertTrue(rs.next(), "No result found for inserted vector.");

                //Attempt to retrieve the vector as a string and check for exception
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

    /*
     * Test to check backward compatibility of vector data type. 
     * This test checks if the vector data can be retrieved as a json formatted string representation.
     * For null vector data, it should return null as a string.
     */
    @Test
    void validateNullVectorDataUsingGetString() throws SQLException {
        String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (id, v) VALUES (?, ?)";
        Vector initialVector = new Vector(3, VectorDimensionType.FLOAT32, null);

        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 26);
            pstmt.setObject(2, initialVector, microsoft.sql.Types.VECTOR);
            pstmt.executeUpdate();
        }

        String query = "SELECT id, v FROM " + AbstractSQLGenerator.escapeIdentifier(tableName) + " WHERE id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, 26);
            try (ResultSet rs = stmt.executeQuery()) {

                assertTrue(rs.next(), "No result found for inserted vector.");

                String vectorString = rs.getString("v");
                assertNull(vectorString, "Retrieved vector string should be null.");

            }
        }
    }

    /**
     * Test for inserting a vector with Double[] data type. There will be lossy conversion from double to float.
     * 
     * @throws SQLException
     */
    @Test
    void testInsertDoubleVectorData() throws SQLException {
        Object[] originalData = new Double[] { 0.45, 7.9, 63.0 };
        try {
            Vector initialVector = new Vector(3, VectorDimensionType.FLOAT32, originalData);
            fail("Expected an exception due to type mismatch, but none was thrown.");
        } catch (IllegalArgumentException e) {
            // Expected exception due to type mismatch
            assertTrue(e.getMessage().contains("Invalid vector data type."),
                    "Unexpected error message: " + e.getMessage());
        }
    }

    @Test
    void testVectorDataWithNegativeDimensionCount() throws SQLException {
        Object[] originalData = new Float[] { 0.45f, 7.9f, 63.0f };
        try {
            Vector initialVector = new Vector(-5, VectorDimensionType.FLOAT32, originalData);
            fail("Expected an exception due to type mismatch, but none was thrown.");
        } catch (IllegalArgumentException e) {
            // Expected exception due to type mismatch
            assertTrue(e.getMessage().contains("Invalid vector dimension count."),
                    "Unexpected error message: " + e.getMessage());
        }
    }

    @Test
    void testVectorDataWithIncorrectDimensionCount() throws SQLException {
        Object[] originalData = new Float[] { 0.45f, 7.9f, 63.0f };
        try {
            Vector initialVector = new Vector(7, VectorDimensionType.FLOAT32, originalData);
            fail("Expected an exception due to type mismatch, but none was thrown.");
        } catch (IllegalArgumentException e) {
            // Expected exception due to type mismatch
            assertTrue(e.getMessage().contains("Mismatch between vector dimension count and provided data."),
                    "Unexpected error message: " + e.getMessage());
        }
    }

    /**
     * Test for validating vector data when the vectorTypeSupport feature is "off".
     * The expected behavior is that the server should return the vector as a string.
     * 
     * @throws SQLException
     */
    @Test
    void testValidateVectorWhenVectorFEIsDisabled() throws SQLException {
        // Setup: create a logs table with VECTOR and VARCHAR columns
        String logsTable = TestUtils.escapeSingleQuotes(
                AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("vectorLogsFeiDisabled")));
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(logsTable, stmt);
            stmt.executeUpdate("CREATE TABLE " + logsTable + " (v VECTOR(3))");

            // Insert a row
            Object[] vectorData = new Float[] { 1.23f, 4.56f, 7.89f };
            Vector vector = new Vector(3, VectorDimensionType.FLOAT32, vectorData);
            String insertSql = "INSERT INTO " + logsTable + " (v) VALUES (?)";
            try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
                pstmt.setObject(1, vector, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            // Use a new connection with disableVectorV1=true
            try (SQLServerConnection conn = getConnectionWithVectorFlag("off");
                    Statement stmt2 = conn.createStatement();
                    ResultSet rs = stmt2.executeQuery("SELECT * FROM " + logsTable)) {

                ResultSetMetaData meta = rs.getMetaData();
                int columnCount = meta.getColumnCount();

                assertTrue(rs.next(), "No result found in logs table.");

                for (int i = 1; i <= columnCount; i++) {
                    int columnType = meta.getColumnType(i);

                    Object value = null;
                    switch (columnType) {
                        case java.sql.Types.VARCHAR: // It will be returned as a string
                            value = rs.getString(i);
                            assertEquals("[1.2300000e+000,4.5599999e+000,7.8899999e+000]", value,
                                    "VARCHAR column value mismatch.");
                            break;
                        case microsoft.sql.Types.VECTOR:
                            value = rs.getObject(i, Vector.class);
                            assertNotNull(value, "Vector column is null.");
                            assertArrayEquals(vectorData, ((Vector) value).getData(), "Vector data mismatch.");
                            assertEquals(3, ((Vector) value).getDimensionCount(), "Dimension count mismatch.");
                            assertEquals(VectorDimensionType.FLOAT32, ((Vector) value).getVectorDimensionType(),
                                    "Dimension type mismatch.");
                            String expectedToString = "VECTOR(FLOAT32, 3) : [1.23, 4.56, 7.89]";
                            assertEquals(expectedToString, ((Vector) value).toString(), "Vector toString output mismatch.");
                            break;
                        default:
                            throw new SQLException("Unexpected column type: " + columnType);
                    }
                }
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(logsTable, stmt);
            }
        }
    }

    /**
     * Test for validating vector data when the vectorTypeSupport feature is "v1".
     * The expected behavior is that the server should return the vector as a vector object.
     * 
     * @throws SQLException
     */
    @Test
    void testValidateVectorWhenVectorFEIsEnabled() throws SQLException {
        // Setup: create a logs table with VECTOR and VARCHAR columns
        String logsTable = TestUtils.escapeSingleQuotes(
                AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("vectorLogsFeiEnabled")));
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(logsTable, stmt);
            stmt.executeUpdate("CREATE TABLE " + logsTable + " (v VECTOR(3))");

            // Insert a row
            Object[] vectorData = new Float[] { 1.23f, 4.56f, 7.89f };
            Vector vector = new Vector(3, VectorDimensionType.FLOAT32, vectorData);
            String insertSql = "INSERT INTO " + logsTable + " (v) VALUES (?)";
            try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
                pstmt.setObject(1, vector, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            // Use a new connection with disableVectorV1=false
            try (SQLServerConnection conn = getConnectionWithVectorFlag("v1");
                    Statement stmt2 = conn.createStatement();
                    ResultSet rs = stmt2.executeQuery("SELECT * FROM " + logsTable)) {

                ResultSetMetaData meta = rs.getMetaData();
                int columnCount = meta.getColumnCount();

                assertTrue(rs.next(), "No result found in logs table.");

                for (int i = 1; i <= columnCount; i++) {
                    int columnType = meta.getColumnType(i);

                    Object value = null;
                    switch (columnType) {
                        case java.sql.Types.VARCHAR:
                            value = rs.getString(i);
                            assertEquals("[1.2300000e+000,4.5599999e+000,7.8899999e+000]", value,
                                    "VARCHAR column value mismatch.");
                            break;
                        case microsoft.sql.Types.VECTOR: // It will be returned as a vector
                            value = rs.getObject(i, Vector.class);
                            assertNotNull(value, "Vector column is null.");
                            assertArrayEquals(vectorData, ((Vector) value).getData(), "Vector data mismatch.");
                            assertEquals(3, ((Vector) value).getDimensionCount(), "Dimension count mismatch.");
                            assertEquals(VectorDimensionType.FLOAT32, ((Vector) value).getVectorDimensionType(),
                                    "Dimension type mismatch.");
                            String expectedToString = "VECTOR(FLOAT32, 3) : [1.23, 4.56, 7.89]";
                            assertEquals(expectedToString, ((Vector) value).toString(), "Vector toString output mismatch.");
                            break;
                        default:
                            throw new SQLException("Unexpected column type: " + columnType);
                    }
                }
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(logsTable, stmt);
            }
        }
    }

    /**
     * Test that an invalid vectorTypeSupport value in the connection string throws
     * an IllegalArgumentException. The only valid values are "off" and "v1".
     */
    @Test
    void testInvalidVectorTypeSupportConnectionProperty() throws SQLException {
        // Setup: create a logs table with VECTOR and VARCHAR columns
        String logsTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("vectorLogs")));
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(logsTable, stmt);
            stmt.executeUpdate("CREATE TABLE " + logsTable + " (v VECTOR(3))");

            // Insert a row
            Object[] vectorData = new Float[] { 1.23f, 4.56f, 7.89f };
            Vector vector = new Vector(3, VectorDimensionType.FLOAT32, vectorData);
            String insertSql = "INSERT INTO " + logsTable + " (v) VALUES (?)";
            try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
                pstmt.setObject(1, vector, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            // Try to open a connection with an invalid vectorTypeSupport value and check
            // for failure
            boolean exceptionThrown = false;
            try {
                SQLServerConnection conn = getConnectionWithVectorFlag("1");
                // If no exception, close the connection and fail the test
                conn.close();
                fail("Expected IllegalArgumentException for invalid vectorTypeSupport value, but none was thrown.");
            } catch (IllegalArgumentException e) {
                exceptionThrown = true;
                assertTrue(
                        e.getMessage().contains("Incorrect connection string property for vectorTypeSupport: 1"),
                        "Unexpected exception message: " + e.getMessage());
            }

            assertTrue(exceptionThrown, "Expected IllegalArgumentException was not thrown.");
        } finally {
            // Cleanup
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(logsTable, stmt);
            }
        }
    }

    @Test
    void validateMaxVectorData() throws SQLException {
        String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(maxVectorDataTableName)
                + " (id, v) VALUES (?, ?)";

        int dimensionCount = 1998;
        Object[] originalData = new Float[dimensionCount];

        for (int i = 0; i < dimensionCount; i++) {
            originalData[i] = i + 0.5f;
        }

        Vector initialVector = new Vector(dimensionCount, VectorDimensionType.FLOAT32, originalData);

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
                assertArrayEquals(originalData, resultVector.getData(), "Vector data mismatch.");
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
        Object[] vectorData = new Float[dimensionCount];

        for (int i = 0; i < dimensionCount; i++) {
            vectorData[i] = i + 0.5f;
        }

        Vector vector = new Vector(dimensionCount, VectorDimensionType.FLOAT32, vectorData);

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
                    // assertArrayEquals(vectorData, resultVector.getData(), "Vector data mismatch at ID " + id);

                    rowsRead++;
                }
            }
            long readEnd = System.nanoTime();
            long readDurationMs = (readEnd - readStart) / 1000000;
            System.out.println("Read Time for " + rowsRead + " records: " + readDurationMs + " ms");

            assertEquals(recordCount, rowsRead, "Mismatch in number of rows read vs inserted.");
        }
    }

    /**
     * Test for inserting a vector that exceeds the maximum allowed dimension count(1998).
     * The expected behavior is that the database should throw an exception due to exceeding the maximum allowed vector size.
     */
    @Test
    void validateVectorExceedingMaxDimension() throws SQLException {
        String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(maxVectorDataTableName)
                + " (id, v) VALUES (?, ?)";

        int dimensionCount = 1999; // Exceeding the maximum allowed dimension count(1998)
        Object[] originalData = new Float[dimensionCount];

        for (int i = 0; i < dimensionCount; i++) {
            originalData[i] = i + 0.5f;
        }

        Vector initialVector = new Vector(dimensionCount, VectorDimensionType.FLOAT32, originalData);

        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 101);
            pstmt.setObject(2, initialVector, microsoft.sql.Types.VECTOR);

            try {
                pstmt.executeUpdate();
                fail("Expected SQLServerException due to exceeding maximum allowed vector size, but none was thrown.");
            } catch (SQLServerException e) {
                assertTrue(e.getMessage().contains("The size (" + dimensionCount + ") given to the type 'vector' exceeds the maximum allowed (1998)."),
                        "Unexpected error message: " + e.getMessage());
            }
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
            Vector inputVector = new Vector(3, VectorDimensionType.FLOAT32, new Float[] {0.5f, 1.0f, 1.5f});

            cstmt.setObject(1, inputVector, microsoft.sql.Types.VECTOR);
            cstmt.registerOutParameter(2, microsoft.sql.Types.VECTOR, 3, 4);
            cstmt.execute();

            Vector result = cstmt.getObject(2, Vector.class);
            assertNotNull(result, "Returned vector should not be null");
            assertArrayEquals(inputVector.getData(), result.getData(), "Vector data mismatch.");
        }
    }

    /**
     * Test for calling a stored procedure with a null vector input and output parameters. The expected behavior is that
     * the database should accept the null vector and return it as an output parameter.
     */
    @Test
    public void testNullVectorStoredProcedureInputOutput() throws SQLException {
        createProcedure();

        String call = "{call " + AbstractSQLGenerator.escapeIdentifier(procedureName) + "(?, ?)}";
        try (SQLServerCallableStatement cstmt = (SQLServerCallableStatement) connection.prepareCall(call)) {
            Vector inputVector = new Vector(3, VectorDimensionType.FLOAT32, null);

            cstmt.setObject(1, inputVector, microsoft.sql.Types.VECTOR);
            cstmt.registerOutParameter(2, microsoft.sql.Types.VECTOR, 3, 4);
            cstmt.execute();

            Vector result = cstmt.getObject(2, Vector.class);
            assertNotNull(result, "Returned vector should not be null");
            assertArrayEquals(inputVector.getData(), result.getData(), "Vector data mismatch.");
        }
    }

    /**
     * Test for inserting a vector into a TVP. The expected behavior is that the database should accept the vector and
     * store it in the table.
     */
    @Test
    public void testVectorTVP() throws SQLException {
        Vector expectedVector = new Vector(3, VectorDimensionType.FLOAT32, new Float[] {0.1f, 0.2f, 0.3f});

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
                    assertArrayEquals(expectedVector.getData(), actual.getData(), "Vector data mismatch.");
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

        Vector expectedVector = new Vector(3, VectorDimensionType.FLOAT32, null); // Null vector data

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

    private static void createTVPReturnProcedure(String procName, String tvpTypeName) throws SQLException {
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

    /**
     * Test for a stored procedure that returns a TVP containing vector data.
     */
    @Test
    public void testStoredProcedureReturnsTVPWithVector() throws SQLException {
        String tvpTypeName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("VectorTVPType"));
        String procName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("sp_ReturnVectorTVP"));

        // Create TVP type
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("CREATE TYPE " + tvpTypeName + " AS TABLE (c1 VECTOR(3) NULL)");
        }

        // Create the stored procedure
        createTVPReturnProcedure(procName, tvpTypeName);

        // Prepare TVP data
        SQLServerDataTable tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", microsoft.sql.Types.VECTOR);
        Vector v1 = new Vector(3, VectorDimensionType.FLOAT32, new Float[] { 1.0f, 2.0f, 3.0f });
        Vector v2 = new Vector(3, VectorDimensionType.FLOAT32, new Float[] { 4.0f, 5.0f, 6.0f });
        tvp.addRow(v1);
        tvp.addRow(v2);

        // Call the stored procedure and validate the result set
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
                        assertArrayEquals(v1.getData(), v.getData(), "Vector data mismatch in row 1.");
                    } else if (rowCount == 1) {
                        assertArrayEquals(v2.getData(), v.getData(), "Vector data mismatch in row 2.");
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

    private static void createVectorUdf() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            String sql =
                "CREATE OR ALTER FUNCTION " + AbstractSQLGenerator.escapeIdentifier(functionName) + "\n" +
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

        Vector inputVector = new Vector(3, VectorDimensionType.FLOAT32, new Float[] {1.1f, 2.2f, 3.3f});
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE #vec_input (v VECTOR(3));");
        }
        try (PreparedStatement pstmt = connection.prepareStatement("INSERT INTO #vec_input VALUES (?);")) {
            pstmt.setObject(1, inputVector, microsoft.sql.Types.VECTOR);
            pstmt.executeUpdate();
        }

        String query = "SELECT dbo." + AbstractSQLGenerator.escapeIdentifier(functionName) + "((SELECT TOP 1 v FROM #vec_input));";
        try (PreparedStatement selectStmt = connection.prepareStatement(query);
            ResultSet rs = selectStmt.executeQuery()) {
            assertTrue(rs.next(), "Result set is empty");

            Vector result = rs.getObject(1, Vector.class);
            assertNotNull(result, "Returned vector should not be null");
            assertArrayEquals(inputVector.getData(), result.getData(), "Vector data mismatch.");
        } finally {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS #vec_input;");
            }
        }
    }

    /**
     * Test UDF that returns a TVP containing vector data.
     */
    @Test
    public void testVectorUdfReturnsTVP() throws SQLException {

        createTVPReturnUdf();

        SQLServerDataTable tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", microsoft.sql.Types.VECTOR);
        Vector v1 = new Vector(4, VectorDimensionType.FLOAT32, new Float[] { 1.0f, 2.0f, 3.0f, 4.0f });
        Vector v2 = new Vector(4, VectorDimensionType.FLOAT32, new Float[] { 4.0f, 5.0f, 6.0f, 7.0f });
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
                        assertArrayEquals(v1.getData(), v.getData(), "Vector data mismatch in row 1.");
                    } else if (rowCount == 1) {
                        assertArrayEquals(v2.getData(), v.getData(), "Vector data mismatch in row 2.");
                    }
                    rowCount++;
                }
                assertEquals(2, rowCount, "Row count mismatch.");
            } finally {
                try (Statement stmt = connection.createStatement()) {
                    TestUtils.dropFunctionIfExists(AbstractSQLGenerator.escapeIdentifier(vectorUdf), stmt);
                }
            }
        }
    }

    private static void createTVPReturnUdf() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            String sql = "CREATE OR ALTER FUNCTION " + AbstractSQLGenerator.escapeIdentifier(vectorUdf) +
                    "(@tvpInput " + AbstractSQLGenerator.escapeIdentifier(TVP) + " READONLY)\n" +
                    "RETURNS TABLE\n" +
                    "AS\n" +
                    "RETURN SELECT c1 FROM @tvpInput;";
            stmt.execute(sql);
        }
    }

    /**
     * Test for inserting a vector into a table using SELECT INTO statement
     * 
     * @throws SQLException
     */
    @Test
    public void testSelectIntoForVector() throws SQLException {
        String sourceTable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("srcTableVector"));
        String destinationTable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("desTable"));

        try (Statement stmt = connection.createStatement()) {
            // Create the source table
            String createSourceTableSql = "CREATE TABLE " + sourceTable + " (id INT, v VECTOR(3))";
            stmt.executeUpdate(createSourceTableSql);

            // Insert sample data into the source table
            Object[] vectorData = new Float[] { 1.1f, 2.2f, 3.3f };
            Vector vector = new Vector(3, VectorDimensionType.FLOAT32, vectorData);

            String insertSql = "INSERT INTO " + sourceTable + " (id, v) VALUES (?, ?)";
            try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
                pstmt.setInt(1, 1);
                pstmt.setObject(2, vector, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            Object[] vectorData2 = null;
            Vector vector2 = new Vector(3, VectorDimensionType.FLOAT32, vectorData2);

            String insertSql2 = "INSERT INTO " + sourceTable + " (id, v) VALUES (?, ?)";
            try (PreparedStatement pstmt = connection.prepareStatement(insertSql2)) {
                pstmt.setInt(1, 2);
                pstmt.setObject(2, vector2, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            // Drop the destination table if it already exists
            TestUtils.dropTableIfExists(destinationTable, stmt);

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
                        assertArrayEquals(vector.getData(), resultVector.getData(), "Vector data mismatch in destination table for ID 1.");
                    } else if (id == 2) {
                        assertArrayEquals(vector2.getData(), resultVector.getData(), "Vector data mismatch in destination table for ID 2.");
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
            Object[] vectorData = new Float[] { 1.0f, 2.0f, 3.0f };
            Vector vector = new Vector(3, VectorDimensionType.FLOAT32, vectorData);

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
                assertArrayEquals(new Float[] { 1.0f, 2.0f, 3.0f }, resultVector.getData(), "Vector data mismatch.");
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
                Object[] vectorData = new Float[] { 4.0f, 5.0f, 6.0f };
                Vector vector = new Vector(3, VectorDimensionType.FLOAT32, vectorData);

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
                    assertArrayEquals(new Float[] { 4.0f, 5.0f, 6.0f }, resultVector.getData(), "Vector data mismatch.");
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
        String udfName = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(functionTvpName));

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
                Object[] vectorData = new Float[] { 1.0f, 2.0f, 3.0f };
                Vector vector = new Vector(3, VectorDimensionType.FLOAT32, vectorData);

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

                Object[] expectedNormalizedData = new Float[] { 0.2673f, 0.5345f, 0.8018f }; // Normalized values for [1, 2, 3]
                Object[] actualNormalizedData = normalizedVector.getData();

                for (int i = 0; i < expectedNormalizedData.length; i++) {
                    assertEquals((float) expectedNormalizedData[i], (float) actualNormalizedData[i], 0.0001f,
                            "Normalized vector mismatch at index " + i);
                }
            }
        } finally {
            // Cleanup: Drop the UDF and table
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
            
            // Create schema if not exists
            stmt.executeUpdate(
                    "IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '" + schemaName + "') " +
                            "EXEC('CREATE SCHEMA " + escapedSchema + "')");

            // Create scalar-valued function
            String createSvfSQL = "CREATE FUNCTION " + escapedSchema + "." + escapedFunc + " (@p VECTOR(3)) " +
                    "RETURNS VECTOR(3) AS " +
                    "BEGIN " +
                    "    DECLARE @v VECTOR(3); " +
                    "    SET @v = vector_normalize(@p, 'norm2'); " +
                    "    RETURN @v; " +
                    "END";
            stmt.executeUpdate(createSvfSQL);

            String createTableSQL = "CREATE TABLE " + tableName + " (id INT PRIMARY KEY, vec VECTOR(3))";
            stmt.execute(createTableSQL);

        }
    }

    /**
     * Test for vector normalization using a scalar-valued function.
     * The function normalizes the input vector and returns the normalized vector.
     */
    @Test
    public void testVectorIdentityScalarFunction() throws SQLException {
        String schemaName = "testschemaVector" + uuid;
        String funcName = "svf" + uuid;
        String escapedSchema = AbstractSQLGenerator.escapeIdentifier(schemaName);
        String escapedFunc = AbstractSQLGenerator.escapeIdentifier(funcName);
        String tableName = escapedSchema + "." + "Vectors" + uuid;

        try {
            // Setup: create schema, function, and table
            setupSVF(schemaName, funcName, tableName);

            // Insert a vector row
            try (PreparedStatement pstmt = connection.prepareStatement(
                    "INSERT INTO " + tableName + " (id, vec) VALUES (?, ?)")) {
                Vector v = new Vector(3, VectorDimensionType.FLOAT32, new Float[] { 1.0f, 2.0f, 3.0f });
                pstmt.setInt(1, 1);
                pstmt.setObject(2, v, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            // Call the scalar function and validate output
            String svfTestSQL = "DECLARE @v VECTOR(3) = (SELECT vec FROM " + tableName + " WHERE id = 1); " +
                    "SELECT " + escapedSchema + "." + escapedFunc + "(@v) AS normalizedVector";

            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery(svfTestSQL)) {

                assertTrue(rs.next(), "No result from SVF.");
                Vector normalizedVector = rs.getObject(1, Vector.class);
                assertNotNull(normalizedVector, "Returned vector is null.");

                Object[] expectedNormalizedData = new Float[] { 0.26726124f, 0.5345225f, 0.8017837f }; // Normalized values for [1, 2, 3]
                assertArrayEquals(expectedNormalizedData, normalizedVector.getData(), "Vector roundtrip mismatch.");
            }

        } finally {
            // Cleanup: drop function, table, and schema
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropFunctionWithSchemaIfExists(schemaName + "." + funcName, stmt);
                TestUtils.dropTableWithSchemaIfExists(tableName, stmt);
                TestUtils.dropSchemaIfExists(schemaName, stmt);
            }
        }
    }

    /**
     * Test for transaction rollback with VECTOR data type.
     * This test simulates a transaction with insert, update, and delete operations,
     * triggers a failure, and verifies that the rollback restores the original
     * state.
     */
    @Test
    public void testTransactionRollbackForVector() throws SQLException {
        String transactionTable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("transactionTable"));

        // Create the table
        try (Statement outerStmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(transactionTable, outerStmt);
            String createTableSQL = "CREATE TABLE " + transactionTable + " (id INT PRIMARY KEY, v VECTOR(3))";
            outerStmt.executeUpdate(createTableSQL);

            // Insert initial data
            Object[] initialData = new Float[] { 1.0f, 2.0f, 3.0f };
            Vector initialVector = new Vector(3, VectorDimensionType.FLOAT32, initialData);

            try (PreparedStatement pstmt = connection.prepareStatement(
                    "INSERT INTO " + transactionTable + " (id, v) VALUES (?, ?)")) {
                pstmt.setInt(1, 1);
                pstmt.setObject(2, initialVector, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            // Start a transaction
            connection.setAutoCommit(false);
            try {
                // Insert new data
                Object[] newData = new Float[] { 4.0f, 5.0f, 6.0f };
                Vector newVector = new Vector(3, VectorDimensionType.FLOAT32, newData);

                try (PreparedStatement pstmt = connection.prepareStatement(
                        "INSERT INTO " + transactionTable + " (id, v) VALUES (?, ?)")) {
                    pstmt.setInt(1, 2);
                    pstmt.setObject(2, newVector, microsoft.sql.Types.VECTOR);
                    pstmt.executeUpdate();
                }

                // Update existing data
                Object[] updatedData = new Float[] { 7.0f, 8.0f, 9.0f };
                Vector updatedVector = new Vector(3, VectorDimensionType.FLOAT32, updatedData);

                try (PreparedStatement pstmt = connection.prepareStatement(
                        "UPDATE " + transactionTable + " SET v = ? WHERE id = ?")) {
                    pstmt.setObject(1, updatedVector, microsoft.sql.Types.VECTOR);
                    pstmt.setInt(2, 1);
                    pstmt.executeUpdate();
                }

                // Delete a row
                try (PreparedStatement pstmt = connection.prepareStatement(
                        "DELETE FROM " + transactionTable + " WHERE id = ?")) {
                    pstmt.setInt(1, 2);
                    pstmt.executeUpdate();
                }

                // Simulate a failure
                throw new RuntimeException("Simulated failure to trigger rollback");

            } catch (RuntimeException e) {
                // Rollback the transaction
                connection.rollback();
                System.out.println("Transaction rolled back due to: " + e.getMessage());
            } finally {
                // Restore auto-commit mode
                connection.setAutoCommit(true);
            }

            // Validate that the data is restored to its original state
            String validateSql = "SELECT id, v FROM " + transactionTable + " ORDER BY id";
            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery(validateSql)) {

                assertTrue(rs.next(), "No data found in the table after rollback.");
                int id = rs.getInt("id");
                Vector resultVector = rs.getObject("v", Vector.class);

                assertEquals(1, id, "ID mismatch after rollback.");
                assertNotNull(resultVector, "Vector is null after rollback.");
                assertArrayEquals(initialData, resultVector.getData(), "Vector data mismatch after rollback.");

                assertFalse(rs.next(), "Unexpected additional rows found after rollback.");
            }
        } finally {
            // Cleanup: Drop the table
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(transactionTable, stmt);
            }
        }
    }

    /**
     * Test for creating and querying a view with VECTOR data type.
     * This test creates a table with a VECTOR column, inserts data, creates a view
     * on the table,
     * and validates that the view correctly retrieves the VECTOR data.
     */
    @Test
    public void testViewWithVectorDataType() throws SQLException {
        String tableName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("VectorTable"));
        String viewName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("VectorView"));

        try (Statement stmt = connection.createStatement()) {
            // Create the table
            String createTableSQL = "CREATE TABLE " + tableName + " (id INT PRIMARY KEY, v VECTOR(3))";
            stmt.executeUpdate(createTableSQL);

            // Insert sample data into the table
            Object[] vectorData1 = new Float[] { 1.1f, 2.2f, 3.3f };
            Vector vector1 = new Vector(3, VectorDimensionType.FLOAT32, vectorData1);

            Object[] vectorData2 = new Float[] { 4.4f, 5.5f, 6.6f };
            Vector vector2 = new Vector(3, VectorDimensionType.FLOAT32, vectorData2);

            String insertSQL = "INSERT INTO " + tableName + " (id, v) VALUES (?, ?)";
            try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) {
                pstmt.setInt(1, 1);
                pstmt.setObject(2, vector1, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();

                pstmt.setInt(1, 2);
                pstmt.setObject(2, vector2, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            // Create a view on the table
            String createViewSQL = "CREATE VIEW " + viewName + " AS SELECT id, v FROM " + tableName;
            stmt.executeUpdate(createViewSQL);

            // Query the view and validate the data
            String queryViewSQL = "SELECT id, v FROM " + viewName + " ORDER BY id";
            try (ResultSet rs = stmt.executeQuery(queryViewSQL)) {
                int rowCount = 0;
                while (rs.next()) {
                    int id = rs.getInt("id");
                    Vector resultVector = rs.getObject("v", Vector.class);

                    assertNotNull(resultVector, "Vector is null in view for ID " + id);

                    if (id == 1) {
                        assertArrayEquals(vectorData1, resultVector.getData(), "Vector data mismatch in view for ID 1.");
                    } else if (id == 2) {
                        assertArrayEquals(vectorData2, resultVector.getData(), "Vector data mismatch in view for ID 2.");
                    } else {
                        fail("Unexpected ID found in view: " + id);
                    }
                    rowCount++;
                }
                assertEquals(2, rowCount, "Row count mismatch in view.");
            }
        } finally {
            // Cleanup: Drop the view and table
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropViewIfExists(viewName, stmt);
                TestUtils.dropTableIfExists(tableName, stmt);
            }
        }
    }

    /**
     * Test for validating the metadata of a prepared statement with vector data type.
     * queries sp_describe_undeclared_parameters
     * @throws SQLException
     */
    @Test
    void validatePreparedStatementMetaData() throws SQLException {

        String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (id, v) VALUES (?, ?)";
        System.out.println("Generated SQL: " + insertSql);
        Object[] originalData = new Float[] { 0.45f, 7.9f, 63.0f };
        Vector initialVector = new Vector(3, VectorDimensionType.FLOAT32, originalData);

        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            ParameterMetaData pMetaData = pstmt.getParameterMetaData();
            
            pstmt.setInt(1, 101);
            pstmt.setObject(2, initialVector, microsoft.sql.Types.VECTOR);
            pstmt.executeUpdate();
            
            for (int i = 1; i <= pMetaData.getParameterCount(); i++) {
                System.out.println("Parameter " + i + " details:");
                System.out.println("  Type: " + pMetaData.getParameterType(i));
                System.out.println("  Type Name: " + pMetaData.getParameterTypeName(i));
                System.out.println("  Class Name: " + pMetaData.getParameterClassName(i));
                System.out.println("  Mode: " + pMetaData.getParameterMode(i));
                System.out.println("  Precision: " + pMetaData.getPrecision(i));
                System.out.println("  Scale: " + pMetaData.getScale(i));
                System.out.println("  Is Nullable: " + pMetaData.isNullable(i));
                System.out.println("  Is Signed: " + pMetaData.isSigned(i));
                System.out.println("---");
            }
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

    /**
     * Helper method to get a connection with the vectorTypeSupport setting.
     * 
     * @param vectorTypeSupport "v1", "off", or any other value for testing
     * @return SQLServerConnection
     * @throws SQLException
     */
    private SQLServerConnection getConnectionWithVectorFlag(String vectorTypeSupport) throws SQLException {
        String connStr = connectionString;
        if ("off".equalsIgnoreCase(vectorTypeSupport) || "v1".equalsIgnoreCase(vectorTypeSupport)) {
            connStr = connStr + ";vectorTypeSupport=" + vectorTypeSupport;
        } else {
            throw new IllegalArgumentException(
                    "Incorrect connection string property for vectorTypeSupport: " + vectorTypeSupport);
        }
        return (SQLServerConnection) DriverManager.getConnection(connStr);
    }


}
