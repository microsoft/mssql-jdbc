/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.DBStatement;
import com.microsoft.sqlserver.testframework.DBTable;
import com.microsoft.sqlserver.testframework.sqlType.SqlType;

import microsoft.sql.Vector;


/**
 * Test bulk copy decimal scale and precision
 */
@RunWith(JUnitPlatform.class)
@DisplayName("Test ISQLServerBulkRecord")
@Tag(Constants.xAzureSQLDW)
public class BulkCopyISQLServerBulkRecordTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @Test
    public void testISQLServerBulkRecord() throws SQLException {
        DBTable dstTable = null;
        try (DBConnection con = new DBConnection(connectionString); DBStatement stmt = con.createStatement()) {
            dstTable = new DBTable(true);
            stmt.createTable(dstTable);
            BulkData Bdata = new BulkData(dstTable);

            BulkCopyTestWrapper bulkWrapper = new BulkCopyTestWrapper(connectionString);
            bulkWrapper.setUsingConnection((0 == Constants.RANDOM.nextInt(2)) ? true : false, ds);
            bulkWrapper.setUsingXAConnection((0 == Constants.RANDOM.nextInt(2)) ? true : false, dsXA);
            bulkWrapper.setUsingPooledConnection((0 == Constants.RANDOM.nextInt(2)) ? true : false, dsPool);
            BulkCopyTestUtil.performBulkCopy(bulkWrapper, Bdata, dstTable);
        } finally {
            if (null != dstTable) {
                try (DBConnection con = new DBConnection(connectionString); DBStatement stmt = con.createStatement()) {
                    stmt.dropTable(dstTable);
                }
            }
        }
    }

    @Test
    public void testBulkCopyDateTimePrecision() throws SQLException {
        String dstTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTable")));

        try (Connection conn = DriverManager.getConnection(connectionString);) {
            try (Statement dstStmt = conn.createStatement(); SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn)) {

                dstStmt.executeUpdate(
                        "CREATE TABLE " + dstTable + " (Dataid int IDENTITY(1,1) PRIMARY KEY, testCol datetime2);");

                bulkCopy.setDestinationTableName(dstTable);
                LocalDateTime data = LocalDateTime.of(LocalDate.now(), LocalTime.of(Constants.RANDOM.nextInt(24),
                        Constants.RANDOM.nextInt(60), Constants.RANDOM.nextInt(60), 123456700));
                LocalDateTime data1 = LocalDateTime.of(LocalDate.now(), LocalTime.of(Constants.RANDOM.nextInt(24),
                        Constants.RANDOM.nextInt(60), Constants.RANDOM.nextInt(60), 0));
                LocalDateTime data2 = LocalDateTime.of(LocalDate.now(), LocalTime.of(Constants.RANDOM.nextInt(24),
                        Constants.RANDOM.nextInt(60), Constants.RANDOM.nextInt(60), 100000000));
                LocalDateTime data3 = LocalDateTime.of(LocalDate.now(), LocalTime.of(Constants.RANDOM.nextInt(24),
                        Constants.RANDOM.nextInt(60), Constants.RANDOM.nextInt(60), 120000000));
                LocalDateTime data4 = LocalDateTime.of(LocalDate.now(), LocalTime.of(Constants.RANDOM.nextInt(24),
                        Constants.RANDOM.nextInt(60), Constants.RANDOM.nextInt(60), 123000000));
                LocalDateTime data5 = LocalDateTime.of(LocalDate.now(), LocalTime.of(Constants.RANDOM.nextInt(24),
                        Constants.RANDOM.nextInt(60), Constants.RANDOM.nextInt(60), 123400000));
                LocalDateTime data6 = LocalDateTime.of(LocalDate.now(), LocalTime.of(Constants.RANDOM.nextInt(24),
                        Constants.RANDOM.nextInt(60), Constants.RANDOM.nextInt(60), 123450000));
                LocalDateTime data7 = LocalDateTime.of(LocalDate.now(), LocalTime.of(Constants.RANDOM.nextInt(24),
                        Constants.RANDOM.nextInt(60), Constants.RANDOM.nextInt(60), 123456000));
                LocalDateTime data8 = LocalDateTime.of(LocalDate.now(), LocalTime.of(0, 0, 0, 0));
                bulkCopy.writeToServer(new BulkRecordDT(data));
                bulkCopy.writeToServer(new BulkRecordDT(data1));
                bulkCopy.writeToServer(new BulkRecordDT(data2));
                bulkCopy.writeToServer(new BulkRecordDT(data3));
                bulkCopy.writeToServer(new BulkRecordDT(data4));
                bulkCopy.writeToServer(new BulkRecordDT(data5));
                bulkCopy.writeToServer(new BulkRecordDT(data6));
                bulkCopy.writeToServer(new BulkRecordDT(data7));
                bulkCopy.writeToServer(new BulkRecordDT(data8));

                String select = "SELECT * FROM " + dstTable + " order by Dataid";
                ResultSet rs = dstStmt.executeQuery(select);

                assertTrue(rs.next());
                assertTrue(data.equals(rs.getObject(2, LocalDateTime.class)));
                assertTrue(rs.next());
                assertTrue(data1.equals(rs.getObject(2, LocalDateTime.class)));
                assertTrue(rs.next());
                assertTrue(data2.equals(rs.getObject(2, LocalDateTime.class)));
                assertTrue(rs.next());
                assertTrue(data3.equals(rs.getObject(2, LocalDateTime.class)));
                assertTrue(rs.next());
                assertTrue(data4.equals(rs.getObject(2, LocalDateTime.class)));
                assertTrue(rs.next());
                assertTrue(data5.equals(rs.getObject(2, LocalDateTime.class)));
                assertTrue(rs.next());
                assertTrue(data6.equals(rs.getObject(2, LocalDateTime.class)));
                assertTrue(rs.next());
                assertTrue(data7.equals(rs.getObject(2, LocalDateTime.class)));
                assertTrue(rs.next());
                assertTrue(data8.equals(rs.getObject(2, LocalDateTime.class)));

            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                try (Statement stmt = conn.createStatement();) {
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }
    }

    /**
     * Test bulk copy with a single Vector row.
     */
    @Test
    public void testBulkCopyVector() throws SQLException {
        String dstTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTable")));

        try (Connection conn = DriverManager.getConnection(connectionString);) {
            try (Statement dstStmt = conn.createStatement();
                    SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn)) {

                dstStmt.executeUpdate(
                        "CREATE TABLE " + dstTable + " (vectorCol VECTOR(3));");

                bulkCopy.setDestinationTableName(dstTable);
                float[] vectorData = new float[] { 1.0f, 2.0f, 3.0f };
                microsoft.sql.Vector vector = new microsoft.sql.Vector(vectorData.length,
                        microsoft.sql.Vector.VectorDimensionType.F32, vectorData);
                VectorBulkData vectorBulkData = new VectorBulkData(vector, vectorData.length, vector.getVectorDimensionType());
                bulkCopy.writeToServer(vectorBulkData);

                String select = "SELECT * FROM " + dstTable;
                try (ResultSet rs = dstStmt.executeQuery(select)) {
                    assertTrue(rs.next());
                    microsoft.sql.Vector resultVector = rs.getObject("vectorCol", microsoft.sql.Vector.class);
                    assertNotNull(resultVector, "Retrieved vector is null.");
                    assertEquals(3, resultVector.getDimensionCount(), "Dimension count mismatch.");
                    assertArrayEquals(vectorData, resultVector.getData(), 0.0001f, "Vector data mismatch.");
                }

            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                try (Statement stmt = conn.createStatement();) {
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }
    }

    /**
     * Test bulk copy with null Vector data.
     */
    @Test
    public void testBulkCopyVectorNull() throws SQLException {
        String dstTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTable")));

        try (Connection conn = DriverManager.getConnection(connectionString);) {
            try (Statement dstStmt = conn.createStatement();
                    SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn)) {

                dstStmt.executeUpdate(
                        "CREATE TABLE " + dstTable + " (vectorCol VECTOR(3));");

                bulkCopy.setDestinationTableName(dstTable);
                microsoft.sql.Vector vector = new microsoft.sql.Vector(3,
                        microsoft.sql.Vector.VectorDimensionType.F32, null);
                VectorBulkData vectorBulkData = new VectorBulkData(vector, 3, vector.getVectorDimensionType());
                bulkCopy.writeToServer(vectorBulkData);

                String select = "SELECT * FROM " + dstTable;
                try (ResultSet rs = dstStmt.executeQuery(select)) {
                    int rowCount = 0;
                    while (rs.next()) {
                        microsoft.sql.Vector vectorObject = rs.getObject("vectorCol", microsoft.sql.Vector.class);
                        assertEquals(null, vectorObject, "Mismatch in vector data");
                        rowCount++;
                    }
                    assertEquals(1, rowCount, "Row count mismatch after inserting null vector data.");
                }

            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                try (Statement stmt = conn.createStatement();) {
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }
    }

    /**
     * Test bulk copy from a varbinary source column to VECTOR as destination column.
     * The operation should fail with an error: "Operand type clash: varbinary(max) is 
     * incompatible with vector".
     */
    @Test
    public void testBulkCopyVectorUsingBulkCopySourceAsVarBinary() {
        String varbinaryTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier("testVarbinaryTable"));
        String vectorTable = TestUtils.escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier("testVectorTable"));

        try (Connection connection = DriverManager.getConnection(connectionString);
                Statement statement = connection.createStatement()) {

            // Create the source table with a varbinary column
            statement.executeUpdate("CREATE TABLE " + varbinaryTable + " (varbinaryCol VARBINARY(MAX))");

            // Insert sample data into the source table
            statement.executeUpdate("INSERT INTO " + varbinaryTable + " (varbinaryCol) VALUES (0xDEADBEEF)");

            // Create the destination table with a VECTOR column
            statement.executeUpdate("CREATE TABLE " + vectorTable + " (vectorCol VECTOR(3))");

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
                assertTrue(e.getMessage().contains("Operand type clash: varbinary(max) is incompatible with vector"),
                        "Unexpected exception message: " + e.getMessage());
            }

        } catch (Exception e) {
            fail("Test failed with unexpected exception: " + e.getMessage());
        } finally {
            // Cleanup: Drop the tables
            try (Connection connection = DriverManager.getConnection(connectionString);
                    Statement statement = connection.createStatement()) {
                TestUtils.dropTableIfExists(varbinaryTable, statement);
                TestUtils.dropTableIfExists(vectorTable, statement);
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
    public void testBulkCopyVectorUsingBulkCopyDestinationAsVarBinary1() {
        String vectorTable = TestUtils.escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier("testVectorTable"));
        String varbinaryTable = TestUtils
                .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier("testVarbinaryTable"));

        try (Connection connection = DriverManager.getConnection(connectionString);
                Statement statement = connection.createStatement()) {

            // Create the source table with a VECTOR column
            statement.executeUpdate("CREATE TABLE " + vectorTable + " (vectorCol VECTOR(3))");

            // Insert sample data into the source table
            float[] data = { 1.0f, 2.0f, 3.0f };
            Vector vectorData = new Vector(3, Vector.VectorDimensionType.F32, data);

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
                assertTrue(e.getMessage().contains("Operand type clash: vector is incompatible with varbinary(max)"),
                        "Unexpected exception message: " + e.getMessage());
            }

        } catch (Exception e) {
            fail("Test failed with unexpected exception: " + e.getMessage());
        } finally {
            // Cleanup: Drop the tables
            try (Connection connection = DriverManager.getConnection(connectionString);
                    Statement statement = connection.createStatement()) {
                TestUtils.dropTableIfExists(varbinaryTable, statement);
                TestUtils.dropTableIfExists(vectorTable, statement);
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
        String varcharTable = TestUtils.escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier("testVarcharTable"));
        String vectorTable = TestUtils.escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier("testVectorTable"));

        try (Connection connection = DriverManager.getConnection(connectionString);
                Statement statement = connection.createStatement()) {

            // Create the source table with a VARCHAR column
            statement.executeUpdate("CREATE TABLE " + varcharTable + " (varcharCol VARCHAR(MAX))");

            // Insert sample data into the source table
            String vectorString = "[1.0, 2.0, 3.0]";
            statement.executeUpdate("INSERT INTO " + varcharTable + " (varcharCol) VALUES ('" + vectorString + "')");

            // Create the destination table with a VECTOR column
            statement.executeUpdate("CREATE TABLE " + vectorTable + " (vectorCol VECTOR(3))");

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
                    microsoft.sql.Vector resultVector = rs.getObject("vectorCol", microsoft.sql.Vector.class);
                    assertNotNull(resultVector, "Retrieved vector is null.");
                    assertEquals(3, resultVector.getDimensionCount(), "Dimension count mismatch.");
                    assertEquals(microsoft.sql.Vector.VectorDimensionType.F32, resultVector.getVectorDimensionType(),
                            "Vector dimension type mismatch.");
                    assertArrayEquals(new float[] { 1.0f, 2.0f, 3.0f }, resultVector.getData(), 0.0001f,
                            "Vector data mismatch.");
                }

            }

        } catch (Exception e) {
            fail("Test failed with unexpected exception: " + e.getMessage());
        } finally {
            // Cleanup: Drop the tables
            try (Connection connection = DriverManager.getConnection(connectionString);
                    Statement statement = connection.createStatement()) {
                TestUtils.dropTableIfExists(varcharTable, statement);
                TestUtils.dropTableIfExists(vectorTable, statement);
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
        String vectorTable = TestUtils.escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier("testVectorTable"));
        String varcharTable = TestUtils.escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier("testVarcharTable"));

        try (Connection connection = DriverManager.getConnection(connectionString);
                Statement statement = connection.createStatement()) {

            // Create the source table with a VECTOR column
            statement.executeUpdate("CREATE TABLE " + vectorTable + " (vectorCol VECTOR(3))");

            // Insert sample data into the source table
            float[] data = new float[] { 1.0f, 2.0f, 3.0f };
            microsoft.sql.Vector vector = new microsoft.sql.Vector(data.length,
                    microsoft.sql.Vector.VectorDimensionType.F32, data);
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
                    assertNotNull(resultString, "Retrieved string is null.");
                    assertEquals("[1.0000000e+000,2.0000000e+000,3.0000000e+000]", resultString,
                     "String data mismatch.");
                }

            }

        } catch (Exception e) {
            fail("Test failed with unexpected exception: " + e.getMessage());
        } finally {
            // Cleanup: Drop the tables
            try (Connection connection = DriverManager.getConnection(connectionString);
                    Statement statement = connection.createStatement()) {
                TestUtils.dropTableIfExists(varcharTable, statement);
                TestUtils.dropTableIfExists(vectorTable, statement);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Test bulk copy from a VECTOR source column to a VARCHAR destination column with different dimensions.
     * The operation should fail with an error: "The vector dimensions 3 and 4 do not match."
     */
    @Test
    public void testBulkCopyVectorWithMismatchedDimensions() {
        String srcTable = TestUtils.escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier("testSrcTable"));
        String desTable = TestUtils.escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier("testDesTable"));

        try (Connection connection = DriverManager.getConnection(connectionString);
                Statement statement = connection.createStatement()) {

            // Create the source table with a VECTOR column
            statement.executeUpdate("CREATE TABLE " + srcTable + " (vectorCol1 VECTOR(3))");

            // Insert sample data into the source table
            float[] data = new float[] { 1.0f, 2.0f, 3.0f };
            microsoft.sql.Vector vector = new microsoft.sql.Vector(data.length,
                    microsoft.sql.Vector.VectorDimensionType.F32, data);
            try (PreparedStatement pstmt = connection.prepareStatement(
                    "INSERT INTO " + srcTable + " (vectorCol1) VALUES (?)")) {
                pstmt.setObject(1, vector, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            // Create the destination table with a VARCHAR column
            statement.executeUpdate("CREATE TABLE " + desTable + " (vectorCol2 VECTOR(4))");

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
            try (Connection connection = DriverManager.getConnection(connectionString);
                    Statement statement = connection.createStatement()) {
                TestUtils.dropTableIfExists(srcTable, statement);
                TestUtils.dropTableIfExists(desTable, statement);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * Test bulk copy with a large number of records to check performance.
     */
    @Test
    public void testBulkCopyPerformance() throws SQLException {
        String tableName = AbstractSQLGenerator.escapeIdentifier("srcTable");
        // For testing, we can use a smaller set of records to avoid long execution time
        int recordCount = 100; // Number of records to insert
        int dimensionCount = 1998; // Dimension count for the vector
        float[] vectorData = new float[dimensionCount];

        // Initialize vector data
        for (int i = 0; i < dimensionCount; i++) {
            vectorData[i] = i + 0.5f;
        }

        // Drop the table if it already exists
        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("IF OBJECT_ID('" + tableName + "', 'U') IS NOT NULL DROP TABLE " + tableName);
        }

        // Create the destination table with a single VECTOR column
        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + tableName + " (v VECTOR(" + dimensionCount + "))");
        }

        // Prepare bulk data
        List<Object[]> bulkData = new ArrayList<>();
        for (int i = 1; i <= recordCount; i++) {
            microsoft.sql.Vector vector = new microsoft.sql.Vector(dimensionCount,
                    microsoft.sql.Vector.VectorDimensionType.F32, vectorData);
            bulkData.add(new Object[] { vector });
        }

        // Measure bulk copy performance
        long startTime = System.nanoTime();
        try (Connection conn = DriverManager.getConnection(connectionString);
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn)) {

            SQLServerBulkCopyOptions bulkCopyOptions = new SQLServerBulkCopyOptions();
            bulkCopyOptions.setBulkCopyTimeout(60000);
            bulkCopyOptions.setBatchSize(1000001);
            bulkCopy.setBulkCopyOptions(bulkCopyOptions);

            bulkCopy.setDestinationTableName(tableName);

            // Use VectorBulkData for bulk copy
            ISQLServerBulkData vectorBulkData = new VectorBulkDataPerformance(bulkData, dimensionCount,
                    microsoft.sql.Vector.VectorDimensionType.F32);
            bulkCopy.writeToServer(vectorBulkData);
        }
        long endTime = System.nanoTime();

        // Calculate and print performance metrics
        long durationMs = (endTime - startTime) / 1_000_000;
        System.out.println("Bulk copy completed for " + recordCount + " records in " + durationMs + " ms.");
    }
    
    class BulkData implements ISQLServerBulkData {

        private static final long serialVersionUID = 1L;

        private class ColumnMetadata {
            String columnName;
            int columnType;
            int precision;
            int scale;

            ColumnMetadata(String name, int type, int precision, int scale) {
                columnName = name;
                columnType = type;
                this.precision = precision;
                this.scale = scale;
            }
        }

        int totalColumn = 0;
        int counter = 0;
        int rowCount = 1;
        Map<Integer, ColumnMetadata> columnMetadata;
        List<Object[]> data;

        BulkData(DBTable dstTable) {
            columnMetadata = new HashMap<>();
            totalColumn = dstTable.totalColumns();

            // add metadata
            for (int i = 0; i < totalColumn; i++) {
                SqlType sqlType = dstTable.getSqlType(i);
                int precision = sqlType.getPrecision();
                if (JDBCType.TIMESTAMP == sqlType.getJdbctype()) {
                    // TODO: update the test to use correct precision once bulkCopy is fixed
                    precision = 50;
                }
                columnMetadata.put(i + 1, new ColumnMetadata(sqlType.getName(),
                        sqlType.getJdbctype().getVendorTypeNumber(), precision, sqlType.getScale()));
            }

            // add data
            rowCount = dstTable.getTotalRows();
            data = new ArrayList<>(rowCount);
            for (int i = 0; i < rowCount; i++) {
                Object[] CurrentRow = new Object[totalColumn];
                for (int j = 0; j < totalColumn; j++) {
                    SqlType sqlType = dstTable.getSqlType(j);
                    if (JDBCType.BIT == sqlType.getJdbctype()) {
                        CurrentRow[j] = ((0 == Constants.RANDOM.nextInt(2)) ? Boolean.FALSE : Boolean.TRUE);
                    } else {
                        if (j == 0) {
                            CurrentRow[j] = i + 1;
                        } else {
                            CurrentRow[j] = sqlType.createdata();
                        }
                    }
                }
                data.add(CurrentRow);
            }
        }

        @Override
        public Set<Integer> getColumnOrdinals() {
            return columnMetadata.keySet();
        }

        @Override
        public String getColumnName(int column) {
            return columnMetadata.get(column).columnName;
        }

        @Override
        public int getColumnType(int column) {
            return columnMetadata.get(column).columnType;
        }

        @Override
        public int getPrecision(int column) {
            return columnMetadata.get(column).precision;
        }

        @Override
        public int getScale(int column) {
            return columnMetadata.get(column).scale;
        }

        @Override
        public Object[] getRowData() throws SQLServerException {
            return data.get(counter++);
        }

        @Override
        public boolean next() throws SQLServerException {
            if (counter < rowCount)
                return true;
            return false;
        }

        /**
         * reset the counter when using the interface for validating the data
         */
        public void reset() {
            counter = 0;
        }
    }

    private static class BulkRecordDT implements ISQLServerBulkData {
        boolean anyMoreData = true;
        Object[] data;

        BulkRecordDT(Object data) {
            this.data = new Object[2];
            this.data[1] = data;
        }

        @Override
        public Set<Integer> getColumnOrdinals() {
            Set<Integer> ords = new HashSet<>();
            ords.add(1);
            ords.add(2);
            return ords;
        }

        @Override
        public String getColumnName(int column) {
            if (column == 1) {
                return "Dataid";
            } else {
                return "testCol";
            }
        }

        @Override
        public int getColumnType(int column) {
            if (column == 1) {
                return java.sql.Types.INTEGER;
            } else {
                return java.sql.Types.TIMESTAMP;
            }
        }

        @Override
        public int getPrecision(int column) {
            if (column == 1) {
                return 1;
            } else {
                return 0;
            }
        }

        @Override
        public int getScale(int column) {
            if (column == 1) {
                return 0;
            } else {
                return 7;
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

    public class VectorBulkData implements ISQLServerBulkData {
        boolean anyMoreData = true;
        Object[] data;
        int precision;
        microsoft.sql.Vector.VectorDimensionType scale;

        VectorBulkData(Object data, int precision, microsoft.sql.Vector.VectorDimensionType scale) {
            this.data = new Object[1];
            this.data[0] = data;
            this.scale = scale;
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
            if (scale == microsoft.sql.Vector.VectorDimensionType.F32) {
                return 4;
            } else if (scale == microsoft.sql.Vector.VectorDimensionType.F16) {
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

    public class VectorBulkDataPerformance implements ISQLServerBulkData {
        List<Object[]> data;
        int precision;
        microsoft.sql.Vector.VectorDimensionType scale;
        int counter = 0;

        VectorBulkDataPerformance(List<Object[]> data, int precision, microsoft.sql.Vector.VectorDimensionType scale) {
            this.data = data;
            this.scale = scale;
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
            if (scale == microsoft.sql.Vector.VectorDimensionType.F32) {
                return 4;
            } else if (scale == microsoft.sql.Vector.VectorDimensionType.F16) {
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
