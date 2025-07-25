/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ComparisonUtil;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCSVFileRecord;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.DBResultSet;
import com.microsoft.sqlserver.testframework.DBStatement;
import com.microsoft.sqlserver.testframework.DBTable;
import com.microsoft.sqlserver.testframework.sqlType.SqlType;

import microsoft.sql.Vector;


/**
 * Test bulkcopy with CSV file input
 * 
 * In the input csv, first row is comma separated datatype name of values to follow Precision and scale are separated by
 * hyphen in csv to distinguish between column and scale/precision ie decimal(18-6) in csv is decimal type with
 * precision 18 and scale 6
 * 
 * Destination table contains one column for each datatype name in the csv header(first line of csv)
 */
@RunWith(JUnitPlatform.class)
@DisplayName("Test bulkCopy with CSV")
@Tag(Constants.xAzureSQLDW)
public class BulkCopyCSVTest extends AbstractTest {

    static String inputFile = "BulkCopyCSVTestInput.csv";
    static String jsonInputFile = "BulkCopyCSVTestInputWithJson.csv";
    static String inputFileNoColumnName = "BulkCopyCSVTestInputNoColumnName.csv";
    static String inputFileDelimiterEscape = "BulkCopyCSVTestInputDelimiterEscape.csv";
    static String inputFileDelimiterEscapeNoNewLineAtEnd = "BulkCopyCSVTestInputDelimiterEscapeNoNewLineAtEnd.csv";
    static String inputFileMultipleDoubleQuotes = "BulkCopyCSVTestInputMultipleDoubleQuotes.csv";
    static String computeColumnCsvFile = "BulkCopyCSVTestInputComputeColumn.csv";
    static String vectorInputCsvFile = "BulkCopyCSVTestInputWithVector.csv";
    static String vectorInputCsvFileWithMultipleColumn = "BulkCopyCSVTestInputWithMultipleVectorColumn.csv";
    static String vectorInputCsvFileWithMultipleColumnWithPipeDelimiter = "BulkCopyCSVTestWithMultipleVectorColumnWithPipeDelimiter.csv";
    static String encoding = "UTF-8";
    static String delimiter = ",";

    static DBConnection con = null;
    static DBStatement stmt = null;
    static String filePath = null;

    /**
     * Create connection, statement and generate path of resource file
     */
    @BeforeAll
    public static void setUpConnection() throws Exception {
        setConnection();

        con = new DBConnection(connectionString);
        stmt = con.createStatement();
        filePath = TestUtils.getCurrentClassPath();
    }

    /**
     * test simple csv file for bulkcopy
     */
    @Test
    @DisplayName("Test SQLServerBulkCSVFileRecord")
    public void testCSV() {
        String fileName = filePath + inputFile;
        try (SQLServerBulkCSVFileRecord f1 = new SQLServerBulkCSVFileRecord(fileName, encoding, delimiter, true);
                SQLServerBulkCSVFileRecord f2 = new SQLServerBulkCSVFileRecord(fileName, encoding, delimiter, true);) {
            testBulkCopyCSV(f1, true);

            f2.setEscapeColumnDelimitersCSV(true);
            testBulkCopyCSV(f2, true);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    /**
     * test simple csv file for bulkcopy first line not being column name
     */
    @Test
    @DisplayName("Test SQLServerBulkCSVFileRecord First line not being column name")
    public void testCSVFirstLineNotColumnName() {
        String fileName = filePath + inputFileNoColumnName;
        try (SQLServerBulkCSVFileRecord f1 = new SQLServerBulkCSVFileRecord(fileName, encoding, delimiter, false);
                SQLServerBulkCSVFileRecord f2 = new SQLServerBulkCSVFileRecord(fileName, encoding, delimiter, false)) {
            testBulkCopyCSV(f1, false);

            f2.setEscapeColumnDelimitersCSV(true);
            testBulkCopyCSV(f2, false);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    /**
     * test simple csv file for bulkcopy by passing a file from url
     * 
     * @throws SQLException
     */
    @Test
    @DisplayName("Test SQLServerBulkCSVFileRecord with passing file from url")
    public void testCSVFromURL() throws SQLException {
        try (InputStream csvFileInputStream = new URL(
                "https://raw.githubusercontent.com/Microsoft/mssql-jdbc/master/src/test/resources/BulkCopyCSVTestInput.csv")
                        .openStream();
                SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(csvFileInputStream, encoding,
                        delimiter, true)) {
            testBulkCopyCSV(fileRecord, true);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * A test to validate that the driver parses CSV file according to RFC4180 when setEscapeColumnDelimitersCSV is set
     * to true.
     *
     * @throws Exception
     */
    @Test
    @DisplayName("Test setEscapeColumnDelimitersCSV")
    public void testEscapeColumnDelimitersCSV() throws Exception {
        String tableName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("BulkEscape"));
        String fileName = filePath + inputFileDelimiterEscape;
        /*
         * The list below is the copy of inputFileDelimiterEsc ape with quotes removed.
         */
        String[][] expectedEscaped = new String[12][4];
        expectedEscaped[0] = new String[] {"test", " test\"", "no@split", " testNoQuote", ""};
        expectedEscaped[1] = new String[] {null, null, null, null, ""};
        expectedEscaped[2] = new String[] {"\"", "test\"test", "test@\"  test", null, ""};
        expectedEscaped[3] = new String[] {"testNoQuote  ", " testSpaceAround ", " testSpaceInside ",
                "  testSpaceQuote\" ", ""};
        expectedEscaped[4] = new String[] {null, null, null, " testSpaceInside ", ""};
        expectedEscaped[5] = new String[] {"1997", "Ford", "E350", "E63", ""};
        expectedEscaped[6] = new String[] {"1997", "Ford", "E350", "E63", ""};
        expectedEscaped[7] = new String[] {"1997", "Ford", "E350", "Super@ luxurious truck", ""};
        expectedEscaped[8] = new String[] {"1997", "Ford", "E350", "Super@ \"luxurious\" truck", ""};
        expectedEscaped[9] = new String[] {"1997", "Ford", "E350", "E63", ""};
        expectedEscaped[10] = new String[] {"1997", "Ford", "E350", " Super luxurious truck ", ""};
        expectedEscaped[11] = new String[] {"1997", "F\r\no\r\nr\r\nd", "E350", "\"Super\" \"luxurious\" \"truck\"",
                ""};

        try (Connection con = getConnection(); Statement stmt = con.createStatement();
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
                SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(fileName, encoding, "@",
                        false)) {
            bulkCopy.setDestinationTableName(tableName);
            fileRecord.setEscapeColumnDelimitersCSV(true);
            fileRecord.addColumnMetadata(1, null, java.sql.Types.INTEGER, 0, 0);
            fileRecord.addColumnMetadata(2, null, java.sql.Types.VARCHAR, 50, 0);
            fileRecord.addColumnMetadata(3, null, java.sql.Types.VARCHAR, 50, 0);
            fileRecord.addColumnMetadata(4, null, java.sql.Types.VARCHAR, 50, 0);
            fileRecord.addColumnMetadata(5, null, java.sql.Types.VARCHAR, 50, 0);
            fileRecord.addColumnMetadata(6, null, java.sql.Types.VARCHAR, 50, 0);
            stmt.executeUpdate("CREATE TABLE " + tableName
                    + " (id INT IDENTITY(1,1), c1 VARCHAR(50), c2 VARCHAR(50), c3 VARCHAR(50), c4 VARCHAR(50), c5 VARCHAR(50))");
            bulkCopy.writeToServer(fileRecord);

            int i = 0;
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " ORDER BY id");
                    BufferedReader br = new BufferedReader(new FileReader(fileName));) {
                while (rs.next()) {
                    assertEquals(expectedEscaped[i][0], rs.getString("c1"));
                    assertEquals(expectedEscaped[i][1], rs.getString("c2"));
                    assertEquals(expectedEscaped[i][2], rs.getString("c3"));
                    assertEquals(expectedEscaped[i][3], rs.getString("c4"));
                    i++;
                }
                assertEquals(i, 12, "Expected to load 12 records, but loaded " + i + " records");
            }

            TestUtils.dropTableIfExists(tableName, stmt);
        }
    }

    @Test
    @DisplayName("Test setEscapeColumnDelimitersCSVNoNewLineAtEnd")
    public void testEscapeColumnDelimitersCSVNoNewLineAtEnd() throws Exception {
        String tableName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("BulkEscape"));
        String fileName = filePath + inputFileDelimiterEscapeNoNewLineAtEnd;
        /*
         * The list below is the copy of inputFileDelimiterEsc ape with quotes removed.
         */
        String[][] expectedEscaped = new String[12][4];
        expectedEscaped[0] = new String[] {"test", " test\"", "no@split", " testNoQuote", ""};
        expectedEscaped[1] = new String[] {null, null, null, null, ""};
        expectedEscaped[2] = new String[] {"\"", "test\"test", "test@\"  test", null, ""};
        expectedEscaped[3] = new String[] {"testNoQuote  ", " testSpaceAround ", " testSpaceInside ",
                "  testSpaceQuote\" ", ""};
        expectedEscaped[4] = new String[] {null, null, null, " testSpaceInside ", ""};
        expectedEscaped[5] = new String[] {"1997", "Ford", "E350", "E63", ""};
        expectedEscaped[6] = new String[] {"1997", "Ford", "E350", "E63", ""};
        expectedEscaped[7] = new String[] {"1997", "Ford", "E350", "Super@ luxurious truck", ""};
        expectedEscaped[8] = new String[] {"1997", "Ford", "E350", "Super@ \"luxurious\" truck", ""};
        expectedEscaped[9] = new String[] {"1997", "Ford", "E350", "E63", ""};
        expectedEscaped[10] = new String[] {"1997", "Ford", "E350", " Super luxurious truck ", ""};
        expectedEscaped[11] = new String[] {"1997", "F\r\no\r\nr\r\nd", "E350", "\"Super\" \"luxurious\" \"truck\"",
                ""};

        try (Connection con = getConnection(); Statement stmt = con.createStatement();
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
                SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(fileName, encoding, "@",
                        false)) {
            bulkCopy.setDestinationTableName(tableName);
            fileRecord.setEscapeColumnDelimitersCSV(true);
            fileRecord.addColumnMetadata(1, null, java.sql.Types.INTEGER, 0, 0);
            fileRecord.addColumnMetadata(2, null, java.sql.Types.VARCHAR, 50, 0);
            fileRecord.addColumnMetadata(3, null, java.sql.Types.VARCHAR, 50, 0);
            fileRecord.addColumnMetadata(4, null, java.sql.Types.VARCHAR, 50, 0);
            fileRecord.addColumnMetadata(5, null, java.sql.Types.VARCHAR, 50, 0);
            fileRecord.addColumnMetadata(6, null, java.sql.Types.VARCHAR, 50, 0);
            stmt.executeUpdate("CREATE TABLE " + tableName
                    + " (id INT IDENTITY(1,1), c1 VARCHAR(50), c2 VARCHAR(50), c3 VARCHAR(50), c4 VARCHAR(50), c5 VARCHAR(50))");
            bulkCopy.writeToServer(fileRecord);

            int i = 0;
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " ORDER BY id");
                    BufferedReader br = new BufferedReader(new FileReader(fileName));) {
                while (rs.next()) {
                    assertEquals(expectedEscaped[i][0], rs.getString("c1"));
                    assertEquals(expectedEscaped[i][1], rs.getString("c2"));
                    assertEquals(expectedEscaped[i][2], rs.getString("c3"));
                    assertEquals(expectedEscaped[i][3], rs.getString("c4"));
                    i++;
                }
                assertEquals(i, 12, "Expected to load 12 records, but loaded " + i + " records");
            } finally {
                TestUtils.dropTableIfExists(tableName, stmt);
            }
        }
    }

    /**
     * test simple csv file for bulkcopy, for GitHub issue 1391 Tests to ensure that the set returned by
     * getColumnOrdinals doesn't have to be ordered
     */
    @Test
    @DisplayName("Test SQLServerBulkCSVFileRecord GitHb 1391")
    public void testCSV1391() {
        String fileName = filePath + inputFile;
        try (SQLServerBulkCSVFileRecord f1 = new BulkData1391(fileName, encoding, delimiter, true);
                SQLServerBulkCSVFileRecord f2 = new BulkData1391(fileName, encoding, delimiter, true);) {
            testBulkCopyCSV(f1, true);

            f2.setEscapeColumnDelimitersCSV(true);
            testBulkCopyCSV(f2, true);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    // Used for testing issue reported in https://github.com/microsoft/mssql-jdbc/issues/1391
    private class BulkData1391 extends SQLServerBulkCSVFileRecord {

        public BulkData1391(String fileToParse, String encoding, String delimiter,
                boolean firstLineIsColumnNames) throws SQLServerException {
            super(fileToParse, encoding, delimiter, firstLineIsColumnNames);
        }

        @Override
        public Set<Integer> getColumnOrdinals() {
            List<Integer> list = new ArrayList<>(columnMetadata.keySet());
            Integer temp = list.get(0);
            list.set(0, list.get(1));
            list.set(1, temp);
            return new LinkedHashSet<>(list);
        }
    }

    private void testBulkCopyCSV(SQLServerBulkCSVFileRecord fileRecord, boolean firstLineIsColumnNames) {
        DBTable destTable = null;
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(new FileInputStream(filePath + inputFile), encoding))) {
            // read the first line from csv and parse it to get datatypes to create destination column
            String[] columnTypes = br.readLine().substring(1)/* Skip the Byte order mark */.split(delimiter, -1);
            br.close();

            int numberOfColumns = columnTypes.length;
            destTable = new DBTable(false);
            try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy((Connection) con.product())) {
                bulkCopy.setDestinationTableName(destTable.getEscapedTableName());

                // add a column in destTable for each datatype in csv
                for (int i = 0; i < numberOfColumns; i++) {
                    SqlType sqlType = null;
                    int precision = -1;
                    int scale = -1;

                    String columnType = columnTypes[i].trim().toLowerCase();
                    int indexOpenParenthesis = columnType.lastIndexOf("(");
                    // skip the parenthesis in case of precision and scale type
                    if (-1 != indexOpenParenthesis) {
                        String precision_scale = columnType.substring(indexOpenParenthesis + 1,
                                columnType.length() - 1);
                        columnType = columnType.substring(0, indexOpenParenthesis);
                        sqlType = SqlTypeMapping.valueOf(columnType.toUpperCase()).sqlType;

                        // add scale if exist
                        int indexPrecisionScaleSeparator = precision_scale.indexOf("-");
                        if (-1 != indexPrecisionScaleSeparator) {
                            scale = Integer.parseInt(precision_scale.substring(indexPrecisionScaleSeparator + 1));
                            sqlType.setScale(scale);
                            precision_scale = precision_scale.substring(0, indexPrecisionScaleSeparator);
                        }
                        // add precision
                        precision = Integer.parseInt(precision_scale);
                        sqlType.setPrecision(precision);
                    } else {
                        sqlType = SqlTypeMapping.valueOf(columnType.toUpperCase()).sqlType;
                    }

                    destTable.addColumn(sqlType);
                    fileRecord.addColumnMetadata(i + 1, "", sqlType.getJdbctype().getVendorTypeNumber(),
                            (-1 == precision) ? 0 : precision, (-1 == scale) ? 0 : scale);
                }
                stmt.createTable(destTable);
                bulkCopy.writeToServer(fileRecord);
            }
            if (firstLineIsColumnNames)
                validateValuesFromCSV(destTable, inputFile);
            else
                validateValuesFromCSV(destTable, inputFileNoColumnName);

        } catch (Exception e) {
            fail(e.getMessage());
        } finally {
            if (null != destTable) {
                stmt.dropTable(destTable);
            }
        }
    }

    /**
     * validate value in csv and in destination table as string
     * 
     * @param destinationTable
     */
    static void validateValuesFromCSV(DBTable destinationTable, String inputFile) {
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(new FileInputStream(filePath + inputFile), encoding))) {
            if (inputFile.equalsIgnoreCase("BulkCopyCSVTestInput.csv"))
                br.readLine(); // skip first line as it is header

            try (DBResultSet dstResultSet = stmt
                    .executeQuery("SELECT * FROM " + destinationTable.getEscapedTableName() + Constants.SEMI_COLON)) {
                ResultSetMetaData destMeta = ((ResultSet) dstResultSet.product()).getMetaData();
                int totalColumns = destMeta.getColumnCount();
                while (dstResultSet.next()) {
                    String[] srcValues = br.readLine().split(delimiter);
                    if ((0 == srcValues.length) && (srcValues.length != totalColumns)) {
                        srcValues = new String[totalColumns];
                        Arrays.fill(srcValues, null);
                    }
                    for (int i = 1; i <= totalColumns; i++) {
                        String srcValue = srcValues[i - 1];
                        String dstValue = dstResultSet.getString(i);
                        srcValue = (null != srcValue) ? srcValue.trim() : srcValue;
                        dstValue = (null != dstValue) ? dstValue.trim() : dstValue;
                        // get the value from csv as string and compare them
                        ComparisonUtil.compareExpectedAndActual(java.sql.Types.VARCHAR, srcValue, dstValue);
                    }
                }
            }
        } catch (Exception e) {
            fail("CSV validation failed with " + e.getMessage());
        }
    }

    /**
     * Test simple csv file for bulkcopy, for GitHub issue 2400. Tests to ensure that recursion in the CSV
     * parsing does not blow the default Java stack
     * Success is if there is no exception when parsing the CSV file in bulkCopy.writeToServer()
     */
    @Test
    @DisplayName("Test SQLServerBulkCSVFileRecord GitHb 2400")
    public void testCSV2400() {
        String tableName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("BulkEscape"));
        String fileName = filePath + inputFileMultipleDoubleQuotes;

        try (Connection con = getConnection(); Statement stmt = con.createStatement();
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
                SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(fileName, encoding, ",",
                        false)) {
            bulkCopy.setDestinationTableName(tableName);
            fileRecord.setEscapeColumnDelimitersCSV(true);
            fileRecord.addColumnMetadata(1, null, java.sql.Types.INTEGER, 0, 0);
            fileRecord.addColumnMetadata(2, null, java.sql.Types.VARCHAR, 1, 0);
            fileRecord.addColumnMetadata(3, null, java.sql.Types.VARCHAR, 50, 0);
            fileRecord.addColumnMetadata(4, null, java.sql.Types.VARCHAR, 200, 0);
            fileRecord.addColumnMetadata(5, null, java.sql.Types.VARCHAR, 1000, 0);
            fileRecord.addColumnMetadata(6, null, java.sql.Types.VARCHAR, 4000, 0);
            fileRecord.addColumnMetadata(7, null, java.sql.Types.VARCHAR, 8000, 0);
            fileRecord.addColumnMetadata(8, null, java.sql.Types.VARCHAR, 8000, 0);
            fileRecord.addColumnMetadata(9, null, java.sql.Types.VARCHAR, 1, 0);
            fileRecord.addColumnMetadata(10, null, java.sql.Types.VARCHAR, 50, 0);
            fileRecord.addColumnMetadata(11, null, java.sql.Types.VARCHAR, 200, 0);
            fileRecord.addColumnMetadata(12, null, java.sql.Types.VARCHAR, 1000, 0);
            fileRecord.addColumnMetadata(13, null, java.sql.Types.VARCHAR, 4000, 0);
            fileRecord.addColumnMetadata(14, null, java.sql.Types.VARCHAR, 8000, 0);
            fileRecord.addColumnMetadata(15, null, java.sql.Types.VARCHAR, 8000, 0);

            stmt.executeUpdate("CREATE TABLE " + tableName
                    + " (C1 INT, C2 VARCHAR(1), C3 VARCHAR(50), C4 VARCHAR(200), C5 VARCHAR(1000), C6 VARCHAR(4000),"
                    + " C7 VARCHAR(8000), C8 VARCHAR(8000), C9 VARCHAR(1), C10 VARCHAR(50), C11 VARCHAR(200), C12 VARCHAR(1000),"
                    + " C13 VARCHAR(4000), C14 VARCHAR(8000), C15 VARCHAR(8000))");

            bulkCopy.writeToServer(fileRecord);

            TestUtils.dropTableIfExists(tableName, stmt);
        } catch (StackOverflowError e) {
            fail("Stack overflow: " + e.getMessage());
        } catch (SQLException e) {
            fail("SQL exception: " + e.getMessage());
        }
    }

    /**
     * Test bulk copy with JSON data type
     * 
     * This test reads a CSV file with JSON data and performs a bulk copy into a table with a JSON column.
     * It verifies that the data is copied correctly by comparing the values in the table with the expected values.
     */
    @Test
    @DisplayName("Test Bulk Copy with JSON Data")
    @Tag(Constants.JSONTest)
    public void testBulkCopyWithJson() throws Exception {
        String tableName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("BulkJsonTest"));
        String fileName = filePath + jsonInputFile;

        // Expected values as read from the CSV file
        String[][] expectedValues = new String[][]{
            {"0", "testing", "{\"age\":25,\"address\":{\"pincode\":123456,\"state\":\"NY\"}}"},
            {"1","test }","{\"age\":25,\"city\":\"Los Angeles\"}"},
            {"0","test {0}","{\"age\":40,\"city\":\"Chicago\"}"}
        };

        try (Connection con = getConnection(); Statement stmt = con.createStatement();
            SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
            SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(fileName, encoding, ",", false)) {
            bulkCopy.setDestinationTableName(tableName);

            // Define column metadata
            fileRecord.addColumnMetadata(1, null, java.sql.Types.BIT, 0, 0);
            fileRecord.addColumnMetadata(2, null, java.sql.Types.NCHAR, 10, 0);
            fileRecord.addColumnMetadata(3, null, microsoft.sql.Types.JSON, 0, 0); // JSON column

            // Create table
            stmt.executeUpdate("CREATE TABLE " + tableName + " ("
                    + "c1 BIT, c2 nchar(50), c3 JSON)");

            // Perform bulk copy
            fileRecord.setEscapeColumnDelimitersCSV(true);
            bulkCopy.writeToServer(fileRecord);

            // Verify the data
            int i = 0;
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
                BufferedReader br = new BufferedReader(new FileReader(fileName))) {

                while (rs.next()) {
                    for (int j = 1; j <= 3; j++) {
                        String actual = rs.getString(j);
                        String expected = expectedValues[i][j - 1];
                        assertEquals(expected.trim(), actual.trim(), "Mismatch in column " + j);
                    }
                    i++;
                }
            } finally {
                TestUtils.dropTableIfExists(tableName, stmt);
            }
        }
    }

    /**
     * Test to perform bulk copy with a computed column as the last column in the table.
     */
    @Test
    @DisplayName("Test bulk copy with computed column as last column")
    public void testBulkCopyWithComputedColumnAsLastColumn() {
        String tableName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("BulkEscape"));
        String fileName = filePath + computeColumnCsvFile;

        try (Connection con = getConnection(); Statement stmt = con.createStatement();
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
                SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(fileName, encoding, ",", true)) {

            String createTableSQL = "CREATE TABLE " + tableName + " (" + "[NAME] varchar(50) NOT NULL,"
                    + "[AGE] int NULL," + "[CAL_COL] numeric(17, 2) NULL," + "[ORIGINAL] varchar(50) NOT NULL,"
                    + "[COMPUTED_COL] AS (right([NAME], 8)) PERSISTED" + ")";
            stmt.executeUpdate(createTableSQL);

            fileRecord.addColumnMetadata(1, "NAME", java.sql.Types.VARCHAR, 50, 0);
            fileRecord.addColumnMetadata(2, "AGE", java.sql.Types.INTEGER, 0, 0);
            fileRecord.addColumnMetadata(3, "CAL_COL", java.sql.Types.NUMERIC, 17, 2);
            fileRecord.addColumnMetadata(4, "ORIGINAL", java.sql.Types.VARCHAR, 50, 0);

            bulkCopy.setDestinationTableName(tableName);

            bulkCopy.addColumnMapping("NAME", "NAME");
            bulkCopy.addColumnMapping("AGE", "AGE");
            bulkCopy.addColumnMapping("CAL_COL", "CAL_COL");
            bulkCopy.addColumnMapping("ORIGINAL", "ORIGINAL");

            SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
            options.setKeepIdentity(false);
            options.setTableLock(true);
            bulkCopy.setBulkCopyOptions(options);

            bulkCopy.writeToServer(fileRecord);

            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
                if (rs.next()) {
                    int rowCount = rs.getInt(1);
                    assertTrue(rowCount > 0);
                }
            } finally {
                TestUtils.dropTableIfExists(tableName, stmt);
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test to perform bulk copy with a computed column not as the last column in the table.
     */
    @Test
    @DisplayName("Test bulk copy with computed column not as last column")
    public void testBulkCopyWithComputedColumnNotAsLastColumn() {
        String tableName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("BulkEscape"));
        String fileName = filePath + computeColumnCsvFile;

        try (Connection con = getConnection(); Statement stmt = con.createStatement();
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
                SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(fileName, encoding, ",", true)) {

            String createTableSQL = "CREATE TABLE " + tableName + " (" + "[NAME] varchar(50) NOT NULL,"
                    + "[AGE] int NULL," + "[CAL_COL] numeric(17, 2) NULL," + "[ORIGINAL] varchar(50) NOT NULL,"
                    + "[COMPUTED_COL] AS (right([NAME], 8)) PERSISTED," + "[LAST_COL] varchar(50) NULL" + ")";
            stmt.executeUpdate(createTableSQL);

            fileRecord.addColumnMetadata(1, "NAME", java.sql.Types.VARCHAR, 50, 0);
            fileRecord.addColumnMetadata(2, "AGE", java.sql.Types.INTEGER, 0, 0);
            fileRecord.addColumnMetadata(3, "CAL_COL", java.sql.Types.NUMERIC, 17, 2);
            fileRecord.addColumnMetadata(4, "ORIGINAL", java.sql.Types.VARCHAR, 50, 0);
            fileRecord.addColumnMetadata(5, "LAST_COL", java.sql.Types.VARCHAR, 50, 0);

            bulkCopy.setDestinationTableName(tableName);

            bulkCopy.addColumnMapping("NAME", "NAME");
            bulkCopy.addColumnMapping("AGE", "AGE");
            bulkCopy.addColumnMapping("CAL_COL", "CAL_COL");
            bulkCopy.addColumnMapping("ORIGINAL", "ORIGINAL");
            bulkCopy.addColumnMapping("LAST_COL", "LAST_COL");

            SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
            options.setKeepIdentity(false);
            options.setTableLock(true);
            bulkCopy.setBulkCopyOptions(options);

            bulkCopy.writeToServer(fileRecord);

            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
                if (rs.next()) {
                    int rowCount = rs.getInt(1);
                    assertTrue(rowCount > 0);
                }
            } finally {
                TestUtils.dropTableIfExists(tableName, stmt);
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test bulk copy with different format of vector data in
     * BulkCopyCSVTestInputWithVector.csv file
     */
    @Test
    @Tag(Constants.vectorTest)
    public void testBulkCopyVectorFromCSV() throws SQLException {
        String dstTable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTable"));
        String fileName = filePath + vectorInputCsvFile;

        try (Connection con = getConnection();
                Statement stmt = con.createStatement();
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
                SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(fileName, null, ",", true)) {

            // Create the destination table
            stmt.executeUpdate(
                    "CREATE TABLE " + dstTable + " (id INT, vectorCol VECTOR(3));");

            // Add column metadata for the CSV file
            fileRecord.addColumnMetadata(1, "id", java.sql.Types.INTEGER, 0, 0);
            fileRecord.addColumnMetadata(2, "vectorCol", microsoft.sql.Types.VECTOR, 3, 4);
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
                        new Object[] { 1, new Object[] { 1.0f, 2.0f, 3.0f } },
                        new Object[] { 2, new Object[] { 4.0f, 5.0f, 6.0f } },
                        new Object[] { 3, null });

                while (rs.next()) {
                    int id = rs.getInt("id");
                    Vector vectorObject = rs.getObject("vectorCol", Vector.class);

                    // Validate ID
                    assertEquals(expectedData.get(rowCount)[0], id, "Mismatch in ID at row " + (rowCount + 1));

                    // Validate vector data
                    if (expectedData.get(rowCount)[1] == null) {
                        assertEquals(null, vectorObject.getData(), "Expected null vector at row " + (rowCount + 1));
                    } else {
                        assertNotNull(vectorObject, "Expected non-null vector at row " + (rowCount + 1));
                        assertArrayEquals((Object[]) expectedData.get(rowCount)[1], vectorObject.getData(),
                                "Mismatch in vector data at row " + (rowCount + 1));
                    }

                    rowCount++;
                }

                // Validate row count
                assertEquals(expectedData.size(), rowCount, "Row count mismatch.");
            } finally {
                TestUtils.dropTableIfExists(dstTable, stmt);
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test bulk copy with multiple columns of vector data in
     * BulkCopyCSVTestInputWithMultipleVectorColumn.csv file
     */
    @Test
    @Tag(Constants.vectorTest)
    public void testBulkCopyVectorFromCSVWithMultipleColumns() throws SQLException {
        String dstTable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTable"));
        String fileName = filePath + vectorInputCsvFileWithMultipleColumn;

        try (Connection con = getConnection();
                Statement stmt = con.createStatement();
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
                SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(fileName, null, ",", true)) {

            stmt.executeUpdate(
                    "CREATE TABLE " + dstTable + " (vectorCol1 VECTOR(3), vectorCol2 VECTOR(3));");

            fileRecord.addColumnMetadata(1, "vectorCol1", microsoft.sql.Types.VECTOR, 3, 4);
            fileRecord.addColumnMetadata(2, "vectorCol2", microsoft.sql.Types.VECTOR, 3, 4);
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

                // Expected data from the CSV file
                List<String[]> expectedData = Arrays.asList(
                        new String[] { "null", "null" },
                        new String[] { "[1.0, 2.0, 3.0]", "[1.0, 2.0, 3.0]" },
                        new String[] { "[3.0, 4.0, 5.0]", "[6.0, 7.0, 8.0]" });

                while (rs.next()) {
                    Vector vectorCol1 = rs.getObject("vectorCol1", Vector.class);
                    Vector vectorCol2 = rs.getObject("vectorCol2", Vector.class);

                    assertEquals(expectedData.get(rowCount)[0],
                            vectorCol1 == null ? "null" : Arrays.toString(vectorCol1.getData()),
                            "Mismatch in vectorCol1 data at row " + (rowCount + 1));
                    assertEquals(expectedData.get(rowCount)[1],
                            vectorCol2 == null ? "null" : Arrays.toString(vectorCol2.getData()),
                            "Mismatch in vectorCol2 data at row " + (rowCount + 1));
                    rowCount++;
                }

                // Validate row count
                assertEquals(expectedData.size(), rowCount, "Row count mismatch.");
            } finally {
                TestUtils.dropTableIfExists(dstTable, stmt);
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test bulk copy with multiple columns of vector data with pipe delimiter in
     * BulkCopyCSVTestWithMultipleVectorColumnWithPipeDelimiter.csv file
     */
    @Test
    @Tag(Constants.vectorTest)
    public void testBulkCopyVectorFromCSVWithMultipleColumnsWithPipeDelimiter() throws SQLException {
        String dstTable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTable"));
        String fileName = filePath + vectorInputCsvFileWithMultipleColumnWithPipeDelimiter;

        try (Connection con = getConnection();
                Statement stmt = con.createStatement();
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
                SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(fileName, null, "|", true)) {

            stmt.executeUpdate(
                    "CREATE TABLE " + dstTable + " (vectorCol1 VECTOR(3), vectorCol2 VECTOR(3));");

            fileRecord.addColumnMetadata(1, "vectorCol1", microsoft.sql.Types.VECTOR, 3, 4);
            fileRecord.addColumnMetadata(2, "vectorCol2", microsoft.sql.Types.VECTOR, 3, 4);
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

                // Expected data from the CSV file
                List<String[]> expectedData = Arrays.asList(
                        new String[] { "null", "null" },
                        new String[] { "[1.0, 2.0, 3.0]", "[1.0, 2.0, 3.0]" },
                        new String[] { "[3.0, 4.0, 5.0]", "[6.0, 7.0, 8.0]" });

                while (rs.next()) {
                    Vector vectorCol1 = rs.getObject("vectorCol1", Vector.class);
                    Vector vectorCol2 = rs.getObject("vectorCol2", Vector.class);

                    assertEquals(expectedData.get(rowCount)[0],
                            vectorCol1 == null ? "null" : Arrays.toString(vectorCol1.getData()),
                            "Mismatch in vectorCol1 data at row " + (rowCount + 1));
                    assertEquals(expectedData.get(rowCount)[1],
                            vectorCol2 == null ? "null" : Arrays.toString(vectorCol2.getData()),
                            "Mismatch in vectorCol2 data at row " + (rowCount + 1));
                    rowCount++;
                }

                // Validate row count
                assertEquals(expectedData.size(), rowCount, "Row count mismatch.");
            } finally {
                TestUtils.dropTableIfExists(dstTable, stmt);
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test bulk copy with mismatched vector dimensions in file and provided column
     * metadata.
     */
    @Test
    @Tag(Constants.vectorTest)
    public void testBulkCopyVectorFromCSVWithIncorrectDimension() throws SQLException {
        String dstTable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("dstTable"));
        String fileName = filePath + vectorInputCsvFile;

        try (Connection con = getConnection();
                Statement stmt = con.createStatement();
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
                SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(fileName, null, ",", true)) {

            // Create the destination table
            stmt.executeUpdate(
                    "CREATE TABLE " + dstTable + " (id INT, vectorCol VECTOR(3));");

            // Add column metadata for the CSV file
            fileRecord.addColumnMetadata(1, "id", java.sql.Types.INTEGER, 0, 0);
            fileRecord.addColumnMetadata(2, "vectorCol", microsoft.sql.Types.VECTOR, 4, 4);
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
        }
    }

    /**
     * drop source table after testing bulk copy
     * 
     * @throws SQLException
     */
    @AfterAll
    public static void tearConnection() throws SQLException {
        stmt.close();
        con.close();
    }
}
