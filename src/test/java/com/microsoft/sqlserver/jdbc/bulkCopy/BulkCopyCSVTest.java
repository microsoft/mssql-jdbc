/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCSVFileRecord;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.DBResultSet;
import com.microsoft.sqlserver.testframework.DBStatement;
import com.microsoft.sqlserver.testframework.DBTable;
import com.microsoft.sqlserver.testframework.Utils;
import com.microsoft.sqlserver.testframework.sqlType.SqlType;
import com.microsoft.sqlserver.testframework.util.ComparisonUtil;

/**
 * Test bulkcopy with CSV file input
 * 
 * In the input csv, first row is comma separated datatype name of values to follow Precision and scale are separated by hyphen in csv to distinguish
 * between column and scale/precision ie decimal(18-6) in csv is decimal type with precision 18 and scale 6
 * 
 * Destination table contains one column for each datatype name in the csv header(first line of csv)
 */
@RunWith(JUnitPlatform.class)
@DisplayName("Test bulkCopy with CSV")
public class BulkCopyCSVTest extends AbstractTest {

    static String inputFile = "BulkCopyCSVTestInput.csv";
    static String inputFileNoColumnName = "BulkCopyCSVTestInputNoColumnName.csv";
    static String encoding = "UTF-8";
    static String delimiter = ",";

    static DBConnection con = null;
    static DBStatement stmt = null;
    static String filePath = null;

    /**
     * Create connection, statement and generate path of resource file
     */
    @BeforeAll
    static void setUpConnection() {
        con = new DBConnection(connectionString);
        stmt = con.createStatement();
        filePath = Utils.getCurrentClassPath();
    }

    /**
     * test simple csv file for bulkcopy
     */
    @Test
    @DisplayName("Test SQLServerBulkCSVFileRecord")
    void testCSV() {
        try (SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(filePath + inputFile, encoding, delimiter, true)) {
            testBulkCopyCSV(fileRecord, true);
        }
        catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    /**
     * test simple csv file for bulkcopy first line not being column name
     */
    @Test
    @DisplayName("Test SQLServerBulkCSVFileRecord First line not being column name")
    void testCSVFirstLineNotColumnName() {
        try (SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(filePath + inputFileNoColumnName, encoding, delimiter, false)) {
            testBulkCopyCSV(fileRecord, false);
        }
        catch (SQLException e) {
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
    void testCSVFromURL() throws SQLException {
        try (InputStream csvFileInputStream = new URL(
                    "https://raw.githubusercontent.com/Microsoft/mssql-jdbc/master/src/test/resources/BulkCopyCSVTestInput.csv").openStream();
            SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(csvFileInputStream, encoding, delimiter, true)) {
            testBulkCopyCSV(fileRecord, true);
        }
        catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void testBulkCopyCSV(SQLServerBulkCSVFileRecord fileRecord,
            boolean firstLineIsColumnNames) {
        DBTable destTable = null;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filePath + inputFile), encoding))) {
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
	                    String precision_scale = columnType.substring(indexOpenParenthesis + 1, columnType.length() - 1);
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
	                }
	                else {
	                    sqlType = SqlTypeMapping.valueOf(columnType.toUpperCase()).sqlType;
	                }
	
	                destTable.addColumn(sqlType);
	                fileRecord.addColumnMetadata(i + 1, "", sqlType.getJdbctype().getVendorTypeNumber(), (-1 == precision) ? 0 : precision,
	                        (-1 == scale) ? 0 : scale);
	            }
	            stmt.createTable(destTable);
	            bulkCopy.writeToServer((ISQLServerBulkRecord) fileRecord);
            }
            if (firstLineIsColumnNames)
                validateValuesFromCSV(destTable, inputFile);
            else
                validateValuesFromCSV(destTable, inputFileNoColumnName);

        }
        catch (Exception e) {
            fail(e.getMessage());
        }
        finally {
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
    static void validateValuesFromCSV(DBTable destinationTable,
            String inputFile) {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filePath + inputFile), encoding))) {
            if (inputFile.equalsIgnoreCase("BulkCopyCSVTestInput.csv"))
                br.readLine();   // skip first line as it is header

            try (DBResultSet dstResultSet = stmt.executeQuery("SELECT * FROM " + destinationTable.getEscapedTableName() + ";")) {
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
        }
        catch (Exception e) {
            fail("CSV validation failed with " + e.getMessage());
        }
    }

    /**
     * drop source table after testing bulk copy
     * 
     * @throws SQLException
     */
    @AfterAll
    static void tearConnection() throws SQLException {
        stmt.close();
        con.close();
    }
}
