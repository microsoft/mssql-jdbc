/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ComparisonUtil;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCSVFileRecord;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * Test connection property sendTemporalDataTypesAsStringForBulkCopy
 * This connection string, when set to FALSE, will send DATE, DATETIME, DATIMETIME2 DATETIMEOFFSET, SMALLDATETIME, and
 * TIME
 * datatypes as their respective types instead of sending them as String.
 * Additionally, even without setting this connection string to FALSE, MONEY and SMALLMONEY datatypes will be
 * sent as MONEY / SMALLMONEY datatypes instead of DECIMAL after these changes.
 * 
 * Note that with this connection property set to FALSE, the driver will only accept the default string literal format
 * of each temporal datatype, for example:
 * 
 * DATE: YYYY-MM-DD
 * DATETIME: YYYY-MM-DD hh:mm:ss[.nnn]
 * DATETIME2: YYYY-MM-DD hh:mm:ss[.nnnnnnn]
 * DATETIMEOFFSET: YYYY-MM-DD hh:mm:ss[.nnnnnnn] [{+|-}hh:mm]
 * SMALLDATETIME:YYYY-MM-DD hh:mm:ss
 * TIME: hh:mm:ss[.nnnnnnn]
 * 
 */
@RunWith(JUnitPlatform.class)
public class BulkCopySendTemporalDataTypesAsString extends AbstractTest {
    static String inputFile = "BulkCopyCSVSendTemporalDataTypesAsStringForBulkCopy.csv";
    static String encoding = "UTF-8";
    static String delimiter = ",";

    static String destTableName = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("sendTemporalDataTypesAsStringForBulkCopyDestTable"));
    static String destTableName2 = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("sendTemporalDataTypesAsStringForBulkCopyDestTable2"));
    static String destTableNameEncrypted = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("sendTemporalDataTypesAsStringForBulkCopyDestTableEncrypted"));
    static String filePath = null;

    /**
     * Test basic case with sendTemporalDataTypesAsStringForBulkCopy connection property.
     * 
     * @throws SQLException
     */
    @Test
    public void testSendTemporalDataTypesAsStringForBulkCopy() throws SQLException {
        beforeEachSetup();
        try (Connection conn = PrepUtil
                .getConnection(connectionString + ";sendTemporalDataTypesAsStringForBulkCopy=false")) {
            SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(filePath + inputFile, encoding,
                    delimiter, true);

            testBulkCopyCSV(conn, fileRecord);

        }
    }
    
    /**
     * Test basic case with sendTemporalDataTypesAsStringForBulkCopy connection property, using a resultset.
     * 
     * @throws SQLException
     */
    @Test
    public void testSendTemporalDataTypesAsStringForBulkCopyRS() throws SQLException {
        beforeEachSetup();
        try (Connection conn = PrepUtil
                .getConnection(connectionString + ";sendTemporalDataTypesAsStringForBulkCopy=false")) {
            SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(filePath + inputFile, encoding,
                    delimiter, true);

            testBulkCopyResultSet(conn, fileRecord);
        }
    }

    /**
     * Test basic case with sendTemporalDataTypesAsStringForBulkCopy connection property, using a data source.
     * 
     * @throws SQLException
     */
    @Test
    public void testSendTemporalDataTypesAsStringForBulkCopyDS() throws SQLException {
        beforeEachSetup();
        SQLServerDataSource dsLocal = new SQLServerDataSource();
        AbstractTest.updateDataSource(connectionString, dsLocal);
        dsLocal.setSendTemporalDataTypesAsStringForBulkCopy(false);

        try (Connection conn = dsLocal.getConnection()) {
            SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(filePath + inputFile, encoding,
                    delimiter, true);

            testBulkCopyCSV(conn, fileRecord);

        }
    }

    private void testBulkCopyCSV(Connection conn, SQLServerBulkCSVFileRecord fileRecord) {
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(new FileInputStream(filePath + inputFile), encoding));
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn); Statement stmt = conn.createStatement()) {

            fileRecord.addColumnMetadata(1, "c1", java.sql.Types.DATE, 0, 0); // with Date
            fileRecord.addColumnMetadata(2, "c2", java.sql.Types.TIMESTAMP, 0, 0); // with Datetime
            fileRecord.addColumnMetadata(3, "c3", java.sql.Types.TIMESTAMP, 0, 7); // with Datetime2
            fileRecord.addColumnMetadata(4, "c4", java.sql.Types.TIME, 0, 7); // with time
            fileRecord.addColumnMetadata(5, "c5", microsoft.sql.Types.DATETIMEOFFSET, 0, 7); // with datetimeoffset
            fileRecord.addColumnMetadata(6, "c6", java.sql.Types.TIMESTAMP, 0, 0); // with SmallDatetime
            fileRecord.addColumnMetadata(7, "c7", java.sql.Types.DECIMAL, 19, 4); // with money
            fileRecord.addColumnMetadata(8, "c8", java.sql.Types.DECIMAL, 10, 4); // with smallmoney

            bulkCopy.setDestinationTableName(destTableName);
            bulkCopy.writeToServer(fileRecord);

            validateValuesFromCSV(stmt, destTableName, inputFile);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
    
    private void testBulkCopyResultSet(Connection conn, SQLServerBulkCSVFileRecord fileRecord) {
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(new FileInputStream(filePath + inputFile), encoding));
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn); Statement stmt = conn.createStatement()) {

            fileRecord.addColumnMetadata(1, "c1", java.sql.Types.DATE, 0, 0); // with Date
            fileRecord.addColumnMetadata(2, "c2", java.sql.Types.TIMESTAMP, 0, 0); // with Datetime
            fileRecord.addColumnMetadata(3, "c3", java.sql.Types.TIMESTAMP, 0, 7); // with Datetime2
            fileRecord.addColumnMetadata(4, "c4", java.sql.Types.TIME, 0, 7); // with time
            fileRecord.addColumnMetadata(5, "c5", microsoft.sql.Types.DATETIMEOFFSET, 0, 7); // with datetimeoffset
            fileRecord.addColumnMetadata(6, "c6", java.sql.Types.TIMESTAMP, 0, 0); // with SmallDatetime
            fileRecord.addColumnMetadata(7, "c7", java.sql.Types.DECIMAL, 19, 4); // with money
            fileRecord.addColumnMetadata(8, "c8", java.sql.Types.DECIMAL, 10, 4); // with smallmoney

            bulkCopy.setDestinationTableName(destTableName);
            bulkCopy.writeToServer(fileRecord);

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + destTableName);
                    SQLServerBulkCopy bcOperation = new SQLServerBulkCopy(conn);) {
                bcOperation.setDestinationTableName(destTableName2);
                bcOperation.writeToServer(rs);
            }
            
            validateValuesFromCSV(stmt, destTableName2, inputFile);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    static void validateValuesFromCSV(Statement stmt, String destinationTable, String inputFile) {
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(new FileInputStream(filePath + inputFile), encoding));
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + destinationTable)) {
            br.readLine(); // skip first line as it is header

            ResultSetMetaData destMeta = rs.getMetaData();
            int totalColumns = destMeta.getColumnCount();
            while (rs.next()) {
                String[] srcValues = br.readLine().split(delimiter);
                if ((0 == srcValues.length) && (srcValues.length != totalColumns)) {
                    srcValues = new String[totalColumns];
                    Arrays.fill(srcValues, null);
                }
                for (int i = 1; i <= totalColumns; i++) {
                    String srcValue = srcValues[i - 1];
                    String dstValue = rs.getString(i);
                    srcValue = (null != srcValue) ? srcValue.trim() : srcValue;
                    dstValue = (null != dstValue) ? dstValue.trim() : dstValue;
                    // get the value from csv as string and compare them
                    ComparisonUtil.compareExpectedAndActual(java.sql.Types.VARCHAR, srcValue, dstValue);
                }
            }
        } catch (Exception e) {
            fail("CSV validation failed with " + e.getMessage());
        }
    }

    private void beforeEachSetup() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            TestUtils.dropTableIfExists(destTableName, stmt);
            TestUtils.dropTableIfExists(destTableName2, stmt);

            String table = "create table " + destTableName
                    + " (c1 date, c2 datetime, c3 datetime2, c4 time, c5 datetimeoffset, c6 smalldatetime, c7 money, c8 smallmoney)";
            stmt.execute(table);
            table = "create table " + destTableName2
                    + " (c1 date, c2 datetime, c3 datetime2, c4 time, c5 datetimeoffset, c6 smalldatetime, c7 money, c8 smallmoney)";
            stmt.execute(table);
        }
    }

    @BeforeAll
    public static void setupTest() throws SQLException {
        filePath = TestUtils.getCurrentClassPath();
    }

    @AfterAll
    public static void cleanTest() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            TestUtils.dropTableIfExists(destTableName, stmt);
            TestUtils.dropTableIfExists(destTableName2, stmt);
        }
    }
}
