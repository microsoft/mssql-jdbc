/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.AlwaysEncrypted;

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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ComparisonUtil;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCSVFileRecord;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.Constants;
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
@Tag(Constants.xSQLv11)
@Tag(Constants.xSQLv12)
@Tag(Constants.xAzureSQLDB)
@Tag(Constants.xAzureSQLDW)
@Tag(Constants.reqExternalSetup)
@Tag(Constants.requireSecret)
public class BulkCopySendTemporalDataTypesAsStringAETest extends AESetup {
    static String inputFile = "BulkCopyCSVSendTemporalDataTypesAsStringForBulkCopy.csv";
    static String encoding = "UTF-8";
    static String delimiter = ",";
    static String filePath = null;

    static String destTableNameAE = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("sendTemporalDataTypesAsStringForBulkCopyDestTableAE"));

    /**
     * Test basic case with sendTemporalDataTypesAsStringForBulkCopy connection property, with AE enabled.
     * 
     * @throws SQLException
     */
    @Test
    public void testSendTemporalDataTypesAsStringForBulkCopyAE() throws SQLException {
        beforeEachSetupAE();
        try (Connection conn = PrepUtil
                .getConnection(AETestConnectionString + ";sendTemporalDataTypesAsStringForBulkCopy=false", AEInfo)) {
            SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(filePath + inputFile, encoding,
                    delimiter, true);

            testBulkCopyCSV(conn, fileRecord, destTableNameAE);
        }
    }

    private void testBulkCopyCSV(Connection conn, SQLServerBulkCSVFileRecord fileRecord, String tableName) {
        try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn); Statement stmt = conn.createStatement()) {

            fileRecord.addColumnMetadata(1, "id", java.sql.Types.INTEGER, 0, 0);
            fileRecord.addColumnMetadata(2, "c1", java.sql.Types.DATE, 0, 0); // with Date
            fileRecord.addColumnMetadata(3, "c2", java.sql.Types.TIMESTAMP, 0, 0); // with Datetime
            fileRecord.addColumnMetadata(4, "c3", java.sql.Types.TIMESTAMP, 0, 7); // with Datetime2
            fileRecord.addColumnMetadata(5, "c4", java.sql.Types.TIME, 0, 7); // with time
            fileRecord.addColumnMetadata(6, "c5", microsoft.sql.Types.DATETIMEOFFSET, 0, 7); // with datetimeoffset
            fileRecord.addColumnMetadata(7, "c6", java.sql.Types.TIMESTAMP, 0, 0); // with SmallDatetime
            fileRecord.addColumnMetadata(8, "c7", java.sql.Types.DECIMAL, 19, 4); // with money
            fileRecord.addColumnMetadata(9, "c8", java.sql.Types.DECIMAL, 10, 4); // with smallmoney

            bulkCopy.setDestinationTableName(tableName);
            bulkCopy.writeToServer(fileRecord);

            validateValuesFromCSV(stmt, tableName, inputFile);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    static void validateValuesFromCSV(Statement stmt, String destinationTable, String inputFile) {
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(new FileInputStream(filePath + inputFile), encoding));
                ResultSet rs = stmt.executeQuery("select * FROM " + destinationTable + " order by id")) {
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

    private void beforeEachSetupAE() throws SQLException {
        try (Connection con = PrepUtil
                .getConnection(AETestConnectionString + ";sendTemporalDataTypesAsStringForBulkCopy=false", AEInfo);
                Statement stmt = con.createStatement()) {
            TestUtils.dropTableIfExists(destTableNameAE, stmt);
            String table = "create table " + destTableNameAE + " (id int, "
                    + "c1 date ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                    + cekJks + ") NULL,"
                    + "c2 datetime ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                    + cekJks + ") NULL,"
                    + "c3 datetime2 ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                    + cekJks + ") NULL,"
                    + "c4 time ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                    + cekJks + ") NULL,"
                    + "c5 datetimeoffset ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                    + cekJks + ") NULL,"
                    + "c6 smalldatetime ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                    + cekJks + ") NULL,"
                    + "c7 money ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                    + cekJks + ") NULL,"
                    + "c8 smallmoney ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                    + cekJks + ") NULL," + ");";
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
            TestUtils.dropTableIfExists(destTableNameAE, stmt);
        }
    }
}
