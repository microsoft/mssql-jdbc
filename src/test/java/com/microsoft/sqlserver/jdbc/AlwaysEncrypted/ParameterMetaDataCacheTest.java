/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.AlwaysEncrypted;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * Tests for caching parameter metadata in sp_describe_parameter_encryption calls
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.xSQLv11)
@Tag(Constants.xSQLv12)
@Tag(Constants.xSQLv14)
public class ParameterMetaDataCacheTest extends AESetup {

    @BeforeAll
    public static void setupTests() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString, "columnEncryptionSetting", "Enabled");
        setConnection();
    }

    /**
     * 
     * Tests caching of parameter metadata by running a query to be cached, another to replace parameter information,
     * then the first again to measure the difference in time between the two runs.
     * 
     * @throws Exception
     */
    @Test
    @Tag(Constants.xSQLv11)
    @Tag(Constants.xSQLv12)
    @Tag(Constants.xSQLv14)
    @Tag(Constants.reqExternalSetup)
    public void testParameterMetaDataCache() throws Exception {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] charValues = createCharValues(false);
            String[] numericValues = createNumericValues(false);
            TestUtils.dropTableIfExists(CHAR_TABLE_AE, stmt);
            TestUtils.dropTableIfExists(NUMERIC_TABLE_AE, stmt);
            createTable(CHAR_TABLE_AE, cekAkv, charTable);
            createTable(NUMERIC_TABLE_AE, cekAkv, numericTable);
            populateCharNormalCase(charValues);
            populateNumeric(numericValues);

            long firstRun = timedTestSelect(CHAR_TABLE_AE);
            timedTestSelect(NUMERIC_TABLE_AE);
            long secondRun = timedTestSelect(CHAR_TABLE_AE);

            // As long as there is a noticeable performance improvement, caching is working as intended. For now
            // the threshold measured is 5%.
            double threshold = 0.05;

            assertTrue(1 - (secondRun / firstRun) > threshold);
        }
    }

    /**
     * 
     * Tests that the enclave is retried when using secure enclaves (assuming the server supports this). This is done by
     * executing a query generating metadata in the cache, changing the column encryption type to make the metadata
     * stale, and running the query again. The query should fail, but retry and pass. Currently disabled as secure
     * enclaves are not supported.
     * 
     * @throws Exception
     */
    @Test
    @Tag(Constants.xSQLv11)
    @Tag(Constants.xSQLv12)
    @Tag(Constants.xSQLv14)
    @Tag(Constants.reqExternalSetup)
    public void testRetryWithSecureCache() throws Exception {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = createCharValues(false);
            TestUtils.dropTableIfExists(CHAR_TABLE_AE, stmt);
            createTable(CHAR_TABLE_AE, cekAkv, charTable);
            populateCharNormalCase(values);

            String sql = "select * from " + CHAR_TABLE_AE;
            ArrayList<String> results = new ArrayList<>();
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    results.add(rs.getString(1));
                }
                rs.close();
                testAlterColumnEncryption(stmt, CHAR_TABLE_AE, charTable, cekAkv);

                try (ResultSet rs2 = stmt.executeQuery(sql)) {
                    for (int i = 0; rs2.next(); i++) {
                        assertTrue(rs2.getString(1).equals(results.get(i)));
                    }
                    rs2.close();
                }
            }
            con.close();
        }
    }

    /**
     * Used to time how long data retrieval from the server takes. This is turn is used to confirm that metadata is
     * caching correctly.
     * 
     * @param tblName
     *        the table to select data from
     * @return the time in milliseconds, as a long
     * @throws SQLException
     */
    private long timedTestSelect(String tblName) throws SQLException {
        long timer = System.currentTimeMillis();
        String sql = "select * from " + tblName;
        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(sql)) {
            pstmt.execute();
            pstmt.close();
        }
        return System.currentTimeMillis() - timer;
    }
}
