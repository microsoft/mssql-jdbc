/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.AlwaysEncrypted;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.opentest4j.TestAbortedException;

import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerStatementColumnEncryptionSetting;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.Utils;

/**
 * Tests Decryption and encryption of values
 *
 */
@RunWith(JUnitPlatform.class)
public class JDBCEncryptionDecryptionTest extends AESetup {
    private static SQLServerPreparedStatement pstmt = null;
    String[] values = {"10"};

    /**
     * Test encryption and decryption of numeric values
     * 
     * @throws Exception
     * @throws TestAbortedException
     */
    @Test
    @DisplayName("test numeric values")
    public void testNumeric() throws TestAbortedException, Exception {
        assumeTrue(13 <= new DBConnection(connectionString).getServerVersion(),
                "Aborting test case as SQL Server version is not compatible with Always encrypted ");

        try {
            createCEK(storeProvider, certStore);
            createNumericTable();
            populateNumeric(values);
            verifyResults();
        }
        finally {
            Utils.dropTableIfExists(numericTable, stmt);
        }
    }

    /**
     * Dropping all CMKs and CEKs and any open resources.
     * 
     * @throws SQLServerException
     * @throws SQLException
     */
    @AfterAll
    static void dropAll() throws SQLServerException, SQLException {
        Utils.dropTableIfExists(numericTable, stmt);
        dropCEK();
        dropCMK();
        stmt.close();
        con.close();
    }

    /**
     * Populating the table
     * 
     * @param values
     * @throws SQLException
     */
    private void populateNumeric(String[] values) throws SQLException {
        String sql = "insert into " + numericTable + " values( " + "?,?,?" + ")";

        pstmt = (SQLServerPreparedStatement) con.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                connection.getHoldability(), SQLServerStatementColumnEncryptionSetting.Enabled);

        for (int i = 1; i <= 3; i++) {
            pstmt.setShort(i, Short.valueOf(values[0]));
        }
        pstmt.execute();
        if (null != pstmt) {
            pstmt.close();
        }
    }

    /**
     * Verify the decryption and encryption of values
     * 
     * @throws NumberFormatException
     * @throws SQLException
     */
    private void verifyResults() throws NumberFormatException, SQLException {
        String sql = "select * from " + numericTable;
        pstmt = (SQLServerPreparedStatement) connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                connection.getHoldability(), SQLServerStatementColumnEncryptionSetting.Enabled);
        ResultSet rs = null;

        rs = pstmt.executeQuery();

        while (rs.next()) {
            assertEquals(Short.valueOf(values[0]), rs.getObject(1));
            assertEquals(Short.valueOf(values[0]), rs.getObject(2));
            assertEquals(Short.valueOf(values[0]), rs.getObject(3));
        }

        if (null != rs) {
            rs.close();
        }
        if (null != pstmt) {
            pstmt.close();
        }
    }

}
