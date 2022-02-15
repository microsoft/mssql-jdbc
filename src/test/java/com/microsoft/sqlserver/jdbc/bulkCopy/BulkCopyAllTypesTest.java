/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ComparisonUtil;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.DBStatement;
import com.microsoft.sqlserver.testframework.DBTable;
import com.microsoft.sqlserver.testframework.PrepUtil;


@RunWith(JUnitPlatform.class)
public class BulkCopyAllTypesTest extends AbstractTest {

    private static DBTable tableSrc = null;
    private static DBTable tableDest = null;

    @BeforeAll
    public static void setupTests() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"trustServerCertificate", "true");
        setConnection();
    }

    /**
     * Test TVP with result set
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testTVPResultSet() throws SQLException {
        if (isSqlAzureDW()) {
            // TODO : Fix this test to run with Azure DW
            testBulkCopyResultSet(false, null, null);
            testBulkCopyResultSet(false, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        } else {
            testBulkCopyResultSet(false, null, null);
            testBulkCopyResultSet(true, null, null);
            testBulkCopyResultSet(false, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            testBulkCopyResultSet(false, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
            testBulkCopyResultSet(false, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            testBulkCopyResultSet(false, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        }
    }

    private void testBulkCopyResultSet(boolean setSelectMethod, Integer resultSetType,
            Integer resultSetConcurrency) throws SQLException {
        setupVariation();

        try (Connection connnection = PrepUtil
                .getConnection(connectionString + (setSelectMethod ? ";selectMethod=cursor;" : ""));
                Statement statement = (null != resultSetType || null != resultSetConcurrency) ? connnection
                        .createStatement(resultSetType, resultSetConcurrency) : connnection.createStatement();
                ResultSet rs = statement.executeQuery("select * from " + tableSrc.getEscapedTableName())) {

            SQLServerBulkCopy bcOperation = new SQLServerBulkCopy(connection);
            bcOperation.setDestinationTableName(tableDest.getEscapedTableName());
            bcOperation.writeToServer(rs);
            bcOperation.close();

            ComparisonUtil.compareSrcTableAndDestTableIgnoreRowOrder(new DBConnection(connection), tableSrc, tableDest);
        } finally {
            terminateVariation();
        }
    }

    private void setupVariation() throws SQLException {
        try (DBConnection dbConnection = new DBConnection(connectionString);
                DBStatement dbStmt = dbConnection.createStatement()) {

            tableSrc = new DBTable(true);
            tableDest = tableSrc.cloneSchema();

            dbStmt.createTable(tableSrc);
            dbStmt.createTable(tableDest);

            dbStmt.populateTable(tableSrc);
        }
    }

    private void terminateVariation() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(tableSrc.getEscapedTableName(), stmt);
            TestUtils.dropTableIfExists(tableDest.getEscapedTableName(), stmt);
        }
    }
}
