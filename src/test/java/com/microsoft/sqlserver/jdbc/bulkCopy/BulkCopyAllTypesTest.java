/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ComparisonUtil;
import com.microsoft.sqlserver.jdbc.RandomData;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.DBStatement;
import com.microsoft.sqlserver.testframework.DBTable;
import com.microsoft.sqlserver.testframework.PrepUtil;

import javax.sql.RowSetMetaData;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


@RunWith(JUnitPlatform.class)
public class BulkCopyAllTypesTest extends AbstractTest {

    private static DBTable tableSrc = null;
    private static DBTable tableDest = null;

    @BeforeAll
    public static void setupTests() throws Exception {
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
            TestUtils.dropTableIfExists(dateTimeTestTable, stmt);
        }
    }

    private static final int DATETIME_COL_COUNT = 2;
    private static final int DATETIME_ROW_COUNT = 1;
    private static final String dateTimeTestTable = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("bulkCopyTimestampTest"));

    @Test
    public void testBulkCopyTimestamp() throws SQLException {
        List<Timestamp> timeStamps = new ArrayList<>();
        try (Connection con = getConnection(); Statement stmt = connection.createStatement()) {
            String colSpec = IntStream.range(1, DATETIME_COL_COUNT + 1).mapToObj(x -> String.format("c%d datetime", x))
                    .collect(Collectors.joining(","));
            String sql1 = String.format("create table %s (%s)", dateTimeTestTable, colSpec);
            stmt.execute(sql1);

            RowSetFactory rsf = RowSetProvider.newFactory();
            CachedRowSet crs = rsf.createCachedRowSet();
            RowSetMetaData rsmd = new RowSetMetaDataImpl();
            rsmd.setColumnCount(DATETIME_COL_COUNT);

            for (int i = 1; i <= DATETIME_COL_COUNT; i++) {
                rsmd.setColumnName(i, String.format("c%d", i));
                rsmd.setColumnType(i, Types.TIMESTAMP);
            }
            crs.setMetaData(rsmd);

            for (int i = 0; i < DATETIME_COL_COUNT; i++) {
                timeStamps.add(RandomData.generateDatetime(false));
            }

            for (int ri = 0; ri < DATETIME_ROW_COUNT; ri++) {
                crs.moveToInsertRow();

                for (int i = 1; i <= DATETIME_COL_COUNT; i++) {
                    crs.updateTimestamp(i, timeStamps.get(i - 1));
                }
                crs.insertRow();
            }
            crs.moveToCurrentRow();

            try (SQLServerBulkCopy bcOperation = new SQLServerBulkCopy(con)) {
                SQLServerBulkCopyOptions bcOptions = new SQLServerBulkCopyOptions();
                bcOptions.setBatchSize(5000);
                bcOperation.setDestinationTableName(dateTimeTestTable);
                bcOperation.setBulkCopyOptions(bcOptions);
                bcOperation.writeToServer(crs);
            }

            try (ResultSet rs = stmt.executeQuery("select * from " + dateTimeTestTable)) {
                assertTrue(rs.next());

                for (int i = 1; i <= DATETIME_COL_COUNT; i++) {
                    long expectedTimestamp = getTime(timeStamps.get(i - 1));
                    long actualTimestamp = getTime(rs.getTimestamp(i));

                    assertEquals(expectedTimestamp, actualTimestamp);
                }
            }
        }
    }

    private static long getTime(Timestamp time) {
        return (3 * time.getTime() + 5) / 10;
    }
}
