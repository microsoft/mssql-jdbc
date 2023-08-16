package com.microsoft.sqlserver.jdbc.bulkCopy;

import com.microsoft.sqlserver.jdbc.*;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import javax.sql.RowSetMetaData;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


@RunWith(JUnitPlatform.class)
public class BulkCopyTimestampTest extends AbstractTest {

    public static final int COLUMN_COUNT = 16;
    public static final int ROW_COUNT = 10000;
    private static final String tableName =
            AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("bulkCopyTimestampTest"));

    @Test
    public void testBulkCopyTimestamp() throws SQLException {
        List<Timestamp> timeStamps = new ArrayList<>();
        try (Connection con = getConnection(); Statement stmt = connection.createStatement()) {
            RowSetFactory rsf = RowSetProvider.newFactory();
            CachedRowSet crs = rsf.createCachedRowSet();
            RowSetMetaData rsmd = new RowSetMetaDataImpl();
            rsmd.setColumnCount(COLUMN_COUNT);

            for (int i = 1; i <= COLUMN_COUNT; i++) {
                rsmd.setColumnName(i, String.format("c%d", i));
                rsmd.setColumnType(i, Types.TIMESTAMP);
            }

            crs.setMetaData(rsmd);


            for (int i = 0; i < COLUMN_COUNT; i++) {
                timeStamps.add(RandomData.generateDatetime(false));
            }

            for (int ri = 0; ri < ROW_COUNT; ri++) {
                crs.moveToInsertRow();

                for (int i = 1; i <= COLUMN_COUNT; i++) {
                    crs.updateTimestamp(i, timeStamps.get(i - 1));

                }

                crs.insertRow();
            }

            crs.moveToCurrentRow();

            try (SQLServerBulkCopy bcOperation = new SQLServerBulkCopy(con)) {
                SQLServerBulkCopyOptions bcOptions = new SQLServerBulkCopyOptions();
                bcOptions.setBatchSize(5000);
                bcOperation.setDestinationTableName(tableName);
                bcOperation.setBulkCopyOptions(bcOptions);
                bcOperation.writeToServer(crs);
            }

            try (ResultSet rs = stmt.executeQuery("select * from " + tableName)) {
                assertTrue(rs.next());

                for (int i = 1; i <= COLUMN_COUNT; i++) {
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

    @BeforeAll
    public static void testSetup() throws Exception {
        setConnection();

        try (Statement stmt = connection.createStatement()) {
            String colSpec = IntStream.range(1, COLUMN_COUNT + 1).mapToObj(x -> String.format("c%d datetime", x)).collect(Collectors.joining(","));
            String sql1 = String.format("create table %s (%s)", tableName, colSpec);
            stmt.execute(sql1);
        }
    }

    @AfterAll
    public static void terminateVariation() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
        }
    }
}
