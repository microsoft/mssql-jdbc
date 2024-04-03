package com.microsoft.sqlserver.jdbc.bulkCopy;

import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCSVFileRecord;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 *
 *
 */
@RunWith(JUnitPlatform.class)
public class BulkCopyMoneyTest extends AbstractTest {
    static String encoding = "UTF-8";
    static String delimiter = ",";
    static String destTableName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("moneyBulkCopyDest"));
    static String destTableName2 = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("moneyBulkCopyDest"));
    @Test
    public void testMoneyWithBulkCopy() throws SQLException {
        beforeEachSetup();

        try (Connection conn = PrepUtil.getConnection(connectionString)) {
            testMoneyLimits(-214799.3648, 922337203685387.5887, conn);   // SMALLMONEY MIN
            testMoneyLimits(214799.3698, 922337203685387.5887, conn);   // SMALLMONEY MAX
            testMoneyLimits(214719.3698, -922337203685497.5808, conn);   // MONEY MIN
            testMoneyLimits(214719.3698, 922337203685478.5807, conn);   // MONEY MAX
        }
    }

    private void testMoneyLimits(double smallMoneyVal, double moneyVal, Connection conn) throws SQLException {
        SQLServerBulkCSVFileRecord fileRecord = constructFileRecord(smallMoneyVal, moneyVal);

        try {
            testMoneyWithBulkCopy(conn, fileRecord);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_valueOutOfRange")),
                    e.getMessage());
        }
    }

    private SQLServerBulkCSVFileRecord constructFileRecord(double smallMoneyVal, double moneyVal) throws SQLException {
        Map<Object, Object> data = new HashMap();
        data.put(smallMoneyVal, moneyVal);

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("smallmoneycol, moneycol\n");

        for (Map.Entry entry : data.entrySet()) {
            stringBuilder.append(
                    String.format("%s,%s\n", entry.getKey(), entry.getValue()));
        }

        byte[] bytes = stringBuilder.toString().getBytes(StandardCharsets.UTF_8);
        SQLServerBulkCSVFileRecord fileRecord = null;
        try (InputStream inputStream = new ByteArrayInputStream(bytes)) {
            fileRecord = new SQLServerBulkCSVFileRecord(inputStream, encoding, delimiter, true);
        } catch (IOException e) {}
        return fileRecord;
    }

    private void testMoneyWithBulkCopy(Connection conn, SQLServerBulkCSVFileRecord fileRecord) throws SQLException {
        try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn); Statement stmt = conn.createStatement()) {

            fileRecord.addColumnMetadata(1, "c1", java.sql.Types.DECIMAL, 10, 4); // with smallmoney
            fileRecord.addColumnMetadata(2, "c2", java.sql.Types.DECIMAL, 19, 4); // with money


            bulkCopy.setDestinationTableName(destTableName);
            bulkCopy.writeToServer(fileRecord);

            try (ResultSet rs = stmt.executeQuery("select * FROM " + destTableName + " order by c1");
                    SQLServerBulkCopy bcOperation = new SQLServerBulkCopy(conn);) {
                bcOperation.setDestinationTableName(destTableName2);
                bcOperation.writeToServer(rs);
            }

        }
    }

    private void beforeEachSetup() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            TestUtils.dropTableIfExists(destTableName, stmt);
            TestUtils.dropTableIfExists(destTableName2, stmt);

            String table = "create table " + destTableName + " (c1 smallmoney, c2 money)";
            stmt.execute(table);
            table = "create table " + destTableName2 + " (c1 smallmoney, c2 money)";
            stmt.execute(table);
        }
    }
}
