package com.microsoft.sqlserver.jdbc.bulkCopy;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.RowSetMetaData;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.opentest4j.TestAbortedException;

import com.microsoft.sqlserver.jdbc.RandomData;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
public class BulkCopyRowSetTest extends AbstractTest {

    private static String tableName = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("BulkCopyFloatTest"));
    private static String tableName2 = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("BulkCopyFloatTest2"));

    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testBulkCopyFloatRowSet() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = connection.createStatement()) {
            RowSetFactory rsf = RowSetProvider.newFactory();
            CachedRowSet crs = rsf.createCachedRowSet();
            RowSetMetaData rsmd = new RowSetMetaDataImpl();
            rsmd.setColumnCount(2);
            rsmd.setColumnName(1, "c1");
            rsmd.setColumnName(2, "c2");
            rsmd.setColumnType(1, java.sql.Types.FLOAT);
            rsmd.setColumnType(2, java.sql.Types.FLOAT);

            Float floatData = RandomData.generateReal(false);

            crs.setMetaData(rsmd);
            crs.moveToInsertRow();
            crs.updateFloat(1, floatData);
            crs.updateFloat(2, floatData);
            crs.insertRow();
            crs.moveToCurrentRow();

            try (SQLServerBulkCopy bcOperation = new SQLServerBulkCopy(con)) {
                bcOperation.setDestinationTableName(tableName);
                bcOperation.writeToServer(crs);
            }

            try (ResultSet rs = stmt.executeQuery("select * from " + tableName)) {
                rs.next();
                assertEquals(floatData, (Float) rs.getFloat(1));
                assertEquals(floatData, (Float) rs.getFloat(2));
            }
        }
    }

    @Test
    public void testBulkCopyJapaneseCollation() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = connection.createStatement();
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);) {
            RowSetFactory rsf = RowSetProvider.newFactory();
            CachedRowSet crs = rsf.createCachedRowSet();
            RowSetMetaData rsmd = new RowSetMetaDataImpl();
            String unicodeData = "ああ";
            rsmd.setColumnCount(1);
            rsmd.setColumnName(1, "c1");
            rsmd.setColumnType(1, java.sql.Types.VARCHAR);
            rsmd.setTableName(1, tableName2);

            crs.setMetaData(rsmd);
            crs.moveToInsertRow();
            crs.updateString("c1", unicodeData);
            crs.insertRow();
            crs.moveToCurrentRow();

            bulkCopy.setDestinationTableName(tableName2);
            bulkCopy.writeToServer(crs);

            try (ResultSet rs = stmt.executeQuery("select * from " + tableName2)) {
                rs.next();
                assertEquals(unicodeData, (String) rs.getString(1));
            }
        }
    }

    @BeforeAll
    public static void testSetup() throws TestAbortedException, Exception {
        setConnection();

        try (Statement stmt = connection.createStatement()) {
            String sql1 = "create table " + tableName + " (c1 float, c2 real)";
            stmt.execute(sql1);
            String sql2 = "create table " + tableName2 + " (c1 varchar(10) COLLATE Japanese_CS_AS_KS_WS NOT NULL)";
            stmt.execute(sql2);
        }
    }

    @AfterAll
    public static void terminateVariation() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            TestUtils.dropTableIfExists(tableName2, stmt);
        }
    }
}
