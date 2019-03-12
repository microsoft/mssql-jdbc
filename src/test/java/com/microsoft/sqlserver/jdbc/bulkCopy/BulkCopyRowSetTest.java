package com.microsoft.sqlserver.jdbc.bulkCopy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.RowSetMetaData;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opentest4j.TestAbortedException;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;

public class BulkCopyRowSetTest extends AbstractTest {
    
    private static String tableName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("BulkCopyFloatTest"));
    
    @Test
    public void testBulkCopyFloatRowSet() throws SQLException {
        try (Connection con = DriverManager.getConnection(connectionString)) {
            RowSetFactory rsf = RowSetProvider.newFactory();
            CachedRowSet crs = rsf.createCachedRowSet();
            RowSetMetaData rsmd = new RowSetMetaDataImpl();
            rsmd.setColumnCount(1);
            rsmd.setColumnName(1, "c1");
            rsmd.setColumnType(1, java.sql.Types.FLOAT);
            
            crs.setMetaData(rsmd);
            crs.setFloat(1, 123.45f);
            
            try (SQLServerBulkCopy bcOperation = new SQLServerBulkCopy(con)) {
                bcOperation.setDestinationTableName(tableName);
                bcOperation.writeToServer(crs);
            }
        }
    }
    
    @BeforeEach
    public void testSetup() throws TestAbortedException, Exception {
        try (Connection connection = DriverManager
                .getConnection(connectionString)) {
            try (Statement stmt = (SQLServerStatement) connection.createStatement()) {
                TestUtils.dropTableIfExists(tableName, stmt);
                String sql1 = "create table " + tableName + " (c1 float)";
                stmt.execute(sql1);
            }
        }
    }

    @AfterAll
    public static void terminateVariation() throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionString)) {
            try (Statement stmt = (SQLServerStatement) connection.createStatement()) {
                TestUtils.dropTableIfExists(tableName, stmt);
            }
        }
    }
}
