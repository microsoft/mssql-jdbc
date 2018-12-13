package com.microsoft.sqlserver.jdbc.datatypes;

import java.sql.*;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.Assert.fail;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;


@RunWith(JUnitPlatform.class)
public class SparseTest extends AbstractTest {
    final static String tableName = RandomUtil.getIdentifier("SparseTestTable");
    final static String escapedTableName = AbstractSQLGenerator.escapeIdentifier(tableName);

    @Test
    public void testSparse() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            assumeTrue(!TestUtils.isSqlAzureDW(conn), TestResource.getResource("R_skipAzure"));
            try (Statement stmt = conn.createStatement()) {

                // Create the test table
                TestUtils.dropTableIfExists(escapedTableName, stmt);

                StringBuilder bd = new StringBuilder();
                bd.append("create table " + escapedTableName + " (col1 int, col2 varbinary(max)");
                for (int i = 3; i <= 1024; i++) {
                    bd.append(", col" + i + " varchar(20) SPARSE NULL");
                }
                bd.append(")");
                String query = bd.toString();

                stmt.executeUpdate(query);

                stmt.executeUpdate("insert into " + escapedTableName + " (col1, col2, col1023)values(1, 0x45, 'yo')");

                try (ResultSet rs = stmt.executeQuery("Select * from   " + escapedTableName)) {
                    rs.next();
                    assertEquals(rs.getString("col1023"), "yo", "Wrong value returned");
                }
            } finally {
                try (Statement stmt = conn.createStatement()) {
                    TestUtils.dropTableIfExists(escapedTableName, stmt);
                } catch (Exception e) {
                    fail(TestResource.getResource("R_createDropTableFailed") + e.toString());
                }
            }
        }
    }
}
