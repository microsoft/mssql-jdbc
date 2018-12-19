package com.microsoft.sqlserver.jdbc.datatypes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

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
            assumeTrue(!isSqlAzureDW(), TestResource.getResource("R_skipAzure"));
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
                    assertEquals("yo", rs.getString("col1023"));
                    assertEquals(1, rs.getInt("col1"));
                    assertEquals(0x45, rs.getBytes("col2")[0]);
                }
            } finally {
                try (Statement stmt = conn.createStatement()) {
                    TestUtils.dropTableIfExists(escapedTableName, stmt);
                }
            }
        }
    }
}
