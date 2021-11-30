package com.microsoft.sqlserver.jdbc.datatypes;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
public class SparseTest extends AbstractTest {
    final static String tableName = RandomUtil.getIdentifier("SparseTestTable");
    final static String escapedTableName = AbstractSQLGenerator.escapeIdentifier(tableName);

    @BeforeAll
    public static void setupTests() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"trustServerCertificate", "true");
        setConnection();
    }

    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testSparse() throws Exception {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {

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
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(escapedTableName, stmt);
            }
        }
    }
}
