package com.microsoft.sqlserver.jdbc.datatypes;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import microsoft.sql.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.UUID;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.microsoft.sqlserver.testframework.Constants;


/*
 * This test is for testing the serialisation of String as microsoft.sql.Types.GUID
 */
@RunWith(JUnitPlatform.class)
public class GuidTest extends AbstractTest {

    final static String tableName = RandomUtil.getIdentifier("GuidTestTable");
    final static String escapedTableName = AbstractSQLGenerator.escapeIdentifier(tableName);

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    /*
     * Test UUID conversions
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testGuid() throws Exception {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {

            // Create the test table
            TestUtils.dropTableIfExists(escapedTableName, stmt);

            String query = "create table " + escapedTableName + " (uuid uniqueidentifier, id int IDENTITY primary key)";
            stmt.executeUpdate(query);

            UUID uuid = UUID.randomUUID();
            String uuidString = uuid.toString();
            int id = 1;

            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + escapedTableName
                    + " VALUES(?) SELECT * FROM " + escapedTableName + " where id = ?")) {

                pstmt.setObject(1, uuidString, Types.GUID);
                pstmt.setObject(2, id++);
                pstmt.execute();
                pstmt.getMoreResults();
                try (SQLServerResultSet rs = (SQLServerResultSet) pstmt.getResultSet()) {
                    rs.next();
                    assertEquals(uuid, UUID.fromString(rs.getUniqueIdentifier(1)));
                }

                // Test NULL GUID
                pstmt.setObject(1, null, Types.GUID);
                pstmt.setObject(2, id++);
                pstmt.execute();
                pstmt.getMoreResults();
                try (SQLServerResultSet rs = (SQLServerResultSet) pstmt.getResultSet()) {
                    rs.next();
                    String s = rs.getUniqueIdentifier(1);
                    assertNull(s);
                    assertTrue(rs.wasNull());
                }

                // Test Illegal GUID
                try {
                    pstmt.setObject(1, "garbage", Types.GUID);
                    fail(TestResource.getResource("R_expectedFailPassed"));
                } catch (IllegalArgumentException e) {
                    assertEquals("Invalid UUID string: garbage", e.getMessage());
                }
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(escapedTableName, stmt);
            }
        }
    }

}
