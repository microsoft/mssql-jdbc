package com.microsoft.sqlserver.jdbc.datatypes;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import microsoft.sql.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;


/*
 * This test is for testing the serialisation of String as microsoft.sql.Types.GUID
 */
@RunWith(JUnitPlatform.class)
public class GuidTest extends AbstractTest {

    enum TestType {
        SETOBJECT_WITHTYPE, // This is to test conversions with type
        SETOBJECT_WITHOUTTYPE, // This is to test conversions without type
        SETNULL // This is to test setNull method
    }

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

            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + escapedTableName
                    + " VALUES(?) SELECT * FROM " + escapedTableName + " where id = ?")) {

                UUID uuid = UUID.randomUUID();
                String uuidString = uuid.toString();

                int row = 1;

                // Test setObject method with SQL TYPE parameter
                testSetObject(uuidString, row++, pstmt, TestType.SETOBJECT_WITHTYPE);
                testSetObject(uuid, row++, pstmt, TestType.SETOBJECT_WITHTYPE);

                // Test setObject method without SQL TYPE parameter
                testSetObject(uuidString, row++, pstmt, TestType.SETOBJECT_WITHOUTTYPE);
                testSetObject(uuid, row++, pstmt, TestType.SETOBJECT_WITHOUTTYPE);

                // Test setNull
                testSetObject(uuid, row, pstmt, TestType.SETNULL);

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

    private void testSetObject(Object obj, int id, PreparedStatement pstmt,
                               GuidTest.TestType testType) throws SQLException {
        if (TestType.SETOBJECT_WITHTYPE == testType) {
            pstmt.setObject(1, obj, Types.GUID);
        } else if (GuidTest.TestType.SETOBJECT_WITHOUTTYPE == testType) {
            pstmt.setObject(1, obj);
        } else if (GuidTest.TestType.SETNULL == testType) {
            pstmt.setNull(1, Types.GUID);
        } else
            return;

        // The id column
        pstmt.setObject(2, id);

        pstmt.execute();
        pstmt.getMoreResults();
        try (SQLServerResultSet rs = (SQLServerResultSet) pstmt.getResultSet()) {
            rs.next();

            if (TestType.SETNULL == testType) {
                String s = rs.getUniqueIdentifier(1);
                assertNull(s);
                assertTrue(rs.wasNull());
            } else {
                UUID expected = obj instanceof UUID ? (UUID) obj : UUID.fromString(obj.toString());
                assertEquals(expected, UUID.fromString(rs.getUniqueIdentifier(1)));
                assertEquals(expected, UUID.fromString(rs.getObject(1, String.class)));
                assertEquals(expected, rs.getObject(1, UUID.class));
            }
        }
    }

}
