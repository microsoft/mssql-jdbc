package com.microsoft.sqlserver.jdbc.preparedStatement;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.RandomData;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.PrepUtil;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.testframework.AbstractTest;


public class BatchExecutionWithBulkCopyWithoutUnicode extends AbstractTest {

    static String tableName = RandomUtil.getIdentifier("BulkCopyParseTest");

    @Test
    public void testSetStringWithoutUnicode() throws Exception {
        String valid = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName)
                + " (c1, c2, c3, c4) values " + "(" + "?, " + "?, " + "?, " + "? " + ")";

        try (Connection connection = PrepUtil.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=true;sendStringParametersAsUnicode=false;");
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(valid);
                Statement stmt = (SQLServerStatement) connection.createStatement();) {
            Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
            f1.setAccessible(true);
            f1.set(connection, true);

            String randomChar = RandomData.generateCharTypes("1", false, false);
            String randomString = RandomData.generateCharTypes("6", false, false);
            pstmt.clearBatch();
            pstmt.setString(1, randomChar); // char
            pstmt.setString(2, randomChar); // nchar
            pstmt.setString(3, randomString); // nvarchar(20)
            pstmt.setString(4, randomString); // varchar(20)
            pstmt.addBatch();

            pstmt.executeBatch();

            try (ResultSet rs = stmt.executeQuery(
                    "select c1, c2, c3, c4 from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {

                Object[] expected = new Object[4];

                expected[0] = randomChar;
                expected[1] = randomChar;
                expected[2] = randomString;
                expected[3] = randomString;
                rs.next();

                for (int i = 0; i < expected.length; i++) {
                    if (null != rs.getObject(i + 1)) {
                        assertEquals(expected[i].toString(), rs.getObject(i + 1).toString());
                    }
                }
            }
        }
    }

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @BeforeEach
    public void testSetup() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            String sql1 = "create table " + AbstractSQLGenerator.escapeIdentifier(tableName) + " " 
                    + "(" 
                    + "c1 char, " + "c2 nchar, " 
                    + "c3 nvarchar(20), " + "c4 varchar(20) " 
                    + ")";

            stmt.execute(sql1);
        }
    }

    @AfterAll
    public static void terminateVariation() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
        }
    }
}
