package com.microsoft.sqlserver.jdbc.datatypes;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;


/*
 * This test is for testing the setObject methods for the data type mappings in JDBC for java.math.BigInteger
 */
@RunWith(JUnitPlatform.class)
public class BigIntegerTest extends AbstractTest {

    enum TestType {
        SETOBJECT_WITHTYPE, // This is to test conversions with type
        SETOBJECT_WITHOUTTYPE, // This is to test conversions without type
        SETNULL // This is to test setNull method
    };

    final static String tableName = RandomUtil.getIdentifier("BigIntegerTestTable");
    final static String escapedTableName = AbstractSQLGenerator.escapeIdentifier(tableName);

    /*
     * Test BigInteger conversions
     */
    @Test
    public void testBigInteger() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            try (Statement stmt = conn.createStatement()) {

                // Create the test table
                TestUtils.dropTableIfExists(escapedTableName, stmt);

                String query = "create table " + escapedTableName
                        + " (col1 varchar(100), col2 bigint, col3 real, col4 float, "
                        + "col5 numeric(38,0), col6 int, col7 smallint, col8 char(100), col9 varchar(max), "
                        + "id int IDENTITY primary key)";
                stmt.executeUpdate(query);

                try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + escapedTableName
                        + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?) SELECT * FROM " + escapedTableName + " where id = ?")) {

                    /*
                     * test conversion of BigInteger values greater than LONG.MAX_VALUE and lesser than LONG.MIN_VALUE
                     */

                    // A random value that is bigger than LONG.MAX_VALUE
                    BigInteger bigIntPos = new BigInteger("922337203685477580776767676");
                    // A random value that is smaller than LONG.MIN_VALUE
                    BigInteger bigIntNeg = new BigInteger("-922337203685477580776767676");

                    // Test the setObject method for different types of BigInteger values
                    int row = 1;
                    testSetObject(escapedTableName, BigInteger.valueOf(Long.MAX_VALUE), row++, pstmt,
                            TestType.SETOBJECT_WITHTYPE);

                    testSetObject(escapedTableName, BigInteger.valueOf(Long.MIN_VALUE), row++, pstmt,
                            TestType.SETOBJECT_WITHTYPE);
                    testSetObject(escapedTableName, BigInteger.valueOf(10), row++, pstmt, TestType.SETOBJECT_WITHTYPE);
                    testSetObject(escapedTableName, BigInteger.valueOf(-10), row++, pstmt, TestType.SETOBJECT_WITHTYPE);
                    testSetObject(escapedTableName, BigInteger.ZERO, row++, pstmt, TestType.SETOBJECT_WITHTYPE);
                    testSetObject(escapedTableName, bigIntPos, row++, pstmt, TestType.SETOBJECT_WITHTYPE);
                    testSetObject(escapedTableName, bigIntNeg, row++, pstmt, TestType.SETOBJECT_WITHTYPE);

                    // Test setObject method with SQL TYPE parameter
                    testSetObject(escapedTableName, BigInteger.valueOf(Long.MAX_VALUE), row++, pstmt,
                            TestType.SETOBJECT_WITHOUTTYPE);
                    testSetObject(escapedTableName, BigInteger.valueOf(Long.MIN_VALUE), row++, pstmt,
                            TestType.SETOBJECT_WITHOUTTYPE);
                    testSetObject(escapedTableName, BigInteger.valueOf(1000), row++, pstmt,
                            TestType.SETOBJECT_WITHOUTTYPE);
                    testSetObject(escapedTableName, BigInteger.valueOf(-1000), row++, pstmt,
                            TestType.SETOBJECT_WITHOUTTYPE);
                    testSetObject(escapedTableName, BigInteger.ZERO, row++, pstmt, TestType.SETOBJECT_WITHOUTTYPE);
                    testSetObject(escapedTableName, bigIntPos, row++, pstmt, TestType.SETOBJECT_WITHOUTTYPE);
                    testSetObject(escapedTableName, bigIntNeg, row++, pstmt, TestType.SETOBJECT_WITHOUTTYPE);

                    // Test setNull
                    testSetObject(escapedTableName, bigIntNeg, row++, pstmt, TestType.SETNULL);
                }
            } finally {
                try (Statement stmt = conn.createStatement()) {
                    TestUtils.dropTableIfExists(escapedTableName, stmt);
                }
            }
        }
    }

    static void testSetObject(String tableName, BigInteger obj, int id, PreparedStatement pstmt,
            TestType testType) throws SQLException {
        if (TestType.SETOBJECT_WITHTYPE == testType) {
            callSetObjectWithType(obj, pstmt);
        } else if (TestType.SETOBJECT_WITHOUTTYPE == testType) {
            callSetObjectWithoutType(obj, pstmt);
        } else if (TestType.SETNULL == testType) {
            callSetNull(obj, pstmt);
        } else
            return;

        // The id column
        pstmt.setObject(10, id);

        pstmt.execute();
        pstmt.getMoreResults();
        try (ResultSet rs = pstmt.getResultSet()) {
            rs.next();

            if (TestType.SETNULL == testType) {
                for (int i = 1; 9 >= i; ++i) {
                    // Get the data first before calling rs.wasNull()
                    rs.getString(i);
                    assertEquals(true, rs.wasNull());
                }
                return;
            }

            if ((0 > obj.compareTo(BigInteger.valueOf(Long.MIN_VALUE)))
                    || (0 < obj.compareTo(BigInteger.valueOf(Long.MAX_VALUE)))) {
                /*
                 * For the BigInteger values greater/less than Long limits test only the long data type. This tests when
                 * the value is bigger/smaller than JDBC BIGINT
                 */
                assertEquals(Long.valueOf(obj.longValue()).toString(), rs.getString(1));
                assertEquals(obj.longValue(), rs.getLong(2));

                /*
                 * As CHAR is fixed length, rs.getString() returns a string of the size allocated in the database. Need
                 * to trim it for comparison.
                 */
                assertEquals(Long.valueOf(obj.longValue()).toString(), rs.getString(8).trim());
                assertEquals(Long.valueOf(obj.longValue()).toString(), rs.getString(9));
            } else {
                assertEquals(obj.toString(), rs.getString(1));
                assertEquals(obj.longValue(), rs.getLong(2));
                assertEquals(obj.floatValue(), rs.getFloat(3));
                assertEquals(obj.doubleValue(), rs.getDouble(4));
                assertEquals(obj.doubleValue(), rs.getDouble(5));

                if (obj.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) >= 0) {
                    assertEquals(Integer.MAX_VALUE, rs.getInt(6));
                } else if (obj.compareTo(BigInteger.valueOf(Integer.MIN_VALUE)) <= 0) {
                    assertEquals(Integer.MIN_VALUE, rs.getInt(6));
                } else {
                    assertEquals(obj.intValue(), rs.getInt(6));
                }

                if (obj.compareTo(BigInteger.valueOf(Short.MAX_VALUE)) >= 0) {
                    assertEquals(Short.MAX_VALUE, rs.getShort(7));
                } else if (obj.compareTo(BigInteger.valueOf(Short.MIN_VALUE)) <= 0) {
                    assertEquals(Short.MIN_VALUE, rs.getShort(7));
                } else {
                    assertEquals(obj.shortValue(), rs.getShort(7));
                }

                assertEquals(obj.toString(), rs.getString(8).trim());
                assertEquals(obj.toString(), rs.getString(9));
            }
        }
    }

    static void callSetObjectWithType(BigInteger obj, PreparedStatement pstmt) throws SQLException {
        pstmt.setObject(1, obj, java.sql.Types.VARCHAR);
        pstmt.setObject(2, obj, java.sql.Types.BIGINT);
        pstmt.setObject(3, obj, java.sql.Types.FLOAT);
        pstmt.setObject(4, obj, java.sql.Types.DOUBLE);
        pstmt.setObject(5, obj, java.sql.Types.NUMERIC);

        // Use Integer/Short limits instead of Long limits for the int/smallint column
        if (obj.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) >= 0) {
            pstmt.setObject(6, BigInteger.valueOf(Integer.MAX_VALUE), java.sql.Types.INTEGER);
        } else if (obj.compareTo(BigInteger.valueOf(Integer.MIN_VALUE)) <= 0) {
            pstmt.setObject(6, BigInteger.valueOf(Integer.MIN_VALUE), java.sql.Types.INTEGER);
        } else {
            pstmt.setObject(6, obj, java.sql.Types.INTEGER);
        }

        if (obj.compareTo(BigInteger.valueOf(Short.MAX_VALUE)) >= 0) {
            pstmt.setObject(7, BigInteger.valueOf(Short.MAX_VALUE), java.sql.Types.SMALLINT);
        } else if (obj.compareTo(BigInteger.valueOf(Short.MIN_VALUE)) <= 0) {
            pstmt.setObject(7, BigInteger.valueOf(Short.MIN_VALUE), java.sql.Types.SMALLINT);
        } else {
            pstmt.setObject(7, obj, java.sql.Types.SMALLINT);
        }
        pstmt.setObject(8, obj, java.sql.Types.CHAR);
        pstmt.setObject(9, obj, java.sql.Types.LONGVARCHAR);
    }

    static void callSetObjectWithoutType(BigInteger obj, PreparedStatement pstmt) throws SQLException {
        /*
         * Cannot send a long value to a column of type int/smallint (even if the long value is small enough to fit in
         * those types)
         */
        pstmt.setObject(1, obj);
        pstmt.setObject(2, obj);
        pstmt.setObject(3, obj);
        pstmt.setObject(4, obj);
        pstmt.setObject(5, obj);

        // Use Integer/Short limits instead of Long limits for the int/smallint column
        if (obj.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) >= 0) {
            pstmt.setObject(6, BigInteger.valueOf(Integer.MAX_VALUE));
        } else if (obj.compareTo(BigInteger.valueOf(Integer.MIN_VALUE)) <= 0) {
            pstmt.setObject(6, BigInteger.valueOf(Integer.MIN_VALUE));
        } else {
            pstmt.setObject(6, obj);
        }

        if (obj.compareTo(BigInteger.valueOf(Short.MAX_VALUE)) >= 0) {
            pstmt.setObject(7, BigInteger.valueOf(Short.MAX_VALUE));
        } else if (obj.compareTo(BigInteger.valueOf(Short.MIN_VALUE)) <= 0) {
            pstmt.setObject(7, BigInteger.valueOf(Short.MIN_VALUE));
        } else {
            pstmt.setObject(7, obj);
        }

        pstmt.setObject(8, obj);
        pstmt.setObject(9, obj);
    }

    static void callSetNull(BigInteger obj, PreparedStatement pstmt) throws SQLException {
        pstmt.setNull(1, java.sql.Types.VARCHAR);
        pstmt.setNull(2, java.sql.Types.BIGINT);
        pstmt.setNull(3, java.sql.Types.FLOAT);
        pstmt.setNull(4, java.sql.Types.DOUBLE);
        pstmt.setNull(5, java.sql.Types.NUMERIC);
        pstmt.setNull(6, java.sql.Types.INTEGER);
        pstmt.setNull(7, java.sql.Types.SMALLINT);
        pstmt.setNull(8, java.sql.Types.CHAR);
        pstmt.setNull(9, java.sql.Types.LONGVARCHAR);
    }
}
