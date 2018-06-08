package com.microsoft.sqlserver.jdbc.preparedStatement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.opentest4j.TestAbortedException;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.Utils;

@RunWith(JUnitPlatform.class)
public class BatchExecutionWithBulkCopyTest extends AbstractTest {

    static SQLServerPreparedStatement pstmt = null;
    static Statement stmt = null;
    static Connection connection = null;
    static String tableName = "BulkCopyParseTest" + System.currentTimeMillis();

    @Test
    public void testIsInsert() throws SQLException, NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        String valid1 = "INSERT INTO PeterTable values (1, 2)";
        String valid2 = " INSERT INTO PeterTable values (1, 2)";
        String valid3 = "/* asdf */ INSERT INTO PeterTable values (1, 2)";
        String invalid = "Select * from PEterTable";

        stmt = connection.createStatement();
        Method method = stmt.getClass().getDeclaredMethod("isInsert", String.class);
        method.setAccessible(true);
        assertTrue((boolean) method.invoke(stmt, valid1));
        assertTrue((boolean) method.invoke(stmt, valid2));
        assertTrue((boolean) method.invoke(stmt, valid3));
        assertFalse((boolean) method.invoke(stmt, invalid));
    }

    @Test
    public void testComments() throws SQLException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        pstmt = (SQLServerPreparedStatement) connection.prepareStatement("");

        String valid = "/* rando comment *//* rando comment */ INSERT /* rando comment */ INTO /* rando comment *//*rando comment*/ PeterTable /*rando comment */"
                + " /* rando comment */values/* rando comment */ (1, 2)";

        Field f1 = pstmt.getClass().getSuperclass().getDeclaredField("localUserSQL");
        f1.setAccessible(true);
        f1.set(pstmt, valid);

        Method method = pstmt.getClass().getSuperclass().getDeclaredMethod("parseUserSQLForTableNameDW", boolean.class, boolean.class);
        method.setAccessible(true);

        assertEquals((String) method.invoke(pstmt, false, false), "PeterTable");
    }

    @Test
    public void testBrackets() throws SQLException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        pstmt = (SQLServerPreparedStatement) connection.prepareStatement("");

        String valid = "/* rando comment *//* rando comment */ INSERT /* rando comment */ INTO /* rando comment *//*rando comment*/ [Peter[]]Table] /*rando comment */"
                + " /* rando comment */values/* rando comment */ (1, 2)";

        Field f1 = pstmt.getClass().getSuperclass().getDeclaredField("localUserSQL");
        f1.setAccessible(true);
        f1.set(pstmt, valid);

        Method method = pstmt.getClass().getSuperclass().getDeclaredMethod("parseUserSQLForTableNameDW", boolean.class, boolean.class);
        method.setAccessible(true);

        assertEquals((String) method.invoke(pstmt, false, false), "Peter[]Table");
    }

    @Test
    public void testDoubleQuotes() throws SQLException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        pstmt = (SQLServerPreparedStatement) connection.prepareStatement("");

        String valid = "/* rando comment *//* rando comment */ INSERT /* rando comment */ INTO /* rando comment *//*rando comment*/ \"Peter\"\"\"\"Table\" /*rando comment */"
                + " /* rando comment */values/* rando comment */ (1, 2)";

        Field f1 = pstmt.getClass().getSuperclass().getDeclaredField("localUserSQL");
        f1.setAccessible(true);
        f1.set(pstmt, valid);

        Method method = pstmt.getClass().getSuperclass().getDeclaredMethod("parseUserSQLForTableNameDW", boolean.class, boolean.class);
        method.setAccessible(true);

        assertEquals((String) method.invoke(pstmt, false, false), "Peter\"\"Table");
    }

    @Test
    public void testAll() throws SQLException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        pstmt = (SQLServerPreparedStatement) connection.prepareStatement("");

        String valid = "/* rando comment *//* rando comment */ INSERT /* rando comment */ INTO /* rando comment *//*rando comment*/ \"Peter\"\"\"\"Table\" /*rando comment */"
                + " /* rando comment */ (\"c1\"/* rando comment */, /* rando comment */[c2]/* rando comment */, /* rando comment */ /* rando comment */c3/* rando comment */, c4)"
                + "values/* rando comment */ (/* rando comment */1/* rando comment */, /* rando comment */2/* rando comment */ , '?', ?)/* rando comment */";

        Field f1 = pstmt.getClass().getSuperclass().getDeclaredField("localUserSQL");
        f1.setAccessible(true);
        f1.set(pstmt, valid);

        Method method = pstmt.getClass().getSuperclass().getDeclaredMethod("parseUserSQLForTableNameDW", boolean.class, boolean.class);
        method.setAccessible(true);

        assertEquals((String) method.invoke(pstmt, false, false), "Peter\"\"Table");

        method = pstmt.getClass().getSuperclass().getDeclaredMethod("parseUserSQLForColumnListDW");
        method.setAccessible(true);

        ArrayList<String> columnList = (ArrayList<String>) method.invoke(pstmt);
        ArrayList<String> columnListExpected = new ArrayList<String>();
        columnListExpected.add("c1");
        columnListExpected.add("c2");
        columnListExpected.add("c3");
        columnListExpected.add("c4");

        for (int i = 0; i < columnListExpected.size(); i++) {
            assertEquals(columnList.get(i), columnListExpected.get(i));
        }

        method = pstmt.getClass().getSuperclass().getDeclaredMethod("parseUserSQLForValueListDW", boolean.class);
        method.setAccessible(true);

        ArrayList<String> valueList = (ArrayList<String>) method.invoke(pstmt, false);
        ArrayList<String> valueListExpected = new ArrayList<String>();
        valueListExpected.add("1");
        valueListExpected.add("2");
        valueListExpected.add("'?'");
        valueListExpected.add("?");

        for (int i = 0; i < valueListExpected.size(); i++) {
            assertEquals(valueList.get(i), valueListExpected.get(i));
        }
    }
    
    @Test
    public void testAllcolumns() throws Exception {
        Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
        f1.setAccessible(true);
        f1.set(connection, true);
        
        String valid = "INSERT INTO " + tableName + " values "
                + "("
                + "?, "
                + "?, "
                + "?, "
                + "?, "
                + "?, "
                + "?, "
                + "?, "
                + "?, "
                + "?, "
                + ")";
        
        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(valid);
        SQLServerStatement stmt = (SQLServerStatement) connection.createStatement();
        
        Timestamp myTimestamp = new Timestamp(114550L);
        
        Date d = new Date(114550L);
        
        pstmt.setInt(1, 1234);
        pstmt.setBoolean(2, false);
        pstmt.setString(3, "a");
        pstmt.setDate(4, d);
        pstmt.setDateTime(5, myTimestamp);
        pstmt.setFloat(6, (float) 123.45);
        pstmt.setString(7, "b");
        pstmt.setString(8, "varc");
        pstmt.setString(9, "varcmax");
        pstmt.addBatch();
        
        pstmt.executeBatch();
        
        ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
        
        Object[] expected = new Object[9];
        
        expected[0] = 1234;
        expected[1] = false;
        expected[2] = "a";
        expected[3] = d;
        expected[4] = myTimestamp;
        expected[5] = 123.45;
        expected[6] = "b";
        expected[7] = "varc";
        expected[8] = "varcmax";
        
        rs.next();
        for (int i=0; i < expected.length; i++) {
            assertEquals(rs.getObject(i + 1).toString(), expected[i].toString());
        }
    }

    @Test
    public void testMixColumns() throws Exception {
        Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
        f1.setAccessible(true);
        f1.set(connection, true);
        
        String valid = "INSERT INTO " + tableName + " (c1, c3, c5, c8) values "
                + "("
                + "?, "
                + "?, "
                + "?, "
                + "?, "
                + ")";
        
        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(valid);
        SQLServerStatement stmt = (SQLServerStatement) connection.createStatement();
        
        Timestamp myTimestamp = new Timestamp(114550L);
        
        Date d = new Date(114550L);
        
        pstmt.setInt(1, 1234);
        pstmt.setString(2, "a");
        pstmt.setDateTime(3, myTimestamp);
        pstmt.setString(4, "varc");
        pstmt.addBatch();
        
        pstmt.executeBatch();
        
        ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
        
        Object[] expected = new Object[9];
        
        expected[0] = 1234;
        expected[1] = false;
        expected[2] = "a";
        expected[3] = d;
        expected[4] = myTimestamp;
        expected[5] = 123.45;
        expected[6] = "b";
        expected[7] = "varc";
        expected[8] = "varcmax";
        
        rs.next();
        for (int i=0; i < expected.length; i++) {
            if (null != rs.getObject(i + 1)) {
                assertEquals(rs.getObject(i + 1).toString(), expected[i].toString());
            }
        }
    }
    
    @Test
    public void testNullOrEmptyColumns() throws Exception {
        Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
        f1.setAccessible(true);
        f1.set(connection, true);
        
        String valid = "INSERT INTO " + tableName + " (c1, c2, c3, c4, c5, c6, c7) values "
                + "("
                + "?, "
                + "?, "
                + "?, "
                + "?, "
                + "?, "
                + "?, "
                + "?, "
                + ")";

        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(valid);
        SQLServerStatement stmt = (SQLServerStatement) connection.createStatement();
        
        pstmt.setInt(1, 1234);
        pstmt.setBoolean(2, false);
        pstmt.setString(3, null);
        pstmt.setDate(4, null);
        pstmt.setDateTime(5, null);
        pstmt.setFloat(6, (float) 123.45);
        pstmt.setString(7, "");
        pstmt.addBatch();
        
        pstmt.executeBatch();
        
        ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
        
        Object[] expected = new Object[9];
        
        expected[0] = 1234;
        expected[1] = false;
        expected[2] = null;
        expected[3] = null;
        expected[4] = null;
        expected[5] = 123.45;
        expected[6] = " ";
        
        rs.next();
        for (int i=0; i < expected.length; i++) {
            if (null != rs.getObject(i + 1)) {
                assertEquals(rs.getObject(i + 1), expected[i]);
            }
        }
    }
    
    @BeforeEach
    public void testSetup() throws TestAbortedException, Exception {
        connection = DriverManager.getConnection(connectionString + ";useBulkCopyForBatchInsert=true;");
        SQLServerStatement stmt = (SQLServerStatement) connection.createStatement();
        
        Utils.dropTableIfExists(tableName, stmt);
        String sql1 = "create table " + tableName + " "
                + "("
                + "c1 int DEFAULT 1234, "
                + "c2 bit, "
                + "c3 char DEFAULT NULL, "
                + "c4 date, "
                + "c5 datetime2, "
                + "c6 float, "
                + "c7 nchar, "
                + "c8 varchar(20), "
                + "c9 varchar(max)"
                + ")";
        
        stmt.execute(sql1);
        stmt.close();
    }
    
    @AfterAll
    public static void terminateVariation() throws SQLException {
        connection = DriverManager.getConnection(connectionString);
        
        SQLServerStatement stmt = (SQLServerStatement) connection.createStatement();
        Utils.dropTableIfExists(tableName, stmt);

        if (null != pstmt) {
            pstmt.close();
        }
        if (null != stmt) {
            stmt.close();
        }
        if (null != connection) {
            connection.close();
        }
    }
}
