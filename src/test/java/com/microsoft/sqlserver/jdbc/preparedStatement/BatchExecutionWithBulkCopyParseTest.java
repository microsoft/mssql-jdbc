package com.microsoft.sqlserver.jdbc.preparedStatement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.opentest4j.TestAbortedException;

import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.Utils;


@RunWith(JUnitPlatform.class)
public class BatchExecutionWithBulkCopyParseTest extends AbstractTest {

    static SQLServerPreparedStatement pstmt = null;
    static Statement stmt = null;
    static Connection connection = null;
    
    @Test
    public void testIsInsert() throws SQLException, NoSuchMethodException, SecurityException,
    IllegalAccessException, IllegalArgumentException, InvocationTargetException {
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
    public void testComments() throws SQLException, NoSuchFieldException, SecurityException,
    IllegalArgumentException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
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
    public void testBrackets() throws SQLException, NoSuchFieldException, SecurityException,
    IllegalArgumentException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
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
    public void testDoubleQuotes() throws SQLException, NoSuchFieldException, SecurityException,
    IllegalArgumentException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
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
    public void testAll() throws SQLException, NoSuchFieldException, SecurityException,
    IllegalArgumentException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
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
    
    @BeforeEach
    public void testSetup() throws TestAbortedException, Exception {
        assumeTrue(13 <= new DBConnection(connectionString).getServerVersion(),
                "Aborting test case as SQL Server version is not compatible with Always encrypted ");

        connection = DriverManager.getConnection(connectionString);
        SQLServerStatement stmt = (SQLServerStatement) connection.createStatement();
        Utils.dropTableIfExists("esimple", stmt);
        String sql1 = "create table esimple (id integer not null, name varchar(255), constraint pk_esimple primary key (id))";
        stmt.execute(sql1);
        stmt.close();
    }
}
