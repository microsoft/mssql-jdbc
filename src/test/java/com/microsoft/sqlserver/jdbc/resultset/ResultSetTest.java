package com.microsoft.sqlserver.jdbc.resultset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBSQLServerJDBC42;
import com.microsoft.sqlserver.testframework.util.RandomUtil;

/**
 * Tests proper exception for unsupported feature
 * 
 * @author Microsoft
 */
@RunWith(JUnitPlatform.class)
public class ResultSetTest extends AbstractTest {
    private static final String tableName = "[" + RandomUtil.getIdentifier("StatementParam") + "]";

    @Test
    public void testJdbc41ResultSetMethods() throws Exception {
        Connection con = DriverManager.getConnection(connectionString);
        Statement stmt = con.createStatement();
        try {
            stmt.executeUpdate("create table " + tableName + " (col1 int, col2 text, col3 int identity(1,1) primary key)");

            stmt.executeUpdate("Insert into " + tableName + " values(0, 'hello')");

            stmt.executeUpdate("Insert into " + tableName + " values(0, 'yo')");

            ResultSet rs = stmt.executeQuery("select * from " + tableName);
            rs.next();
            // Both methods throw exceptions
            try {

                int col1 = rs.getObject(1, Integer.class);
            }
            catch (Exception e) {
                if (DBSQLServerJDBC42.value >= 41) {
                    // unsupported feature
                    Class type = new DBSQLServerJDBC42().getSqlFeatureNotSupportedExceptionClass();
                    assertEquals(e.getClass(), type, "Verify exception type " + e.getMessage());
                }
                else {
                    // unsupported operation
                    assertEquals(e.getClass(), java.lang.UnsupportedOperationException.class, "Verify exception type " + e.getMessage());
                }
            }
            try {
                String col2 = rs.getObject("col2", String.class);
            }
            catch (Exception e) {
                if (DBSQLServerJDBC42.value >= 41) {
                    // unsupported feature
                    Class type = new DBSQLServerJDBC42().getSqlFeatureNotSupportedExceptionClass();
                    assertEquals(e.getClass(), type, "Verify exception type " + e.getMessage());
                }
                else {
                    // unsupported operation
                    assertEquals(e.getClass(), java.lang.UnsupportedOperationException.class, "Verify exception type " + e.getMessage());
                }
            }

            try {
                stmt.executeUpdate("drop table " + tableName);
            }
            catch (Exception ex) {
                fail(ex.toString());
            }
        }
        finally {
            stmt.close();
            con.close();
        }
    }

}
