package com.microsoft.sqlserver.jdbc;

import com.microsoft.sqlserver.testframework.AbstractTest;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import java.sql.*;
import java.util.Properties;

public class TestingTest extends AbstractTest {
    
    @Test()
        public void testSqlServer() throws SQLException {
            try (Connection conn = getConnection()) {
                try (Statement statement = conn.createStatement()) {
                    ResultSet resultSet = statement.executeQuery(
                        "select val from t_charset_test  where val = ('中') ");
                    boolean next = resultSet.next();
                    Assert.assertTrue(next);
                    System.out.println(resultSet.getString(1));
                }
            }
        }
}
