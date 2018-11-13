package com.microsoft.sqlserver.jdbc.callablestatement;

import java.sql.*;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import static org.junit.jupiter.api.Assertions.fail;


@RunWith(JUnitPlatform.class)
public class CallableMixed {

    @Test
    public void datatypestest() throws Exception {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        String connectionString = TestUtils.getConfiguredProperty("mssql_jdbc_test_connection_properties");
        String tableName = RandomUtil.getIdentifier("TFOO3");
        String procName = RandomUtil.getIdentifier("SPFOO3");

        try (Connection conn = DriverManager.getConnection(connectionString)) {
            try (Statement stmt = conn.createStatement()) {
                try {
                    stmt.executeUpdate("DROP TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName));
                    stmt.executeUpdate(" DROP PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(procName));
                } catch (Exception e) {}

                String createSQL = "create table " + AbstractSQLGenerator.escapeIdentifier(tableName)
                        + "(c1_int int primary key, col2 int)";
                stmt.executeUpdate(createSQL);

                stmt.executeUpdate("Insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values(0, 1)");
            }

            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(procName)
                        + " (@p2_int int, @p2_int_out int OUTPUT, @p4_smallint smallint,  @p4_smallint_out smallint OUTPUT) AS begin transaction SELECT * FROM "
                        + AbstractSQLGenerator.escapeIdentifier(tableName)
                        + "  ; SELECT @p2_int_out=@p2_int, @p4_smallint_out=@p4_smallint commit transaction RETURN -2147483648");
            }

            try (CallableStatement cstmt = conn.prepareCall(
                    "{  ? = CALL " + AbstractSQLGenerator.escapeIdentifier(procName) + " (?, ?, ?, ?) }")) {
                cstmt.registerOutParameter((int) 1, (int) 4);
                cstmt.setObject((int) 2, Integer.valueOf("31"), (int) 4);
                cstmt.registerOutParameter((int) 3, (int) 4);
                cstmt.registerOutParameter((int) 5, java.sql.Types.BINARY); // Test OUT param
                                                                            // re-registration
                                                                            // (Defect 60921)
                cstmt.registerOutParameter((int) 5, (int) 5);
                cstmt.setObject((int) 4, Short.valueOf("-5372"), (int) 5);

                // get results and a value
                ResultSet rs = cstmt.executeQuery();
                rs.next();

                if (rs.getInt(1) != 0) {
                    fail("Received data not equal to setdata");

                }

                if (cstmt.getInt((int) 5) != -5372) {
                    fail("Received data not equal to setdata");

                }
                // do nothing and reexecute
                rs = cstmt.executeQuery();
                // get the param without getting the resultset
                rs = cstmt.executeQuery();
                if (cstmt.getInt((int) 1) != -2147483648) {
                    fail("Received data not equal to setdata");

                }

                if (cstmt.getInt((int) 1) != -2147483648) {
                    fail("Received data not equal to setdata");

                }

                rs = cstmt.executeQuery();
                rs.next();

                if (rs.getInt(1) != 0) {
                    fail("Received data not equal to setdata");

                }

                if (cstmt.getInt((int) 1) != -2147483648) {
                    fail("Received data not equal to setdata");

                }

                if (cstmt.getInt((int) 5) != -5372) {
                    fail("Received data not equal to setdata");

                }

                rs = cstmt.executeQuery();
            }
        } finally {
            try (Connection conn = DriverManager.getConnection(connectionString);
                    Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            } catch (SQLException e) {
                fail(e.toString());
            }

        }
    }

}
