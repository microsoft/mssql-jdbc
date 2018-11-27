package com.microsoft.sqlserver.jdbc.callablestatement;

import java.sql.*;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import static org.junit.jupiter.api.Assertions.fail;


@RunWith(JUnitPlatform.class)
public class CallableMixedTest extends AbstractTest {

    @Test
    public void datatypestest() throws Exception {
        String tableName = RandomUtil.getIdentifier("TFOO3");
        String escapedTableName = AbstractSQLGenerator.escapeIdentifier(tableName);
        String procName = RandomUtil.getIdentifier("SPFOO3");

        try (Connection conn = DriverManager.getConnection(connectionString)) {
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(escapedTableName, stmt);

                String createSQL = "create table " + escapedTableName + "(c1_int int primary key, col2 int)";
                stmt.executeUpdate(createSQL);

                stmt.executeUpdate("Insert into " + escapedTableName + " values(0, 1)");

                stmt.executeUpdate("CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(procName)
                        + " (@p2_int int, @p2_int_out int OUTPUT, @p4_smallint smallint,  @p4_smallint_out smallint OUTPUT) AS begin transaction SELECT * FROM "
                        + escapedTableName
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
                TestUtils.dropTableIfExists(escapedTableName, stmt);
            } catch (SQLException e) {
                fail(e.toString());
            }
        }
    }
}
