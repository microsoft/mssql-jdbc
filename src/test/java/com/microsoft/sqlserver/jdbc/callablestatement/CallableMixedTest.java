package com.microsoft.sqlserver.jdbc.callablestatement;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDW)
public class CallableMixedTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"trustServerCertificate", "true");
        setConnection();
    }

    @Test
    public void datatypestest() throws Exception {
        String tableName = RandomUtil.getIdentifier("TFOO3");
        String escapedTableName = AbstractSQLGenerator.escapeIdentifier(tableName);
        String procName = RandomUtil.getIdentifier("SPFOO3");
        String escapedProcName = AbstractSQLGenerator.escapeIdentifier(procName);

        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(escapedTableName, stmt);

            String createSQL = "create table " + escapedTableName + "(c1_int int primary key, col2 int)";
            stmt.executeUpdate(createSQL);

            stmt.executeUpdate("Insert into " + escapedTableName + " values(0, 1)");

            stmt.executeUpdate("CREATE PROCEDURE " + escapedProcName
                    + " (@p2_int int, @p2_int_out int OUTPUT, @p4_smallint smallint,  @p4_smallint_out smallint OUTPUT) AS begin transaction SELECT * FROM "
                    + escapedTableName
                    + "  ; SELECT @p2_int_out=@p2_int, @p4_smallint_out=@p4_smallint commit transaction RETURN -2147483648");

            try (CallableStatement cstmt = conn.prepareCall("{  ? = CALL " + escapedProcName + " (?, ?, ?, ?) }")) {
                cstmt.registerOutParameter((int) 1, (int) 4);
                cstmt.setObject((int) 2, Integer.valueOf("31"), (int) 4);
                cstmt.registerOutParameter((int) 3, (int) 4);

                // Test OUT param re-registration
                cstmt.registerOutParameter((int) 5, java.sql.Types.BINARY);
                cstmt.registerOutParameter((int) 5, (int) 5);
                cstmt.setObject((int) 4, Short.valueOf("-5372"), (int) 5);

                // get results and a value
                try (ResultSet rs = cstmt.executeQuery()) {
                    rs.next();
                    assertEquals(0, rs.getInt(1));
                    assertEquals(-5372, cstmt.getInt((int) 5));
                }

                // get the param without getting the resultset
                try (ResultSet rs = cstmt.executeQuery()) {
                    assertEquals(-2147483648, cstmt.getInt((int) 1));
                }

                try (ResultSet rs = cstmt.executeQuery()) {
                    rs.next();
                    assertEquals(0, rs.getInt(1));
                    assertEquals(-2147483648, cstmt.getInt((int) 1));
                    assertEquals(-5372, cstmt.getInt((int) 5));
                }
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(tableName, stmt);
                TestUtils.dropProcedureIfExists(procName, stmt);
            }
        }
    }

    @Test
    @Tag("xAzureSQLDB")
    @Tag("xAzureSQLDW")
    @Tag("xAzureSQLMI")
    public void noPrivilegeTest() throws SQLException {
        try (Connection c = getConnection(); Statement stmt = c.createStatement()) {
            String tableName = RandomUtil.getIdentifier("jdbc_priv");
            String procName = RandomUtil.getIdentifier("priv_proc");
            String user = "priv_user" + UUID.randomUUID();
            String pass = "priv_pass" + UUID.randomUUID();

            stmt.execute(
                    "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (id int, name varchar(50))");
            stmt.execute("CREATE PROC " + AbstractSQLGenerator.escapeIdentifier(procName)
                    + " @id int, @str varchar(50) as INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " values(@id,@str)");
            stmt.execute(
                    "CREATE LOGIN " + AbstractSQLGenerator.escapeIdentifier(user) + " WITH password='" + pass + "'");
            stmt.execute("CREATE USER " + AbstractSQLGenerator.escapeIdentifier(user) + "");
            try {
                stmt.execute("EXECUTE AS USER='" + user + "';EXECUTE " + AbstractSQLGenerator.escapeIdentifier(procName)
                        + " 1,'hi';");
                fail(TestResource.getResource("R_shouldThrowException"));
            } catch (SQLException e) {
                assertTrue(e.getMessage().matches(TestResource.formatErrorMsg("R_NoPrivilege")));
            } finally {
                TestUtils.dropProcedureIfExists(procName, stmt);
                TestUtils.dropTableIfExists(tableName, stmt);
                stmt.close();
                c.close();
                try (Connection c2 = getConnection(); Statement stmt2 = c2.createStatement()) {
                    stmt2.execute("DROP USER " + AbstractSQLGenerator.escapeIdentifier(user));
                    stmt2.execute("DROP LOGIN " + AbstractSQLGenerator.escapeIdentifier(user));
                }
            }
        }
    }
}
