package com.microsoft.sqlserver.jdbc.callablestatement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.MessageFormat;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.TimeZone;
import java.util.UUID;

import com.microsoft.sqlserver.testframework.PrepUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


/**
 * Test CallableStatement
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDW)
public class CallableStatementTest extends AbstractTest {
    private static String tableNameGUID = RandomUtil.getIdentifier("uniqueidentifier_Table");
    private static String outputProcedureNameGUID = RandomUtil.getIdentifier("uniqueidentifier_SP");
    private static String setNullProcedureName = RandomUtil.getIdentifier("CallableStatementTest_setNull_SP");
    private static String inputParamsProcedureName = RandomUtil.getIdentifier("CallableStatementTest_inputParams_SP");
    private static String getObjectLocalDateTimeProcedureName = RandomUtil.getIdentifier("CallableStatementTest_getObjectLocalDateTime_SP");
    private static String getObjectOffsetDateTimeProcedureName = RandomUtil.getIdentifier("CallableStatementTest_getObjectOffsetDateTime_SP");
    private static String procName = RandomUtil.getIdentifier("procedureTestCallableStatementSpPrepare");
    private static String manyParamsTable = RandomUtil.getIdentifier("manyParam_Table");
    private static String manyParamProc = RandomUtil.getIdentifier("manyParam_Procedure");
    private static String manyParamUserDefinedType = RandomUtil.getIdentifier("manyParam_definedType");

    /**
     * Setup before test
     * 
     * @throws SQLException
     */
    @BeforeAll
    public static void setupTest() throws Exception {
        setConnection();

        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableNameGUID), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(outputProcedureNameGUID), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(setNullProcedureName), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(inputParamsProcedureName), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(getObjectLocalDateTimeProcedureName), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(getObjectOffsetDateTimeProcedureName), stmt);
            TestUtils.dropTypeIfExists(manyParamUserDefinedType, stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(manyParamProc), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(manyParamsTable), stmt);

            createGUIDTable(stmt);
            createGUIDStoredProcedure(stmt);
            createSetNullProcedure(stmt);
            createInputParamsProcedure(stmt);
            createGetObjectLocalDateTimeProcedure(stmt);
            createUserDefinedType();
            createTableManyParams();
            createProcedureManyParams();createGetObjectOffsetDateTimeProcedure(stmt);
        }
    }

    @Test
    public void testCallableStatementManyParameters() throws SQLException {
        String dropLogin = "IF EXISTS (select * from sys.sql_logins where name = 'NewLogin') DROP LOGIN NewLogin";
        String dropUser = "IF EXISTS (select * from sys.sysusers where name = 'NewUser') DROP USER NewUser";
        String createLogin = "USE MASTER;CREATE LOGIN NewLogin WITH PASSWORD=N'P4ssword', " +
                "DEFAULT_DATABASE = MASTER, DEFAULT_LANGUAGE = US_ENGLISH;ALTER LOGIN NewLogin ENABLE;";
        String createUser = "USE MASTER;CREATE USER NewUser FOR LOGIN NewLogin WITH DEFAULT_SCHEMA = [DBO];";
        String grantExecute = "GRANT EXECUTE ON " + AbstractSQLGenerator.escapeIdentifier(manyParamProc) + " TO NewUser;";

        // Need to create a user with limited permissions in order to run through the code block we are testing
        // The user created will execute sp_sproc_columns internally by the driver, which should not return all
        // the column names as the user has limited permissions
        try (Connection conn = PrepUtil.getConnection(connectionString)) {
            Statement stmt = conn.createStatement();
            stmt.execute(dropLogin);
            stmt.execute(dropUser);
            stmt.execute(createLogin);
            stmt.execute(createUser);
            stmt.execute(grantExecute);
        }

        try (Connection conn = PrepUtil.getConnection(connectionString + ";user=NewLogin;password=P4ssword;")) {
            BigDecimal money = new BigDecimal("9999.99");

            // Should not throw an "Index is out of range error"
            // Should not throw R_parameterNotDefinedForProcedure
            try (CallableStatement callableStatement = conn.prepareCall(
                    "{call " + AbstractSQLGenerator.escapeIdentifier(manyParamProc) + "(?,?,?,?,?,?,?,?,?,?)}")) {
                callableStatement.setObject("@p1", money, microsoft.sql.Types.MONEY);
                callableStatement.setObject("@p2", money, microsoft.sql.Types.MONEY);
                callableStatement.setObject("@p3", money, microsoft.sql.Types.MONEY);
                callableStatement.setObject("@p4", money, microsoft.sql.Types.MONEY);
                callableStatement.setObject("@p5", money, microsoft.sql.Types.MONEY);
                callableStatement.setObject("@p6", money, microsoft.sql.Types.MONEY);
                callableStatement.setObject("@p7", money, microsoft.sql.Types.MONEY);
                callableStatement.setObject("@p8", money, microsoft.sql.Types.MONEY);
                callableStatement.setObject("@p9", money, microsoft.sql.Types.MONEY);
                callableStatement.setObject("@p10", money, microsoft.sql.Types.MONEY);
                callableStatement.execute();
            }
        }
    }

    @Test
    public void testCallableStatementSpPrepare() throws SQLException {
        connection.setPrepareMethod("prepare");

        try (Statement statement = connection.createStatement();) {
            statement.executeUpdate("create procedure " + AbstractSQLGenerator.escapeIdentifier(procName) + " as select 1 --");

            try (CallableStatement callableStatement = connection.prepareCall(
                    "{call " + AbstractSQLGenerator.escapeIdentifier(procName) + "}")) {
                try (ResultSet rs = callableStatement.executeQuery()) { // Takes sp_executesql path
                    rs.next();
                    assertEquals(1, rs.getInt(1), TestResource.getResource("R_setDataNotEqual"));
                }

                try (ResultSet rs = callableStatement.executeQuery()) { // Takes sp_prepare path
                    rs.next();
                    assertEquals(1, rs.getInt(1), TestResource.getResource("R_setDataNotEqual"));
                }
            }
        }
    }

    /**
     * Tests CallableStatement.getString() with uniqueidentifier parameter
     * 
     * @throws SQLException
     */
    @Test
    public void getStringGUIDTest() throws SQLException {

        String sql = "{call " + AbstractSQLGenerator.escapeIdentifier(outputProcedureNameGUID) + "(?)}";

        try (SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) connection.prepareCall(sql)) {

            UUID originalValue = UUID.randomUUID();

            callableStatement.registerOutParameter(1, microsoft.sql.Types.GUID);
            callableStatement.setObject(1, originalValue.toString(), microsoft.sql.Types.GUID);
            callableStatement.execute();

            String retrievedValue = callableStatement.getString(1);

            assertEquals(originalValue.toString().toLowerCase(), retrievedValue.toLowerCase());

        }
    }

    /**
     * test for setNull(index, varchar) to behave as setNull(index, nvarchar) when SendStringParametersAsUnicode is true
     * 
     * @throws SQLException
     */
    @Test
    public void getSetNullWithTypeVarchar() throws SQLException {
        String polishchar = "\u0143";

        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setURL(connectionString);
        ds.setSendStringParametersAsUnicode(true);
        String sql = "{? = call " + AbstractSQLGenerator.escapeIdentifier(setNullProcedureName) + " (?,?)}";
        try (Connection connection = ds.getConnection();
                SQLServerCallableStatement cs = (SQLServerCallableStatement) connection.prepareCall(sql);
                SQLServerCallableStatement cs2 = (SQLServerCallableStatement) connection.prepareCall(sql)) {

            cs.registerOutParameter(1, Types.INTEGER);
            cs.setString(2, polishchar);
            cs.setString(3, null);
            cs.registerOutParameter(3, Types.VARCHAR);
            cs.execute();

            String expected = cs.getString(3);

            cs2.registerOutParameter(1, Types.INTEGER);
            cs2.setString(2, polishchar);
            cs2.setNull(3, Types.VARCHAR);
            cs2.registerOutParameter(3, Types.NVARCHAR);
            cs2.execute();

            String actual = cs2.getString(3);

            assertEquals(expected, actual);
        }
    }


    /**
     * Tests getObject(n, java.time.LocalDateTime.class).
     *
     * @throws SQLException
     */
    @Test
    public void testGetObjectAsLocalDateTime() throws SQLException {
        String sql = "{CALL " + AbstractSQLGenerator.escapeIdentifier(getObjectLocalDateTimeProcedureName) + " (?)}";
        try (Connection con = DriverManager.getConnection(connectionString); CallableStatement cs = con.prepareCall(sql)) {
            cs.registerOutParameter(1, Types.TIMESTAMP);
            TimeZone prevTimeZone = TimeZone.getDefault();
            TimeZone.setDefault(TimeZone.getTimeZone("America/Edmonton"));

            // a local date/time that does not actually exist because of Daylight Saving Time
            final String testValueDate = "2018-03-11";
            final String testValueTime = "02:00:00.1234567";
            final String testValueDateTime = testValueDate + "T" + testValueTime;

            try {
                cs.execute();

                LocalDateTime expectedLocalDateTime = LocalDateTime.parse(testValueDateTime);
                LocalDateTime actualLocalDateTime = cs.getObject(1, LocalDateTime.class);
                assertEquals(expectedLocalDateTime, actualLocalDateTime);

                LocalDate expectedLocalDate = LocalDate.parse(testValueDate);
                LocalDate actualLocalDate = cs.getObject(1, LocalDate.class);
                assertEquals(expectedLocalDate, actualLocalDate);

                LocalTime expectedLocalTime = LocalTime.parse(testValueTime);
                LocalTime actualLocalTime = cs.getObject(1, LocalTime.class);
                assertEquals(expectedLocalTime, actualLocalTime);
            } finally {
                TimeZone.setDefault(prevTimeZone);
            }
        }
    }

    /**
     * Tests getObject(n, java.time.OffsetDateTime.class) and getObject(n, java.time.OffsetTime.class).
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testGetObjectAsOffsetDateTime() throws SQLException {
        String sql = "{CALL " + AbstractSQLGenerator.escapeIdentifier(getObjectOffsetDateTimeProcedureName) + " (?, ?)}";
        try (Connection con = DriverManager.getConnection(connectionString); CallableStatement cs = con.prepareCall(sql)) {
            cs.registerOutParameter(1, Types.TIMESTAMP_WITH_TIMEZONE);
            cs.registerOutParameter(2, Types.TIMESTAMP_WITH_TIMEZONE);

            final String testValue = "2018-01-02T11:22:33.123456700+12:34";

            cs.execute();

            OffsetDateTime expected = OffsetDateTime.parse(testValue);
            OffsetDateTime actual = cs.getObject(1, OffsetDateTime.class);
            assertEquals(expected, actual);
            assertNull(cs.getObject(2, OffsetDateTime.class));

            OffsetTime expectedTime = OffsetTime.parse(testValue.split("T")[1]);
            OffsetTime actualTime = cs.getObject(1, OffsetTime.class);
            assertEquals(expectedTime, actualTime);
            assertNull(cs.getObject(2, OffsetTime.class));
        }
    }

    /**
     * recognize parameter names with and without leading '@'
     * 
     * @throws SQLException
     */
    @Test
    public void inputParamsTest() throws SQLException {
        String call = "{CALL " + AbstractSQLGenerator.escapeIdentifier(inputParamsProcedureName) + " (?,?)}";

        // the historical way: no leading '@', parameter names respected (not positional)
        try (CallableStatement cs = connection.prepareCall(call)) {
            cs.setString("p2", "world");
            cs.setString("p1", "hello");
            try (ResultSet rs = cs.executeQuery()) {
                rs.next();
                assertEquals("helloworld", rs.getString(1));
            }
        }

        // the "new" way: leading '@', parameter names still respected (not positional)
        try (CallableStatement cs = connection.prepareCall(call)) {
            cs.setString("@p2", "world!");
            cs.setString("@p1", "Hello ");
            try (ResultSet rs = cs.executeQuery()) {
                rs.next();
                assertEquals("Hello world!", rs.getString(1));
            }
        }

        // sanity check: unrecognized parameter name
        try (CallableStatement cs = connection.prepareCall(call)) {
            cs.setString("@whatever", "test");
            fail(TestResource.getResource("R_shouldThrowException"));
        } catch (SQLException sse) {

            MessageFormat form = new MessageFormat(TestResource.getResource("R_parameterNotDefined"));
            Object[] msgArgs = {"@whatever"};

            if (!sse.getMessage().startsWith(form.format(msgArgs))) {
                fail(TestResource.getResource("R_unexpectedExceptionContent"));
            }
        }
    }

    /**
     * Cleanup after test
     * 
     * @throws SQLException
     */
    @AfterAll
    public static void cleanup() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableNameGUID), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(outputProcedureNameGUID), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(setNullProcedureName), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(inputParamsProcedureName), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(getObjectLocalDateTimeProcedureName), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(getObjectOffsetDateTimeProcedureName), stmt);
        }
    }

    private static void createGUIDStoredProcedure(Statement stmt) throws SQLException {
        String sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(outputProcedureNameGUID)
                + "(@p1 uniqueidentifier OUTPUT) AS SELECT @p1 = c1 FROM "
                + AbstractSQLGenerator.escapeIdentifier(tableNameGUID) + Constants.SEMI_COLON;
        stmt.execute(sql);
    }

    private static void createGUIDTable(Statement stmt) throws SQLException {
        String sql = "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableNameGUID)
                + " (c1 uniqueidentifier null)";
        stmt.execute(sql);
    }

    private static void createSetNullProcedure(Statement stmt) throws SQLException {
        stmt.execute("create procedure " + AbstractSQLGenerator.escapeIdentifier(setNullProcedureName)
                + " (@p1 nvarchar(255), @p2 nvarchar(255) output) as select @p2=@p1 return 0");
    }

    private static void createInputParamsProcedure(Statement stmt) throws SQLException {
        String sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(inputParamsProcedureName)
                + "    @p1 nvarchar(max) = N'parameter1', " + "    @p2 nvarchar(max) = N'parameter2' " + "AS "
                + "BEGIN " + "    SET NOCOUNT ON; " + "    SELECT @p1 + @p2 AS result; " + "END ";

        stmt.execute(sql);
    }

    private static void createGetObjectLocalDateTimeProcedure(Statement stmt) throws SQLException {
        String sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(getObjectLocalDateTimeProcedureName)
                + "(@p1 datetime2(7) OUTPUT) AS "
                + "SELECT @p1 = '2018-03-11T02:00:00.1234567'";
        stmt.execute(sql);
    }

    private static void createGetObjectOffsetDateTimeProcedure(Statement stmt) throws SQLException {
        String sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(getObjectOffsetDateTimeProcedureName)
                + "(@p1 DATETIMEOFFSET OUTPUT, @p2 DATETIMEOFFSET OUTPUT) AS "
                + "SELECT @p1 = '2018-01-02T11:22:33.123456700+12:34', @p2 = NULL";
        stmt.execute(sql);
    }

    private static void createProcedureManyParams() throws SQLException {
        String type = AbstractSQLGenerator.escapeIdentifier(manyParamUserDefinedType);
        String sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(manyParamProc)
                + " @p1 " + type
                + ", @p2 " + type
                + ", @p3 " + type
                + ", @p4 " + type
                + ", @p5 " + type
                + ", @p6 " + type
                + ", @p7 " + type
                + ", @p8 " + type
                + ", @p9 " + type
                + ", @p10 " + type
                + " AS INSERT INTO "
                + AbstractSQLGenerator.escapeIdentifier(manyParamsTable) + " VALUES(@p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10)";
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    private static void createTableManyParams() throws SQLException {
        String type = AbstractSQLGenerator.escapeIdentifier(manyParamUserDefinedType);
        String sql = "create table " + AbstractSQLGenerator.escapeIdentifier(manyParamsTable) +
                " (c1 " + type + " null, " +
                "c2 " + type + " null, " +
                "c3 " + type + " null, " +
                "c4 " + type + " null, " +
                "c5 " + type + " null, " +
                "c6 " + type + " null, " +
                "c7 " + type + " null, " +
                "c8 " + type + " null, " +
                "c9 " + type + " null, " +
                "c10 " + type + " null);";
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    private static void createUserDefinedType() throws SQLException {
        String TVPCreateCmd = "CREATE TYPE " + AbstractSQLGenerator.escapeIdentifier(manyParamUserDefinedType) + " FROM MONEY";
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(TVPCreateCmd);
        }
    }
}
