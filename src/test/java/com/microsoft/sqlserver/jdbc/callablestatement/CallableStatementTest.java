package com.microsoft.sqlserver.jdbc.callablestatement;

import static org.junit.Assert.assertTrue;
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

import org.junit.Assert;
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
import com.microsoft.sqlserver.jdbc.SQLServerDataTable;
import com.microsoft.sqlserver.testframework.PrepUtil;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;

import javax.sql.DataSource;


/**
 * Test CallableStatement
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDW)
public class CallableStatementTest extends AbstractTest {
    private static String tableNameGUID = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("uniqueidentifier_Table"));
    private static String outputProcedureNameGUID = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("uniqueidentifier_SP"));
    private static String setNullProcedureName = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("CallableStatementTest_setNull_SP"));
    private static String inputParamsProcedureName = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("CallableStatementTest_inputParams_SP"));
    private static String getObjectLocalDateTimeProcedureName = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("CallableStatementTest_getObjectLocalDateTime_SP"));
    private static String getObjectOffsetDateTimeProcedureName = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("CallableStatementTest_getObjectOffsetDateTime_SP"));
    private static String procName = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("procedureTestCallableStatementSpPrepare"));
    private static String manyParamsTable = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("manyParam_Table"));
    private static String manyParamProc = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("manyParam_Procedure"));
    private static String manyParamUserDefinedType = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("manyParam_definedType"));
    private static String zeroParamSproc = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("zeroParamSproc"));
    private static String outOfOrderSproc = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("outOfOrderSproc"));
    private static String byParamNameSproc = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("byParamNameSproc"));
    private static String userDefinedFunction = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("userDefinedFunction"));

    /**
     * Setup before test
     * 
     * @throws SQLException
     */
    @BeforeAll
    public static void setupTest() throws Exception {
        setConnection();

        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(tableNameGUID, stmt);
            TestUtils.dropProcedureIfExists(outputProcedureNameGUID, stmt);
            TestUtils.dropProcedureIfExists(setNullProcedureName, stmt);
            TestUtils.dropProcedureIfExists(inputParamsProcedureName, stmt);
            TestUtils.dropProcedureIfExists(getObjectLocalDateTimeProcedureName, stmt);
            TestUtils.dropProcedureIfExists(getObjectOffsetDateTimeProcedureName, stmt);
            TestUtils.dropProcedureIfExists(zeroParamSproc, stmt);
            TestUtils.dropProcedureIfExists(outOfOrderSproc, stmt);
            TestUtils.dropProcedureIfExists(byParamNameSproc, stmt);
            TestUtils.dropFunctionIfExists(userDefinedFunction, stmt);
            TestUtils.dropUserDefinedTypeIfExists(manyParamUserDefinedType, stmt);
            TestUtils.dropProcedureIfExists(manyParamProc, stmt);
            TestUtils.dropTableIfExists(manyParamsTable, stmt);

            createGUIDTable(stmt);
            createGUIDStoredProcedure(stmt);
            createSetNullProcedure(stmt);
            createInputParamsProcedure(stmt);
            createGetObjectLocalDateTimeProcedure(stmt);
            createUserDefinedType();
            createTableManyParams();
            createProcedureManyParams();
            createGetObjectOffsetDateTimeProcedure(stmt);
            createProcedureZeroParams();
            createOutOfOrderSproc();
            createByParamNameSproc();
            createUserDefinedFunction();
        }
    }

    @Test
    public void testCallableStatementClosedConnection() {
        try (SQLServerCallableStatement stmt = (SQLServerCallableStatement) connection.prepareCall("sproc")) {
            stmt.close(); // Prematurely close the statement, which causes inOutParams to be null.
            stmt.setStructured("myParam", "myTvp", (SQLServerDataTable) null);
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (Exception e) {
            assertEquals(TestResource.getResource("R_statementClosed"), e.getMessage());
        }
    }

    // Test Needs more work to be configured to run on azureDB as there are slight differences
    // between the regular SQL Server vs. azureDB
    @Test
    @Tag(Constants.xAzureSQLDB)
    public void testCallableStatementManyParameters() throws SQLException {
        String tempPass = UUID.randomUUID().toString();
        String dropLogin = "IF EXISTS (select * from sys.sql_logins where name = 'NewLogin') DROP LOGIN NewLogin";
        String dropUser = "IF EXISTS (select * from sys.sysusers where name = 'NewUser') DROP USER NewUser";
        String createLogin = "USE MASTER;CREATE LOGIN NewLogin WITH PASSWORD=N'" + tempPass + "', "
                + "DEFAULT_DATABASE = MASTER, DEFAULT_LANGUAGE = US_ENGLISH;ALTER LOGIN NewLogin ENABLE;";
        String createUser = "USE MASTER;CREATE USER NewUser FOR LOGIN NewLogin WITH DEFAULT_SCHEMA = [DBO];";
        String grantExecute = "GRANT EXECUTE ON " + manyParamProc + " TO NewUser;";

        // Need to create a user with limited permissions in order to run through the code block we are testing
        // The user created will execute sp_sproc_columns internally by the driver, which should not return all
        // the column names as the user has limited permissions
        try (Connection conn = PrepUtil.getConnection(connectionString)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(dropLogin);
                stmt.execute(dropUser);
                stmt.execute(createLogin);
                stmt.execute(createUser);
                stmt.execute(grantExecute);
            }
        }

        try (Connection conn = PrepUtil.getConnection(connectionString + ";user=NewLogin;password=" + tempPass + ";")) {
            BigDecimal money = new BigDecimal("9999.99");

            // Should not throw an "Index is out of range error"
            // Should not throw R_parameterNotDefinedForProcedure
            try (CallableStatement callableStatement = conn
                    .prepareCall("{call " + manyParamProc + "(?,?,?,?,?,?,?,?,?,?)}")) {
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
            statement.executeUpdate("create procedure " + procName + " as select 1 --");

            try (CallableStatement callableStatement = connection.prepareCall("{call " + procName + "}")) {
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

        String sql = "{call " + outputProcedureNameGUID + "(?)}";

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
        String sql = "{? = call " + setNullProcedureName + " (?,?)}";
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
        String sql = "{CALL " + getObjectLocalDateTimeProcedureName + " (?)}";
        try (Connection con = DriverManager.getConnection(connectionString);
                CallableStatement cs = con.prepareCall(sql)) {
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
        String sql = "{CALL " + getObjectOffsetDateTimeProcedureName + " (?, ?)}";
        try (Connection con = DriverManager.getConnection(connectionString);
                CallableStatement cs = con.prepareCall(sql)) {
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
        String call = "{CALL " + inputParamsProcedureName + " (?,?)}";

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

    @Test
    public void testZeroParamSproc() throws SQLException {
        String call = "{? = CALL " + zeroParamSproc + "}";

        try (CallableStatement cs = connection.prepareCall(call)) {
            cs.registerOutParameter(1, Types.INTEGER);
            cs.execute();
            assertEquals(1, cs.getInt(1));
        }
    }

    @Test
    public void testSprocCastingError() {
        String call = "{? = CALL " + zeroParamSproc + "}";

        try (CallableStatement cs = connection.prepareCall(call)) {
            cs.registerOutParameter(1, Types.BINARY);
            cs.execute(); // Should not be able to get return value as bytes
            cs.getBytes(1);
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("cannot be cast to"));
        }
    }

    @Test
    public void testNonOrderedRegisteringAndSettingOfParams() throws SQLException {
        String call = "{CALL " + outOfOrderSproc + " (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}";

        try (CallableStatement cstmt = connection.prepareCall(call)) {
            int scale = 6;
            Double obj1 = 2015.0123;
            Double obj2 = 2015.012345;
            Integer obj3 = -3;
            Float obj4 = 2015.04f;
            Integer obj5 = 3;
            String obj6 = "foo";
            String obj7 = "bar";
            Long obj8 = 2015L;

            cstmt.setObject("i5", obj5, Types.CHAR);
            cstmt.setObject("i6", obj6, Types.VARCHAR);
            cstmt.setObject("i7", obj7, Types.CHAR);
            cstmt.setObject("i8", obj8, Types.SMALLINT);

            cstmt.setObject(1, obj1, Types.NUMERIC);
            cstmt.setObject(2, obj2, Types.NUMERIC, scale);
            cstmt.setObject(3, obj3, Types.INTEGER);
            cstmt.setObject(4, obj4, Types.FLOAT);

            cstmt.registerOutParameter(9, Types.NUMERIC);
            cstmt.registerOutParameter("o2", Types.NUMERIC, scale);
            cstmt.registerOutParameter("o3", Types.INTEGER);
            cstmt.registerOutParameter("o4", Types.FLOAT);

            cstmt.registerOutParameter(13, Types.CHAR);
            cstmt.registerOutParameter(14, Types.VARCHAR);
            cstmt.registerOutParameter(15, Types.CHAR);
            cstmt.registerOutParameter(16, Types.SMALLINT);
            cstmt.execute();
        }
    }

    @Test
    public void testNamedParametersUseFlexibleCallableStatementFalse() throws SQLException {
        String call = "{CALL " + outOfOrderSproc + " (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}";
        String connectionString = TestUtils.addOrOverrideProperty(getConnectionString(),
                "useFlexibleCallableStatements", "false");

        // useFlexibleCallableStatement=false and using all named parameters
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            try (CallableStatement cstmt = conn.prepareCall(call)) {
                int scale = 6;
                double obj1 = 2015.0123;
                double obj2 = 2015.012345;
                int obj3 = -3;
                float obj4 = 2015.04f;
                int obj5 = 3;
                String obj6 = "foo";
                String obj7 = "b";
                long obj8 = 2015L;

                cstmt.setObject("i1", obj1, Types.NUMERIC);
                cstmt.setObject("i2", obj2, Types.NUMERIC, scale);
                cstmt.setObject("i3", obj3, Types.INTEGER);
                cstmt.setObject("i4", obj4, Types.FLOAT);

                cstmt.setObject("i5", obj5, Types.CHAR);
                cstmt.setObject("i6", obj6, Types.VARCHAR);
                cstmt.setObject("i7", obj7, Types.CHAR);
                cstmt.setObject("i8", obj8, Types.SMALLINT);

                cstmt.registerOutParameter("o1", Types.NUMERIC);
                cstmt.registerOutParameter("o2", Types.NUMERIC, scale);
                cstmt.registerOutParameter("o3", Types.INTEGER);
                cstmt.registerOutParameter("o4", Types.FLOAT);

                cstmt.registerOutParameter("o5", Types.CHAR);
                cstmt.registerOutParameter("o6", Types.VARCHAR);
                cstmt.registerOutParameter("o7", Types.CHAR);
                cstmt.registerOutParameter("o8", Types.SMALLINT);
                cstmt.execute();

                assertEquals(obj1, cstmt.getDouble("o1"));
                assertEquals(obj2, cstmt.getDouble("o2"));
                assertEquals(obj3, cstmt.getInt("o3"));
                assertEquals(obj4, cstmt.getFloat("o4"));
                assertEquals(obj5, cstmt.getInt("o5"));
                assertEquals(obj6, cstmt.getString("o6"));
                assertEquals(obj7, cstmt.getString("o7"));
                assertEquals(obj8, cstmt.getLong("o8"));
            }
        }
    }

    @Test
    public void testNamedParametersAcquireOutputParamValuesByIndexUseFlexibleCallableStatementFalse() throws SQLException {
        String call = "{CALL " + outOfOrderSproc + " (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}";
        String connectionString = TestUtils.addOrOverrideProperty(getConnectionString(),
                "useFlexibleCallableStatements", "false");

        // useFlexibleCallableStatement=false
        // Setting parameters by name
        // Registering output parameters by name
        // Acquiring output parameter values by index
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            try (CallableStatement cstmt = conn.prepareCall(call)) {
                int scale = 6;
                double obj1 = 2015.0123;
                double obj2 = 2015.012345;
                int obj3 = -3;
                float obj4 = 2015.04f;
                int obj5 = 3;
                String obj6 = "foo";
                String obj7 = "b";
                long obj8 = 2015L;

                cstmt.setObject("i1", obj1, Types.NUMERIC);
                cstmt.setObject("i2", obj2, Types.NUMERIC, scale);
                cstmt.setObject("i3", obj3, Types.INTEGER);
                cstmt.setObject("i4", obj4, Types.FLOAT);

                cstmt.setObject("i5", obj5, Types.CHAR);
                cstmt.setObject("i6", obj6, Types.VARCHAR);
                cstmt.setObject("i7", obj7, Types.CHAR);
                cstmt.setObject("i8", obj8, Types.SMALLINT);

                cstmt.registerOutParameter("o1", Types.NUMERIC);
                cstmt.registerOutParameter("o2", Types.NUMERIC, scale);
                cstmt.registerOutParameter("o3", Types.INTEGER);
                cstmt.registerOutParameter("o4", Types.FLOAT);

                cstmt.registerOutParameter("o5", Types.CHAR);
                cstmt.registerOutParameter("o6", Types.VARCHAR);
                cstmt.registerOutParameter("o7", Types.CHAR);
                cstmt.registerOutParameter("o8", Types.SMALLINT);
                cstmt.execute();

                assertEquals(obj1, cstmt.getDouble(9));
                assertEquals(obj2, cstmt.getDouble(10));
                assertEquals(obj3, cstmt.getInt(11));
                assertEquals(obj4, cstmt.getFloat(12));
                assertEquals(obj5, cstmt.getInt(13));
                assertEquals(obj6, cstmt.getString(14));
                assertEquals(obj7, cstmt.getString(15));
                assertEquals(obj8, cstmt.getLong(16));
            }
        }
    }

    @Test
    public void testNamedParametersAndOutParamByIndexUseFlexibleCallableStatementFalse() throws SQLException {
        String call = "{CALL " + outOfOrderSproc + " (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}";
        String connectionString = TestUtils.addOrOverrideProperty(getConnectionString(),
                "useFlexibleCallableStatements", "false");

        // useFlexibleCallableStatement=false
        // Setting parameters by name
        // Registering output params by index
        // Acquiring output param values by index
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            try (CallableStatement cstmt = conn.prepareCall(call)) {
                int scale = 6;
                double obj1 = 2015.0123;
                double obj2 = 2015.012345;
                int obj3 = -3;
                float obj4 = 2015.04f;
                int obj5 = 3;
                String obj6 = "foo";
                String obj7 = "b";
                long obj8 = 2015L;

                cstmt.setObject("i1", obj1, Types.NUMERIC);
                cstmt.setObject("i2", obj2, Types.NUMERIC, scale);
                cstmt.setObject("i3", obj3, Types.INTEGER);
                cstmt.setObject("i4", obj4, Types.FLOAT);

                cstmt.setObject("i5", obj5, Types.CHAR);
                cstmt.setObject("i6", obj6, Types.VARCHAR);
                cstmt.setObject("i7", obj7, Types.CHAR);
                cstmt.setObject("i8", obj8, Types.SMALLINT);

                cstmt.registerOutParameter(9, Types.NUMERIC);
                cstmt.registerOutParameter(10, Types.NUMERIC, scale);
                cstmt.registerOutParameter(11, Types.INTEGER);
                cstmt.registerOutParameter(12, Types.FLOAT);

                cstmt.registerOutParameter(13, Types.CHAR);
                cstmt.registerOutParameter(14, Types.VARCHAR);
                cstmt.registerOutParameter(15, Types.CHAR);
                cstmt.registerOutParameter(16, Types.SMALLINT);
                cstmt.execute();

                assertEquals(obj1, cstmt.getDouble(9));
                assertEquals(obj2, cstmt.getDouble(10));
                assertEquals(obj3, cstmt.getInt(11));
                assertEquals(obj4, cstmt.getFloat(12));
                assertEquals(obj5, cstmt.getInt(13));
                assertEquals(obj6, cstmt.getString(14));
                assertEquals(obj7, cstmt.getString(15));
                assertEquals(obj8, cstmt.getLong(16));
            }
        }
    }

    @Test
    public void testNamedParameterOutputParameterErrorFlexibleCallableStatementFalse() throws SQLException {
        String call = "{CALL " + outOfOrderSproc + " (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}";
        String connectionString = TestUtils.addOrOverrideProperty(getConnectionString(),
                "useFlexibleCallableStatements", "false");

        // useFlexibleCallableStatement=false
        // Setting parameters by name
        // Registering output params by index
        // Acquiring output param value 'o1' by name
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            try (CallableStatement cstmt = conn.prepareCall(call)) {
                int scale = 6;
                double obj1 = 2015.0123;
                double obj2 = 2015.012345;
                int obj3 = -3;
                float obj4 = 2015.04f;
                int obj5 = 3;
                String obj6 = "foo";
                String obj7 = "b";
                long obj8 = 2015L;

                cstmt.setObject("i1", obj1, Types.NUMERIC);
                cstmt.setObject("i2", obj2, Types.NUMERIC, scale);
                cstmt.setObject("i3", obj3, Types.INTEGER);
                cstmt.setObject("i4", obj4, Types.FLOAT);

                cstmt.setObject("i5", obj5, Types.CHAR);
                cstmt.setObject("i6", obj6, Types.VARCHAR);
                cstmt.setObject("i7", obj7, Types.CHAR);
                cstmt.setObject("i8", obj8, Types.SMALLINT);

                cstmt.registerOutParameter(9, Types.NUMERIC);
                cstmt.registerOutParameter(10, Types.NUMERIC, scale);
                cstmt.registerOutParameter(11, Types.INTEGER);
                cstmt.registerOutParameter(12, Types.FLOAT);

                cstmt.registerOutParameter(13, Types.CHAR);
                cstmt.registerOutParameter(14, Types.VARCHAR);
                cstmt.registerOutParameter(15, Types.CHAR);
                cstmt.registerOutParameter(16, Types.SMALLINT);
                cstmt.execute();

                assertEquals(obj1, cstmt.getDouble("o1"));
                assertEquals(obj2, cstmt.getDouble(10));
                assertEquals(obj3, cstmt.getInt(11));
                assertEquals(obj4, cstmt.getFloat(12));
                assertEquals(obj5, cstmt.getInt(13));
                assertEquals(obj6, cstmt.getString(14));
                assertEquals(obj7, cstmt.getString(15));
                assertEquals(obj8, cstmt.getLong(16));

                fail(TestResource.getResource("R_expectedFailPassed"));
            } catch (Exception e) {
                assertEquals(TestResource.getResource("R_unknownOutputParameter"), e.getMessage());
            }
        }
    }

    @Test
    public void testNamedParametersAndByIndexErrorUseFlexibleCallableStatementFalse() throws SQLException {
        String call = "{CALL " + outOfOrderSproc + " (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}";
        String connectionString = TestUtils.addOrOverrideProperty(getConnectionString(),
                "useFlexibleCallableStatements", "false");

        // useFlexibleCallableStatement=false
        // Using majority named parameters and setting parameter by index
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            try (CallableStatement cstmt = conn.prepareCall(call)) {
                int scale = 6;
                double obj1 = 2015.0123;
                double obj2 = 2015.012345;
                int obj3 = -3;
                float obj4 = 2015.04f;
                int obj5 = 3;
                String obj6 = "foo";
                String obj7 = "b";
                long obj8 = 2015L;

                cstmt.setObject("i1", obj1, Types.NUMERIC);
                cstmt.setObject("i2", obj2, Types.NUMERIC, scale);
                cstmt.setObject("i3", obj3, Types.INTEGER);
                cstmt.setObject("i4", obj4, Types.FLOAT);

                cstmt.setObject("i5", obj5, Types.CHAR);
                cstmt.setObject("i6", obj6, Types.VARCHAR);
                cstmt.setObject("i7", obj7, Types.CHAR);
                cstmt.setObject(8, obj8, Types.SMALLINT);

                cstmt.registerOutParameter("o1", Types.NUMERIC);
                cstmt.registerOutParameter("o2", Types.NUMERIC, scale);
                cstmt.registerOutParameter("o3", Types.INTEGER);
                cstmt.registerOutParameter("o4", Types.FLOAT);

                cstmt.registerOutParameter("o5", Types.CHAR);
                cstmt.registerOutParameter("o6", Types.VARCHAR);
                cstmt.registerOutParameter("o7", Types.CHAR);
                cstmt.registerOutParameter("o8", Types.SMALLINT);
                cstmt.execute();

                assertEquals(obj1, cstmt.getDouble("o1"));
                assertEquals(obj2, cstmt.getDouble("o2"));
                assertEquals(obj3, cstmt.getInt("o3"));
                assertEquals(obj4, cstmt.getFloat("o4"));
                assertEquals(obj5, cstmt.getInt("o5"));
                assertEquals(obj6, cstmt.getString("o6"));
                assertEquals(obj7, cstmt.getString("o7"));
                assertEquals(obj8, cstmt.getLong("o8"));

                fail(TestResource.getResource("R_expectedFailPassed"));
            } catch (Exception e) {
                assertEquals(TestResource.getResource("R_noNamedAndIndexedParameters"), e.getMessage());
            }
        }
    }

    @Test
    public void testIndexedParametersUseFlexibleCallableStatementFalse() throws SQLException {
        String call = "{CALL " + outOfOrderSproc + " (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}";
        String connectionString = TestUtils.addOrOverrideProperty(getConnectionString(),
                "useFlexibleCallableStatements", "false");

        // useFlexibleCallableStatement=false and using all index parameters
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            try (CallableStatement cstmt = conn.prepareCall(call)) {
                int scale = 6;
                double obj1 = 2015.0123;
                double obj2 = 2015.012345;
                int obj3 = -3;
                float obj4 = 2015.04f;
                int obj5 = 3;
                String obj6 = "foo";
                String obj7 = "b";
                long obj8 = 2015L;

                cstmt.setObject(1, obj1, Types.NUMERIC);
                cstmt.setObject(2, obj2, Types.NUMERIC, scale);
                cstmt.setObject(3, obj3, Types.INTEGER);
                cstmt.setObject(4, obj4, Types.FLOAT);

                cstmt.setObject(5, obj5, Types.CHAR);
                cstmt.setObject(6, obj6, Types.VARCHAR);
                cstmt.setObject(7, obj7, Types.CHAR);
                cstmt.setObject(8, obj8, Types.SMALLINT);

                cstmt.registerOutParameter(9, Types.NUMERIC);
                cstmt.registerOutParameter(10, Types.NUMERIC, scale);
                cstmt.registerOutParameter(11, Types.INTEGER);
                cstmt.registerOutParameter(12, Types.FLOAT);

                cstmt.registerOutParameter(13, Types.CHAR);
                cstmt.registerOutParameter(14, Types.VARCHAR);
                cstmt.registerOutParameter(15, Types.CHAR);
                cstmt.registerOutParameter(16, Types.SMALLINT);
                cstmt.execute();

                assertEquals(obj1, cstmt.getDouble(9));
                assertEquals(obj2, cstmt.getDouble(10));
                assertEquals(obj3, cstmt.getInt(11));
                assertEquals(obj4, cstmt.getFloat(12));
                assertEquals(obj5, cstmt.getInt(13));
                assertEquals(obj6, cstmt.getString(14));
                assertEquals(obj7, cstmt.getString(15));
                assertEquals(obj8, cstmt.getLong(16));
            }
        }
    }

    @Test
    public void testIndexedParametersAcquireOutputValueByNameErrorUseFlexibleCallableStatementFalse() throws SQLException {
        String call = "{CALL " + outOfOrderSproc + " (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}";
        String connectionString = TestUtils.addOrOverrideProperty(getConnectionString(),
                "useFlexibleCallableStatements", "false");

        try (Connection conn = DriverManager.getConnection(connectionString)) {
            try (CallableStatement cstmt = conn.prepareCall(call)) {
                int scale = 6;
                double obj1 = 2015.0123;
                double obj2 = 2015.012345;
                int obj3 = -3;
                float obj4 = 2015.04f;
                int obj5 = 3;
                String obj6 = "foo";
                String obj7 = "b";
                long obj8 = 2015L;

                cstmt.setObject(1, obj1, Types.NUMERIC);
                cstmt.setObject(2, obj2, Types.NUMERIC, scale);
                cstmt.setObject(3, obj3, Types.INTEGER);
                cstmt.setObject(4, obj4, Types.FLOAT);

                cstmt.setObject(5, obj5, Types.CHAR);
                cstmt.setObject(6, obj6, Types.VARCHAR);
                cstmt.setObject(7, obj7, Types.CHAR);
                cstmt.setObject(8, obj8, Types.SMALLINT);

                cstmt.registerOutParameter(9, Types.NUMERIC);
                cstmt.registerOutParameter(10, Types.NUMERIC, scale);
                cstmt.registerOutParameter(11, Types.INTEGER);
                cstmt.registerOutParameter(12, Types.FLOAT);

                cstmt.registerOutParameter(13, Types.CHAR);
                cstmt.registerOutParameter(14, Types.VARCHAR);
                cstmt.registerOutParameter(15, Types.CHAR);
                cstmt.registerOutParameter(16, Types.SMALLINT);
                cstmt.execute();

                assertEquals(obj1, cstmt.getDouble(9));
                assertEquals(obj2, cstmt.getDouble(10));
                assertEquals(obj3, cstmt.getInt(11));
                assertEquals(obj4, cstmt.getFloat(12));
                assertEquals(obj5, cstmt.getInt(13));
                assertEquals(obj6, cstmt.getString(14));
                assertEquals(obj7, cstmt.getString(15));
                assertEquals(obj8, cstmt.getLong("o8"));
                fail(TestResource.getResource("R_expectedFailPassed"));
            } catch (Exception e) {
                assertEquals(TestResource.getResource("R_noNamedAndIndexedParameters"), e.getMessage());
            }
        }
    }

    @Test
    public void testIndexedParametersRegisterOutputParamByNameUseFlexibleCallableStatementFalse() throws SQLException {
        String call = "{CALL " + outOfOrderSproc + " (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}";
        String connectionString = TestUtils.addOrOverrideProperty(getConnectionString(),
                "useFlexibleCallableStatements", "false");

        try (Connection conn = DriverManager.getConnection(connectionString)) {
            try (CallableStatement cstmt = conn.prepareCall(call)) {
                int scale = 6;
                double obj1 = 2015.0123;
                double obj2 = 2015.012345;
                int obj3 = -3;
                float obj4 = 2015.04f;
                int obj5 = 3;
                String obj6 = "foo";
                String obj7 = "b";
                long obj8 = 2015L;

                cstmt.setObject(1, obj1, Types.NUMERIC);
                cstmt.setObject(2, obj2, Types.NUMERIC, scale);
                cstmt.setObject(3, obj3, Types.INTEGER);
                cstmt.setObject(4, obj4, Types.FLOAT);

                cstmt.setObject(5, obj5, Types.CHAR);
                cstmt.setObject(6, obj6, Types.VARCHAR);
                cstmt.setObject(7, obj7, Types.CHAR);
                cstmt.setObject(8, obj8, Types.SMALLINT);

                cstmt.registerOutParameter("o1", Types.NUMERIC);
                cstmt.registerOutParameter(10, Types.NUMERIC, scale);
                cstmt.registerOutParameter(11, Types.INTEGER);
                cstmt.registerOutParameter(12, Types.FLOAT);

                cstmt.registerOutParameter(13, Types.CHAR);
                cstmt.registerOutParameter(14, Types.VARCHAR);
                cstmt.registerOutParameter(15, Types.CHAR);
                cstmt.registerOutParameter(16, Types.SMALLINT);
                cstmt.execute();

                assertEquals(obj1, cstmt.getDouble(9));
                assertEquals(obj2, cstmt.getDouble(10));
                assertEquals(obj3, cstmt.getInt(11));
                assertEquals(obj4, cstmt.getFloat(12));
                assertEquals(obj5, cstmt.getInt(13));
                assertEquals(obj6, cstmt.getString(14));
                assertEquals(obj7, cstmt.getString(15));
                assertEquals(obj8, cstmt.getLong(16));

                fail(TestResource.getResource("R_expectedFailPassed"));
            } catch (Exception e) {
                assertEquals(TestResource.getResource("R_noNamedAndIndexedParameters"), e.getMessage());
            }
        }
    }

    @Test
    public void testExecutingUserDefinedFunctionDirectly() throws SQLException {
        String call = "{? = CALL " + userDefinedFunction + " (?,?,?,?,?,?)}";

        try (CallableStatement cstmt = connection.prepareCall(call)) {
            cstmt.setObject(2, "param");
            cstmt.setObject(3, "param");
            cstmt.setObject(4, "param");
            cstmt.setObject(5, "param");
            cstmt.setObject(6, "param");
            cstmt.setObject(7, "param");
            cstmt.registerOutParameter(1, Types.VARCHAR);
            cstmt.execute();
            assertEquals("foobar", cstmt.getString(1));

            // Re-execute again to test skipping of unread return values on statement close.
            // If it fails, TDS errors will be thrown.
            cstmt.execute();
        }
    }

    @Test
    public void testRegisteringOutputByIndexandAcquiringOutputParamByName() throws SQLException {
        String call = "{CALL " + byParamNameSproc + " (?,?)}";

        // Param names are p1 and p2
        try (CallableStatement cstmt = connection.prepareCall(call)) {
            cstmt.setString(1, "foobar");
            cstmt.registerOutParameter(2, Types.NVARCHAR);
            cstmt.execute();

            assertEquals("foobar", cstmt.getString("p2"));
        }

        try (CallableStatement cstmt = connection.prepareCall(call)) {
            cstmt.setString("p1", "foobar");
            cstmt.registerOutParameter("p2", Types.NVARCHAR);
            cstmt.execute();

            assertEquals("foobar", cstmt.getString("p2"));
        }

        try (CallableStatement cstmt = connection.prepareCall(call)) {
            cstmt.setString("p1", "foobar");
            cstmt.registerOutParameter("p2", Types.NVARCHAR);
            cstmt.execute();

            assertEquals("foobar", cstmt.getString(2));
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
            TestUtils.dropTableIfExists(tableNameGUID, stmt);
            TestUtils.dropTableIfExists(manyParamsTable, stmt);
            TestUtils.dropProcedureIfExists(outputProcedureNameGUID, stmt);
            TestUtils.dropProcedureIfExists(setNullProcedureName, stmt);
            TestUtils.dropProcedureIfExists(inputParamsProcedureName, stmt);
            TestUtils.dropProcedureIfExists(getObjectLocalDateTimeProcedureName, stmt);
            TestUtils.dropProcedureIfExists(getObjectOffsetDateTimeProcedureName, stmt);
            TestUtils.dropProcedureIfExists(zeroParamSproc, stmt);
            TestUtils.dropProcedureIfExists(outOfOrderSproc, stmt);
            TestUtils.dropProcedureIfExists(byParamNameSproc, stmt);
            TestUtils.dropFunctionIfExists(userDefinedFunction, stmt);
        }
    }

    private static void createGUIDStoredProcedure(Statement stmt) throws SQLException {
        String sql = "CREATE PROCEDURE " + outputProcedureNameGUID
                + "(@p1 uniqueidentifier OUTPUT) AS SELECT @p1 = c1 FROM " + tableNameGUID + Constants.SEMI_COLON;
        stmt.execute(sql);
    }

    private static void createGUIDTable(Statement stmt) throws SQLException {
        String sql = "CREATE TABLE " + tableNameGUID + " (c1 uniqueidentifier null)";
        stmt.execute(sql);
    }

    private static void createSetNullProcedure(Statement stmt) throws SQLException {
        stmt.execute("create procedure " + setNullProcedureName
                + " (@p1 nvarchar(255), @p2 nvarchar(255) output) as select @p2=@p1 return 0");
    }

    private static void createInputParamsProcedure(Statement stmt) throws SQLException {
        String sql = "CREATE PROCEDURE " + inputParamsProcedureName + "    @p1 nvarchar(max) = N'parameter1', "
                + "    @p2 nvarchar(max) = N'parameter2' " + "AS " + "BEGIN " + "    SET NOCOUNT ON; "
                + "    SELECT @p1 + @p2 AS result; " + "END ";

        stmt.execute(sql);
    }

    private static void createGetObjectLocalDateTimeProcedure(Statement stmt) throws SQLException {
        String sql = "CREATE PROCEDURE " + getObjectLocalDateTimeProcedureName + "(@p1 datetime2(7) OUTPUT) AS "
                + "SELECT @p1 = '2018-03-11T02:00:00.1234567'";
        stmt.execute(sql);
    }

    private static void createGetObjectOffsetDateTimeProcedure(Statement stmt) throws SQLException {
        String sql = "CREATE PROCEDURE " + getObjectOffsetDateTimeProcedureName
                + "(@p1 DATETIMEOFFSET OUTPUT, @p2 DATETIMEOFFSET OUTPUT) AS "
                + "SELECT @p1 = '2018-01-02T11:22:33.123456700+12:34', @p2 = NULL";
        stmt.execute(sql);
    }

    private static void createProcedureManyParams() throws SQLException {
        String type = manyParamUserDefinedType;
        String sql = "CREATE PROCEDURE " + manyParamProc + " @p1 " + type + ", @p2 " + type + ", @p3 " + type + ", @p4 "
                + type + ", @p5 " + type + ", @p6 " + type + ", @p7 " + type + ", @p8 " + type + ", @p9 " + type
                + ", @p10 " + type + " AS INSERT INTO " + manyParamsTable
                + " VALUES(@p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10)";
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    private static void createTableManyParams() throws SQLException {
        String type = manyParamUserDefinedType;
        String sql = "CREATE TABLE" + manyParamsTable + " (c1 " + type + " null, " + "c2 " + type + " null, " + "c3 "
                + type + " null, " + "c4 " + type + " null, " + "c5 " + type + " null, " + "c6 " + type + " null, "
                + "c7 " + type + " null, " + "c8 " + type + " null, " + "c9 " + type + " null, " + "c10 " + type
                + " null);";
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    private static void createUserDefinedType() throws SQLException {
        String TVPCreateCmd = "CREATE TYPE " + manyParamUserDefinedType + " FROM MONEY";
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(TVPCreateCmd);
        }
    }

    private static void createProcedureZeroParams() throws SQLException {
        String sql = "CREATE PROCEDURE " + zeroParamSproc + " AS RETURN 1";
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    private static void createOutOfOrderSproc() throws SQLException {
        String sql = "CREATE PROCEDURE " + outOfOrderSproc + " @i1 NUMERIC(16,10)," + " @i2 NUMERIC(16,6),"
                + " @i3 INT," + " @i4 REAL," + " @i5 CHAR," + " @i6 VARCHAR(6)," + " @i7 CHAR," + " @i8 SMALLINT, "
                + " @o1 NUMERIC(16,10) OUTPUT," + " @o2 NUMERIC(16,6) OUTPUT," + " @o3 INT OUTPUT,"
                + " @o4 REAL OUTPUT," + " @o5 CHAR OUTPUT," + " @o6 VARCHAR(6) OUTPUT," + " @o7 CHAR OUTPUT,"
                + " @o8 SMALLINT OUTPUT" + " as begin " + " set @o1=@i1;" + " set @o2=@i2;" + " set @o3=@i3;"
                + " set @o4=@i4;" + " set @o5=@i5;" + " set @o6=@i6;" + " set @o7=@i7;" + " set @o8=@i8;" + " end";
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    private static void createByParamNameSproc() throws SQLException {
        String sql = "CREATE PROCEDURE " + byParamNameSproc
                + " (@p1 nvarchar(30), @p2 nvarchar(30) output) AS BEGIN SELECT @p2 = @p1 END;";
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    private static void createUserDefinedFunction() throws SQLException {
        String sql = "CREATE FUNCTION " + userDefinedFunction
                + " (@p0 char(20), @p1 varchar(50), @p2 varchar(max), @p3 nchar(30), @p4 nvarchar(60), @p5 nvarchar(max)) "
                + "RETURNS varchar(50) AS BEGIN " + "DECLARE @ret varchar(50); " + "SELECT @ret = 'foobar'; "
                + " RETURN @ret; end;";
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }
}
