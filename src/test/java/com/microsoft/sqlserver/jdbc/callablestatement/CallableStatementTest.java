package com.microsoft.sqlserver.jdbc.callablestatement;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Types;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collections;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
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
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.AzureDB;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


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
    private static String conditionalSproc = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("CallableStatementTest_conditionalSproc"));
    private static String simpleRetValSproc = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("CallableStatementTest_simpleSproc"));
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
    private static String currentTimeProc = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("currentTime_Procedure"));
    private static String manyParamUserDefinedType = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("manyParam_definedType"));
    private static String zeroParamSproc = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("zeroParamSproc"));
    private static String tableNameJSON = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("TestJSONTable"));
    private static String procedureNameJSON = AbstractSQLGenerator
	    .escapeIdentifier(RandomUtil.getIdentifier("TestJSONProcedure"));
    private static String allOutParamsProcName = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("TestAllOutParams"));
    private static String getObjectTypesProcName = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("TestGetObjectTypesProc"));
    private static String sqlTypeOverloadsProcName = AbstractSQLGenerator
        .escapeIdentifier(RandomUtil.getIdentifier("SQLTypeOverloadsProc"));
    private static String streamGetterSetterProcName = AbstractSQLGenerator
        .escapeIdentifier(RandomUtil.getIdentifier("streamGetterSetterProc"));
    private static String tvpProcName = AbstractSQLGenerator
        .escapeIdentifier(RandomUtil.getIdentifier("TVPProc"));
    private static String tvpTypeName = "TVPType";
    
    /**
     * Setup before test
     * 
     * @throws SQLException
     */
    @BeforeAll
    public static void setupTest() throws Exception {
        setConnection();

        try (Statement stmt = connection.createStatement()) {
            // Drop order matters. Can't drop objects still referenced by other objects.
            TestUtils.dropProcedureIfExists(outputProcedureNameGUID, stmt);
            TestUtils.dropProcedureIfExists(setNullProcedureName, stmt);
            TestUtils.dropProcedureIfExists(inputParamsProcedureName, stmt);
            TestUtils.dropProcedureIfExists(getObjectLocalDateTimeProcedureName, stmt);
            TestUtils.dropProcedureIfExists(getObjectOffsetDateTimeProcedureName, stmt);
            TestUtils.dropProcedureIfExists(conditionalSproc, stmt);
            TestUtils.dropProcedureIfExists(simpleRetValSproc, stmt);
            TestUtils.dropProcedureIfExists(zeroParamSproc, stmt);
            TestUtils.dropProcedureIfExists(manyParamProc, stmt);
            TestUtils.dropProcedureIfExists(procedureNameJSON, stmt);
            TestUtils.dropTableIfExists(tableNameGUID, stmt);
            TestUtils.dropTableIfExists(manyParamsTable, stmt);
            TestUtils.dropTableIfExists(tableNameJSON, stmt);
            TestUtils.dropUserDefinedTypeIfExists(manyParamUserDefinedType, stmt);
            TestUtils.dropTableIfExists(tableNameJSON, stmt);
			TestUtils.dropProcedureIfExists(procedureNameJSON, stmt);

            createGUIDTable(stmt);
            createGUIDStoredProcedure(stmt);
            createSetNullProcedure(stmt);
            createInputParamsProcedure(stmt);
            createGetObjectLocalDateTimeProcedure(stmt);
            createUserDefinedType();
            createTableManyParams();
            createProcedureManyParams();
            createProcedureZeroParams();
            createProcedureCurrentTime();
            createGetObjectOffsetDateTimeProcedure(stmt);
            createConditionalProcedure();
            createSimpleRetValSproc();
        }
    }

    // Test Needs more work to be configured to run on azureDB as there are slight differences
    // between the regular SQL Server vs. azureDB
    @Test
    @Tag(Constants.xAzureSQLDB)
    public void testCallableStatementManyParameters() throws SQLException {
        String tempPass = UUID.randomUUID().toString();
        String loginName = "JDBCLogin_" + UUID.randomUUID().toString().replace("-", "");
        String userName = "JDBCUser_" + UUID.randomUUID().toString().replace("-", "");

        // Need to create a user with limited permissions in order to run through the code block we are testing
        // The user created will execute sp_sproc_columns internally by the driver, which should not return all
        // the column names as the user has limited permissions
        try (Connection conn = PrepUtil.getConnection(connectionString)) {
            String databaseName = conn.getCatalog();
            String dropUser = "IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = '" + userName
                    + "') DROP USER [" + userName + "]";
            String dropLogin = "IF EXISTS (SELECT 1 FROM sys.sql_logins WHERE name = '" + loginName
                    + "') DROP LOGIN [" + loginName + "]";
            String createLogin = "CREATE LOGIN [" + loginName + "] WITH PASSWORD=N'" + tempPass
                    + "', DEFAULT_DATABASE = [" + databaseName
                    + "], DEFAULT_LANGUAGE = US_ENGLISH, CHECK_POLICY = OFF, CHECK_EXPIRATION = OFF;";
            String createUser = "CREATE USER [" + userName + "] FOR LOGIN [" + loginName
                    + "] WITH DEFAULT_SCHEMA = [dbo];";
            String grantExecute = "GRANT EXECUTE ON " + manyParamProc + " TO [" + userName + "];";

            try (Statement stmt = conn.createStatement()) {
                stmt.execute(dropUser);
                stmt.execute(dropLogin);
                stmt.execute(createLogin);
                stmt.execute(createUser);
                stmt.execute(grantExecute);
            }
        }

        try (Connection conn = PrepUtil.getConnection(
                connectionString + ";user=" + loginName + ";password=" + tempPass + ";")) {
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

        // Clean up the temporary principal after execution
        try (Connection conn = PrepUtil.getConnection(connectionString);
                Statement stmt = conn.createStatement()) {
            stmt.execute("IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = '" + userName
                    + "') DROP USER [" + userName + "]");
            stmt.execute("IF EXISTS (SELECT 1 FROM sys.sql_logins WHERE name = '" + loginName
                    + "') DROP LOGIN [" + loginName + "]");
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
            } finally {
                TestUtils.dropProcedureIfExists(procName, statement);
            }
        }
    }

    @Test
    public void testCallableStatementExec() throws SQLException {
        connection.setPrepareMethod("exec");

        try (Statement statement = connection.createStatement();) {
            statement.executeUpdate("create procedure " + procName + " as select 1 --");

            try (CallableStatement callableStatement = connection.prepareCall("{call " + procName + "}")) {
                try (ResultSet rs = callableStatement.executeQuery()) {
                    rs.next();
                    assertEquals(1, rs.getInt(1), TestResource.getResource("R_setDataNotEqual"));
                }

                try (ResultSet rs = callableStatement.executeQuery()) {
                    rs.next();
                    assertEquals(1, rs.getInt(1), TestResource.getResource("R_setDataNotEqual"));
                }
            } finally {
                TestUtils.dropProcedureIfExists(procName, statement);
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

        // Test zero parameter sproc with return value with parentheses
        call = "{? = CALL " + zeroParamSproc + "()}";

        try (CallableStatement cs = connection.prepareCall(call)) {
            cs.registerOutParameter(1, Types.INTEGER);
            cs.execute();
            // Calling zero parameter sproc with return value with parentheses
            // should return a value that's not zero
            assertEquals(1, cs.getInt(1));
        }
    }

    @Test
    public void testExecuteSystemStoredProcedureNamedParametersAndIndexedParameterNoResultset() throws SQLException {
        String call0 = "EXEC sp_getapplock @Resource=?, @LockTimeout='0', @LockMode='Exclusive', @LockOwner='Session'";
        String call1 = "\rEXEC\r\rsp_getapplock @Resource=?, @LockTimeout='0', @LockMode='Exclusive', @LockOwner='Session'";
        String call2 = "  EXEC   sp_getapplock @Resource=?, @LockTimeout='0', @LockMode='Exclusive', @LockOwner='Session'";
        String call3 = "\tEXEC\t\t\tsp_getapplock @Resource=?, @LockTimeout='0', @LockMode='Exclusive', @LockOwner='Session'";

        try (CallableStatement cstmt0 = connection.prepareCall(call0);
                CallableStatement cstmt1 = connection.prepareCall(call1);
                CallableStatement cstmt2 = connection.prepareCall(call2);
                CallableStatement cstmt3 = connection.prepareCall(call3);) {
            cstmt0.setString(1, "Resource-" + UUID.randomUUID());
            cstmt0.execute();

            cstmt1.setString(1, "Resource-" + UUID.randomUUID());
            cstmt1.execute();

            cstmt2.setString(1, "Resource-" + UUID.randomUUID());
            cstmt2.execute();

            cstmt3.setString(1, "Resource-" + UUID.randomUUID());
            cstmt3.execute();
        }
    }

    @Test
    public void testExecSystemStoredProcedureNamedParametersAndIndexedParameterResultSet() throws SQLException {
        String call = "exec sp_sproc_columns_100 ?, @ODBCVer=3, @fUsePattern=0";

        try (CallableStatement cstmt = connection.prepareCall(call)) {
            cstmt.setString(1, "sp_getapplock");

            try (ResultSet rs = cstmt.executeQuery()) {
                while (rs.next()) {
                    assertTrue(TestResource.getResource("R_resultSetEmpty"), !rs.getString(4).isEmpty());
                }
            }
        }
    }

    @Test
    public void testExecSystemStoredProcedureNoIndexedParametersResultSet() throws SQLException {
        String call = "execute sp_sproc_columns_100 sp_getapplock, @ODBCVer=3, @fUsePattern=0";

        try (CallableStatement cstmt = connection.prepareCall(call); ResultSet rs = cstmt.executeQuery()) {
            while (rs.next()) {
                assertTrue(TestResource.getResource("R_resultSetEmpty"), !rs.getString(4).isEmpty());
            }
        }
    }

    @Test
    public void testExecDocumentedSystemStoredProceduresIndexedParameters() throws SQLException {
        String serverName;
        String testTableName = "testTable";
        Integer integer = new Integer(1);

        try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery("SELECT @@SERVERNAME")) {
            rs.next();
            serverName = rs.getString(1);
        }

        String[] sprocs = {"EXEC sp_column_privileges ?", "exec sp_catalogs ?", "execute sp_column_privileges ?",
                "EXEC sp_column_privileges_ex ?", "EXECUTE sp_columns ?", "execute sp_datatype_info ?",
                "EXEC sp_sproc_columns ?", "EXECUTE sp_server_info ?", "exec sp_special_columns ?",
                "execute sp_statistics ?", "EXEC sp_table_privileges ?", "exec sp_tables ?"};

        Object[] params = {testTableName, serverName, testTableName, serverName, testTableName, integer,
                "sp_column_privileges", integer, testTableName, testTableName, testTableName, testTableName};

        int paramIndex = 0;

        for (String sproc : sprocs) {
            try (CallableStatement cstmt = connection.prepareCall(sproc)) {
                cstmt.setObject(1, params[paramIndex]);
                cstmt.execute();
                paramIndex++;
            } catch (Exception e) {
                fail("Failed executing '" + sproc + "' with indexed parameter '" + params[paramIndex]);
            }
        }
    }

    @Test
    public void testCallableStatementDefaultValues() throws SQLException {
        String call0 = "{call " + conditionalSproc + " (?, ?, 1)}";
        String call1 = "{call " + conditionalSproc + " (?, ?, 2)}";
        int expectedValue = 5; // The sproc should return this value

        try (CallableStatement cstmt = connection.prepareCall(call0)) {
            cstmt.setInt(1, 1);
            cstmt.setInt(2, 2);
            cstmt.execute();
            ResultSet rs = cstmt.getResultSet();
            rs.next();
            fail(TestResource.getResource("R_expectedFailPassed"));

        } catch (Exception e) {
            String msg = e.getMessage();
            assertTrue(TestResource
                    .getResource("R_nullPointerExceptionFromResultSet").equalsIgnoreCase(msg)
                    || msg == null);
        }

        try (CallableStatement cstmt = connection.prepareCall(call1)) {
            cstmt.setInt(1, 1);
            cstmt.setInt(2, 2);
            cstmt.execute();
            ResultSet rs = cstmt.getResultSet();
            rs.next();

            assertEquals(Integer.toString(expectedValue), rs.getString(1));
        }
    }

    @Test
    public void testCallableStatementSetByAnnotatedArgs() throws SQLException {
        String call = "{? = call " + simpleRetValSproc + " (@Arg1 = ?)}";
        int expectedValue = 1; // The sproc should return this value

        try (CallableStatement cstmt = connection.prepareCall(call)) {
            cstmt.registerOutParameter(1, Types.INTEGER);
            cstmt.setInt(1, 2);
            cstmt.setString(2, "foo");
            cstmt.execute();

            Assert.assertEquals(expectedValue, cstmt.getInt(1));
        }
    }

    // @Test
    // @Tag(Constants.reqExternalSetup)
    // @Tag(Constants.xAzureSQLDB)
    // @Tag(Constants.xAzureSQLDW)
    // @Tag(Constants.xAzureSQLMI)
    // public void testFourPartSyntaxCallEscapeSyntax() throws SQLException {
    //     String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("serverList"));
    //     try {

    //         try (Statement stmt = connection.createStatement()) {
    //             TestUtils.dropTableIfExists(table, stmt);
    //             stmt.execute("CREATE TABLE " + table
    //                     + " (serverName varchar(100),network varchar(100),serverStatus varchar(4000), id int, collation varchar(100), connectTimeout int, queryTimeout int)");
    //             stmt.execute("INSERT " + table + " EXEC sp_helpserver");

    //             ResultSet rs = stmt
    //                     .executeQuery("SELECT COUNT(*) FROM " + table + " WHERE serverName = N'" + linkedServer + "'");
    //             rs.next();

    //             if (rs.getInt(1) == 1) {
    //                 // Drop all linked logins associated with the server
    //                 stmt.execute("EXEC sp_dropserver @server='" + linkedServer + "', @droplogins='droplogins';");
    //             }

    //             stmt.execute("EXEC sp_addlinkedserver @server='" + linkedServer + "';");
    //             stmt.execute("EXEC sp_addlinkedsrvlogin @rmtsrvname=N'" + linkedServer + "', @useself=false"
    //                     + ", @rmtuser=N'" + linkedServerUser + "', @rmtpassword=N'" + linkedServerPassword + "'");
    //             stmt.execute("EXEC sp_serveroption '" + linkedServer + "', 'rpc', true;");
    //             stmt.execute("EXEC sp_serveroption '" + linkedServer + "', 'rpc out', true;");
    //         }

    //         SQLServerDataSource ds = new SQLServerDataSource();
    //         ds.setServerName(linkedServer);
    //         ds.setUser(linkedServerUser);
    //         ds.setPassword(linkedServerPassword);
    //         ds.setEncrypt(false);
    //         ds.setTrustServerCertificate(true);

    //         try (Connection linkedServerConnection = ds.getConnection();
    //                 Statement stmt = linkedServerConnection.createStatement()) {
    //             stmt.execute(
    //                     "create or alter procedure dbo.TestAdd(@Num1 int, @Num2 int, @Result int output) as begin set @Result = @Num1 + @Num2; end;");

    //             stmt.execute("create or alter procedure dbo.TestReturn(@Num1 int) as select @Num1 return @Num1*3  ");
    //         }

    //         try (CallableStatement cstmt = connection
    //                 .prepareCall("{call [" + linkedServer + "].master.dbo.TestAdd(?,?,?)}")) {
    //             int sum = 11;
    //             int param0 = 1;
    //             int param1 = 10;
    //             cstmt.setInt(1, param0);
    //             cstmt.setInt(2, param1);
    //             cstmt.registerOutParameter(3, Types.INTEGER);
    //             cstmt.execute();
    //             assertEquals(sum, cstmt.getInt(3));
    //         }

    //         try (CallableStatement cstmt = connection
    //                 .prepareCall("exec [" + linkedServer + "].master.dbo.TestAdd ?,?,?")) {
    //             int sum = 11;
    //             int param0 = 1;
    //             int param1 = 10;
    //             cstmt.setInt(1, param0);
    //             cstmt.setInt(2, param1);
    //             cstmt.registerOutParameter(3, Types.INTEGER);
    //             cstmt.execute();
    //             assertEquals(sum, cstmt.getInt(3));
    //         }

    //         try (CallableStatement cstmt = connection
    //                 .prepareCall("{? = call [" + linkedServer + "].master.dbo.TestReturn(?)}")) {
    //             int expected = 15;
    //             cstmt.registerOutParameter(1, java.sql.Types.INTEGER);
    //             cstmt.setInt(2, 5);
    //             cstmt.execute();
    //             assertEquals(expected, cstmt.getInt(1));
    //         }
    //     } finally {
    //         try (Statement stmt = connection.createStatement()) {
    //             TestUtils.dropTableIfExists(table, stmt);
    //         }
    //     }
    // }

    @Test
    public void testTimestampStringConversion() throws SQLException {
        try (CallableStatement stmt = connection.prepareCall("{call " + currentTimeProc + "(?)}")) {
            String timestamp = "2024-05-29 15:35:53.461";
            stmt.setObject(1, timestamp, Types.TIMESTAMP);
            stmt.registerOutParameter(1, Types.TIMESTAMP);
            stmt.execute();
            stmt.getObject("currentTimeStamp");
        }
    }
    
    /**
     * Tests JSON column in a table with setObject
     * 
     * @throws SQLException
     */
    @Test
    @AzureDB
    @Tag(Constants.JSONTest)
	public void testJSONColumnInTableWithSetObject() throws SQLException {

		try (Connection con = DriverManager.getConnection(connectionString); Statement stmt = con.createStatement()) {
            createJSONTestTable(stmt);
			String jsonString = "{\"key\":\"value\"}";
			try (CallableStatement callableStatement = con
					.prepareCall("INSERT INTO " + tableNameJSON + " (col1) VALUES (?)")) {
				callableStatement.setObject(1, jsonString);
				callableStatement.execute();
			}

			try (Statement queryStmt = con.createStatement();
					ResultSet rs = queryStmt.executeQuery("SELECT col1 FROM " + tableNameJSON)) {
				assertTrue(rs.next());
				assertEquals(jsonString, rs.getObject(1));
			}
		}
	}

	@Test
    @Tag(Constants.JSONTest)
	public void testJSONProcedureWithSetObject() throws SQLException {

		try (Connection con = DriverManager.getConnection(connectionString); Statement stmt = con.createStatement()) {
			createJSONStoredProcedure(stmt);
            String jsonString = "{\"key\":\"value\"}";
			try (CallableStatement callableStatement = con.prepareCall("{call " + procedureNameJSON + " (?)}")) {
				callableStatement.setObject(1, jsonString);
				callableStatement.execute();

				try (ResultSet rs = callableStatement.getResultSet()) {
					assertTrue(rs.next());
					assertEquals(jsonString, rs.getObject("col1"));
				}
			}
		}
	}

    @Test
    @Tag(Constants.CodeCov)
    public void testAllOutParamGettersByName() throws Exception {
        TestUtils.dropProcedureIfExists(allOutParamsProcName, connection.createStatement());

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                "CREATE PROCEDURE " + allOutParamsProcName + " " +
                "@char CHAR(10) OUTPUT, @nchar NCHAR(10) OUTPUT, @bit BIT OUTPUT, @tinyint TINYINT OUTPUT, " +
                "@binary BINARY(4) OUTPUT, @date DATE OUTPUT, @float FLOAT OUTPUT, @real REAL OUTPUT, " +
                "@bigint BIGINT OUTPUT, @int INT OUTPUT, @smallint SMALLINT OUTPUT, @decimal DECIMAL(10,2) OUTPUT, " +
                "@money MONEY OUTPUT, @smallmoney SMALLMONEY OUTPUT, @varbinary VARBINARY(MAX) OUTPUT, " +
                "@blob VARBINARY(MAX) OUTPUT, @clob VARCHAR(MAX) OUTPUT, @nclob NVARCHAR(MAX) OUTPUT " +
                "AS BEGIN RETURN END"
            );
        }

        try (SQLServerCallableStatement cs = (SQLServerCallableStatement) connection.prepareCall(
                "{call " + allOutParamsProcName + "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}")) {

            cs.setString("char", "charValue", false);
            cs.setString("nchar", "hello     ");
            cs.setBoolean("bit", true, false);
            cs.setByte("tinyint", (byte) 42, false);
            cs.setBytes("binary", new byte[]{1, 2, 3, 4}, false);
            cs.setDate("date", Date.valueOf("2024-07-16"));
            cs.setDouble("float", 123.456, false);
            cs.setFloat("real", 78.9f, false);
            cs.setLong("bigint", 9876543210L, false);
            cs.setInt("int", 12345, false);
            cs.setShort("smallint", (short) 123, false);
            cs.setBigDecimal("decimal", new BigDecimal("123.45"), 5, 2);
            cs.setBigDecimal("money", new BigDecimal("999.99"), 5, 2);
            cs.setBigDecimal("money", new BigDecimal("999.99"), 5, 2, false);
            cs.setMoney("money", new BigDecimal("999.99"));
            cs.setMoney("money", new BigDecimal("999.99"), false);
            cs.setBigDecimal("smallmoney", new BigDecimal("55.55"));
            cs.setSmallMoney("smallmoney", new BigDecimal("55.55"));
            cs.setSmallMoney("smallmoney", new BigDecimal("55.55"), false);
            cs.setBytes("varbinary", new byte[]{0x11, 0x22, 0x33, 0x44});
            cs.setBytes("blob", new byte[]{0x55, 0x66, 0x77, (byte) 0x88});
            cs.setClob("clob", new javax.sql.rowset.serial.SerialClob("ascii-stream".toCharArray()));
            cs.setNClob("nclob", new java.io.StringReader("nchar-stream"));

            for (int i = 1; i <= 18; i++) {
                cs.registerOutParameter(i, Types.VARCHAR);
            }
            cs.registerOutParameter(1, Types.CHAR);
            cs.registerOutParameter(2, Types.NCHAR);
            cs.registerOutParameter(3, Types.BIT);
            cs.registerOutParameter(4, Types.TINYINT);
            cs.registerOutParameter(5, Types.BINARY);
            cs.registerOutParameter(6, Types.DATE);
            cs.registerOutParameter(7, Types.DOUBLE);
            cs.registerOutParameter(8, Types.REAL);
            cs.registerOutParameter(9, Types.BIGINT);
            cs.registerOutParameter(10, Types.INTEGER);
            cs.registerOutParameter(11, Types.SMALLINT);
            cs.registerOutParameter(12, Types.DECIMAL);
            cs.registerOutParameter(13, Types.DECIMAL);
            cs.registerOutParameter(14, microsoft.sql.Types.SMALLMONEY);
            cs.registerOutParameter(15, Types.VARBINARY);
            cs.registerOutParameter(16, Types.VARBINARY);
            cs.registerOutParameter(17, Types.VARCHAR);
            cs.registerOutParameter(18, Types.NVARCHAR);

            cs.execute();

            assertEquals("charValue ", cs.getString("char")); // Covers getString(String)
            assertEquals("hello     ", cs.getNString("nchar"));
            assertTrue(cs.getBoolean("bit"));
            assertEquals((byte) 42, cs.getByte("tinyint"));
            assertArrayEquals(new byte[]{1, 2, 3, 4}, cs.getBytes("binary"));
            assertEquals(Date.valueOf("2024-07-16"), cs.getDate("date"));
            assertEquals(123.456, cs.getDouble("float"), 0.0001);
            assertEquals(78.9f, cs.getFloat("real"), 0.0001f);
            assertEquals(9876543210L, cs.getLong("bigint"));
            assertEquals(12345, cs.getInt("int"));
            assertEquals((short) 123, cs.getShort("smallint"));
            assertEquals(0, cs.getBigDecimal("decimal").compareTo(new BigDecimal("123.45")));
            assertEquals(0, cs.getMoney("money").compareTo(new BigDecimal("999.99")));
            assertEquals(0, cs.getSmallMoney("smallmoney").compareTo(new BigDecimal("55.55")));
            try (InputStream is = cs.getBinaryStream("varbinary");
                ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
                
                byte[] temp = new byte[1024];
                int bytesRead;
                while ((bytesRead = is.read(temp)) != -1) {
                    buffer.write(temp, 0, bytesRead);
                }

                byte[] actualBytes = buffer.toByteArray();
                assertArrayEquals(new byte[]{0x11, 0x22, 0x33, 0x44}, actualBytes);
            }

            Blob blob = cs.getBlob("blob");
            assertArrayEquals(new byte[]{0x55, 0x66, 0x77, (byte) 0x88}, blob.getBytes(1, (int) blob.length()));

            try (Reader reader = cs.getCharacterStream("clob")) {
                assertEquals("ascii-stream", new BufferedReader(reader).readLine());
            }

            try (Reader reader = cs.getNCharacterStream("nclob")) {
                assertEquals("nchar-stream", new BufferedReader(reader).readLine());
            }

            // Covers getObject(String, Map)
            assertThrows(SQLException.class, () -> cs.getObject("char", Collections.emptyMap()));

            // Covers getRef(String)
            assertThrows(SQLException.class, () -> cs.getRef("char"));

            // Covers getArray(String)
            assertThrows(SQLException.class, () -> cs.getArray("char"));
        }

        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropProcedureIfExists(allOutParamsProcName, stmt);
        }
    }

    @Test
    @Tag(Constants.CodeCov)
    public void testAllSettersWithParameterName() throws Exception {
        TestUtils.dropProcedureIfExists(streamGetterSetterProcName, connection.createStatement());

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                "CREATE PROCEDURE " + streamGetterSetterProcName + " " +
                "@asciiStream VARCHAR(MAX), " +
                "@binaryStream VARBINARY(MAX), " +
                "@blob VARBINARY(MAX), " +
                "@clob VARCHAR(MAX), " +
                "@nclob NVARCHAR(MAX) OUTPUT, " +         // mark as OUTPUT
                "@nstring XML OUTPUT " +        // mark as OUTPUT
                "AS BEGIN " +
                "SELECT @asciiStream AS asciiStream, @binaryStream AS binaryStream, @blob AS blob, " +
                "@clob AS clob, @nclob AS nclob, @nstring AS nstring; " +
                "END"
            );

        }

        try (SQLServerCallableStatement cs = (SQLServerCallableStatement)
                connection.prepareCall("{call " + streamGetterSetterProcName + " (?, ?, ?, ?, ?, ?)}")) {

            byte[] bytes = "binary-data".getBytes(StandardCharsets.UTF_8);
            InputStream binaryStream = new ByteArrayInputStream(bytes);
            InputStream asciiStream = new ByteArrayInputStream("ascii".getBytes(StandardCharsets.US_ASCII));
            Blob blob = connection.createBlob();
            blob.setBytes(1, bytes);

            Reader clobReader = new StringReader("clob data");
            NClob nclob = connection.createNClob();
            nclob.setString(1, "nclob string");

            SQLXML sqlxml = connection.createSQLXML();
            sqlxml.setString("<root>xml</root>");

            // Set values
            cs.setAsciiStream("asciiStream", asciiStream);
            cs.setBinaryStream("binaryStream", binaryStream);
            cs.setBlob("blob", blob);
            cs.setClob("clob", clobReader);
            cs.setNClob("nclob", nclob);
            cs.setNString("nstring", "nstringValue");

            // Additional API coverage
            cs.setCharacterStream("clob", new StringReader("updated clob"));
            cs.setCharacterStream("clob", new StringReader("updated clob"), 12);
            cs.setNCharacterStream("nclob", new StringReader("updated nclob"));
            cs.setNCharacterStream("nclob", new StringReader("updated nclob"), 13L);
            cs.setNull("nstring", Types.NVARCHAR);
            cs.setSQLXML("nstring", sqlxml); // overwrite nstring

            cs.registerOutParameter("nclob", Types.NVARCHAR);
            cs.registerOutParameter("nstring", Types.SQLXML);

            cs.execute();


            // Validate outputs
            String returnedNString = cs.getNString("nclob");
            assertNotNull(returnedNString);
            assertTrue(returnedNString.contains("updated"));

            SQLXML returnedXML = cs.getSQLXML("nstring");
            assertNotNull(returnedXML);
            assertEquals("<root>xml</root>", returnedXML.getString());
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropProcedureIfExists(streamGetterSetterProcName, stmt);
            }
        }
    }

    @Test
    @Tag(Constants.CodeCov)
    public void testAllOutParamGettersByIndex() throws Exception {
        TestUtils.dropProcedureIfExists(allOutParamsProcName, connection.createStatement());

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                "CREATE PROCEDURE " + allOutParamsProcName + " " +
                "@char CHAR(10) OUTPUT, @nchar NCHAR(10) OUTPUT, @bit BIT OUTPUT, @tinyint TINYINT OUTPUT, " +
                "@binary BINARY(4) OUTPUT, @date DATE OUTPUT, @float FLOAT OUTPUT, @real REAL OUTPUT, " +
                "@bigint BIGINT OUTPUT, @int INT OUTPUT, @smallint SMALLINT OUTPUT, @decimal DECIMAL(10,2) OUTPUT, " +
                "@money MONEY OUTPUT, @smallmoney SMALLMONEY OUTPUT, @varbinary VARBINARY(MAX) OUTPUT, " +
                "@blob VARBINARY(MAX) OUTPUT, @clob VARCHAR(MAX) OUTPUT, @nclob NVARCHAR(MAX) OUTPUT " +
                "AS BEGIN RETURN END"
            );
        }

        try (SQLServerCallableStatement cs = (SQLServerCallableStatement) connection.prepareCall(
                "{call " + allOutParamsProcName + "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}")) {

            cs.setObject(1, "charValue ", Types.CHAR);
            cs.setObject(2, "hello     ", Types.NCHAR);
            cs.setObject(3, true, Types.BIT);
            cs.setObject(4, (byte) 42, Types.TINYINT);
            cs.setObject(5, new byte[]{1, 2, 3, 4}, Types.BINARY);
            cs.setObject(6, Date.valueOf("2024-07-16"), Types.DATE);
            cs.setObject(7, 123.456, Types.DOUBLE);
            cs.setObject(8, 78.9f, Types.REAL);
            cs.setObject(9, 9876543210L, Types.BIGINT);
            cs.setObject(10, 12345, Types.INTEGER);
            cs.setObject(11, (short) 123, Types.SMALLINT);
            cs.setObject(12, new BigDecimal("123.45"), Types.DECIMAL);
            cs.setObject(13, new BigDecimal("999.99"), Types.DECIMAL);
            cs.setObject(14, new BigDecimal("55.55"), microsoft.sql.Types.SMALLMONEY);
            cs.setObject(15, new byte[]{0x11, 0x22, 0x33, 0x44}, Types.VARBINARY);
            cs.setObject(16, new byte[]{0x55, 0x66, 0x77, (byte) 0x88}, Types.VARBINARY);
            cs.setClob(17, new javax.sql.rowset.serial.SerialClob("ascii-stream".toCharArray()));
            cs.setNClob(18, new java.io.StringReader("nchar-stream"));

            for (int i = 1; i <= 18; i++) {
                cs.registerOutParameter(i, Types.VARCHAR);
            }
            cs.registerOutParameter(1, Types.CHAR);
            cs.registerOutParameter(2, Types.NCHAR);
            cs.registerOutParameter(3, Types.BIT);
            cs.registerOutParameter(4, Types.TINYINT);
            cs.registerOutParameter(5, Types.BINARY);
            cs.registerOutParameter(6, Types.DATE);
            cs.registerOutParameter(7, Types.DOUBLE);
            cs.registerOutParameter(8, Types.REAL);
            cs.registerOutParameter(9, Types.BIGINT);
            cs.registerOutParameter(10, Types.INTEGER);
            cs.registerOutParameter(11, Types.SMALLINT);
            cs.registerOutParameter(12, Types.DECIMAL);
            cs.registerOutParameter(13, Types.DECIMAL);
            cs.registerOutParameter(14, microsoft.sql.Types.SMALLMONEY);
            cs.registerOutParameter(15, Types.VARBINARY);
            cs.registerOutParameter(16, Types.VARBINARY);
            cs.registerOutParameter(17, Types.VARCHAR);
            cs.registerOutParameter(18, Types.NVARCHAR);

            cs.execute();

            assertEquals("charValue ", cs.getString(1));
            assertEquals("hello     ", cs.getNString(2));
            assertTrue(cs.getBoolean(3));
            assertEquals((byte) 42, cs.getByte(4));
            assertArrayEquals(new byte[]{1, 2, 3, 4}, cs.getBytes(5));
            assertEquals(Date.valueOf("2024-07-16"), cs.getDate(6));
            assertEquals(123.456, cs.getDouble(7), 0.0001);
            assertEquals(78.9f, cs.getFloat(8), 0.0001f);
            assertEquals(9876543210L, cs.getLong(9));
            assertEquals(12345, cs.getInt(10));
            assertEquals((short) 123, cs.getShort(11));
            assertEquals(0, cs.getBigDecimal(12).compareTo(new BigDecimal("123.45")));
            assertEquals(0, cs.getMoney(13).compareTo(new BigDecimal("999.99")));
            assertEquals(0, cs.getSmallMoney(14).compareTo(new BigDecimal("55.55")));

            try (InputStream is = cs.getBinaryStream(15);
                ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
                
                byte[] temp = new byte[1024];
                int bytesRead;
                while ((bytesRead = is.read(temp)) != -1) {
                    buffer.write(temp, 0, bytesRead);
                }

                byte[] actualBytes = buffer.toByteArray();
                assertArrayEquals(new byte[]{0x11, 0x22, 0x33, 0x44}, actualBytes);
            }


            Blob blob = cs.getBlob(16);
            assertArrayEquals(new byte[]{0x55, 0x66, 0x77, (byte) 0x88}, blob.getBytes(1, (int) blob.length()));

            try (Reader reader = cs.getCharacterStream(17)) {
                assertEquals("ascii-stream", new BufferedReader(reader).readLine());
            }

            try (Reader reader = cs.getNCharacterStream(18)) {
                assertEquals("nchar-stream", new BufferedReader(reader).readLine());
            }

            // Covers getObject(int, Map)
            assertThrows(SQLException.class, () -> cs.getObject(1, Collections.emptyMap()));

            // Covers getRef(int)
            assertThrows(SQLException.class, () -> cs.getRef(1));

            // Covers getArray(int)
            assertThrows(SQLException.class, () -> cs.getArray(1));
        }

        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropProcedureIfExists(allOutParamsProcName, stmt);
        }
    }



    @Test
    @Tag(Constants.CodeCov)
    public void testGetObjectVariousTypes() throws SQLException {
        TestUtils.dropProcedureIfExists(getObjectTypesProcName, connection.createStatement());
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                "CREATE PROCEDURE " + getObjectTypesProcName +
                " @tinyint TINYINT OUTPUT, @smallint SMALLINT OUTPUT, @bigint BIGINT OUTPUT, @decimal DECIMAL(10,2) OUTPUT, @bit BIT OUTPUT, @ldt DATETIME2 OUTPUT AS " +
                "BEGIN " +
                // Assign output params from their current values (set by input)
                "SELECT @tinyint = @tinyint, @smallint = @smallint, @bigint = @bigint, @decimal = @decimal, @bit = @bit, @ldt = @ldt " +
                "END"
            );
        }
        try (SQLServerCallableStatement cs = (SQLServerCallableStatement) connection.prepareCall(
                "{call " + getObjectTypesProcName + "(?,?,?,?,?,?)}")) {
            // Register out parameters by name
            cs.registerOutParameter("tinyint", Types.TINYINT);
            cs.registerOutParameter("smallint", Types.SMALLINT);
            cs.registerOutParameter("bigint", Types.BIGINT);
            cs.registerOutParameter("decimal", Types.DECIMAL);
            cs.registerOutParameter("bit", Types.BIT);
            cs.registerOutParameter("ldt", Types.TIMESTAMP);

            // Use all setObject overloads with parameter names
            cs.setObject("tinyint", (byte) 7); // setObject(String, Object)
            cs.setObject("smallint", (short) 123, Types.SMALLINT); // setObject(String, Object, int)
            cs.setObject("bigint", 9876543210L, Types.BIGINT, 0); // setObject(String, Object, int, int)
            cs.setObject("decimal", new BigDecimal("123.45"), Types.DECIMAL, 2, false); // setObject(String, Object, int, int, boolean)
            cs.setObject("bit", true, Types.BIT, null, 0); // setObject(String, Object, int, Integer, int)
            cs.setObject("ldt", null); // setObject(String, Object) with null

            cs.execute();

            // Byte.class
            Byte byteVal = cs.getObject("tinyint", Byte.class);
            assertEquals(Byte.valueOf((byte)7), byteVal);

            // Short.class
            Short shortVal = cs.getObject("smallint", Short.class);
            assertEquals(Short.valueOf((short)123), shortVal);

            // Long.class
            Long longVal = cs.getObject("bigint", Long.class);
            assertEquals(Long.valueOf(9876543210L), longVal);

            // BigDecimal.class
            BigDecimal bdVal = cs.getObject("decimal", BigDecimal.class);
            assertEquals(0, bdVal.compareTo(new BigDecimal("123.45")));

            // Boolean.class
            Boolean boolVal = cs.getObject("bit", Boolean.class);
            assertEquals(Boolean.TRUE, boolVal);

            // LocalDateTime.class (null case)
            LocalDateTime ldtVal = cs.getObject("ldt", LocalDateTime.class);
            assertNull(ldtVal);
        }
        TestUtils.dropProcedureIfExists(getObjectTypesProcName, connection.createStatement());
    }

    @Test
    @Tag(Constants.CodeCov)
    public void testSQLTypeOverloads() throws Exception {
        
        TestUtils.dropProcedureIfExists(sqlTypeOverloadsProcName, connection.createStatement());

        // Simple procedure with one INOUT parameter
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                "CREATE PROCEDURE " + sqlTypeOverloadsProcName + " @val INT OUTPUT AS BEGIN SET @val = @val + 1 END"
            );
        }

        try (SQLServerCallableStatement cs = (SQLServerCallableStatement) connection.prepareCall(
                "{call " + sqlTypeOverloadsProcName + "(?)}")) {
            // Test setObject with SQLType
            cs.setObject(1, 41, java.sql.JDBCType.INTEGER);
            // Test registerOutParameter with SQLType
            cs.registerOutParameter(1, java.sql.JDBCType.INTEGER);
            cs.execute();
            assertEquals(42, cs.getInt(1));

            // Test setObject with parameter name and SQLType
            cs.setObject("val", 100, java.sql.JDBCType.INTEGER);
            cs.registerOutParameter("val", java.sql.JDBCType.INTEGER);
            cs.execute();
            assertEquals(101, cs.getInt("val"));

            // Test registerOutParameter with scale and typeName
            cs.registerOutParameter(1, java.sql.JDBCType.INTEGER, 0);
            cs.registerOutParameter(1, java.sql.JDBCType.INTEGER, "INTEGER");
            cs.registerOutParameter("val", java.sql.JDBCType.INTEGER, 0);
            cs.registerOutParameter("val", java.sql.JDBCType.INTEGER, "INTEGER");
        }

        TestUtils.dropProcedureIfExists(sqlTypeOverloadsProcName, connection.createStatement());
    }

    @Test
    @Tag(Constants.CodeCov)
    public void testCallableStatementParameterNameAPIs() throws Exception {
        // Cleanup
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropProcedureIfExists(tvpProcName, stmt);
            TestUtils.dropTypeIfExists(tvpTypeName, stmt);
        }
        try (Statement stmt = connection.createStatement()) {
            // Create a TVP type and procedure if not exists
            stmt.execute("CREATE TYPE " + tvpTypeName + " AS TABLE (id INT)");
            stmt.execute("CREATE PROCEDURE " + tvpProcName + " @tvp " + tvpTypeName + " READONLY, @val XML = NULL OUTPUT AS SELECT 1");
        }

        try (SQLServerCallableStatement cs = (SQLServerCallableStatement) connection.prepareCall("{call " + tvpProcName + " (?, ?)}")) {
            // setNull(String, int, String)
            cs.setNull("val", java.sql.Types.VARCHAR, "VARCHAR");

            // setURL(String, URL) - expect not supported
            java.net.URL testUrl = java.net.URI.create("http://example.com").toURL();
            assertThrows(SQLServerException.class, () -> cs.setURL("val", testUrl));

            // setStructured(String, String, SQLServerDataTable)
            com.microsoft.sqlserver.jdbc.SQLServerDataTable tvpTable = new com.microsoft.sqlserver.jdbc.SQLServerDataTable();
            tvpTable.addColumnMetadata("id", java.sql.Types.INTEGER);
            tvpTable.addRow(1);
            cs.setStructured("tvp", tvpTypeName, tvpTable);

            // setStructured(String, String, ResultSet)
            // Use a minimal ResultSet: create a statement and select from values
            try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery("SELECT 1 AS id")) {
                cs.setStructured("tvp", tvpTypeName, rs);
            }

            // setStructured(String, String, ISQLServerDataRecord)
            com.microsoft.sqlserver.jdbc.SQLServerDataTable record = new com.microsoft.sqlserver.jdbc.SQLServerDataTable();
            record.addColumnMetadata("id", java.sql.Types.INTEGER);
            record.addRow(1);
            cs.setStructured("tvp", tvpTypeName, record);

            // setSQLXML(String, SQLXML) (already covered, but call again)
            SQLXML sqlxml = connection.createSQLXML();
            sqlxml.setString("<root>test</root>");
            cs.setSQLXML("val", sqlxml);

            // getSQLXML(int)
            cs.registerOutParameter(2, java.sql.Types.SQLXML);
            cs.execute();
            cs.getSQLXML(2);

            // getURL(int) and getURL(String) - expect not supported
            assertThrows(SQLServerException.class, () -> cs.getURL(2));
            assertThrows(SQLServerException.class, () -> cs.getURL("val"));
        }

        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropProcedureIfExists(tvpProcName, stmt);
            TestUtils.dropTypeIfExists(tvpTypeName, stmt);
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
            // Drop order matters. Can't drop objects still referenced by other objects.
            TestUtils.dropProcedureIfExists(outputProcedureNameGUID, stmt);
            TestUtils.dropProcedureIfExists(setNullProcedureName, stmt);
            TestUtils.dropProcedureIfExists(inputParamsProcedureName, stmt);
            TestUtils.dropProcedureIfExists(getObjectLocalDateTimeProcedureName, stmt);
            TestUtils.dropProcedureIfExists(getObjectOffsetDateTimeProcedureName, stmt);
            TestUtils.dropProcedureIfExists(currentTimeProc, stmt);
            TestUtils.dropProcedureIfExists(conditionalSproc, stmt);
            TestUtils.dropProcedureIfExists(simpleRetValSproc, stmt);
            TestUtils.dropProcedureIfExists(zeroParamSproc, stmt);
            TestUtils.dropProcedureIfExists(procedureNameJSON, stmt);
            TestUtils.dropProcedureIfExists(manyParamProc, stmt);
            TestUtils.dropTableIfExists(tableNameGUID, stmt);
            TestUtils.dropTableIfExists(manyParamsTable, stmt);
            TestUtils.dropTableIfExists(tableNameJSON, stmt);
            TestUtils.dropUserDefinedTypeIfExists(manyParamUserDefinedType, stmt); 
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

    private static void createProcedureCurrentTime() throws SQLException {
        String sql = "CREATE PROCEDURE " + currentTimeProc + " @currentTimeStamp datetime = null OUTPUT " +
                "AS BEGIN SET @currentTimeStamp = CURRENT_TIMESTAMP; END";
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    private static void createConditionalProcedure() throws SQLException {
        String sql = "CREATE PROCEDURE " + conditionalSproc + " @param0 INT, @param1 INT, @maybe bigint = 2 " +
                "AS BEGIN IF @maybe >= 2 BEGIN SELECT 5 END END";
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    private static void createSimpleRetValSproc() throws SQLException {
        String sql = "CREATE PROCEDURE " + simpleRetValSproc + " (@Arg1 VARCHAR(128)) AS DECLARE @ReturnCode INT RETURN 1";
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

    private static void createProcedureZeroParams() throws SQLException {
        String sql = "CREATE PROCEDURE " + zeroParamSproc + " AS RETURN 1";
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
    
    private static void createJSONTestTable(Statement stmt) throws SQLException {
        enforceJsonDataTypeSupport(stmt.getConnection());
        String sql = "CREATE TABLE " + tableNameJSON + " (" + "id INT PRIMARY KEY IDENTITY(1,1), " + "col1 JSON)";
        stmt.execute(sql);
    }

    private static void createJSONStoredProcedure(Statement stmt) throws SQLException {
        enforceJsonDataTypeSupport(stmt.getConnection());
        String sql = "CREATE PROCEDURE " + procedureNameJSON + " (@jsonInput JSON) " + "AS " + "BEGIN "
                + "    SELECT @jsonInput AS col1; " + "END";
        stmt.execute(sql);
    }

    private static void enforceJsonDataTypeSupport(Connection connection) throws SQLException {
        assumeTrue(isJsonDataTypeSupported(connection), "JSON data type is not supported on this server instance");
    }

    private static volatile Boolean jsonTypeSupported;

    private static boolean isJsonDataTypeSupported(Connection connection) throws SQLException {
        Boolean cached = jsonTypeSupported;
        if (null != cached) {
            return cached.booleanValue();
        }

        boolean supported = false;
        try (ResultSet typeInfo = connection.getMetaData().getTypeInfo()) {
            while (typeInfo.next()) {
                String typeName = typeInfo.getString("TYPE_NAME");
                if (null != typeName && "JSON".equalsIgnoreCase(typeName.trim())) {
                    supported = true;
                    break;
                }
            }
        }

        jsonTypeSupported = Boolean.valueOf(supported);
        return supported;
    }
}
