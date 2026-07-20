/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.callablestatement;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * Tests CallableStatement parameter length hints for named-parameter APIs.
 *
 * Coverage matrix:
 * - setObject(String, Object, int, int) and setObject(String, Object, int, int, boolean)
 * - defineParameterType(int, int, int) for callable execution paths
 * - precedence (defineParameterType over setObject length hints)
 * - boundary, NULL, empty, and value-exceeds-hint behaviors
 * - sendStringParametersAsUnicode=true/false declaration differences
 * - guard behavior for setObject(String, Object, int, Integer, int)
 */
@RunWith(JUnitPlatform.class)
public class CallableParameterLengthHintTest extends AbstractTest {

    private static final String tableName = RandomUtil.getIdentifier("CallableParamLengthHint");
    private static final String escapedTableName = AbstractSQLGenerator.escapeIdentifier(tableName);

    private static final String procVarchar = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("CallableParamLenVarchar"));
    private static final String procVarbinary = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("CallableParamLenVarbinary"));

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();

        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropProcedureIfExists(procVarchar, stmt);
            TestUtils.dropProcedureIfExists(procVarbinary, stmt);
            TestUtils.dropTableIfExists(escapedTableName, stmt);

            stmt.execute("CREATE TABLE " + escapedTableName + " ("
                    + "id INT IDENTITY PRIMARY KEY,"
                    + "vcol VARCHAR(200) NULL,"
                    + "bincol VARBINARY(200) NULL"
                    + ")");

            stmt.execute("CREATE PROCEDURE " + procVarchar
                    + " @v VARCHAR(200) = NULL AS "
                    + "BEGIN "
                    + "INSERT INTO " + escapedTableName + " (vcol) VALUES (@v); "
                    + "END");

            stmt.execute("CREATE PROCEDURE " + procVarbinary
                    + " @b VARBINARY(200) = NULL AS "
                    + "BEGIN "
                    + "INSERT INTO " + escapedTableName + " (bincol) VALUES (@b); "
                    + "END");
        }
    }

    @AfterAll
    public static void cleanupTests() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropProcedureIfExists(procVarchar, stmt);
            TestUtils.dropProcedureIfExists(procVarbinary, stmt);
            TestUtils.dropTableIfExists(escapedTableName, stmt);
        }
    }

    private static Stream<Arguments> allSupportedTypeCases() {
        // Shared happy-path cases for all six supported bounded types.
        byte[] binaryValue = new byte[] {0x01, 0x02, 0x03};
        return Stream.of(
                Arguments.of(Types.VARCHAR, "@v", "Engineering", "nvarchar(40)", false),
                Arguments.of(Types.CHAR, "@v", "Engineering", "nvarchar(40)", false),
                Arguments.of(Types.NVARCHAR, "@v", "Engineering", "nvarchar(40)", false),
                Arguments.of(Types.NCHAR, "@v", "Engineering", "nvarchar(40)", false),
                Arguments.of(Types.VARBINARY, "@b", binaryValue, "varbinary(40)", true),
                Arguments.of(Types.BINARY, "@b", binaryValue, "varbinary(40)", true));
    }

    @ParameterizedTest(name = "scale-only overload length hint honored for type {0}")
    @MethodSource("allSupportedTypeCases")
    public void testCallableSetObjectNamedScaleAsLengthHintForAllSupportedTypes(int sqlType, String parameterName,
            Object value, String expectedTypeDef, boolean binaryType) throws Exception {
        // Verifies scale-only overload treats scale as length hint for supported types.
        String procName = binaryType ? procVarbinary : procVarchar;
        try (SQLServerCallableStatement cs = (SQLServerCallableStatement) connection
                .prepareCall("{call " + procName + "(?)}")) {

            cs.setObject(parameterName, value, sqlType, 40);
            cs.execute();

            assertEquals(expectedTypeDef, getTypeDefinition(cs, 1));
        }

        if (binaryType) {
            assertArrayEquals((byte[]) value, readLastVarbinary());
        } else {
            assertEquals((String) value, readLastVarchar());
        }
    }

    @ParameterizedTest(name = "forceEncrypt overload length hint honored for type {0}")
    @MethodSource("allSupportedTypeCases")
    public void testCallableSetObjectNamedScaleAsLengthHintWithForceEncryptForAllSupportedTypes(int sqlType,
            String parameterName, Object value, String expectedTypeDef, boolean binaryType) throws Exception {
        // Same length-hint behavior should hold for the overload that also accepts forceEncrypt.
        String procName = binaryType ? procVarbinary : procVarchar;
        try (SQLServerCallableStatement cs = (SQLServerCallableStatement) connection
                .prepareCall("{call " + procName + "(?)}")) {

            cs.setObject(parameterName, value, sqlType, 40, false);
            cs.execute();

            assertEquals(expectedTypeDef, getTypeDefinition(cs, 1));
        }

        if (binaryType) {
            assertArrayEquals((byte[]) value, readLastVarbinary());
        } else {
            assertEquals((String) value, readLastVarchar());
        }
    }

    @ParameterizedTest(name = "defineParameterType precedence for type {0}")
    @MethodSource("allSupportedTypeCases")
    public void testCallableDefineParameterTypePrecedesNamedSetObjectLengthHintForAllSupportedTypes(int sqlType,
            String parameterName, Object value, String ignoredExpectedTypeDef, boolean binaryType) throws Exception {
        // Precedence rule: defineParameterType maxLength must override setObject scaleOrLength.
        String procName = binaryType ? procVarbinary : procVarchar;
        String expectedTypeDef = binaryType ? "varbinary(5)" : "nvarchar(5)";
        Object effectiveValue = binaryType ? value : "Eng";

        try (SQLServerCallableStatement cs = (SQLServerCallableStatement) connection
                .prepareCall("{call " + procName + "(?)}")) {

            cs.defineParameterType(1, sqlType, 5);
            cs.setObject(parameterName, effectiveValue, sqlType, 40);
            cs.execute();

            assertEquals(expectedTypeDef, getTypeDefinition(cs, 1));
        }

        if (binaryType) {
            assertArrayEquals((byte[]) effectiveValue, readLastVarbinary());
        } else {
            assertEquals((String) effectiveValue, readLastVarchar());
        }
    }

    @ParameterizedTest(name = "defineParameterType precedence over forceEncrypt overload for type {0}")
    @MethodSource("allSupportedTypeCases")
    public void testCallableDefineParameterTypePrecedesNamedSetObjectLengthHintWithForceEncryptForAllSupportedTypes(
            int sqlType, String parameterName, Object value, String ignoredExpectedTypeDef, boolean binaryType)
            throws Exception {
        // Precedence rule must also hold when forceEncrypt overload is used.
        String procName = binaryType ? procVarbinary : procVarchar;
        String expectedTypeDef = binaryType ? "varbinary(5)" : "nvarchar(5)";
        Object effectiveValue = binaryType ? value : "Eng";

        try (SQLServerCallableStatement cs = (SQLServerCallableStatement) connection
                .prepareCall("{call " + procName + "(?)}")) {

            cs.defineParameterType(1, sqlType, 5);
            cs.setObject(parameterName, effectiveValue, sqlType, 40, false);
            cs.execute();

            assertEquals(expectedTypeDef, getTypeDefinition(cs, 1));
        }

        if (binaryType) {
            assertArrayEquals((byte[]) effectiveValue, readLastVarbinary());
        } else {
            assertEquals((String) effectiveValue, readLastVarchar());
        }
    }

    @ParameterizedTest(name = "defineParameterType honored for type {0}")
    @MethodSource("allSupportedTypeCases")
    public void testCallableDefineParameterTypeHonoredForAllSupportedTypes(int sqlType, String parameterName,
            Object value, String ignoredExpectedTypeDef, boolean binaryType) throws Exception {
        // Direct defineParameterType path without scale-overload involvement.
        String procName = binaryType ? procVarbinary : procVarchar;
        String expectedTypeDef = binaryType ? "varbinary(40)" : "nvarchar(40)";

        try (SQLServerCallableStatement cs = (SQLServerCallableStatement) connection
                .prepareCall("{call " + procName + "(?)}")) {

            cs.defineParameterType(1, sqlType, 40);
            cs.setObject(parameterName, value, sqlType);
            cs.execute();

            assertEquals(expectedTypeDef, getTypeDefinition(cs, 1));
        }

        if (binaryType) {
            assertArrayEquals((byte[]) value, readLastVarbinary());
        } else {
            assertEquals((String) value, readLastVarchar());
        }
    }

    private static Stream<Arguments> allSupportedTypeCasesForLengthError() {
        // Values intentionally exceed small hints to validate deterministic error behavior.
        byte[] binaryValue = new byte[] {0x01, 0x02, 0x03, 0x04, 0x05};
        return Stream.of(
                Arguments.of(Types.VARCHAR, "@v", "Engineering", false),
                Arguments.of(Types.CHAR, "@v", "Engineering", false),
                Arguments.of(Types.NVARCHAR, "@v", "Engineering", false),
                Arguments.of(Types.NCHAR, "@v", "Engineering", false),
                Arguments.of(Types.VARBINARY, "@b", binaryValue, true),
                Arguments.of(Types.BINARY, "@b", binaryValue, true));
    }

    @ParameterizedTest(name = "value exceeds length hint throws for type {0}")
    @MethodSource("allSupportedTypeCasesForLengthError")
    public void testCallableSetObjectNamedLengthHintSmallerThanValueThrowsForAllSupportedTypes(int sqlType,
            String parameterName, Object value, boolean binaryType) throws Exception {
        // setObject length hint path should fail fast when value exceeds declared hint.
        String procName = binaryType ? procVarbinary : procVarchar;
        try (SQLServerCallableStatement cs = (SQLServerCallableStatement) connection
                .prepareCall("{call " + procName + "(?)}")) {

            cs.setObject(parameterName, value, sqlType, 3);
            try {
                cs.execute();
                fail("Expected SQLServerException for value length exceeding setObject scale hint");
            } catch (SQLServerException e) {
                assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_parameterTypeValueLengthExceedsHint")),
                        "Unexpected error: " + e.getMessage());
            }
        }
    }

    @ParameterizedTest(name = "defineParameterType value exceeds hint throws for type {0}")
    @MethodSource("allSupportedTypeCasesForLengthError")
    public void testCallableDefineParameterTypeValueExceedsHintThrowsForAllSupportedTypes(int sqlType,
            String parameterName, Object value, boolean binaryType) throws Exception {
        // defineParameterType path should fail similarly when runtime value exceeds maxLength.
        String procName = binaryType ? procVarbinary : procVarchar;
        try (SQLServerCallableStatement cs = (SQLServerCallableStatement) connection
                .prepareCall("{call " + procName + "(?)}")) {

            cs.defineParameterType(1, sqlType, 3);
            cs.setObject(parameterName, value, sqlType);
            try {
                cs.execute();
                fail("Expected SQLServerException for value length exceeding defineParameterType hint");
            } catch (SQLServerException e) {
                assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_parameterTypeValueLengthExceedsHint")),
                        "Unexpected error: " + e.getMessage());
            }
        }
    }

    @Test
    public void testCallableSetObjectNamedPrecisionScaleOverloadDoesNotApplyStringLengthHint() throws Exception {
        try (SQLServerCallableStatement cs = (SQLServerCallableStatement) connection
                .prepareCall("{call " + procVarchar + "(?)}")) {

            cs.setObject("@v", "Engineering", Types.VARCHAR, Integer.valueOf(10), 2);
            cs.execute();

            // (String, Object, int, Integer, int) overload keeps scale semantics for numeric/streams,
            // so VARCHAR should fall back to default string declaration (with SSPAU=true).
            assertEquals("nvarchar(4000)", getTypeDefinition(cs, 1));
        }

        assertEquals("Engineering", readLastVarchar());
    }

    private static Stream<Arguments> sspauFalseStringCases() {
        // With SSPAU=false, VARCHAR/CHAR should stay varchar(N) on wire.
        return Stream.of(
                Arguments.of(Types.VARCHAR, "@v", "Engineering", "varchar(40)"),
                Arguments.of(Types.CHAR, "@v", "Engineering", "varchar(40)"));
    }

    @ParameterizedTest(name = "SSPAU=false setObject scale hint for type {0}")
    @MethodSource("sspauFalseStringCases")
    public void testCallableSetObjectNamedScaleAsLengthHintWithSSPAUFalse(int sqlType, String parameterName,
            String value, String expectedTypeDef) throws Exception {
        // Verifies connection-level SSPAU impacts declaration shape for string families.
        String connStr = connectionString + ";sendStringParametersAsUnicode=false;";
        try (Connection con = PrepUtil.getConnection(connStr);
                SQLServerCallableStatement cs = (SQLServerCallableStatement) con
                        .prepareCall("{call " + procVarchar + "(?)}")) {

            cs.setObject(parameterName, value, sqlType, 40);
            cs.execute();

            assertEquals(expectedTypeDef, getTypeDefinition(cs, 1));
        }

        assertEquals(value, readLastVarchar());
    }

    @ParameterizedTest(name = "SSPAU=false defineParameterType hint for type {0}")
    @MethodSource("sspauFalseStringCases")
    public void testCallableDefineParameterTypeWithSSPAUFalse(int sqlType, String parameterName, String value,
            String expectedTypeDef) throws Exception {
        // Same SSPAU behavior when declaration is sourced from defineParameterType.
        String connStr = connectionString + ";sendStringParametersAsUnicode=false;";
        try (Connection con = PrepUtil.getConnection(connStr);
                SQLServerCallableStatement cs = (SQLServerCallableStatement) con
                        .prepareCall("{call " + procVarchar + "(?)}")) {

            cs.defineParameterType(1, sqlType, 40);
            cs.setObject(parameterName, value, sqlType);
            cs.execute();

            assertEquals(expectedTypeDef, getTypeDefinition(cs, 1));
        }

        assertEquals(value, readLastVarchar());
    }

    private static Stream<Arguments> boundaryScaleHintCases() {
        // Boundary coverage for max promotion behavior (non-positive hints are validated separately).
        byte[] oneByte = new byte[] {0x01};
        byte[] threeBytes = new byte[] {0x01, 0x02, 0x03};
        return Stream.of(
                Arguments.of(Types.VARCHAR, "@v", "v", 1, "nvarchar(1)", false),
                Arguments.of(Types.VARCHAR, "@v", "v", 8000, "nvarchar(max)", false),
                Arguments.of(Types.VARCHAR, "@v", "v", 8001, "nvarchar(max)", false),

                Arguments.of(Types.CHAR, "@v", "v", 1, "nvarchar(1)", false),
                Arguments.of(Types.CHAR, "@v", "v", 8000, "nvarchar(max)", false),
                Arguments.of(Types.CHAR, "@v", "v", 8001, "nvarchar(max)", false),

                Arguments.of(Types.NVARCHAR, "@v", "v", 1, "nvarchar(1)", false),
                Arguments.of(Types.NVARCHAR, "@v", "v", 4000, "nvarchar(4000)", false),
                Arguments.of(Types.NVARCHAR, "@v", "v", 4001, "nvarchar(max)", false),

                Arguments.of(Types.NCHAR, "@v", "v", 1, "nvarchar(1)", false),
                Arguments.of(Types.NCHAR, "@v", "v", 4000, "nvarchar(4000)", false),
                Arguments.of(Types.NCHAR, "@v", "v", 4001, "nvarchar(max)", false),

            Arguments.of(Types.VARBINARY, "@b", oneByte, 1, "varbinary(1)", true),
            Arguments.of(Types.VARBINARY, "@b", threeBytes, 8000, "varbinary(8000)", true),
            Arguments.of(Types.VARBINARY, "@b", threeBytes, 8001, "varbinary(max)", true),

            Arguments.of(Types.BINARY, "@b", oneByte, 1, "varbinary(1)", true),
            Arguments.of(Types.BINARY, "@b", threeBytes, 8000, "varbinary(8000)", true),
            Arguments.of(Types.BINARY, "@b", threeBytes, 8001, "varbinary(max)", true));
    }

    @ParameterizedTest(name = "Boundary setObject scale hint for type {0} with hint {3}")
    @MethodSource("boundaryScaleHintCases")
    public void testCallableSetObjectBoundaryScaleHintsForAllSupportedTypes(int sqlType, String parameterName,
            Object value, int hintLength, String expectedTypeDef, boolean binaryType) throws Exception {
        // Confirms computed declaration at key positive boundaries (1/max/max+1).
        String procName = binaryType ? procVarbinary : procVarchar;
        try (SQLServerCallableStatement cs = (SQLServerCallableStatement) connection
                .prepareCall("{call " + procName + "(?)}")) {

            cs.setObject(parameterName, value, sqlType, hintLength);
            cs.execute();

            assertEquals(expectedTypeDef, getTypeDefinition(cs, 1));
        }
    }

    private static Stream<Arguments> nonPositiveHintCases() {
        byte[] oneByte = new byte[] {0x01};
        return Stream.of(
                Arguments.of(Types.VARCHAR, "@v", "a", false, -1),
                Arguments.of(Types.VARCHAR, "@v", "a", false, 0),
                Arguments.of(Types.CHAR, "@v", "a", false, -1),
                Arguments.of(Types.CHAR, "@v", "a", false, 0),
                Arguments.of(Types.NVARCHAR, "@v", "a", false, -1),
                Arguments.of(Types.NVARCHAR, "@v", "a", false, 0),
                Arguments.of(Types.NCHAR, "@v", "a", false, -1),
                Arguments.of(Types.NCHAR, "@v", "a", false, 0),
                Arguments.of(Types.VARBINARY, "@b", oneByte, true, -1),
                Arguments.of(Types.VARBINARY, "@b", oneByte, true, 0),
                Arguments.of(Types.BINARY, "@b", oneByte, true, -1),
                Arguments.of(Types.BINARY, "@b", oneByte, true, 0));
    }

    @ParameterizedTest(name = "Callable setObject non-positive hint rejected: type={0}, hint={4}")
    @MethodSource("nonPositiveHintCases")
    public void testCallableSetObjectNonPositiveHintRejected(int sqlType, String parameterName, Object value,
            boolean binaryType, int hintLength) throws Exception {
        String procName = binaryType ? procVarbinary : procVarchar;
        try (SQLServerCallableStatement cs = (SQLServerCallableStatement) connection
                .prepareCall("{call " + procName + "(?)}")) {

            cs.setObject(parameterName, value, sqlType, hintLength);
            SQLServerException e = org.junit.jupiter.api.Assertions.assertThrows(SQLServerException.class, cs::execute);
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_invalidParameterLength")),
                    "Unexpected error: " + e.getMessage());
        }
    }

    @ParameterizedTest(name = "Callable defineParameterType non-positive hint rejected: type={0}, hint={4}")
    @MethodSource("nonPositiveHintCases")
    public void testCallableDefineParameterTypeNonPositiveHintRejected(int sqlType, String parameterName, Object value,
            boolean binaryType, int hintLength) throws Exception {
        String procName = binaryType ? procVarbinary : procVarchar;
        try (SQLServerCallableStatement cs = (SQLServerCallableStatement) connection
                .prepareCall("{call " + procName + "(?)}")) {

            SQLServerException e = org.junit.jupiter.api.Assertions.assertThrows(SQLServerException.class,
                    () -> cs.defineParameterType(1, sqlType, hintLength));
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_invalidParameterLength")),
                    "Unexpected error: " + e.getMessage());
        }
    }

    private static Stream<Arguments> nullWithHintCases() {
        // NULL still carries declaration derived from application hints.
        return Stream.of(
                Arguments.of(Types.VARCHAR, "@v", "nvarchar(50)", false),
                Arguments.of(Types.CHAR, "@v", "nvarchar(50)", false),
                Arguments.of(Types.NVARCHAR, "@v", "nvarchar(50)", false),
                Arguments.of(Types.NCHAR, "@v", "nvarchar(50)", false),
                Arguments.of(Types.VARBINARY, "@b", "varbinary(50)", true),
                Arguments.of(Types.BINARY, "@b", "varbinary(50)", true));
    }

    @ParameterizedTest(name = "NULL with setObject hint for type {0}")
    @MethodSource("nullWithHintCases")
    public void testCallableSetObjectNullWithHintForAllSupportedTypes(int sqlType, String parameterName,
            String expectedTypeDef, boolean binaryType) throws Exception {
        // setObject hint path with NULL payload.
        String procName = binaryType ? procVarbinary : procVarchar;
        try (SQLServerCallableStatement cs = (SQLServerCallableStatement) connection
                .prepareCall("{call " + procName + "(?)}")) {

            cs.setObject(parameterName, null, sqlType, 50);
            cs.execute();

            assertEquals(expectedTypeDef, getTypeDefinition(cs, 1));
        }

        if (binaryType) {
            assertEquals(null, readLastVarbinary());
        } else {
            assertEquals(null, readLastVarchar());
        }
    }

    @ParameterizedTest(name = "NULL with defineParameterType for type {0}")
    @MethodSource("nullWithHintCases")
    public void testCallableDefineParameterTypeNullForAllSupportedTypes(int sqlType, String parameterName,
            String expectedTypeDef, boolean binaryType) throws Exception {
        // defineParameterType path with NULL payload.
        String procName = binaryType ? procVarbinary : procVarchar;
        try (SQLServerCallableStatement cs = (SQLServerCallableStatement) connection
                .prepareCall("{call " + procName + "(?)}")) {

            cs.defineParameterType(1, sqlType, 50);
            cs.setNull(parameterName, sqlType);
            cs.execute();

            assertEquals(expectedTypeDef, getTypeDefinition(cs, 1));
        }

        if (binaryType) {
            assertEquals(null, readLastVarbinary());
        } else {
            assertEquals(null, readLastVarchar());
        }
    }

    private static Stream<Arguments> emptyWithHintCases() {
        // Empty values validate declaration behavior without triggering length-overflow errors.
        return Stream.of(
                Arguments.of(Types.VARCHAR, "@v", "", "nvarchar(10)", false),
                Arguments.of(Types.CHAR, "@v", "", "nvarchar(10)", false),
                Arguments.of(Types.NVARCHAR, "@v", "", "nvarchar(10)", false),
                Arguments.of(Types.NCHAR, "@v", "", "nvarchar(10)", false),
                Arguments.of(Types.VARBINARY, "@b", new byte[0], "varbinary(10)", true),
                Arguments.of(Types.BINARY, "@b", new byte[0], "varbinary(10)", true));
    }

    @ParameterizedTest(name = "Empty value with setObject hint for type {0}")
    @MethodSource("emptyWithHintCases")
    public void testCallableSetObjectEmptyWithHintForAllSupportedTypes(int sqlType, String parameterName,
            Object value, String expectedTypeDef, boolean binaryType) throws Exception {
        // setObject hint path with empty values.
        String procName = binaryType ? procVarbinary : procVarchar;
        try (SQLServerCallableStatement cs = (SQLServerCallableStatement) connection
                .prepareCall("{call " + procName + "(?)}")) {

            cs.setObject(parameterName, value, sqlType, 10);
            cs.execute();

            assertEquals(expectedTypeDef, getTypeDefinition(cs, 1));
        }

        if (binaryType) {
            assertArrayEquals((byte[]) value, readLastVarbinary());
        } else {
            assertEquals((String) value, readLastVarchar());
        }
    }

    @ParameterizedTest(name = "Empty value with defineParameterType for type {0}")
    @MethodSource("emptyWithHintCases")
    public void testCallableDefineParameterTypeEmptyForAllSupportedTypes(int sqlType, String parameterName,
            Object value, String expectedTypeDef, boolean binaryType) throws Exception {
        // defineParameterType path with empty values.
        String procName = binaryType ? procVarbinary : procVarchar;
        try (SQLServerCallableStatement cs = (SQLServerCallableStatement) connection
                .prepareCall("{call " + procName + "(?)}")) {

            cs.defineParameterType(1, sqlType, 10);
            cs.setObject(parameterName, value, sqlType);
            cs.execute();

            assertEquals(expectedTypeDef, getTypeDefinition(cs, 1));
        }

        if (binaryType) {
            assertArrayEquals((byte[]) value, readLastVarbinary());
        } else {
            assertEquals((String) value, readLastVarchar());
        }
    }

    private static String getTypeDefinition(SQLServerCallableStatement cs, int paramIndex) throws Exception {
        Field inOutParamField = SQLServerStatement.class.getDeclaredField("inOutParam");
        inOutParamField.setAccessible(true);
        Object[] inOutParam = (Object[]) inOutParamField.get(cs);
        Object param = inOutParam[paramIndex - 1];

        SQLServerConnection stmtConnection = (SQLServerConnection) cs.getConnection();

        Class<?> tdsReaderClass = Class.forName("com.microsoft.sqlserver.jdbc.TDSReader");
        java.lang.reflect.Method getTypeDefMethod = param.getClass().getDeclaredMethod(
                "getTypeDefinition", SQLServerConnection.class, tdsReaderClass);
        getTypeDefMethod.setAccessible(true);
        return (String) getTypeDefMethod.invoke(param, stmtConnection, null);
    }

    private String readLastVarchar() throws SQLException {
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt
                        .executeQuery("SELECT TOP 1 vcol FROM " + escapedTableName + " ORDER BY id DESC")) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private byte[] readLastVarbinary() throws SQLException {
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt
                        .executeQuery("SELECT TOP 1 bincol FROM " + escapedTableName + " ORDER BY id DESC")) {
            return rs.next() ? rs.getBytes(1) : null;
        }
    }

    // --- Type family mismatch tests ---

    @Test
    public void testCallableDefineParameterTypeCharacterDeclaredWithBinarySetterThrows() throws Exception {
        try (SQLServerCallableStatement cs = (SQLServerCallableStatement) connection
                .prepareCall("{call " + procVarbinary + "(?)}")) {

            cs.defineParameterType(1, Types.VARCHAR, 50);

            SQLServerException e = org.junit.jupiter.api.Assertions.assertThrows(SQLServerException.class,
                    () -> cs.setBytes(1, new byte[] {0x01, 0x02, 0x03}));
            assertTrue(e.getMessage()
                    .matches(TestUtils.formatErrorMsg("R_defineParameterTypeTypeMismatch")),
                    "Unexpected error: " + e.getMessage());
        }
    }

    @Test
    public void testCallableDefineParameterTypeBinaryDeclaredWithCharacterSetterThrows() throws Exception {
        try (SQLServerCallableStatement cs = (SQLServerCallableStatement) connection
                .prepareCall("{call " + procVarchar + "(?)}")) {

            cs.defineParameterType(1, Types.VARBINARY, 50);

            SQLServerException e = org.junit.jupiter.api.Assertions.assertThrows(SQLServerException.class,
                    () -> cs.setString(1, "hello"));
            assertTrue(e.getMessage()
                    .matches(TestUtils.formatErrorMsg("R_defineParameterTypeTypeMismatch")),
                    "Unexpected error: " + e.getMessage());
        }
    }

    @Test
    public void testCallableDefineParameterTypeSameFamilyCharacterSucceeds() throws Exception {
        try (SQLServerCallableStatement cs = (SQLServerCallableStatement) connection
                .prepareCall("{call " + procVarchar + "(?)}")) {

            cs.defineParameterType(1, Types.VARCHAR, 50);
            cs.setNString(1, "hello");
            cs.execute();

            assertEquals("nvarchar(50)", getTypeDefinition(cs, 1));
        }
    }
}
