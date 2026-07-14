/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.preparedStatement;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * Tests for parameter length hints via {@link SQLServerPreparedStatement#defineParameterType(int, int, int)}
 * and {@link SQLServerPreparedStatement#setObject(int, Object, int, int) setObject(..., scaleOrLength)}.
 *
 * Verifies that caller-supplied max-length hints are correctly declared on the TDS wire for variable-length types
 * (VARCHAR, CHAR, NVARCHAR, NCHAR, VARBINARY, BINARY), that precedence is enforced (defineParameterType takes
 * priority over setObject scaleOrLength), and that out-of-contract usage is rejected with appropriate errors.
 *
 * Test organization:
 * - StringTypeDefinitionTests: Wire type declarations for string types
 * - BinaryTypeDefinitionTests: Wire type declarations for binary types
 * - NullAndEmptyValueTests: Behavior with NULL and empty values
 * - ErrorHandlingTests: Validation errors and constraints
 * - BatchTests: Batch execution with hints
 * - LifecycleTests: Statement lifecycle and hint isolation
 */
@RunWith(JUnitPlatform.class)
public class ParameterLengthHintTest extends AbstractTest {

    static String tableName = RandomUtil.getIdentifier("DefineParamTypeTest");
    static String escapedTable = AbstractSQLGenerator.escapeIdentifier(tableName);

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();

        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(escapedTable, stmt);
            stmt.execute("CREATE TABLE " + escapedTable + " ("
                    + "id          INT IDENTITY PRIMARY KEY,"
                    + "vcol        VARCHAR(200),"
                    + "nvcol       NVARCHAR(200),"
                    + "bincol      VARBINARY(200)"
                    + ")");
        }
    }

    @AfterAll
    public static void cleanup() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(escapedTable, stmt);
        }
    }

    // =========================================================================
    // Wire type definition tests — parameterized
    // =========================================================================

    @Nested
    @DisplayName("Wire type definition for string types")
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class StringTypeDefinitionTests {

        Stream<Arguments> varcharHintCases() {
            return Stream.of(
                    // value, sqlType, hintLength, expectedTypeDef, description
                    // Note: sendStringParametersAsUnicode=true (default) upgrades VARCHAR→NVARCHAR
                    // when setString() is used, so the wire type becomes nvarchar(N).
                    Arguments.of("hello", Types.VARCHAR, 10, "nvarchar(10)", "VARCHAR hint equal to value length"),
                    Arguments.of("hi", Types.VARCHAR, 100, "nvarchar(100)", "VARCHAR hint larger than value"),
                    Arguments.of("A", Types.VARCHAR, 1, "nvarchar(1)", "VARCHAR minimum meaningful hint"),
                    Arguments.of("", Types.VARCHAR, 0, "nvarchar(1)", "VARCHAR hint=0 promoted to nvarchar(1)"),
                    Arguments.of("boundary", Types.VARCHAR, 8000, "nvarchar(max)", "VARCHAR hint=8000 exceeds nvarchar 4000-char limit"),
                    Arguments.of("maxtest", Types.VARCHAR, 8001, "nvarchar(max)", "VARCHAR hint>8000 promotes to max"),

                    Arguments.of("hello", Types.CHAR, 10, "nvarchar(10)", "CHAR hint equal to value length"),
                    Arguments.of("hi", Types.CHAR, 100, "nvarchar(100)", "CHAR hint larger than value"),
                    Arguments.of("A", Types.CHAR, 1, "nvarchar(1)", "CHAR minimum meaningful hint"),
                    Arguments.of("", Types.CHAR, 0, "nvarchar(1)", "CHAR hint=0 promoted to nvarchar(1)"),
                    Arguments.of("boundary", Types.CHAR, 8000, "nvarchar(max)", "CHAR hint=8000 exceeds nvarchar 4000-char limit"),
                    Arguments.of("maxtest", Types.CHAR, 8001, "nvarchar(max)", "CHAR hint>8000 promotes to max")
            );
        }

        // Test the expected wire type definition for VARCHAR/CHAR hints with sendStringParametersAsUnicode=true (the default).
        @ParameterizedTest(name = "VARCHAR/CHAR: {4}")
        @MethodSource("varcharHintCases")
        void testVarcharTypeDefinition(String value, int sqlType, int hintLength,
                String expectedTypeDef, String description) throws Exception {
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {

                pstmt.defineParameterType(1, sqlType, hintLength);
                pstmt.setString(1, value);
                pstmt.executeUpdate();
                
                assertEquals(expectedTypeDef, getTypeDefinition(pstmt, 1));
            }
            assertEquals(value, readLastVarchar());
        }

        Stream<Arguments> nvarcharHintCases() {
            // value, sqlType, hintLength, expectedTypeDef, description
            // Note: sendStringParametersAsUnicode=true (default) upgrades NCHAR/NVARCHAR→NVARCHAR on wire, so all expectedTypeDef are nvarchar(N).
            // NVARCHAR and NCHAR hints are treated the same since both map to NVARCHAR on wire.
            return Stream.of(
                    Arguments.of("hello", Types.NVARCHAR, 5, "nvarchar(5)", "NVARCHAR hint equal to value length"),
                    Arguments.of("hi", Types.NVARCHAR, 100, "nvarchar(100)", "NVARCHAR hint larger than value"),
                    Arguments.of("A", Types.NVARCHAR, 1, "nvarchar(1)", "NVARCHAR minimum meaningful hint"),
                    Arguments.of("", Types.NVARCHAR, 0, "nvarchar(1)", "NVARCHAR hint=0 promoted to nvarchar(1)"),
                    Arguments.of("nboundary", Types.NVARCHAR, 4000, "nvarchar(4000)", "NVARCHAR hint=4000 stays bounded"),
                    Arguments.of("nmaxtest", Types.NVARCHAR, 4001, "nvarchar(max)", "NVARCHAR hint>4000 promotes to max"),

                    Arguments.of("hello", Types.NCHAR, 5, "nvarchar(5)", "NCHAR hint equal to value length"),
                    Arguments.of("hi", Types.NCHAR, 100, "nvarchar(100)", "NCHAR hint larger than value"),
                    Arguments.of("A", Types.NCHAR, 1, "nvarchar(1)", "NCHAR minimum meaningful hint"),
                    Arguments.of("", Types.NCHAR, 0, "nvarchar(1)", "NCHAR hint=0 promoted to nvarchar(1)"),
                    Arguments.of("nboundary", Types.NCHAR, 4000, "nvarchar(4000)", "NCHAR hint=4000 stays bounded"),
                    Arguments.of("nmaxtest", Types.NCHAR, 4001, "nvarchar(max)", "NCHAR hint>4000 promotes to max")
            );
        }

        // Verify NVARCHAR/NCHAR hints produce correct nvarchar(N) wire type with SSPAU=true (default).
        @ParameterizedTest(name = "NVARCHAR/NCHAR: {4}")
        @MethodSource("nvarcharHintCases")
        void testNvarcharTypeDefinition(String value, int sqlType, int hintLength,
                String expectedTypeDef, String description) throws Exception {
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO " + escapedTable + " (nvcol) VALUES (?)")) {

                pstmt.defineParameterType(1, sqlType, hintLength);
                pstmt.setNString(1, value);
                pstmt.executeUpdate();

                assertEquals(expectedTypeDef, getTypeDefinition(pstmt, 1));
            }
            assertEquals(value, readLastNvarchar());
        }

        Stream<Arguments> varcharHintCasesNoUnicode() {
            return Stream.of(
                    // value, sqlType, hintLength, expectedTypeDef, description
                    // With sendStringParametersAsUnicode=false, VARCHAR stays as varchar on wire.
                    // CHAR still routes through VARCHAR on wire, so expectedTypeDef is varchar(N) for both.
                    Arguments.of("hello", Types.VARCHAR, 10, "varchar(10)", "VARCHAR hint equal to value length"),
                    Arguments.of("hi", Types.VARCHAR, 100, "varchar(100)", "VARCHAR hint larger than value"),
                    Arguments.of("A", Types.VARCHAR, 1, "varchar(1)", "VARCHAR minimum meaningful hint"),
                    Arguments.of("", Types.VARCHAR, 0, "varchar(1)", "VARCHAR hint=0 promoted to varchar(1)"),
                    Arguments.of("boundary", Types.VARCHAR, 8000, "varchar(8000)", "VARCHAR hint=8000 stays bounded"),
                    Arguments.of("maxtest", Types.VARCHAR, 8001, "varchar(max)", "VARCHAR hint>8000 promotes to max"),

                    Arguments.of("hello", Types.CHAR, 10, "varchar(10)", "CHAR hint equal to value length"),
                    Arguments.of("hi", Types.CHAR, 100, "varchar(100)", "CHAR hint larger than value"),
                    Arguments.of("A", Types.CHAR, 1, "varchar(1)", "CHAR minimum meaningful hint"),
                    Arguments.of("", Types.CHAR, 0, "varchar(1)", "CHAR hint=0 promoted to varchar(1)"),
                    Arguments.of("boundary", Types.CHAR, 8000, "varchar(8000)", "CHAR hint=8000 stays bounded"),
                    Arguments.of("maxtest", Types.CHAR, 8001, "varchar(max)", "CHAR hint>8000 promotes to max")
            );
        }

        // Verify VARCHAR/CHAR hints produce varchar(N) on wire when SSPAU=false (no unicode upgrade).
        @ParameterizedTest(name = "VARCHAR (no unicode): {4}")
        @MethodSource("varcharHintCasesNoUnicode")
        void testVarcharTypeDefinitionNoUnicode(String value, int sqlType, int hintLength,
                String expectedTypeDef, String description) throws Exception {
            String connStr = connectionString + ";sendStringParametersAsUnicode=false;";
            try (Connection con = PrepUtil.getConnection(connStr);
                 SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con
                         .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {

                pstmt.defineParameterType(1, sqlType, hintLength);
                pstmt.setString(1, value);
                pstmt.executeUpdate();

                assertEquals(expectedTypeDef, getTypeDefinition(pstmt, 1));
            }
            assertEquals(value, readLastVarchar());
        }

        Stream<Arguments> nvarcharHintCasesNoUnicode() {
            return Stream.of(
                    // With sendStringParametersAsUnicode=false, NVARCHAR hints still produce nvarchar
                    // because setNString() explicitly sets the NVARCHAR JDBC type.
                    // NCHAR hints also produce nvarchar on wire since NCHAR routes through NVARCHAR.
                    Arguments.of("hello", Types.NVARCHAR, 5, "nvarchar(5)", "NVARCHAR hint equal to value length"),
                    Arguments.of("hi", Types.NVARCHAR, 100, "nvarchar(100)", "NVARCHAR hint larger than value"),
                    Arguments.of("A", Types.NVARCHAR, 1, "nvarchar(1)", "NVARCHAR minimum meaningful hint"),
                    Arguments.of("", Types.NVARCHAR, 0, "nvarchar(1)", "NVARCHAR hint=0 promoted to nvarchar(1)"),
                    Arguments.of("nboundary", Types.NVARCHAR, 4000, "nvarchar(4000)", "NVARCHAR hint=4000 stays bounded"),
                    Arguments.of("nmaxtest", Types.NVARCHAR, 4001, "nvarchar(max)", "NVARCHAR hint>4000 promotes to max"),

                    Arguments.of("hello", Types.NCHAR, 5, "nvarchar(5)", "NCHAR hint equal to value length"),
                    Arguments.of("hi", Types.NCHAR, 100, "nvarchar(100)", "NCHAR hint larger than value"),
                    Arguments.of("A", Types.NCHAR, 1, "nvarchar(1)", "NCHAR minimum meaningful hint"),
                    Arguments.of("", Types.NCHAR, 0, "nvarchar(1)", "NCHAR hint=0 promoted to nvarchar(1)"),
                    Arguments.of("nboundary", Types.NCHAR, 4000, "nvarchar(4000)", "NCHAR hint=4000 stays bounded"),
                    Arguments.of("nmaxtest", Types.NCHAR, 4001, "nvarchar(max)", "NCHAR hint>4000 promotes to max")
            );
        }

        // Verify NVARCHAR/NCHAR hints still produce nvarchar(N) even when SSPAU=false.
        @ParameterizedTest(name = "NVARCHAR (no unicode): {4}")
        @MethodSource("nvarcharHintCasesNoUnicode")
        void testNvarcharTypeDefinitionNoUnicode(String value, int sqlType, int hintLength,
                String expectedTypeDef, String description) throws Exception {
            String connStr = connectionString + ";sendStringParametersAsUnicode=false;";

            try (Connection con = PrepUtil.getConnection(connStr);
                 SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con
                         .prepareStatement("INSERT INTO " + escapedTable + " (nvcol) VALUES (?)")) {

                pstmt.defineParameterType(1, sqlType, hintLength);
                pstmt.setNString(1, value);
                pstmt.executeUpdate();

                assertEquals(expectedTypeDef, getTypeDefinition(pstmt, 1));
            }
            assertEquals(value, readLastNvarchar());
        }

        Stream<Arguments> setObjectStringLengthHintCases() {
            // setObject(..., scaleOrLength) path for string families.
            return Stream.of(
                    Arguments.of("Engineering", Types.VARCHAR, 40, "vcol", "nvarchar(40)",
                            "setObject VARCHAR scaleOrLength honored"),
                    Arguments.of("Engineering", Types.CHAR, 40, "vcol", "nvarchar(40)",
                            "setObject CHAR scaleOrLength honored"),
                    Arguments.of("Engineering", Types.NVARCHAR, 40, "nvcol", "nvarchar(40)",
                            "setObject NVARCHAR scaleOrLength honored"),
                    Arguments.of("Engineering", Types.NCHAR, 40, "nvcol", "nvarchar(40)",
                            "setObject NCHAR scaleOrLength honored"));
        }

        @ParameterizedTest(name = "{5}")
        @MethodSource("setObjectStringLengthHintCases")
        void testSetObjectStringLengthHint(String value, int sqlType, int scaleOrLength, String column,
                String expectedTypeDef, String description) throws Exception {
            // Verifies inline scaleOrLength controls declaration when defineParameterType is not present.
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO " + escapedTable + " (" + column + ") VALUES (?)")) {

                pstmt.setObject(1, value, sqlType, scaleOrLength);
                pstmt.executeUpdate();

                assertEquals(expectedTypeDef, getTypeDefinition(pstmt, 1));
            }
            if ("nvcol".equals(column)) {
                assertEquals(value, readLastNvarchar());
            } else {
                assertEquals(value, readLastVarchar());
            }
        }

        Stream<Arguments> definePrecedesSetObjectStringCases() {
            // Precedence: defineParameterType must override setObject scaleOrLength.
            return Stream.of(
                    Arguments.of(Types.VARCHAR, "vcol", "nvarchar(5)", "define VARCHAR beats setObject length"),
                    Arguments.of(Types.CHAR, "vcol", "nvarchar(5)", "define CHAR beats setObject length"),
                    Arguments.of(Types.NVARCHAR, "nvcol", "nvarchar(5)", "define NVARCHAR beats setObject length"),
                    Arguments.of(Types.NCHAR, "nvcol", "nvarchar(5)", "define NCHAR beats setObject length"));
        }

        @ParameterizedTest(name = "{3}")
        @MethodSource("definePrecedesSetObjectStringCases")
        void testDefineParameterTypePrecedesSetObjectLengthHintForStrings(int sqlType, String column,
                String expectedTypeDef, String description) throws Exception {
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO " + escapedTable + " (" + column + ") VALUES (?)")) {

                pstmt.defineParameterType(1, sqlType, 5);
                pstmt.setObject(1, "Eng", sqlType, 40);
                pstmt.executeUpdate();

                assertEquals(expectedTypeDef, getTypeDefinition(pstmt, 1));
            }
            if ("nvcol".equals(column)) {
                assertEquals("Eng", readLastNvarchar());
            } else {
                assertEquals("Eng", readLastVarchar());
            }
        }
    }

    @Nested
    @DisplayName("Wire type definition for binary types")
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class BinaryTypeDefinitionTests {

        Stream<Arguments> binaryHintCases() {

            // value, sqlType, hintLength, expectedTypeDef, description
            // VARBINARY and BINARY hints are treated the same since both map to VARBINARY on wire.
            // VARBINARY max length is 8000, so hints >8000 promote to varbinary(max).
            // If hint length is 0, we promote to varbinary(1) since VARBINARY(0) is not valid.
            // For hints smaller than actual value length, we still declare the hinted length on wire and let the server handle truncation errors as needed.
            byte[] threeBytes = {0x01, 0x02, 0x03};
            byte[] oneByte = {0x01};
            return Stream.of(
                    Arguments.of(threeBytes, Types.VARBINARY, 3, "varbinary(3)", "VARBINARY hint equal to value length"),
                    Arguments.of(threeBytes, Types.VARBINARY, 100, "varbinary(100)", "VARBINARY hint larger than value"),
                    Arguments.of(oneByte, Types.VARBINARY, 1, "varbinary(1)", "VARBINARY minimum meaningful hint"),
                    Arguments.of(oneByte, Types.VARBINARY, 0, "varbinary(1)", "VARBINARY hint=0 promoted to varbinary(1)"),
                    Arguments.of(threeBytes, Types.VARBINARY, 8000, "varbinary(8000)", "VARBINARY hint=8000 stays bounded"),
                    Arguments.of(threeBytes, Types.VARBINARY, 8001, "varbinary(max)", "VARBINARY hint>8000 promotes to max"),

                    Arguments.of(threeBytes, Types.BINARY, 3, "varbinary(3)", "BINARY hint equal to value length"),
                    Arguments.of(threeBytes, Types.BINARY, 100, "varbinary(100)", "BINARY hint larger than value"),
                    Arguments.of(oneByte, Types.BINARY, 1, "varbinary(1)", "BINARY minimum meaningful hint"),
                    Arguments.of(oneByte, Types.BINARY, 0, "varbinary(1)", "BINARY hint=0 promoted to varbinary(1)"),
                    Arguments.of(threeBytes, Types.BINARY, 8000, "varbinary(8000)", "BINARY hint=8000 stays bounded"),
                    Arguments.of(threeBytes, Types.BINARY, 8001, "varbinary(max)", "BINARY hint>8000 promotes to max")
            );
        }

        // Verify VARBINARY/BINARY hints produce correct varbinary(N) wire type.
        @ParameterizedTest(name = "{4}")
        @MethodSource("binaryHintCases")
        void testBinaryTypeDefinition(byte[] value, int sqlType, int hintLength, String expectedTypeDef,
                String description) throws Exception {
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO " + escapedTable + " (bincol) VALUES (?)")) {

                pstmt.defineParameterType(1, sqlType, hintLength);
                pstmt.setBytes(1, value);
                pstmt.executeUpdate();

                assertEquals(expectedTypeDef, getTypeDefinition(pstmt, 1));
            }
            assertArrayEquals(value, readLastVarbinary());
        }

        Stream<Arguments> setObjectBinaryLengthHintCases() {
            // setObject(..., scaleOrLength) path for binary families.
            byte[] threeBytes = {0x01, 0x02, 0x03};
            return Stream.of(
                    Arguments.of(threeBytes, Types.VARBINARY, 40, "varbinary(40)",
                            "setObject VARBINARY scaleOrLength honored"),
                    Arguments.of(threeBytes, Types.BINARY, 40, "varbinary(40)",
                            "setObject BINARY scaleOrLength honored"));
        }

        @ParameterizedTest(name = "{4}")
        @MethodSource("setObjectBinaryLengthHintCases")
        void testSetObjectBinaryLengthHint(byte[] value, int sqlType, int scaleOrLength, String expectedTypeDef,
                String description) throws Exception {
            // Verifies scaleOrLength controls varbinary declaration when used via setObject.
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO " + escapedTable + " (bincol) VALUES (?)")) {

                pstmt.setObject(1, value, sqlType, scaleOrLength);
                pstmt.executeUpdate();

                assertEquals(expectedTypeDef, getTypeDefinition(pstmt, 1));
            }
            assertArrayEquals(value, readLastVarbinary());
        }

        Stream<Arguments> definePrecedesSetObjectBinaryCases() {
            // Binary precedence counterpart: defineParameterType must win over setObject hints.
            byte[] threeBytes = {0x01, 0x02, 0x03};
            return Stream.of(
                    Arguments.of(threeBytes, Types.VARBINARY, "varbinary(5)",
                            "define VARBINARY beats setObject length"),
                    Arguments.of(threeBytes, Types.BINARY, "varbinary(5)",
                            "define BINARY beats setObject length"));
        }

        @ParameterizedTest(name = "{3}")
        @MethodSource("definePrecedesSetObjectBinaryCases")
        void testDefineParameterTypePrecedesSetObjectLengthHintForBinary(byte[] value, int sqlType,
                String expectedTypeDef, String description) throws Exception {
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO " + escapedTable + " (bincol) VALUES (?)")) {

                pstmt.defineParameterType(1, sqlType, 5);
                pstmt.setObject(1, value, sqlType, 40);
                pstmt.executeUpdate();

                assertEquals(expectedTypeDef, getTypeDefinition(pstmt, 1));
            }
            assertArrayEquals(value, readLastVarbinary());
        }
    }

    // =========================================================================
    // NULL and empty value handling
    // =========================================================================

    @Nested
    @DisplayName("NULL and empty value handling")
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class NullAndEmptyValueTests {

        // sqlType, hintLength, column, expectedTypeDef, description
        // Note: for string types, even if the value is NULL, the hint should still determine the nvarchar length on wire 
        // since sendStringParametersAsUnicode=true by default. 
        // For binary types, the hint should determine the varbinary length on wire for NULL values as well.
        Stream<Arguments> nullWithHintCases() {
            return Stream.of(
                    Arguments.of(Types.VARCHAR, 50, "vcol", "nvarchar(50)", "VARCHAR NULL with hint"),
                    Arguments.of(Types.CHAR, 50, "vcol", "nvarchar(50)", "CHAR NULL with hint"),
                    Arguments.of(Types.NVARCHAR, 50, "nvcol", "nvarchar(50)", "NVARCHAR NULL with hint"),
                    Arguments.of(Types.NCHAR, 50, "nvcol", "nvarchar(50)", "NCHAR NULL with hint"),
                    Arguments.of(Types.VARBINARY, 50, "bincol", "varbinary(50)", "VARBINARY NULL with hint"),
                    Arguments.of(Types.BINARY, 50, "bincol", "varbinary(50)", "BINARY NULL with hint")
            );
        }

        // Verify that NULL values use the hint length for wire type when defineParameterType is called.
        @ParameterizedTest(name = "{4}")
        @MethodSource("nullWithHintCases")
        void testNullValueWithHint(int sqlType, int hintLength, String column,
                String expectedTypeDef, String description) throws Exception {
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO " + escapedTable + " (" + column + ") VALUES (?)")) {

                pstmt.defineParameterType(1, sqlType, hintLength);
                pstmt.setNull(1, sqlType);
                pstmt.executeUpdate();

                assertEquals(expectedTypeDef, getTypeDefinition(pstmt, 1),
                        "Expected type definition for NULL value with hint");
            }
            if ("bincol".equals(column)) {
                assertNull(readLastVarbinary());
            } else if ("nvcol".equals(column)) {
                assertNull(readLastNvarchar());
            } else {
                assertNull(readLastVarchar());
            }
        }

        // sqlType, column, expectedTypeDef, description
        Stream<Arguments> nullWithoutHintCases() {
            // With sendStringParametersAsUnicode=true (default), string types should still promote to nvarchar on wire even without a hint, 
            // but with a default length of 4000 since no hint is provided. 
            // Binary types should promote to varbinary(8000) by default when no hint is given.
            return Stream.of(
                    Arguments.of(Types.VARCHAR, "vcol", "nvarchar(4000)", "VARCHAR NULL without hint"),
                    Arguments.of(Types.CHAR, "vcol", "nvarchar(4000)", "CHAR NULL without hint"),
                    Arguments.of(Types.NVARCHAR, "nvcol", "nvarchar(4000)", "NVARCHAR NULL without hint"),
                    Arguments.of(Types.NCHAR, "nvcol", "nvarchar(4000)", "NCHAR NULL without hint"),
                    Arguments.of(Types.VARBINARY, "bincol", "varbinary(8000)", "VARBINARY NULL without hint"),
                    Arguments.of(Types.BINARY, "bincol", "varbinary(8000)", "BINARY NULL without hint")
            );
        }

        // Verify that NULL values without defineParameterType fall back to default type widths.
        @ParameterizedTest(name = "{3}")
        @MethodSource("nullWithoutHintCases")
        void testNullValueWithoutHint(int sqlType, String column,
                String expectedTypeDef, String description) throws Exception {

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO " + escapedTable + " (" + column + ") VALUES (?)")) {
                pstmt.setNull(1, sqlType);
                pstmt.executeUpdate();

                assertEquals(expectedTypeDef, getTypeDefinition(pstmt, 1),
                        "Expected default type definition for NULL without hint");
            }
            if ("bincol".equals(column)) {
                assertNull(readLastVarbinary());
            } else if ("nvcol".equals(column)) {
                assertNull(readLastNvarchar());
            } else {
                assertNull(readLastVarchar());
            }
        }

        // sqlType, hintLength, column, expectedTypeDef, description
        Stream<Arguments> emptyValueWithHintCases() {
            return Stream.of(
                    Arguments.of(Types.VARCHAR, 10, "vcol", "nvarchar(10)", "VARCHAR empty string with hint"),
                    Arguments.of(Types.CHAR, 10, "vcol", "nvarchar(10)", "CHAR empty string with hint"),
                    Arguments.of(Types.NVARCHAR, 10, "nvcol", "nvarchar(10)", "NVARCHAR empty string with hint"),
                    Arguments.of(Types.NCHAR, 10, "nvcol", "nvarchar(10)", "NCHAR empty string with hint"),
                    Arguments.of(Types.VARBINARY, 10, "bincol", "varbinary(10)", "VARBINARY empty bytes with hint"),
                    Arguments.of(Types.BINARY, 10, "bincol", "varbinary(10)", "BINARY empty bytes with hint")
            );
        }

        // Verify that empty values (empty string / zero-length byte[]) honor the hint length.
        @ParameterizedTest(name = "{4}")
        @MethodSource("emptyValueWithHintCases")
        void testEmptyValueWithHint(int sqlType, int hintLength, String column,
                String expectedTypeDef, String description) throws Exception {
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO " + escapedTable + " (" + column + ") VALUES (?)")) {

                    pstmt.defineParameterType(1, sqlType, hintLength);
                if ("bincol".equals(column)) {
                    pstmt.setBytes(1, new byte[0]);
                } else if (sqlType == Types.NVARCHAR || sqlType == Types.NCHAR) {
                    pstmt.setNString(1, "");
                } else {
                    pstmt.setString(1, "");
                }
                pstmt.executeUpdate();

                assertEquals(expectedTypeDef, getTypeDefinition(pstmt, 1),
                        "Expected type definition for empty value with hint");
            }
            if ("bincol".equals(column)) {
                assertArrayEquals(new byte[0], readLastVarbinary());
            } else if ("nvcol".equals(column)) {
                assertEquals("", readLastNvarchar());
            } else {
                assertEquals("", readLastVarchar());
            }
        }
    }

    // =========================================================================
    // Error handling
    // =========================================================================

    @Nested
    @DisplayName("Error handling and validation")
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class ErrorHandlingTests {

        // sqlType, typeName
        Stream<Arguments> negativeMaxLengthCases() {
            return Stream.of(
                    Arguments.of(Types.VARCHAR, "VARCHAR"),
                    Arguments.of(Types.CHAR, "CHAR"),
                    Arguments.of(Types.NVARCHAR, "NVARCHAR"),
                    Arguments.of(Types.NCHAR, "NCHAR"),
                    Arguments.of(Types.VARBINARY, "VARBINARY"),
                    Arguments.of(Types.BINARY, "BINARY")
            );
        }

        // Verify that negative maxLength is rejected with R_invalidParameterLength for all supported types.
        @ParameterizedTest(name = "Negative length rejected: {1}")
        @MethodSource("negativeMaxLengthCases")
        void testNegativeMaxLengthRejected(int sqlType, String typeName) throws SQLException {
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {

                SQLServerException e = assertThrows(SQLServerException.class,
                        () -> pstmt.defineParameterType(1, sqlType, -1));

                assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_invalidParameterLength")),
                        "Unexpected error: " + e.getMessage());
            }
        }

        // Verify that unsupported JDBC types (e.g. INTEGER, CLOB) are rejected.
        @ParameterizedTest(name = "Unsupported type: {0}")
        @CsvSource({"INTEGER", "CLOB"})
        void testUnsupportedTypeRejected(String typeName) throws SQLException {
            int sqlType = "INTEGER".equals(typeName) ? Types.INTEGER : Types.CLOB;
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {

                SQLServerException e = assertThrows(SQLServerException.class,
                        () -> pstmt.defineParameterType(1, sqlType, 10));

                assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_unsupportedTypeForDefineParamType")),
                        "Unexpected error: " + e.getMessage());
            }
        }

        // Verify that an out-of-range parameter index throws R_indexOutOfRange.
        @Test
        void testOutOfRangeParameterIndex() throws SQLException {
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {

                SQLServerException e = assertThrows(SQLServerException.class,
                        () -> pstmt.defineParameterType(99, Types.VARCHAR, 10));

                assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_indexOutOfRange")),
                        "Unexpected error: " + e.getMessage());
            }
        }

        // Verify that calling defineParameterType on a closed statement throws R_statementIsClosed.
        @Test
        void testClosedStatementRejected() throws SQLException {
            SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)");
            pstmt.close();

            SQLServerException e = assertThrows(SQLServerException.class,
                    () -> pstmt.defineParameterType(1, Types.VARCHAR, 10));
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_statementIsClosed")),
                    "Unexpected error: " + e.getMessage());
        }

        // value, sqlType, hintLength, column, description
        Stream<Arguments> hintSmallerThanValueCases() {
            return Stream.of(
                // VARCHAR with SSPAU=true → nvarchar on wire, value exceeds hint and execution fails
                Arguments.of("0123456789", Types.VARCHAR, 3, "vcol",
                            "VARCHAR hint=3, value=10 chars"),
                Arguments.of("abcdef", Types.VARCHAR, 2, "vcol",
                            "VARCHAR hint=2, value=6 chars"),
                // CHAR with SSPAU=true → nvarchar on wire, value exceeds hint and execution fails
                Arguments.of("0123456789", Types.CHAR, 3, "vcol",
                            "CHAR hint=3, value=10 chars"),
                Arguments.of("abcdef", Types.CHAR, 4, "vcol",
                            "CHAR hint=4, value=6 chars"),
                // NVARCHAR → nvarchar on wire, value exceeds hint and execution fails
                Arguments.of("0123456789", Types.NVARCHAR, 3, "nvcol",
                            "NVARCHAR hint=3, value=10 chars"),
                Arguments.of("abcdef", Types.NVARCHAR, 2, "nvcol",
                            "NVARCHAR hint=2, value=6 chars"),
                // NCHAR → nvarchar on wire, value exceeds hint and execution fails
                Arguments.of("0123456789", Types.NCHAR, 5, "nvcol",
                            "NCHAR hint=5, value=10 chars"),
                Arguments.of("abcdef", Types.NCHAR, 1, "nvcol",
                            "NCHAR hint=1, value=6 chars")
            );
        }

        // Verify that when the hint is smaller than the value, execution fails with the expected error.
        @ParameterizedTest(name = "Length validation: {4}")
        @MethodSource("hintSmallerThanValueCases")
        void testHintSmallerThanValueThrowsError(String value, int sqlType, int hintLength,
            String column,
                String description) throws Exception {
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO " + escapedTable + " (" + column + ") VALUES (?)")) {

                pstmt.defineParameterType(1, sqlType, hintLength);
                if (sqlType == Types.NVARCHAR || sqlType == Types.NCHAR) {
                    pstmt.setNString(1, value);
                } else {
                    pstmt.setString(1, value);
                }
                try {
                    pstmt.executeUpdate();
                    fail("Expected SQLServerException for value length exceeding defineParameterType hint");
                } catch (SQLServerException e) {
                    assertTrue(e.getMessage()
                            .matches(TestUtils.formatErrorMsg("R_parameterTypeValueLengthExceedsHint")),
                            "Unexpected error: " + e.getMessage());
                }
            }
        }

        Stream<Arguments> binaryHintSmallerThanValueCases() {
            byte[] fiveBytes = {0x01, 0x02, 0x03, 0x04, 0x05};
            return Stream.of(
                    Arguments.of(fiveBytes, Types.VARBINARY, 2, "varbinary(2)", new byte[]{0x01, 0x02},
                            "VARBINARY hint=2, value=5 bytes"),
                    Arguments.of(fiveBytes, Types.VARBINARY, 1, "varbinary(1)", new byte[]{0x01},
                            "VARBINARY hint=1, value=5 bytes"),
                    Arguments.of(fiveBytes, Types.BINARY, 3, "varbinary(3)", new byte[]{0x01, 0x02, 0x03},
                            "BINARY hint=3, value=5 bytes"),
                    Arguments.of(fiveBytes, Types.BINARY, 1, "varbinary(1)", new byte[]{0x01},
                            "BINARY hint=1, value=5 bytes")
            );
        }

        Stream<Arguments> setObjectHintSmallerThanValueCases() {
            return Stream.of(
                    Arguments.of("Engineering", Types.VARCHAR, 5, "setObject VARCHAR hint smaller than value"),
                    Arguments.of("Engineering", Types.NVARCHAR, 5,
                            "setObject NVARCHAR hint smaller than value"));
        }

        @ParameterizedTest(name = "{3}")
        @MethodSource("setObjectHintSmallerThanValueCases")
        void testSetObjectHintSmallerThanValueThrowsError(String value, int sqlType, int hintLength,
                String description) throws Exception {
            String column = Types.NVARCHAR == sqlType ? "nvcol" : "vcol";
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO " + escapedTable + " (" + column + ") VALUES (?)")) {

                pstmt.setObject(1, value, sqlType, hintLength);
                try {
                    pstmt.executeUpdate();
                    fail("Expected SQLServerException for value length exceeding setObject scaleOrLength hint");
                } catch (SQLServerException e) {
                    assertTrue(e.getMessage()
                            .matches(TestUtils.formatErrorMsg("R_parameterTypeValueLengthExceedsHint")),
                            "Unexpected error: " + e.getMessage());
                }
            }
        }

        Stream<Arguments> setObjectBinaryHintSmallerThanValueCases() {
            byte[] fiveBytes = {0x01, 0x02, 0x03, 0x04, 0x05};
            return Stream.of(
                    Arguments.of(fiveBytes, Types.VARBINARY, 2,
                            "setObject VARBINARY hint smaller than value"),
                    Arguments.of(fiveBytes, Types.BINARY, 2, "setObject BINARY hint smaller than value"));
        }

        @ParameterizedTest(name = "{3}")
        @MethodSource("setObjectBinaryHintSmallerThanValueCases")
        void testSetObjectBinaryHintSmallerThanValueThrowsError(byte[] value, int sqlType, int hintLength,
                String description) throws Exception {
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO " + escapedTable + " (bincol) VALUES (?)")) {

                pstmt.setObject(1, value, sqlType, hintLength);
                try {
                    pstmt.executeUpdate();
                    fail("Expected SQLServerException for value length exceeding setObject scaleOrLength hint");
                } catch (SQLServerException e) {
                    assertTrue(e.getMessage()
                            .matches(TestUtils.formatErrorMsg("R_parameterTypeValueLengthExceedsHint")),
                            "Unexpected error: " + e.getMessage());
                }
            }
        }

        // Verify binary data fails execution when the hint is smaller than the byte[] length.
        @ParameterizedTest(name = "Length validation: {5}")
        @MethodSource("binaryHintSmallerThanValueCases")
        void testBinaryHintSmallerThanValueThrowsError(byte[] value, int sqlType, int hintLength,
                String expectedTypeDef, byte[] expectedStoredValue,
                String description) throws Exception {
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO " + escapedTable + " (bincol) VALUES (?)")) {

                pstmt.defineParameterType(1, sqlType, hintLength);
                pstmt.setBytes(1, value);
                try {
                    pstmt.executeUpdate();
                    fail("Expected SQLServerException for value length exceeding defineParameterType hint");
                } catch (SQLServerException e) {
                    assertTrue(e.getMessage()
                            .matches(TestUtils.formatErrorMsg("R_parameterTypeValueLengthExceedsHint")),
                            "Unexpected error: " + e.getMessage());
                }
            }
        }

        // value, sqlType, scaleOrLength, column, description
        Stream<Arguments> setObjectLengthSmallerThanValueCases() {
            return Stream.of(
                    Arguments.of("0123456789", Types.VARCHAR, 3, "vcol",
                            "setObject VARCHAR hint=3, value=10 chars"),
                    Arguments.of("0123456789", Types.CHAR, 3, "vcol",
                            "setObject CHAR hint=3, value=10 chars"),
                    Arguments.of("0123456789", Types.NVARCHAR, 3, "nvcol",
                            "setObject NVARCHAR hint=3, value=10 chars"),
                    Arguments.of("0123456789", Types.NCHAR, 3, "nvcol",
                            "setObject NCHAR hint=3, value=10 chars"));
        }

        @ParameterizedTest(name = "{4}")
        @MethodSource("setObjectLengthSmallerThanValueCases")
        void testSetObjectLengthSmallerThanValueThrowsError(String value, int sqlType, int scaleOrLength,
                String column, String description) throws Exception {
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO " + escapedTable + " (" + column + ") VALUES (?)")) {

                pstmt.setObject(1, value, sqlType, scaleOrLength);
                try {
                    pstmt.executeUpdate();
                    fail("Expected SQLServerException for setObject value length exceeding scaleOrLength");
                } catch (SQLServerException e) {
                    assertTrue(e.getMessage()
                            .matches(TestUtils.formatErrorMsg("R_parameterTypeValueLengthExceedsHint")),
                            "Unexpected error: " + e.getMessage());
                }
            }
        }

        Stream<Arguments> setObjectBinaryLengthSmallerThanValueCases() {
            byte[] fiveBytes = {0x01, 0x02, 0x03, 0x04, 0x05};
            return Stream.of(
                    Arguments.of(fiveBytes, Types.VARBINARY, 2, "setObject VARBINARY hint=2, value=5 bytes"),
                    Arguments.of(fiveBytes, Types.BINARY, 2, "setObject BINARY hint=2, value=5 bytes"));
        }

        @ParameterizedTest(name = "{3}")
        @MethodSource("setObjectBinaryLengthSmallerThanValueCases")
        void testSetObjectBinaryLengthSmallerThanValueThrowsError(byte[] value, int sqlType, int scaleOrLength,
                String description) throws Exception {
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO " + escapedTable + " (bincol) VALUES (?)")) {

                pstmt.setObject(1, value, sqlType, scaleOrLength);
                try {
                    pstmt.executeUpdate();
                    fail("Expected SQLServerException for setObject value length exceeding scaleOrLength");
                } catch (SQLServerException e) {
                    assertTrue(e.getMessage()
                            .matches(TestUtils.formatErrorMsg("R_parameterTypeValueLengthExceedsHint")),
                            "Unexpected error: " + e.getMessage());
                }
            }
        }
    }

    // =========================================================================
    // Batch execution
    // =========================================================================

    @Nested
    @DisplayName("Batch execution with hints")
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class BatchTests {

        // sqlType, hintLength, column, expectedTypeDef, description
        Stream<Arguments> batchHintPersistsCases() {
            return Stream.of(
                    Arguments.of(Types.VARCHAR, 50, "vcol", "nvarchar(50)", "VARCHAR batch 100 rows"),
                    Arguments.of(Types.CHAR, 50, "vcol", "nvarchar(50)", "CHAR batch 100 rows"),
                    Arguments.of(Types.NVARCHAR, 50, "nvcol", "nvarchar(50)", "NVARCHAR batch 100 rows"),
                    Arguments.of(Types.NCHAR, 50, "nvcol", "nvarchar(50)", "NCHAR batch 100 rows"),
                    Arguments.of(Types.VARBINARY, 50, "bincol", "varbinary(50)", "VARBINARY batch 100 rows"),
                    Arguments.of(Types.BINARY, 50, "bincol", "varbinary(50)", "BINARY batch 100 rows")
            );
        }

        // Verify that the defined hint persists across all rows in a 100-row batch.
        @ParameterizedTest(name = "{4}")
        @MethodSource("batchHintPersistsCases")
        void testHintPersistsAcross100BatchRows(int sqlType, int hintLength, String column,
                String expectedTypeDef, String description) throws Exception {
            final int BATCH_SIZE = 100;
            int maxIdBefore = getMaxId();

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO " + escapedTable + " (" + column + ") VALUES (?)")) {

                pstmt.defineParameterType(1, sqlType, hintLength);
                for (int i = 0; i < BATCH_SIZE; i++) {
                    if ("bincol".equals(column)) {
                        pstmt.setBytes(1, ("row" + i).getBytes());
                    } else if (sqlType == Types.NVARCHAR || sqlType == Types.NCHAR) {
                        pstmt.setNString(1, "batchrow" + i);
                    } else {
                        pstmt.setString(1, "batchrow" + i);
                    }
                    pstmt.addBatch();
                }
                assertEquals(expectedTypeDef, getTypeDefinition(pstmt, 1),
                        "Expected type definition to persist across batch rows");
                int[] counts = pstmt.executeBatch();
                assertEquals(BATCH_SIZE, counts.length);
            }
            assertEquals(BATCH_SIZE, countRowsSince(maxIdBefore));
        }

        // sqlType, hintLength, column, expectedTypeDef, description
        Stream<Arguments> batchHintTooSmallCases() {
            return Stream.of(
                    Arguments.of(Types.VARCHAR, 5, "vcol", "nvarchar(5)", "VARCHAR batch over-length error"),
                    Arguments.of(Types.CHAR, 5, "vcol", "nvarchar(5)", "CHAR batch over-length error"),
                    Arguments.of(Types.NVARCHAR, 5, "nvcol", "nvarchar(5)", "NVARCHAR batch over-length error"),
                    Arguments.of(Types.NCHAR, 5, "nvcol", "nvarchar(5)", "NCHAR batch over-length error"),
                    Arguments.of(Types.VARBINARY, 5, "bincol", "varbinary(5)", "VARBINARY batch over-length error"),
                    Arguments.of(Types.BINARY, 5, "bincol", "varbinary(5)", "BINARY batch over-length error")
            );
        }

        // Verify that a small hint causes batch execution to fail when a value exceeds the hint.
        @ParameterizedTest(name = "{4}")
        @MethodSource("batchHintTooSmallCases")
        void testBatchHintTooSmallThrowsError(int sqlType, int hintLength, String column,
                String expectedTypeDef, String description) throws Exception {
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO " + escapedTable + " (" + column + ") VALUES (?)")) {

                pstmt.defineParameterType(1, sqlType, hintLength);
                if ("bincol".equals(column)) {
                    // 3 bytes fits in hint=5, 10 bytes exceeds hint=5
                    pstmt.setBytes(1, new byte[]{0x01, 0x02, 0x03});
                    pstmt.addBatch();
                    pstmt.setBytes(1, new byte[]{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A});
                    pstmt.addBatch();
                } else if (sqlType == Types.NVARCHAR || sqlType == Types.NCHAR) {
                    pstmt.setNString(1, "hi");
                    pstmt.addBatch();
                    pstmt.setNString(1, "truncate_me_please");
                    pstmt.addBatch();
                } else {
                    pstmt.setString(1, "hi");
                    pstmt.addBatch();
                    pstmt.setString(1, "truncate_me_please");
                    pstmt.addBatch();
                }
                assertEquals(expectedTypeDef, getTypeDefinition(pstmt, 1),
                        "Expected type definition for batch with hint");
                try {
                    pstmt.executeBatch();
                    fail("Expected SQLException for value length exceeding defineParameterType hint in batch");
                } catch (SQLException e) {
                    String msg = e.getMessage();
                    String causeMsg = (null != e.getCause()) ? e.getCause().getMessage() : null;
                    assertTrue((null != msg
                            && msg.matches(TestUtils.formatErrorMsg("R_parameterTypeValueLengthExceedsHint")))
                            || (null != causeMsg && causeMsg.matches(TestUtils
                                    .formatErrorMsg("R_parameterTypeValueLengthExceedsHint"))),
                            "Unexpected error: " + e.getMessage());
                }
            }
        }

            Stream<Arguments> batchSetObjectLengthHintCases() {
                return Stream.of(
                    Arguments.of(Types.VARCHAR, "vcol", "batch_setobject_value", "nvarchar(40)",
                        "Batch setObject VARCHAR scaleOrLength honored"),
                    Arguments.of(Types.CHAR, "vcol", "batch_setobject_value", "nvarchar(40)",
                        "Batch setObject CHAR scaleOrLength honored"),
                    Arguments.of(Types.NVARCHAR, "nvcol", "batch_setobject_value", "nvarchar(40)",
                        "Batch setObject NVARCHAR scaleOrLength honored"),
                    Arguments.of(Types.NCHAR, "nvcol", "batch_setobject_value", "nvarchar(40)",
                        "Batch setObject NCHAR scaleOrLength honored"),
                    Arguments.of(Types.VARBINARY, "bincol", new byte[] {0x01, 0x02, 0x03}, "varbinary(40)",
                        "Batch setObject VARBINARY scaleOrLength honored"),
                    Arguments.of(Types.BINARY, "bincol", new byte[] {0x01, 0x02, 0x03}, "varbinary(40)",
                        "Batch setObject BINARY scaleOrLength honored"));
            }

            @ParameterizedTest(name = "{4}")
            @MethodSource("batchSetObjectLengthHintCases")
            void testBatchSetObjectLengthHintHonored(int sqlType, String column, Object value, String expectedTypeDef,
                String description) throws Exception {
                int maxIdBefore = getMaxId();
                try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO " + escapedTable + " (" + column + ") VALUES (?)")) {

                pstmt.setObject(1, value, sqlType, 40);
                pstmt.addBatch();
                pstmt.setObject(1, value, sqlType, 40);
                pstmt.addBatch();

                assertEquals(expectedTypeDef, getTypeDefinition(pstmt, 1),
                    "Expected type definition from setObject scaleOrLength hint");
                int[] counts = pstmt.executeBatch();
                assertEquals(2, counts.length);
                }
                assertEquals(2, countRowsSince(maxIdBefore));
            }

            Stream<Arguments> batchDefinePrecedesSetObjectCases() {
                return Stream.of(
                    Arguments.of(Types.VARCHAR, "vcol", "Eng", "nvarchar(5)",
                        "Batch define VARCHAR beats setObject length"),
                    Arguments.of(Types.CHAR, "vcol", "Eng", "nvarchar(5)",
                        "Batch define CHAR beats setObject length"),
                    Arguments.of(Types.NVARCHAR, "nvcol", "Eng", "nvarchar(5)",
                        "Batch define NVARCHAR beats setObject length"),
                    Arguments.of(Types.NCHAR, "nvcol", "Eng", "nvarchar(5)",
                        "Batch define NCHAR beats setObject length"),
                    Arguments.of(Types.VARBINARY, "bincol", new byte[] {0x01, 0x02, 0x03}, "varbinary(5)",
                        "Batch define VARBINARY beats setObject length"),
                    Arguments.of(Types.BINARY, "bincol", new byte[] {0x01, 0x02, 0x03}, "varbinary(5)",
                        "Batch define BINARY beats setObject length"));
            }

            @ParameterizedTest(name = "{4}")
            @MethodSource("batchDefinePrecedesSetObjectCases")
            void testBatchDefineParameterTypePrecedesSetObjectLengthHint(int sqlType, String column, Object value,
                String expectedTypeDef, String description) throws Exception {
                int maxIdBefore = getMaxId();
                try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO " + escapedTable + " (" + column + ") VALUES (?)")) {

                pstmt.defineParameterType(1, sqlType, 5);
                pstmt.setObject(1, value, sqlType, 40);
                pstmt.addBatch();
                pstmt.setObject(1, value, sqlType, 40);
                pstmt.addBatch();

                assertEquals(expectedTypeDef, getTypeDefinition(pstmt, 1),
                    "Expected defineParameterType to take precedence over setObject length hint");
                int[] counts = pstmt.executeBatch();
                assertEquals(2, counts.length);
                }
                assertEquals(2, countRowsSince(maxIdBefore));
            }

            Stream<Arguments> batchSetObjectHintTooSmallCases() {
                return Stream.of(
                    Arguments.of(Types.VARCHAR, "vcol", "0123456789", "Batch setObject VARCHAR over-length error"),
                    Arguments.of(Types.CHAR, "vcol", "0123456789", "Batch setObject CHAR over-length error"),
                    Arguments.of(Types.NVARCHAR, "nvcol", "0123456789", "Batch setObject NVARCHAR over-length error"),
                    Arguments.of(Types.NCHAR, "nvcol", "0123456789", "Batch setObject NCHAR over-length error"),
                    Arguments.of(Types.VARBINARY, "bincol", new byte[] {0x01, 0x02, 0x03, 0x04, 0x05},
                        "Batch setObject VARBINARY over-length error"),
                    Arguments.of(Types.BINARY, "bincol", new byte[] {0x01, 0x02, 0x03, 0x04, 0x05},
                        "Batch setObject BINARY over-length error"));
            }

            @ParameterizedTest(name = "{3}")
            @MethodSource("batchSetObjectHintTooSmallCases")
            void testBatchSetObjectHintTooSmallThrowsError(int sqlType, String column, Object longValue,
                String description) throws Exception {
                try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO " + escapedTable + " (" + column + ") VALUES (?)")) {

                if ("bincol".equals(column)) {
                    pstmt.setObject(1, new byte[] {0x01, 0x02, 0x03}, sqlType, 5);
                    pstmt.addBatch();
                    pstmt.setObject(1, longValue, sqlType, 5);
                    pstmt.addBatch();
                } else {
                    pstmt.setObject(1, "hi", sqlType, 5);
                    pstmt.addBatch();
                    pstmt.setObject(1, longValue, sqlType, 5);
                    pstmt.addBatch();
                }

                try {
                    pstmt.executeBatch();
                    fail("Expected SQLException for setObject value length exceeding scaleOrLength in batch");
                } catch (SQLException e) {
                    String msg = e.getMessage();
                    String causeMsg = (null != e.getCause()) ? e.getCause().getMessage() : null;
                    assertTrue((null != msg
                        && msg.matches(TestUtils.formatErrorMsg("R_parameterTypeValueLengthExceedsHint")))
                        || (null != causeMsg && causeMsg.matches(TestUtils
                            .formatErrorMsg("R_parameterTypeValueLengthExceedsHint"))),
                        "Unexpected error: " + e.getMessage());
                }
                }
            }

    }

    // =========================================================================
    // Statement lifecycle and isolation
    // =========================================================================

    @Nested
    @DisplayName("Statement lifecycle and isolation")
    class LifecycleTests {

        // Verify that clearParameters() does not reset the defineParameterType hint.
        @Test
        void testClearParametersPreservesHint() throws Exception {
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {
                pstmt.defineParameterType(1, Types.VARCHAR, 50);

                pstmt.setString(1, "first");
                pstmt.executeUpdate();
                // sendStringParametersAsUnicode=true (default) upgrades VARCHAR→NVARCHAR
                assertEquals("nvarchar(50)", getTypeDefinition(pstmt, 1));

                pstmt.clearParameters();
                pstmt.setString(1, "second");
                pstmt.executeUpdate();
                assertEquals("nvarchar(50)", getTypeDefinition(pstmt, 1));
            }
            assertEquals("second", readLastVarchar());
        }

        // Verify that a hint on one PreparedStatement does not leak to another.
        @Test
        void testNewStatementDoesNotInheritHint() throws Exception {
            try (SQLServerPreparedStatement pstmt1 = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)");
                 SQLServerPreparedStatement pstmt2 = (SQLServerPreparedStatement) connection
                         .prepareStatement("INSERT INTO " + escapedTable + " (vcol) VALUES (?)")) {

                pstmt1.defineParameterType(1, Types.VARCHAR, 20);
                pstmt1.setString(1, "ps1value");
                pstmt1.executeUpdate();

                // sendStringParametersAsUnicode=true (default) upgrades VARCHAR→NVARCHAR
                assertEquals("nvarchar(20)", getTypeDefinition(pstmt1, 1));

                pstmt2.setString(1, "ps2value");
                pstmt2.executeUpdate();

                assertEquals("nvarchar(4000)", getTypeDefinition(pstmt2, 1));
            }
        }

        // Verify that different parameters on the same statement can have independent hints.
        @Test
        void testPerParameterHints() throws SQLException {
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO " + escapedTable + " (vcol, nvcol) VALUES (?, ?)")) {

                pstmt.defineParameterType(1, Types.VARCHAR, 30);
                pstmt.defineParameterType(2, Types.NVARCHAR, 60);
                pstmt.setString(1, "vval");
                pstmt.setNString(2, "nval");
                
                pstmt.executeUpdate();
            }
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT TOP 1 vcol, nvcol FROM " + escapedTable + " ORDER BY id DESC")) {
                assertTrue(rs.next());
                assertEquals("vval", rs.getString(1));
                assertEquals("nval", rs.getString(2));
            }
        }
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    // Retrieves the computed TDS type definition string for a parameter via reflection.
    // Uses the connection from the PreparedStatement itself so that connection-level settings
    // (e.g. sendStringParametersAsUnicode) are correctly reflected in the type computation.
    private static String getTypeDefinition(PreparedStatement pstmt, int paramIndex) throws Exception {
        Field inOutParamField = SQLServerStatement.class.getDeclaredField("inOutParam");
        inOutParamField.setAccessible(true);
        Object[] inOutParam = (Object[]) inOutParamField.get(pstmt);
        Object param = inOutParam[paramIndex - 1];

        SQLServerConnection stmtConnection = (SQLServerConnection) pstmt.getConnection();

        // Call Parameter.getTypeDefinition(SQLServerConnection, TDSReader) via reflection
        // to trigger the lazy computation of typeDefinition
        Class<?> tdsReaderClass = Class.forName("com.microsoft.sqlserver.jdbc.TDSReader");
        java.lang.reflect.Method getTypeDefMethod = param.getClass().getDeclaredMethod(
                "getTypeDefinition", SQLServerConnection.class, tdsReaderClass);
        getTypeDefMethod.setAccessible(true);
        return (String) getTypeDefMethod.invoke(param, stmtConnection, null);
    }

    // Returns the current maximum id in the test table, or 0 if empty.
    private int getMaxId() throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT ISNULL(MAX(id), 0) FROM " + escapedTable)) {
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    // Counts rows inserted after the given id.
    private int countRowsSince(int minId) throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT COUNT(*) FROM " + escapedTable + " WHERE id > " + minId)) {
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    // Reads all string values from the given column inserted after the specified id.
    private List<String> readStringsSince(int minId, String column) throws SQLException {
        List<String> result = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT " + column + " FROM " + escapedTable + " WHERE id > " + minId + " ORDER BY id")) {
            while (rs.next()) {
                result.add(rs.getString(1));
            }
        }
        return result;
    }

    private List<String> readVarcharsSince(int minId) throws SQLException {
        return readStringsSince(minId, "vcol");
    }

    private List<String> readNvarcharsSince(int minId) throws SQLException {
        return readStringsSince(minId, "nvcol");
    }

    // Reads all bincol values inserted after the given id, ordered by insertion.
    private List<byte[]> readVarbinariesSince(int minId) throws SQLException {
        List<byte[]> result = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT bincol FROM " + escapedTable + " WHERE id > " + minId + " ORDER BY id")) {
            while (rs.next()) {
                result.add(rs.getBytes(1));
            }
        }
        return result;
    }

    // Returns the most recently inserted value from the specified column, or null if empty.
    private String readLastString(String column) throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT TOP 1 " + column + " FROM " + escapedTable + " ORDER BY id DESC")) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private String readLastVarchar() throws SQLException {
        return readLastString("vcol");
    }

    private String readLastNvarchar() throws SQLException {
        return readLastString("nvcol");
    }

    // Returns the most recently inserted bincol value, or null if empty.
    private byte[] readLastVarbinary() throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT TOP 1 bincol FROM " + escapedTable + " ORDER BY id DESC")) {
            return rs.next() ? rs.getBytes(1) : null;
        }
    }
}
