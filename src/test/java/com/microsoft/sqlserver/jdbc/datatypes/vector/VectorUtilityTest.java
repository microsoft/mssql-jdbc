/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.datatypes.vector;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.vectorJsonTest;

import microsoft.sql.Vector;
import microsoft.sql.Vector.VectorDimensionType;

/**
 * Utility tests for Vector data type that are not parameterized across vector types.
 * 
 * <p>This class contains:</p>
 * <ul>
 *   <li>Connection property tests (vectorTypeSupport: off, v1, v2)</li>
 *   <li>Vector negotiation tests (nested class)</li>
 * </ul>
 * 
 * <p>For parameterized tests across FLOAT32, FLOAT16, etc., see {@link AbstractVectorTest} 
 * and its concrete implementations.</p>
 */
@RunWith(JUnitPlatform.class)
@DisplayName("Vector Utility Tests")
@vectorJsonTest
@Tag(Constants.vectorTest)
public class VectorUtilityTest extends AbstractTest {

    @BeforeAll
    public static void setupTest() throws Exception {
        setConnection();
    }

    // ============================================================================
    // Connection Property Tests (vectorTypeSupport)
    // ============================================================================

    /**
     * Helper method to get a connection with the vectorTypeSupport setting.
     */
    private static SQLServerConnection getConnectionWithVectorFlag(String vectorTypeSupport) throws SQLException {
        String connStr = connectionString + ";vectorTypeSupport=" + vectorTypeSupport;
        return (SQLServerConnection) DriverManager.getConnection(connStr);
    }

    /**
     * Test for validating vector data when the vectorTypeSupport feature is "off".
     * The expected behavior is that the server should return the vector as a string.
     */
    @Test
    public void testValidateVectorWhenVectorFEIsDisabled() throws SQLException {
        String logsTable = TestUtils.escapeSingleQuotes(
                AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("vectorLogsFeiDisabled")));
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(logsTable, stmt);
            stmt.executeUpdate("CREATE TABLE " + logsTable + " (v VECTOR(3))");

            Object[] vectorData = new Float[] { 1.23f, 4.56f, 7.89f };
            Vector vector = new Vector(3, VectorDimensionType.FLOAT32, vectorData);
            String insertSql = "INSERT INTO " + logsTable + " (v) VALUES (?)";
            try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
                pstmt.setObject(1, vector, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            try (SQLServerConnection conn = getConnectionWithVectorFlag("off");
                    Statement stmt2 = conn.createStatement();
                    ResultSet rs = stmt2.executeQuery("SELECT * FROM " + logsTable)) {

                ResultSetMetaData meta = rs.getMetaData();
                int columnCount = meta.getColumnCount();

                assertTrue(rs.next(), "No result found in logs table.");

                for (int i = 1; i <= columnCount; i++) {
                    int columnType = meta.getColumnType(i);

                    Object value = null;
                    switch (columnType) {
                        case java.sql.Types.VARCHAR:
                            value = rs.getString(i);
                            assertEquals("[1.2300000e+000,4.5599999e+000,7.8899999e+000]", value,
                                    "VARCHAR column value mismatch.");
                            break;
                        case microsoft.sql.Types.VECTOR:
                            value = rs.getObject(i, Vector.class);
                            assertNotNull(value, "Vector column is null.");
                            assertArrayEquals(vectorData, ((Vector) value).getData(), "Vector data mismatch.");
                            assertEquals(3, ((Vector) value).getDimensionCount(), "Dimension count mismatch.");
                            assertEquals(VectorDimensionType.FLOAT32, ((Vector) value).getVectorDimensionType(),
                                    "Dimension type mismatch.");
                            String expectedToString = "VECTOR(FLOAT32, 3) : [1.23, 4.56, 7.89]";
                            assertEquals(expectedToString, ((Vector) value).toString(), "Vector toString output mismatch.");
                            break;
                        default:
                            throw new SQLException("Unexpected column type: " + columnType);
                    }
                }
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(logsTable, stmt);
            }
        }
    }

    /**
     * Test for validating vector data when the vectorTypeSupport feature is "v1".
     * The expected behavior is that the server should return the vector as a vector object.
     */
    @Test
    public void testValidateVectorWhenVectorFEIsEnabled() throws SQLException {
        String logsTable = TestUtils.escapeSingleQuotes(
                AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("vectorLogsFeiEnabled")));
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(logsTable, stmt);
            stmt.executeUpdate("CREATE TABLE " + logsTable + " (v VECTOR(3))");

            Object[] vectorData = new Float[] { 1.23f, 4.56f, 7.89f };
            Vector vector = new Vector(3, VectorDimensionType.FLOAT32, vectorData);
            String insertSql = "INSERT INTO " + logsTable + " (v) VALUES (?)";
            try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
                pstmt.setObject(1, vector, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            try (SQLServerConnection conn = getConnectionWithVectorFlag("v1");
                    Statement stmt2 = conn.createStatement();
                    ResultSet rs = stmt2.executeQuery("SELECT * FROM " + logsTable)) {

                ResultSetMetaData meta = rs.getMetaData();
                int columnCount = meta.getColumnCount();

                assertTrue(rs.next(), "No result found in logs table.");

                for (int i = 1; i <= columnCount; i++) {
                    int columnType = meta.getColumnType(i);

                    Object value = null;
                    switch (columnType) {
                        case java.sql.Types.VARCHAR:
                            value = rs.getString(i);
                            assertEquals("[1.2300000e+000,4.5599999e+000,7.8899999e+000]", value,
                                    "VARCHAR column value mismatch.");
                            break;
                        case microsoft.sql.Types.VECTOR:
                            value = rs.getObject(i, Vector.class);
                            assertNotNull(value, "Vector column is null.");
                            assertArrayEquals(vectorData, ((Vector) value).getData(), "Vector data mismatch.");
                            assertEquals(3, ((Vector) value).getDimensionCount(), "Dimension count mismatch.");
                            assertEquals(VectorDimensionType.FLOAT32, ((Vector) value).getVectorDimensionType(),
                                    "Dimension type mismatch.");
                            String expectedToString = "VECTOR(FLOAT32, 3) : [1.23, 4.56, 7.89]";
                            assertEquals(expectedToString, ((Vector) value).toString(), "Vector toString output mismatch.");
                            break;
                        default:
                            throw new SQLException("Unexpected column type: " + columnType);
                    }
                }
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(logsTable, stmt);
            }
        }
    }

    /**
     * Test that an invalid vectorTypeSupport value in the connection string throws
     * an IllegalArgumentException. The only valid values are "off", "v1", and "v2".
     */
    @Test
    public void testInvalidVectorTypeSupportConnectionProperty() throws SQLException {
        String logsTable = TestUtils.escapeSingleQuotes(
                AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("vectorLogs")));
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(logsTable, stmt);
            stmt.executeUpdate("CREATE TABLE " + logsTable + " (v VECTOR(3))");

            Object[] vectorData = new Float[] { 1.23f, 4.56f, 7.89f };
            Vector vector = new Vector(3, VectorDimensionType.FLOAT32, vectorData);
            String insertSql = "INSERT INTO " + logsTable + " (v) VALUES (?)";
            try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
                pstmt.setObject(1, vector, microsoft.sql.Types.VECTOR);
                pstmt.executeUpdate();
            }

            boolean exceptionThrown = false;
            try {
                SQLServerConnection conn = getConnectionWithVectorFlag("1");
                conn.close();
                fail("Expected IllegalArgumentException for invalid vectorTypeSupport value, but none was thrown.");
            } catch (IllegalArgumentException e) {
                exceptionThrown = true;
                String expectedMessage = "Invalid value for vectorTypeSupport: 1. Valid values are \"off\", \"v1\", or \"v2\".";
                assertEquals(expectedMessage, e.getMessage(),
                        "Unexpected exception message: " + e.getMessage());
            }

            assertTrue(exceptionThrown, "Expected IllegalArgumentException was not thrown.");
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(logsTable, stmt);
            }
        }
    }

    // ============================================================================
    // VectorNegotiationTest - Nested test class for negotiation logic
    // ============================================================================

    /**
     * Nested test class for vector negotiation tests.
     * Tests different combinations of vectorTypeSupport settings (v1, v2, off)
     * and validates error handling for invalid values.
     * 
     * Also includes direct unit tests for the negotiateVectorVersion method
     * to validate negotiation logic for all key combinations of client and server versions.
     */
    @RunWith(JUnitPlatform.class)
    @DisplayName("Vector Negotiation Tests")
    @Tag(Constants.vectorFloat16Test)
    public static class VectorNegotiationTest extends AbstractTest {

        // TDS constants for vector support versions (mirroring TDS class values)
        private static final byte VECTORSUPPORT_NOT_SUPPORTED = 0x00;
        private static final byte VECTORSUPPORT_VERSION_1 = 0x01;
        private static final byte VECTORSUPPORT_VERSION_2 = 0x02;

        // VectorTypeSupport enum values accessed via reflection (package-private enum)
        private static Object VECTOR_TYPE_OFF;
        private static Object VECTOR_TYPE_V1;
        private static Object VECTOR_TYPE_V2;
        private static Class<?> vectorTypeSupportClass;

        @BeforeAll
        public static void setupNegotiationTests() throws Exception {
            setConnection();

            // Load VectorTypeSupport enum via reflection (it's package-private)
            vectorTypeSupportClass = Class.forName("com.microsoft.sqlserver.jdbc.VectorTypeSupport");
            VECTOR_TYPE_OFF = Enum.valueOf((Class<Enum>) vectorTypeSupportClass, "OFF");
            VECTOR_TYPE_V1 = Enum.valueOf((Class<Enum>) vectorTypeSupportClass, "V1");
            VECTOR_TYPE_V2 = Enum.valueOf((Class<Enum>) vectorTypeSupportClass, "V2");
        }

        /**
         * Helper method to invoke the private negotiateVectorVersion method via reflection.
         */
        private byte invokeNegotiateVectorVersion(SQLServerConnection conn, 
                Object clientVectorSupport, byte serverVersion) throws Exception {
            java.lang.reflect.Method method = SQLServerConnection.class.getDeclaredMethod(
                    "negotiateVectorVersion", vectorTypeSupportClass, byte.class);
            method.setAccessible(true);
            return (byte) method.invoke(conn, clientVectorSupport, serverVersion);
        }

        /**
         * Test: Comprehensive matrix test for all client/server version combinations.
         * This test validates the negotiation logic truth table covering all 9 combinations:
         * - Client OFF vs Server (NOT_SUPPORTED, V1, V2) -> always OFF
         * - Client V1 vs Server (NOT_SUPPORTED, V1, V2) -> OFF, V1, V1
         * - Client V2 vs Server (NOT_SUPPORTED, V1, V2) -> OFF, V1, V2
         */
        @Test
        @DisplayName("Negotiation: Complete matrix of all client/server combinations")
        public void testNegotiationMatrix() throws Exception {
            try (SQLServerConnection conn = getConnection()) {
                
                byte[][] expectedResults = {
                    // Server: NOT_SUPPORTED, V1, V2
                    {0x00, 0x00, 0x00},  // Client OFF
                    {0x00, 0x01, 0x01},  // Client V1
                    {0x00, 0x01, 0x02}   // Client V2
                };

                Object[] clientVersions = {
                    VECTOR_TYPE_OFF,
                    VECTOR_TYPE_V1,
                    VECTOR_TYPE_V2
                };

                String[] clientVersionNames = {"OFF", "V1", "V2"};

                byte[] serverVersions = {
                    VECTORSUPPORT_NOT_SUPPORTED,
                    VECTORSUPPORT_VERSION_1,
                    VECTORSUPPORT_VERSION_2
                };

                String[] serverVersionNames = {"NOT_SUPPORTED", "V1", "V2"};

                for (int c = 0; c < clientVersions.length; c++) {
                    for (int s = 0; s < serverVersions.length; s++) {
                        byte result = invokeNegotiateVectorVersion(conn, clientVersions[c], serverVersions[s]);
                        assertEquals(expectedResults[c][s], result,
                                String.format("Mismatch for Client=%s, Server=%s: expected 0x%02X but got 0x%02X",
                                        clientVersionNames[c], serverVersionNames[s], expectedResults[c][s], result));
                    }
                }
            }
        }

        /**
         * Test: Verify negotiatedVectorVersion field is properly set after connection
         * when client requests V2 and server supports vectors.
         */
        @Test
        @DisplayName("Verify negotiatedVectorVersion field is set correctly on connection")
        public void testNegotiatedVectorVersionFieldIsSet() throws Exception {
            try (SQLServerConnection conn = getConnectionWithVectorFlag("v2")) {
                byte negotiatedVersion = conn.getNegotiatedVectorVersion();
                
                java.lang.reflect.Field field = SQLServerConnection.class.getDeclaredField("negotiatedVectorVersion");
                field.setAccessible(true);
                byte fieldValue = field.getByte(conn);
                
                assertEquals(fieldValue, negotiatedVersion,
                        "getNegotiatedVectorVersion() should return the same value as the internal field");
                
                assertTrue(negotiatedVersion >= 0x00 && negotiatedVersion <= 0x02,
                        "Negotiated version should be 0x00, 0x01, or 0x02. Actual: " + negotiatedVersion);
            }
        }

        /**
         * Test: Verify serverSupportsVector flag consistency with negotiatedVectorVersion
         */
        @Test
        @DisplayName("Verify serverSupportsVector flag consistency with negotiatedVectorVersion")
        public void testServerSupportsVectorConsistency() throws Exception {
            try (SQLServerConnection conn = getConnectionWithVectorFlag("v2")) {
                byte negotiatedVersion = conn.getNegotiatedVectorVersion();
                
                java.lang.reflect.Method method = SQLServerConnection.class.getDeclaredMethod("getServerSupportsVector");
                method.setAccessible(true);
                boolean serverSupportsVector = (boolean) method.invoke(conn);
                
                if (negotiatedVersion > 0) {
                    assertTrue(serverSupportsVector,
                            "serverSupportsVector should be true when negotiatedVectorVersion > 0");
                } else {
                    assertFalse(serverSupportsVector,
                            "serverSupportsVector should be false when negotiatedVectorVersion == 0");
                }
            }

            try (SQLServerConnection conn = getConnectionWithVectorFlag("off")) {
                java.lang.reflect.Method method = SQLServerConnection.class.getDeclaredMethod("getServerSupportsVector");
                method.setAccessible(true);
                boolean serverSupportsVector = (boolean) method.invoke(conn);
                
                assertFalse(serverSupportsVector,
                        "serverSupportsVector should be false when vectorTypeSupport=off");
            }
        }

        /**
         * Test that invalid vectorTypeSupport values throw IllegalArgumentException.
         */
        @Test
        @DisplayName("Test invalid vectorTypeSupport values throw exception")
        public void testInvalidVectorTypeSupportValues() {
            String[] invalidValues = {"invalid", "", "123", "v1!@#", "v3", "null", "true"};

            for (String invalidValue : invalidValues) {
                String expectedMessage = "Invalid value for vectorTypeSupport: " + invalidValue 
                        + ". Valid values are \"off\", \"v1\", or \"v2\".";
                
                try (SQLServerConnection conn = getConnectionWithVectorFlag(invalidValue)) {
                    fail("Expected IllegalArgumentException for vectorTypeSupport=" + invalidValue);
                } catch (IllegalArgumentException e) {
                    assertEquals(expectedMessage, e.getMessage(),
                            "Exception message mismatch for invalid value: " + invalidValue);
                } catch (SQLException e) {
                    fail("Expected IllegalArgumentException for value '" + invalidValue 
                            + "' but got SQLException: " + e.getMessage());
                }
            }
        }

        /**
         * Test valid vectorTypeSupport edge cases that should succeed:
         * - Whitespace-padded values
         * - Case variations (uppercase, mixed case)
         * - Default behavior when not specified
         */
        @Test
        @DisplayName("Test valid vectorTypeSupport edge cases")
        public void testValidVectorTypeSupportEdgeCases() throws SQLException {
            String[][] validCases = {
                {" v1 ", "whitespace-padded v1", "v1"},
                {"V1", "uppercase V1", "v1"},
                {"V2", "uppercase V2", "v2"},
                {"OFF", "uppercase OFF", "off"},
                {"Off", "mixed case Off", "off"},
            };

            for (String[] testCase : validCases) {
                String value = testCase[0];
                String description = testCase[1];
                String expectedNormalized = testCase[2];
                
                try (SQLServerConnection conn = getConnectionWithVectorFlag(value)) {
                    byte negotiatedVersion = conn.getNegotiatedVectorVersion();
                    
                    assertTrue(negotiatedVersion >= 0x00 && negotiatedVersion <= 0x02,
                            String.format("Connection with %s should succeed. Negotiated: 0x%02X", 
                                    description, negotiatedVersion));
                    
                    if ("off".equals(expectedNormalized)) {
                        assertEquals(0x00, negotiatedVersion,
                                String.format("Connection with %s should have negotiated version 0x00", description));
                    }
                    else if ("v1".equals(expectedNormalized)) {
                        assertTrue(negotiatedVersion == 0x00 || negotiatedVersion == 0x01,
                                String.format("Connection with %s should have negotiated version 0x00 or 0x01. Actual: 0x%02X", 
                                        description, negotiatedVersion));
                    }
                    else if ("v2".equals(expectedNormalized)) {
                        assertTrue(negotiatedVersion >= 0x00 && negotiatedVersion <= 0x02,
                                String.format("Connection with %s should have negotiated version 0x00-0x02. Actual: 0x%02X", 
                                        description, negotiatedVersion));
                    }
                } catch (SQLException e) {
                    fail(String.format("Connection with %s should succeed but threw: %s", 
                            description, e.getMessage()));
                }
            }

            // Test default behavior (no vectorTypeSupport specified) - default is "v1"
            try (SQLServerConnection conn = getConnection()) {
                byte negotiatedVersion = conn.getNegotiatedVectorVersion();
                assertEquals(0x01, negotiatedVersion,
                        "Default negotiated version should be 0x01");
            }
        }
    }
}
