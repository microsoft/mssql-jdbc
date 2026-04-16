/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.globalization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Reader;
import java.io.StringReader;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;

import com.microsoft.sqlserver.jdbc.globalization.GlobalizationTestData.CharacterPosition;
import com.microsoft.sqlserver.jdbc.globalization.GlobalizationTestData.CharacterType;
import com.microsoft.sqlserver.jdbc.globalization.GlobalizationTestData.ReceiveMethod;
import com.microsoft.sqlserver.jdbc.globalization.GlobalizationTestData.SendMethod;

/**
 * Covers FX TCDenaliUTF16 (VSTS #765740): UTF-16 round-trip with 15 send × 18
 * receive
 * methods × 4 char types × 2 positions, using round-robin sampling (~30 combos).
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDW)
@Tag(Constants.xAzureSQLDB)
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxGlobalization)
@DisplayName("Globalization UTF-16 Round-Trip Tests")
public class GlobalizationUTF16Test extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    /**
     * Tests UTF-16 round-trip with sampled combos of 15 send × 18 receive methods
     * × 4 char types × 2 positions.
     * Covers FX TCDenaliUTF16 (VSTS #765740).
     */
    @ParameterizedTest(name = "Send={0} Recv={1} Type={2} Pos={3}")
    @MethodSource("com.microsoft.sqlserver.jdbc.globalization.GlobalizationTestData#utf16RoundTripProvider")
    @DisplayName("TCDenaliUTF16.testUTF16 — UTF-16 round-trip")
    public void testUTF16RoundTrip(SendMethod sendMethod, ReceiveMethod receiveMethod,
            CharacterType charType, CharacterPosition charPosition) throws Exception {

        String testData = GlobalizationTestData.generateTestString(charType, charPosition);

        String tableName = AbstractSQLGenerator.escapeIdentifier(
                RandomUtil.getIdentifier("GlobUTF16"));
        String procName = AbstractSQLGenerator.escapeIdentifier(
                RandomUtil.getIdentifier("GlobUTF16Proc"));

        // Use nvarchar(max) for LOB scenarios
        boolean needsLob = sendMethod.needsLob() || receiveMethod.needsLob();
        String colType = needsLob ? "NVARCHAR(MAX)" : "NVARCHAR(4000)";

        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {

            // Create test table
            stmt.executeUpdate("CREATE TABLE " + tableName + " (" +
                    "id INT IDENTITY(1,1) PRIMARY KEY, " +
                    "data " + colType + ", " +
                    "search_col " + colType + ")");

            // Create stored procedure if needed
            boolean needsProc = sendMethod.name().startsWith("CS_") ||
                    receiveMethod.name().startsWith("CS_");
            if (needsProc) {
                stmt.executeUpdate("CREATE PROCEDURE " + procName +
                        " @dataIn " + colType + ", " +
                        " @dataOut " + colType + " OUTPUT " +
                        " AS BEGIN " +
                        "   INSERT INTO " + tableName + " (data, search_col) VALUES (@dataIn, @dataIn); " +
                        "   SET @dataOut = @dataIn; " +
                        " END");
            }

            // SEND
            sendData(conn, stmt, tableName, procName, testData, sendMethod);

            // RECEIVE
            String receivedData = receiveData(conn, stmt, tableName, procName, receiveMethod);

            // VERIFY
            assertNotNull(receivedData,
                    "Received data is null for send=" + sendMethod + ", recv=" + receiveMethod);

            // Trim to match length (some column types may pad)
            if (receivedData.length() > testData.length()) {
                receivedData = receivedData.substring(0, testData.length());
            }

            assertEquals(testData, receivedData,
                    "Round-trip mismatch: send=" + sendMethod.getDisplayName() +
                            ", recv=" + receiveMethod.getDisplayName() +
                            ", charType=" + charType.getName() +
                            ", pos=" + charPosition.getName());

        } finally {
            try (Connection conn = getConnection();
                    Statement stmt = conn.createStatement()) {
                TestUtils.dropProcedureIfExists(procName, stmt);
                TestUtils.dropTableIfExists(tableName, stmt);
            }
        }
    }

    /**
     * Sends test data to the server using the specified JDBC send method.
     * Covers FX CSendXxx classes (15 variants).
     */
    private void sendData(Connection conn, Statement stmt, String tableName, String procName,
            String testData, SendMethod method) throws Exception {

        switch (method) {

            case LITERAL:
                stmt.executeUpdate("INSERT INTO " + tableName +
                        " (data, search_col) VALUES (N'" +
                        TestUtils.escapeSingleQuotes(testData) + "', N'" +
                        TestUtils.escapeSingleQuotes(testData) + "')");
                break;

            case PS_SET_STRING:
                try (PreparedStatement pstmt = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (data, search_col) VALUES (?, ?)")) {
                    pstmt.setString(1, testData);
                    pstmt.setString(2, testData);
                    pstmt.executeUpdate();
                }
                break;

            case PS_SET_NSTRING:
                try (PreparedStatement pstmt = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (data, search_col) VALUES (?, ?)")) {
                    pstmt.setNString(1, testData);
                    pstmt.setNString(2, testData);
                    pstmt.executeUpdate();
                }
                break;

            case PS_SET_CHARACTER_STREAM:
                try (PreparedStatement pstmt = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (data, search_col) VALUES (?, ?)")) {
                    pstmt.setCharacterStream(1, new StringReader(testData), testData.length());
                    pstmt.setNString(2, testData);
                    pstmt.executeUpdate();
                }
                break;

            case PS_SET_NCHARACTER_STREAM:
                try (PreparedStatement pstmt = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (data, search_col) VALUES (?, ?)")) {
                    pstmt.setNCharacterStream(1, new StringReader(testData), testData.length());
                    pstmt.setNString(2, testData);
                    pstmt.executeUpdate();
                }
                break;

            case PS_SET_CLOB:
                try (PreparedStatement pstmt = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (data, search_col) VALUES (?, ?)")) {
                    Clob clob = conn.createClob();
                    clob.setString(1, testData);
                    pstmt.setClob(1, clob);
                    pstmt.setNString(2, testData);
                    pstmt.executeUpdate();
                }
                break;

            case PS_SET_NCLOB:
                try (PreparedStatement pstmt = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (data, search_col) VALUES (?, ?)")) {
                    NClob nclob = conn.createNClob();
                    nclob.setString(1, testData);
                    pstmt.setNClob(1, nclob);
                    pstmt.setNString(2, testData);
                    pstmt.executeUpdate();
                }
                break;

            case CS_SET_STRING:
                try (CallableStatement cs = conn.prepareCall("{call " + procName + "(?, ?)}")) {
                    cs.setString(1, testData);
                    cs.registerOutParameter(2, java.sql.Types.NVARCHAR);
                    cs.execute();
                }
                break;

            case CS_SET_NSTRING:
                try (CallableStatement cs = conn.prepareCall("{call " + procName + "(?, ?)}")) {
                    cs.setNString(1, testData);
                    cs.registerOutParameter(2, java.sql.Types.NVARCHAR);
                    cs.execute();
                }
                break;

            // RS_UPDATE_* cases all follow the same pattern: insert placeholder, update via
            // updatable RS
            case RS_UPDATE_STRING:
            case RS_UPDATE_NSTRING:
            case RS_UPDATE_CHARACTER_STREAM:
            case RS_UPDATE_NCHARACTER_STREAM:
            case RS_UPDATE_CLOB:
            case RS_UPDATE_NCLOB:
                sendViaUpdatableResultSet(conn, stmt, tableName, testData, method);
                break;
        }
    }

    /**
     * Helper for RS_UPDATE_* send methods — inserts placeholder then updates via
     * updatable ResultSet.
     */
    private void sendViaUpdatableResultSet(Connection conn, Statement stmt, String tableName,
            String testData, SendMethod method) throws Exception {
        stmt.executeUpdate("INSERT INTO " + tableName +
                " (data, search_col) VALUES (N'placeholder', N'placeholder')");
        try (Statement updStmt = conn.createStatement(
                ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                ResultSet rs = updStmt.executeQuery("SELECT * FROM " + tableName)) {
            rs.next();
            switch (method) {
                case RS_UPDATE_STRING:
                    rs.updateString("data", testData);
                    break;
                case RS_UPDATE_NSTRING:
                    rs.updateNString("data", testData);
                    break;
                case RS_UPDATE_CHARACTER_STREAM:
                    rs.updateCharacterStream("data", new StringReader(testData), testData.length());
                    break;
                case RS_UPDATE_NCHARACTER_STREAM:
                    rs.updateNCharacterStream("data", new StringReader(testData), testData.length());
                    break;
                case RS_UPDATE_CLOB: {
                    Clob clob = conn.createClob();
                    clob.setString(1, testData);
                    rs.updateClob("data", clob);
                    break;
                }
                case RS_UPDATE_NCLOB: {
                    NClob nclob = conn.createNClob();
                    nclob.setString(1, testData);
                    rs.updateNClob("data", nclob);
                    break;
                }
                default:
                    break;
            }
            rs.updateNString("search_col", testData);
            rs.updateRow();
        }
    }

    /**
     * Receives data from the server using the specified JDBC receive method.
     * Covers FX CReceiveXxx classes (18 variants).
     */
    private String receiveData(Connection conn, Statement stmt, String tableName, String procName,
            ReceiveMethod method) throws Exception {

        switch (method) {
            case RS_GET_STRING:
                try (ResultSet rs = stmt.executeQuery("SELECT data FROM " + tableName)) {
                    assertTrue(rs.next());
                    return rs.getString(1);
                }
            case RS_GET_NSTRING:
                try (ResultSet rs = stmt.executeQuery("SELECT data FROM " + tableName)) {
                    assertTrue(rs.next());
                    return rs.getNString(1);
                }
            case RS_GET_CHARACTER_STREAM:
                try (ResultSet rs = stmt.executeQuery("SELECT data FROM " + tableName)) {
                    assertTrue(rs.next());
                    return readerToString(rs.getCharacterStream(1));
                }
            case RS_GET_NCHARACTER_STREAM:
                try (ResultSet rs = stmt.executeQuery("SELECT data FROM " + tableName)) {
                    assertTrue(rs.next());
                    return readerToString(rs.getNCharacterStream(1));
                }
            case RS_GET_CLOB:
                try (ResultSet rs = stmt.executeQuery("SELECT data FROM " + tableName)) {
                    assertTrue(rs.next());
                    Clob clob = rs.getClob(1);
                    return clob.getSubString(1, (int) clob.length());
                }
            case RS_GET_NCLOB:
                try (ResultSet rs = stmt.executeQuery("SELECT data FROM " + tableName)) {
                    assertTrue(rs.next());
                    NClob nclob = rs.getNClob(1);
                    return nclob.getSubString(1, (int) nclob.length());
                }
            case CS_OUT_GET_STRING:
            case CS_INOUT_GET_STRING:
                return callProcAndGetOutput(conn, tableName, "getString");
            case CS_OUT_GET_NSTRING:
            case CS_INOUT_GET_NSTRING:
                return callProcAndGetOutput(conn, tableName, "getNString");
            case CS_OUT_GET_CHARACTER_STREAM:
            case CS_INOUT_GET_CHARACTER_STREAM:
                return callProcAndGetOutput(conn, tableName, "getCharacterStream");
            case CS_OUT_GET_NCHARACTER_STREAM:
            case CS_INOUT_GET_NCHARACTER_STREAM:
                return callProcAndGetOutput(conn, tableName, "getNCharacterStream");
            case CS_OUT_GET_CLOB:
            case CS_INOUT_GET_CLOB:
                return callProcAndGetOutput(conn, tableName, "getClob");
            case CS_OUT_GET_NCLOB:
            case CS_INOUT_GET_NCLOB:
                return callProcAndGetOutput(conn, tableName, "getNClob");
            default:
                throw new UnsupportedOperationException("Unknown receive method: " + method);
        }
    }

    /**
     * Calls a stored proc with NVARCHAR(MAX) OUTPUT to retrieve data via the
     * specified getter.
     * Covers FX CReceive CS_OUT/CS_INOUT variants.
     */
    private String callProcAndGetOutput(Connection conn, String tableName,
            String getterMethod) throws Exception {

        String selectProcName = AbstractSQLGenerator.escapeIdentifier(
                RandomUtil.getIdentifier("GlobSelProc"));

        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE PROCEDURE " + selectProcName +
                    " @dataOut NVARCHAR(MAX) OUTPUT " +
                    " AS BEGIN SELECT TOP 1 @dataOut = data FROM " + tableName + "; END");

            try (CallableStatement cs = conn.prepareCall("{call " + selectProcName + "(?)}")) {
                cs.registerOutParameter(1, java.sql.Types.NVARCHAR);
                cs.execute();

                switch (getterMethod) {
                    case "getString":
                        return cs.getString(1);
                    case "getNString":
                        return cs.getNString(1);
                    case "getCharacterStream":
                        return readerToString(cs.getCharacterStream(1));
                    case "getNCharacterStream":
                        return readerToString(cs.getNCharacterStream(1));
                    case "getClob": {
                        Clob c = cs.getClob(1);
                        return c.getSubString(1, (int) c.length());
                    }
                    case "getNClob": {
                        NClob n = cs.getNClob(1);
                        return n.getSubString(1, (int) n.length());
                    }
                    default:
                        throw new UnsupportedOperationException("Unknown getter: " + getterMethod);
                }
            } finally {
                TestUtils.dropProcedureIfExists(selectProcName, stmt);
            }
        }
    }

    /** Reads a Reader into a String. */
    private String readerToString(Reader reader) throws Exception {
        if (reader == null) {
            return null;
        }
        try {
            StringBuilder sb = new StringBuilder();
            char[] buf = new char[1024];
            int len;
            while ((len = reader.read(buf)) != -1) {
                sb.append(buf, 0, len);
            }
            return sb.toString();
        } finally {
            reader.close();
        }
    }
}
